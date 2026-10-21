mod journal;
mod replicated;
mod requests;
mod session;
pub use journal::SqlPersistenceStats;
use journal::{JournalFence, SqlJournal};
pub use session::SqlSessionContext;

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use requests::{INTENT_FILE, PendingRequest, RequestReceipt};
use rsf_constraints::ConstraintRule;
use rsf_core::{
    CommitVersion, Digest, ErrorCode, PlacementGroupId, RsfError, RsfResult,
    catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName},
};
use rsf_kv::{KvEngine, KvMutation, KvVersionedValue};
use rsf_protocol::{ClientRequestId, FabricRouterPort};

use crate::{
    ParsedStatement, SqlEngine, SqlLayoutProfile, SqlMutationEvidence, SqlOutcome,
    SqlRecoveredTable, parse_statement,
};

const STATE_FILE: &str = "sql.state";
const TMP_FILE: &str = "sql.state.tmp";
const FORMAT: &str = "RSF_SQL_STATE_V4";
const JOURNALED_FORMAT: &str = "RSF_SQL_STATE_V6";
const LEGACY_JOURNALED_FORMAT: &str = "RSF_SQL_STATE_V5";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableSqlExecution {
    pub outcome: SqlOutcome,
    pub rows: usize,
    pub version: Option<CommitVersion>,
    /// Data snapshot for a durable session SELECT; `version` certifies the read
    /// dependency acknowledgement, which can be later than this snapshot.
    pub read_snapshot: Option<CommitVersion>,
    pub persisted: bool,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableSqlRecovery {
    pub tables: usize,
    pub row_versions: usize,
    pub live_rows: usize,
    pub tombstones: usize,
}

#[derive(Debug)]
pub struct DurableSqlEngine {
    root: PathBuf,
    // OS-owned lock is released on drop or process death; never unlink its inode.
    _writer_lock: rsf_storage::ExclusiveFileLock,
    engine: SqlEngine,
    recovery: DurableSqlRecovery,
    persistence_failed: bool,
    pending: Option<PendingRequest>,
    receipts: BTreeMap<String, RequestReceipt>,
    namespace: Option<String>,
    commands: BTreeMap<ClientRequestId, rsf_protocol::ReplicatedFabricCommand>,
    journal: Option<SqlJournal>,
    sessions: BTreeMap<String, session::SessionState>,
}

impl DurableSqlEngine {
    pub fn open(root: impl AsRef<Path>) -> RsfResult<Self> {
        let root = root.as_ref().to_path_buf();
        let mut missing = Vec::new();
        for ancestor in root.ancestors().filter(|path| !path.as_os_str().is_empty()) {
            if ancestor.try_exists().map_err(|err| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("cannot inspect SQL directory {}: {err}", ancestor.display()),
                )
            })? {
                break;
            }
            missing.push(ancestor.to_path_buf());
        }
        fs::create_dir_all(&root).map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "failed to create durable sql root {}: {err}",
                    root.display()
                ),
            )
        })?;
        // Persist newly created directory entries before any write is acknowledged.
        for created in missing.iter().rev() {
            let parent = created
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            sync_dir(parent)?;
        }
        let writer_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("sql.lock"))
            .map_err(|err| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("failed to open durable sql lock: {err}"),
                )
            })?;
        let writer_lock = rsf_storage::ExclusiveFileLock::acquire(writer_lock).map_err(|err| {
            RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!(
                    "cannot acquire exclusive durable sql writer lock for {}: {err}",
                    root.display()
                ),
            )
        })?;
        let pending = PendingRequest::read(&root)?;
        let path = root.join(STATE_FILE);
        if !path.try_exists().map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("cannot inspect SQL checkpoint: {err}"),
            )
        })? {
            if root.join(journal::DIRECTORY).exists() || root.join(journal::HEAD_FILE).exists() {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "SQL journal exists without its checkpoint",
                ));
            }
            return Ok(Self {
                root,
                _writer_lock: writer_lock,
                engine: SqlEngine::default(),
                persistence_failed: false,
                pending,
                receipts: BTreeMap::new(),
                namespace: None,
                commands: BTreeMap::new(),
                journal: None,
                sessions: BTreeMap::new(),
                recovery: DurableSqlRecovery {
                    tables: 0,
                    row_versions: 0,
                    live_rows: 0,
                    tombstones: 0,
                },
            });
        }

        let checkpoint = read_checkpoint(&path)?;
        let journal_fence = checkpoint.journal;
        if checkpoint.namespace.is_none()
            && (root.join(journal::DIRECTORY).exists() || root.join(journal::HEAD_FILE).exists())
        {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "unbound SQL checkpoint cannot own a replicated journal",
            ));
        }
        let mut kv = KvEngine::default();
        for (key, versions) in checkpoint.kv {
            for version in versions {
                let mutation = if version.deleted {
                    KvMutation::Clear { key: key.clone() }
                } else {
                    KvMutation::Set {
                        key: key.clone(),
                        value: version.value,
                    }
                };
                kv.apply_committed_batch(version.version, vec![mutation])?;
            }
        }
        let (engine, report) = SqlEngine::recover_from_committed_kv(kv, checkpoint.tables)?;
        replicated::validate_recovered_operations(&engine, &checkpoint.commands)?;
        for receipt in checkpoint.receipts.values() {
            receipt.outcome(&engine)?;
        }
        let recovery = DurableSqlRecovery {
            tables: report.tables,
            row_versions: report.row_versions,
            live_rows: report.live_rows,
            tombstones: report.tombstones,
        };
        let mut sql = Self {
            root,
            _writer_lock: writer_lock,
            engine,
            recovery,
            persistence_failed: false,
            pending,
            receipts: checkpoint.receipts,
            namespace: checkpoint.namespace,
            commands: checkpoint.commands,
            journal: None,
            sessions: checkpoint.sessions,
        };
        if sql.namespace.is_some() {
            session::validate_recovered(&sql)?;
            sql.recover_journal(journal_fence)?;
        }
        Ok(sql)
    }

    pub fn execute(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        statement: &str,
        request_client: impl Into<String>,
    ) -> RsfResult<DurableSqlExecution> {
        if self.persistence_failed {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "durable sql checkpoint failed; reopen and recover before serving requests",
            ));
        }
        if self.namespace.is_some() {
            return self.execute_replicated(router, pg, statement, request_client.into());
        }
        reject_transaction_control(statement)?;
        let client = request_client.into();
        if client.trim().is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL request client must not be empty",
            ));
        }
        // Resolve an interrupted write before exposing any state to another request.
        if let Some(pending) = self.pending.clone() {
            if pending.client == client {
                pending.matches(pg, statement)?;
            }
            self.finish_pending(router, pending)?;
        }
        if let Some(receipt) = self.receipts.get(&client) {
            receipt.request.matches(pg, statement)?;
            return Ok(execution(receipt.outcome(&self.engine)?, true));
        }
        if matches!(parse_statement(statement)?, ParsedStatement::Select { .. }) {
            let outcome = self
                .engine
                .execute_with_request_client(router, pg, statement, client)?;
            return Ok(execution(outcome, false));
        }
        let pending = PendingRequest {
            session: None,
            client,
            statement: statement.to_string(),
            pg,
        };
        if let Err(err) = pending.persist(&self.root) {
            self.persistence_failed = true;
            return Err(err);
        }
        self.pending = Some(pending.clone());
        self.finish_pending(router, pending)
    }

    fn finish_pending(
        &mut self,
        router: &mut impl FabricRouterPort,
        pending: PendingRequest,
    ) -> RsfResult<DurableSqlExecution> {
        if let Some(receipt) = self.receipts.get(&pending.client) {
            receipt.request.matches(pending.pg, &pending.statement)?;
            let outcome = receipt.outcome(&self.engine)?;
            self.clear_pending()?;
            return Ok(execution(outcome, true));
        }
        let request = ClientRequestId::new(pending.client.clone(), 0);
        let replayed = router.request_certificate(&request).is_some();
        let outcome = match self.engine.execute_with_request_client(
            router,
            pending.pg,
            &pending.statement,
            pending.client.clone(),
        ) {
            Ok(outcome) => outcome,
            Err(err) => {
                // A validation rejection with no commit can be discarded. Keep
                // uncertain or committed requests durable for restart recovery.
                if err.code == ErrorCode::InvalidInput
                    && router.request_certificate(&request).is_none()
                {
                    self.clear_pending()?;
                } else {
                    self.persistence_failed = true;
                }
                return Err(err);
            }
        };
        let Some(certificate) = router.request_certificate(&request) else {
            self.persistence_failed = true;
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "SQL mutation has no request commit certificate",
            ));
        };
        self.receipts.insert(
            pending.client.clone(),
            RequestReceipt {
                request: pending,
                evidence: crate::commit_evidence(certificate),
            },
        );
        if let Err(err) = self.persist() {
            self.persistence_failed = true;
            return Err(err);
        }
        self.clear_pending()?;
        Ok(execution(outcome, replayed))
    }

    fn clear_pending(&mut self) -> RsfResult<()> {
        let result = fs::remove_file(self.root.join(INTENT_FILE))
            .map_err(|err| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("cannot remove completed SQL intent: {err}"),
                )
            })
            .and_then(|()| sync_dir(&self.root));
        if result.is_err() {
            self.persistence_failed = true;
        }
        result?;
        self.pending = None;
        Ok(())
    }

    pub fn recovery(&self) -> &DurableSqlRecovery {
        &self.recovery
    }

    fn persist(&self) -> RsfResult<()> {
        let checkpoint = SqlCheckpoint {
            tables: self.engine.recovered_tables_for_checkpoint(),
            kv: self.engine.committed_kv_versions_for_checkpoint(),
            receipts: self.receipts.clone(),
            namespace: self.namespace.clone(),
            commands: self.commands.clone(),
            journal: self.journal.as_ref().map(|journal| journal.tail()),
            sessions: self.sessions.clone(),
        };
        let encoded = encode_checkpoint(&checkpoint)?;
        atomic_write(&self.root, STATE_FILE, TMP_FILE, &encoded)
    }
}

fn execution(outcome: SqlOutcome, replayed: bool) -> DurableSqlExecution {
    DurableSqlExecution {
        read_snapshot: None,
        rows: outcome_row_count(&outcome),
        version: outcome_version(&outcome),
        persisted: outcome_persists(&outcome),
        outcome,
        replayed,
    }
}

fn atomic_write(root: &Path, state_file: &str, tmp_file: &str, encoded: &str) -> RsfResult<()> {
    let tmp = root.join(tmp_file);
    let path = root.join(state_file);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|err| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("failed to open durable sql temp {}: {err}", tmp.display()),
                )
            })?;
        file.write_all(encoded.as_bytes()).map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("failed to write durable sql temp {}: {err}", tmp.display()),
            )
        })?;
        file.sync_all().map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("failed to sync durable sql temp {}: {err}", tmp.display()),
            )
        })?;
    }
    fs::rename(&tmp, &path).map_err(|err| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "failed to publish durable sql state {} from {}: {err}",
                path.display(),
                tmp.display()
            ),
        )
    })?;
    sync_dir(root)
}

#[derive(Clone, Debug)]
struct SqlCheckpoint {
    tables: Vec<SqlRecoveredTable>,
    kv: Vec<(Vec<u8>, Vec<KvVersionedValue>)>,
    receipts: BTreeMap<String, RequestReceipt>,
    namespace: Option<String>,
    commands: BTreeMap<ClientRequestId, rsf_protocol::ReplicatedFabricCommand>,
    journal: Option<JournalFence>,
    sessions: BTreeMap<String, session::SessionState>,
}

fn read_checkpoint(path: &Path) -> RsfResult<SqlCheckpoint> {
    let mut file = File::open(path).map_err(|err| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("failed to open durable sql state {}: {err}", path.display()),
        )
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|err| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("failed to read durable sql state {}: {err}", path.display()),
        )
    })?;
    let text = String::from_utf8(bytes).map_err(|err| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("durable sql state {} is not utf-8: {err}", path.display()),
        )
    })?;
    decode_checkpoint(&text)
}

fn encode_checkpoint(checkpoint: &SqlCheckpoint) -> RsfResult<String> {
    let mut out = String::new();
    out.push_str(if checkpoint.journal.is_some() {
        JOURNALED_FORMAT
    } else {
        FORMAT
    });
    out.push('\n');
    if let Some(fence) = checkpoint.journal {
        out.push_str(&fence.line());
    }
    if let Some(namespace) = &checkpoint.namespace {
        out.push_str(&format!("namespace\t{}\n", hex_bytes(namespace.as_bytes())));
    }
    for (key, state) in &checkpoint.sessions {
        out.push_str(&format!(
            "session\t{}\t{}\n",
            hex_bytes(key.as_bytes()),
            hex_bytes(&state.encode()?)
        ));
    }
    for command in checkpoint.commands.values() {
        out.push_str(&format!("application\t{}\n", hex_bytes(&command.encode()?)));
    }
    for table in &checkpoint.tables {
        out.push_str(&format!(
            "table\t{}\t{}\t{}\t{}\n",
            hex_bytes(table.descriptor.name.0.as_bytes()),
            hex_bytes(table.descriptor.namespace.0.as_bytes()),
            encode_profile(table.profile),
            encode_string_list(&table.descriptor.primary_key)
        ));
        for column in &table.descriptor.columns {
            out.push_str(&format!(
                "column\t{}\t{}\t{}\t{}\n",
                hex_bytes(table.descriptor.name.0.as_bytes()),
                hex_bytes(column.name.as_bytes()),
                encode_column_type(column.ty.clone()),
                if column.nullable { "1" } else { "0" }
            ));
        }
        for constraint in &table.constraints {
            out.push_str(&format!(
                "constraint\t{}\t{}\n",
                hex_bytes(table.descriptor.name.0.as_bytes()),
                encode_constraint(constraint)
            ));
        }
        for index in &table.secondary_indexes {
            out.push_str(&format!(
                "index\t{}\t{}\n",
                hex_bytes(table.descriptor.name.0.as_bytes()),
                hex_bytes(index.as_bytes())
            ));
        }
    }
    for (key, versions) in &checkpoint.kv {
        for version in versions {
            out.push_str(&format!(
                "kv\t{}\t{}\t{}\t{}\n",
                hex_bytes(key),
                version.version.0,
                if version.deleted { "1" } else { "0" },
                hex_bytes(&version.value)
            ));
        }
    }
    for receipt in checkpoint.receipts.values() {
        out.push_str(&receipt.encode()?);
    }
    append_checksum(&mut out);
    Ok(out)
}

fn append_checksum(out: &mut String) {
    let checksum = Digest::blake3_bytes(out.as_bytes());
    out.push_str(&format!(
        "checksum\tblake3\t{}\n",
        hex_bytes(checksum.as_bytes())
    ));
}

fn verify_checksum(text: &str) -> RsfResult<&str> {
    let (text, footer) = text.rsplit_once("checksum\tblake3\t").ok_or_else(|| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "durable sql state is missing its checksum",
        )
    })?;
    let expected = format!(
        "{}\n",
        hex_bytes(Digest::blake3_bytes(text.as_bytes()).as_bytes())
    );
    if footer != expected {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "durable sql state checksum mismatch",
        ));
    }
    Ok(text)
}

fn decode_checkpoint(text: &str) -> RsfResult<SqlCheckpoint> {
    let text = verify_checksum(text)?;
    let mut lines = text.lines();
    let (replicated_format, journaled_format, session_format) = match lines.next() {
        Some(JOURNALED_FORMAT) => (true, true, true),
        Some(LEGACY_JOURNALED_FORMAT) => (true, true, false),
        Some(FORMAT) => (true, false, false),
        Some("RSF_SQL_STATE_V3") => (false, false, false),
        _ => {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "durable sql state has an unsupported format",
            ));
        }
    };

    let mut receipts = BTreeMap::new();
    let mut sessions = BTreeMap::new();
    let mut journal = None;
    let mut namespace = None;
    let mut commands = BTreeMap::new();
    let mut tables = BTreeMap::<String, PendingTable>::new();
    let mut kv = BTreeMap::<Vec<u8>, Vec<KvVersionedValue>>::new();
    for (line_number, line) in lines.enumerate() {
        if line.is_empty() {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["session", key, state] if session_format => {
                let key = decode_string(key, "SQL session key")?;
                let state = session::SessionState::decode(&hex_decode(state)?)?;
                if sessions.insert(key, state).is_some()
                    || sessions.len() > session::MAX_ACTIVE_SQL_SESSIONS
                {
                    return Err(corrupt_line(
                        line_number,
                        "duplicate or excessive SQL sessions",
                    ));
                }
            }
            ["journal", ..] if journaled_format => {
                let fence = JournalFence::decode(&fields)?;
                if journal.replace(fence).is_some() {
                    return Err(corrupt_line(line_number, "duplicate journal fence"));
                }
            }
            ["namespace", encoded] if replicated_format => {
                let value = decode_string(encoded, "SQL namespace")?;
                replicated::validate_namespace(&value)?;
                if namespace.replace(value).is_some() {
                    return Err(corrupt_line(line_number, "duplicate SQL namespace"));
                }
            }
            ["application", encoded] if replicated_format => {
                let command = rsf_protocol::ReplicatedFabricCommand::decode(&hex_decode(encoded)?)?;
                if commands.insert(command.request.clone(), command).is_some() {
                    return Err(corrupt_line(
                        line_number,
                        "duplicate SQL application command",
                    ));
                }
            }
            ["request", fields @ ..] => {
                let receipt = RequestReceipt::decode(fields)?;
                if receipts
                    .insert(receipt.request.client.clone(), receipt)
                    .is_some()
                {
                    return Err(corrupt_line(line_number, "duplicate SQL request receipt"));
                }
            }
            ["table", name, namespace, profile, primary] => {
                let name = decode_string(name, "sql table name")?;
                if tables.contains_key(&name) {
                    return Err(corrupt_line(line_number, "duplicate sql table"));
                }
                tables.insert(
                    name.clone(),
                    PendingTable {
                        descriptor: TableDescriptor {
                            namespace: NamespaceName(decode_string(namespace, "sql namespace")?),
                            name: TableName(name),
                            columns: Vec::new(),
                            primary_key: decode_string_list(primary)?,
                        },
                        profile: decode_profile(profile)?,
                        constraints: Vec::new(),
                        secondary_indexes: Vec::new(),
                    },
                );
            }
            ["column", table, name, ty, nullable] => {
                let table = decode_string(table, "sql column table")?;
                let Some(pending) = tables.get_mut(&table) else {
                    return Err(corrupt_line(line_number, "column references unknown table"));
                };
                pending.descriptor.columns.push(ColumnDescriptor {
                    name: decode_string(name, "sql column name")?,
                    ty: decode_column_type(ty)?,
                    nullable: match *nullable {
                        "0" => false,
                        "1" => true,
                        _ => return Err(corrupt_line(line_number, "column nullable flag invalid")),
                    },
                });
            }
            ["constraint", table, constraint] => {
                let table = decode_string(table, "sql constraint table")?;
                let Some(pending) = tables.get_mut(&table) else {
                    return Err(corrupt_line(
                        line_number,
                        "constraint references unknown table",
                    ));
                };
                pending.constraints.push(decode_constraint(constraint)?);
            }
            ["index", table, column] => {
                let table = decode_string(table, "sql index table")?;
                let Some(pending) = tables.get_mut(&table) else {
                    return Err(corrupt_line(line_number, "index references unknown table"));
                };
                pending
                    .secondary_indexes
                    .push(decode_string(column, "sql index column")?);
            }
            ["kv", key, version, deleted, value] => {
                let key = hex_decode(key)?;
                let version = CommitVersion(
                    version
                        .parse::<u64>()
                        .map_err(|_| corrupt_line(line_number, "kv version is not an integer"))?,
                );
                version.validate_committed()?;
                let deleted = match *deleted {
                    "0" => false,
                    "1" => true,
                    _ => return Err(corrupt_line(line_number, "kv deleted flag invalid")),
                };
                let value = hex_decode(value)?;
                if deleted && !value.is_empty() {
                    return Err(corrupt_line(
                        line_number,
                        "kv tombstone carries value bytes",
                    ));
                }
                kv.entry(key).or_default().push(KvVersionedValue {
                    version,
                    value,
                    deleted,
                });
            }
            _ => return Err(corrupt_line(line_number, "malformed durable sql record")),
        }
    }

    for (key, versions) in &mut kv {
        versions.sort_by_key(|version| version.version);
        for pair in versions.windows(2) {
            if pair[0].version >= pair[1].version {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "durable sql key {} has non-increasing versions",
                        hex_bytes(key)
                    ),
                ));
            }
        }
    }

    replicated::validate_checkpoint_commands(namespace.as_deref(), &commands, &receipts)?;
    if journaled_format != journal.is_some() || (journaled_format && namespace.is_none()) {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "journaled SQL checkpoint requires namespace and journal fence",
        ));
    }
    Ok(SqlCheckpoint {
        journal,
        sessions,
        namespace,
        commands,
        tables: tables
            .into_values()
            .map(|pending| SqlRecoveredTable {
                descriptor: pending.descriptor,
                profile: pending.profile,
                constraints: pending.constraints,
                secondary_indexes: pending.secondary_indexes,
            })
            .collect(),
        kv: kv.into_iter().collect(),
        receipts,
    })
}

#[derive(Clone, Debug)]
struct PendingTable {
    descriptor: TableDescriptor,
    profile: SqlLayoutProfile,
    constraints: Vec<ConstraintRule>,
    secondary_indexes: Vec<String>,
}

fn outcome_persists(outcome: &SqlOutcome) -> bool {
    matches!(
        outcome,
        SqlOutcome::TableCreated { .. }
            | SqlOutcome::IndexCreated { .. }
            | SqlOutcome::Inserted {
                evidence: SqlMutationEvidence::Committed(_),
                ..
            }
            | SqlOutcome::Updated {
                evidence: SqlMutationEvidence::Committed(_),
                ..
            }
            | SqlOutcome::Deleted {
                evidence: SqlMutationEvidence::Committed(_),
                ..
            }
    )
}

fn outcome_row_count(outcome: &SqlOutcome) -> usize {
    match outcome {
        SqlOutcome::Selected { rows } => rows.len(),
        SqlOutcome::Inserted { .. } | SqlOutcome::Updated { .. } => 1,
        SqlOutcome::Deleted { .. } => 1,
        SqlOutcome::TransactionCommitted { mutations, .. } => *mutations,
        _ => 0,
    }
}

fn outcome_version(outcome: &SqlOutcome) -> Option<CommitVersion> {
    match outcome {
        SqlOutcome::TableCreated { version, .. } | SqlOutcome::IndexCreated { version, .. } => {
            Some(*version)
        }
        SqlOutcome::Inserted { evidence, .. }
        | SqlOutcome::Updated { evidence, .. }
        | SqlOutcome::Deleted { evidence, .. } => Some(evidence.version()),
        SqlOutcome::TransactionCommitted { evidence, .. } => Some(evidence.version),
        _ => None,
    }
}

fn reject_transaction_control(statement: &str) -> RsfResult<()> {
    if matches!(
        parse_statement(statement)?,
        ParsedStatement::Begin
            | ParsedStatement::Commit
            | ParsedStatement::Rollback
            | ParsedStatement::Savepoint { .. }
            | ParsedStatement::RollbackToSavepoint { .. }
    ) {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "durable SQL transaction controls require an explicit SQL session context",
        ));
    }
    Ok(())
}

fn encode_profile(profile: SqlLayoutProfile) -> &'static str {
    match profile {
        SqlLayoutProfile::OrderedKv => "ordered-kv",
        SqlLayoutProfile::PageWal => "page-wal",
        SqlLayoutProfile::Hybrid => "hybrid",
    }
}

fn decode_profile(value: &str) -> RsfResult<SqlLayoutProfile> {
    match value {
        "ordered-kv" => Ok(SqlLayoutProfile::OrderedKv),
        "page-wal" => Ok(SqlLayoutProfile::PageWal),
        "hybrid" => Ok(SqlLayoutProfile::Hybrid),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "durable sql table has unknown layout profile",
        )),
    }
}

fn encode_column_type(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Int64 => "int64",
        ColumnType::Bytes => "bytes",
        ColumnType::Text => "text",
        ColumnType::Bool => "bool",
    }
}

fn decode_column_type(value: &str) -> RsfResult<ColumnType> {
    match value {
        "int64" => Ok(ColumnType::Int64),
        "bytes" => Ok(ColumnType::Bytes),
        "text" => Ok(ColumnType::Text),
        "bool" => Ok(ColumnType::Bool),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "durable sql column has unknown type",
        )),
    }
}

fn encode_constraint(rule: &ConstraintRule) -> String {
    match rule {
        ConstraintRule::NotNull { column } => format!("not-null:{}", hex_bytes(column.as_bytes())),
        ConstraintRule::PrimaryKey { columns } => {
            format!("primary:{}", encode_string_list(columns))
        }
        ConstraintRule::Unique { name, columns } => format!(
            "unique:{}:{}",
            hex_bytes(name.as_bytes()),
            encode_string_list(columns)
        ),
        ConstraintRule::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
        } => format!(
            "foreign:{}:{}:{}:{}",
            hex_bytes(name.as_bytes()),
            encode_string_list(columns),
            hex_bytes(referenced_table.0.as_bytes()),
            encode_string_list(referenced_columns)
        ),
    }
}

fn decode_constraint(value: &str) -> RsfResult<ConstraintRule> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["not-null", column] => Ok(ConstraintRule::NotNull {
            column: decode_string(column, "not-null column")?,
        }),
        ["primary", columns] => Ok(ConstraintRule::PrimaryKey {
            columns: decode_string_list(columns)?,
        }),
        ["unique", name, columns] => Ok(ConstraintRule::Unique {
            name: decode_string(name, "unique name")?,
            columns: decode_string_list(columns)?,
        }),
        ["foreign", name, columns, table, referenced_columns] => Ok(ConstraintRule::ForeignKey {
            name: decode_string(name, "foreign key name")?,
            columns: decode_string_list(columns)?,
            referenced_table: TableName(decode_string(table, "referenced table")?),
            referenced_columns: decode_string_list(referenced_columns)?,
        }),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "durable sql constraint is malformed",
        )),
    }
}

fn encode_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| hex_bytes(value.as_bytes()))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_string_list(value: &str) -> RsfResult<Vec<String>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|part| decode_string(part, "string list entry"))
        .collect()
}

fn corrupt_line(line_number: usize, reason: impl Into<String>) -> RsfError {
    RsfError::new(
        ErrorCode::CorruptionDetected,
        format!(
            "durable sql state line {} is corrupt: {}",
            line_number + 2,
            reason.into()
        ),
    )
}

fn decode_string(value: &str, label: &str) -> RsfResult<String> {
    String::from_utf8(hex_decode(value)?).map_err(|err| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("{label} is not utf-8: {err}"),
        )
    })
}

fn hex_decode(value: &str) -> RsfResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "hex field has an odd number of digits",
        ));
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        out.push((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> RsfResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "hex field contains a non-hex character",
        )),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn sync_dir(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "failed to sync durable sql directory {}: {err}",
                    path.display()
                ),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{Epoch, NodeId};
    use rsf_protocol::FabricRouter;
    use rsf_protocol::VsrBackedFabricRouter;

    #[test]
    fn durable_sql_retries_return_original_rows_without_reapplying_mutations() {
        let path = test_path("request-replay");
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        let create = "CREATE TABLE users(id PRIMARY KEY, name)";
        let insert = "INSERT INTO users (id, name) VALUES (1, ada)";
        let created = sql
            .execute(&mut router, PlacementGroupId(1), create, "create")
            .unwrap();
        let inserted = sql
            .execute(&mut router, PlacementGroupId(1), insert, "insert")
            .unwrap();
        assert!(!inserted.replayed);
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "UPDATE users SET name = grace WHERE id = 1",
            "update",
        )
        .unwrap();
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "DELETE FROM users WHERE id = 1",
            "delete",
        )
        .unwrap();
        drop(sql);
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        let version = router.latest_version();
        for (statement, client, original) in
            [(create, "create", created), (insert, "insert", inserted)]
        {
            let replay = sql
                .execute(&mut router, PlacementGroupId(1), statement, client)
                .unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.outcome, original.outcome);
            assert_eq!(replay.version, original.version);
        }
        assert!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users (id, name) VALUES (2, bob)",
                "insert"
            )
            .is_err()
        );
        assert!(
            sql.execute(&mut router, PlacementGroupId(2), insert, "insert")
                .is_err()
        );
        assert_eq!(router.latest_version(), version);
        assert_eq!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users",
                "read"
            )
            .unwrap()
            .rows,
            0
        );
        drop(sql);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn durable_sql_recovers_each_intent_commit_checkpoint_crash_boundary() {
        for stage in 0..3 {
            let path = test_path(&format!("crash-boundary-{stage}"));
            let sql_path = path.join("sql");
            let protocol_path = path.join("protocol");
            let open_router = || {
                VsrBackedFabricRouter::open(
                    &protocol_path,
                    [PlacementGroupId(1)],
                    Epoch(1),
                    NodeId(0),
                    [NodeId(0), NodeId(1), NodeId(2)],
                )
                .unwrap()
            };
            let mut router = open_router();
            let mut sql = DurableSqlEngine::open(&sql_path).unwrap();
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "CREATE TABLE users(id PRIMARY KEY, name)",
                "create",
            )
            .unwrap();
            let request = PendingRequest {
                session: None,
                client: "insert".to_string(),
                statement: "INSERT INTO users (id, name) VALUES (1, ada)".to_string(),
                pg: PlacementGroupId(1),
            };
            request.persist(&sql_path).unwrap();
            if stage == 1 {
                // Protocol commit and application happened, but no checkpoint was published.
                sql.engine
                    .execute_with_request_client(
                        &mut router,
                        request.pg,
                        &request.statement,
                        request.client.clone(),
                    )
                    .unwrap();
            } else if stage == 2 {
                sql.pending = Some(request.clone());
                sql.finish_pending(&mut router, request.clone()).unwrap();
                // Disk state after checkpoint publication but before intent deletion.
                request.persist(&sql_path).unwrap();
            }
            drop(sql);
            drop(router);
            let mut router = open_router();
            let mut sql = DurableSqlEngine::open(&sql_path).unwrap();
            let selected = sql
                .execute(
                    &mut router,
                    PlacementGroupId(1),
                    "SELECT * FROM users",
                    "read",
                )
                .unwrap();
            assert_eq!(selected.rows, 1, "stage {stage}");
            assert_eq!(router.latest_version(), CommitVersion(2), "stage {stage}");
            let replay = sql
                .execute(&mut router, request.pg, &request.statement, &request.client)
                .unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.version, Some(CommitVersion(2)));
            assert_eq!(
                router
                    .recovered_certificates(PlacementGroupId(1))
                    .unwrap()
                    .len(),
                2
            );
            assert!(!sql_path.join(INTENT_FILE).exists());
            drop(sql);
            drop(router);
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn rejected_sql_does_not_leave_an_intent_and_corrupt_intents_fail_closed() {
        let path = test_path("intent-validation");
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        assert!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO missing (id) VALUES (1)",
                "bad"
            )
            .is_err()
        );
        assert!(!path.join(INTENT_FILE).exists());
        let request = PendingRequest {
            session: None,
            client: "create".to_string(),
            statement: "CREATE TABLE users(id PRIMARY KEY)".to_string(),
            pg: PlacementGroupId(1),
        };
        request.persist(&path).unwrap();
        drop(sql);
        let bytes = fs::read_to_string(path.join(INTENT_FILE)).unwrap();
        fs::write(
            path.join(INTENT_FILE),
            bytes.replace("637265617465", "637265617466"),
        )
        .unwrap();
        assert_eq!(
            DurableSqlEngine::open(&path).unwrap_err().code,
            ErrorCode::CorruptionDetected
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn durable_sql_rejects_all_parsed_transaction_controls() {
        for statement in [
            "BEGIN;",
            "BEGIN TRANSACTION",
            " begin transaction; ",
            "COMMIT;",
            "ROLLBACK;",
            "SAVEPOINT s;",
            "ROLLBACK TO s;",
        ] {
            let path = test_path("transaction-control");
            let mut sql = DurableSqlEngine::open(&path).unwrap();
            let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
            let err = sql
                .execute(&mut router, PlacementGroupId(1), statement, "request")
                .expect_err(statement);
            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(
                err.message.contains("explicit SQL session context"),
                "{statement}: {err:?}"
            );
            assert!(sql.engine.transaction.is_none());
            drop(sql);
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn durable_sql_detects_checkpoint_truncation_and_valid_hex_corruption() {
        let path = test_path("checkpoint-integrity");
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "create",
        )
        .unwrap();
        drop(sql);
        let checkpoint = fs::read_to_string(path.join(STATE_FILE)).unwrap();
        let altered = checkpoint.replace("7573657273", "7573657274");
        assert_ne!(altered, checkpoint);
        for broken in [altered, checkpoint.lines().next().unwrap().to_string()] {
            fs::write(path.join(STATE_FILE), broken).unwrap();
            assert_eq!(
                DurableSqlEngine::open(&path)
                    .expect_err("corruption must fail closed")
                    .code,
                ErrorCode::CorruptionDetected
            );
        }
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn durable_sql_excludes_concurrent_writers_and_releases_lock_on_drop() {
        let path = test_path("writer-lock");
        let sql = DurableSqlEngine::open(&path).unwrap();
        assert!(DurableSqlEngine::open(&path).is_err());
        drop(sql);
        drop(DurableSqlEngine::open(&path).unwrap());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn durable_sql_stops_serving_after_checkpoint_failure() {
        let path = test_path("checkpoint-failure");
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "create",
        )
        .unwrap();
        fs::create_dir(path.join(TMP_FILE)).unwrap();
        assert!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users (id, name) VALUES (1, ada)",
                "insert"
            )
            .is_err()
        );
        assert!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users",
                "select"
            )
            .is_err()
        );
        drop(sql);
        let mut reopened = DurableSqlEngine::open(&path).unwrap();
        assert_eq!(reopened.recovery().live_rows, 0);
        fs::remove_dir(path.join(TMP_FILE)).unwrap();
        let retry = reopened
            .execute(
                &mut router,
                PlacementGroupId(1),
                "INSERT INTO users (id, name) VALUES (1, ada)",
                "insert",
            )
            .unwrap();
        assert!(retry.replayed);
        assert_eq!(router.latest_version(), CommitVersion(2));
        assert_eq!(
            reopened
                .execute(
                    &mut router,
                    PlacementGroupId(1),
                    "SELECT * FROM users",
                    "read"
                )
                .unwrap()
                .rows,
            1
        );
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn durable_sql_recovers_update_index_and_delete_history() {
        let path = test_path("mutation-history");
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        for (id, statement) in [
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "INSERT INTO users (id, name) VALUES (1, ada)",
            "CREATE INDEX users_name ON users(name)",
            "UPDATE users SET name = grace WHERE id = 1",
        ]
        .iter()
        .enumerate()
        {
            let mut sql = DurableSqlEngine::open(&path).unwrap();
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                statement,
                format!("request-{id}"),
            )
            .unwrap();
        }
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        let before = fs::read(path.join(STATE_FILE)).unwrap();
        assert!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "CREATE TABLE users(id PRIMARY KEY, other)",
                "duplicate-create"
            )
            .is_err()
        );
        assert_eq!(fs::read(path.join(STATE_FILE)).unwrap(), before);
        let result = sql
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE name = grace",
                "select",
            )
            .unwrap();
        let SqlOutcome::Selected { rows } = result.outcome else {
            panic!("expected rows")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns["name"], "grace");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "DELETE FROM users WHERE id = 1",
            "delete",
        )
        .unwrap();
        drop(sql);
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        assert_eq!(sql.recovery().live_rows, 0);
        assert_eq!(sql.recovery().tombstones, 1);
        assert_eq!(sql.recovery().row_versions, 2);
        assert_eq!(
            sql.execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE name = grace",
                "select"
            )
            .unwrap()
            .rows,
            0
        );
        drop(sql);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_rejects_every_truncated_prefix_and_single_byte_mutation() {
        let checkpoint = encode_checkpoint(&SqlCheckpoint {
            tables: Vec::new(),
            kv: Vec::new(),
            receipts: BTreeMap::new(),
            namespace: None,
            commands: BTreeMap::new(),
            journal: None,
            sessions: BTreeMap::new(),
        })
        .unwrap();
        for end in 0..checkpoint.len() {
            assert!(
                decode_checkpoint(&checkpoint[..end]).is_err(),
                "accepted truncated prefix {end}"
            );
        }
        for offset in 0..checkpoint.len() {
            let mut changed = checkpoint.as_bytes().to_vec();
            changed[offset] ^= 1;
            assert!(
                decode_checkpoint(std::str::from_utf8(&changed).unwrap()).is_err(),
                "accepted byte mutation {offset}"
            );
        }
        assert!(decode_checkpoint(&checkpoint).is_ok());
    }

    #[test]
    fn durable_sql_engine_recovers_autocommit_rows_after_reopen() {
        let path = test_path("durable_sql_engine_recovers_autocommit_rows_after_reopen");
        let _ = fs::remove_dir_all(&path);
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut sql = DurableSqlEngine::open(&path).expect("open sql");

        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "sql-test-create",
        )
        .expect("create");
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users (id, name) VALUES (1, ada)",
            "sql-test-insert",
        )
        .expect("insert");

        drop(sql);
        let mut recovered = DurableSqlEngine::open(&path).expect("recover sql");
        let selected = recovered
            .execute(
                &mut router,
                PlacementGroupId(1),
                "SELECT * FROM users WHERE id = 1",
                "sql-test-select",
            )
            .expect("select");
        assert_eq!(selected.rows, 1);
        assert!(!selected.persisted);

        let err = recovered
            .execute(&mut router, PlacementGroupId(1), "BEGIN", "sql-test-begin")
            .expect_err("transaction control requires an explicit session");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("explicit SQL session context"));

        let _ = fs::remove_dir_all(&path);
    }

    fn test_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-sql-durable-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
