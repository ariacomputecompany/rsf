use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Digest, DigestAlgorithm, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError,
    RsfResult, storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{
    BackupItemKind, RestoreApplied, RestoreCheck, RestoreExecution, RestorePlan, RestoreStep,
};

const BACKUP_PG: PlacementGroupId = PlacementGroupId(54);
const BACKUP_WAL_MAGIC: &[u8; 7] = b"RSFBKP1";
const BACKUP_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RestoreOperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreOperationState {
    Planned,
    Started,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreOperation {
    pub id: RestoreOperationId,
    pub plan: RestorePlan,
    pub state: RestoreOperationState,
    pub execution: Option<RestoreExecution>,
    pub failure: Option<String>,
}

#[derive(Debug)]
pub struct DurableRestoreLedger {
    operations: BTreeMap<RestoreOperationId, RestoreOperation>,
    store: SegmentStore,
    epoch: Epoch,
    next_id: u64,
    next_index: u64,
}

impl DurableRestoreLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(BACKUP_PG),
        })?;
        let mut operations = BTreeMap::new();
        let mut next_id = 1;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Backup)
        {
            apply_backup_record(&mut operations, decode_backup_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        if let Some(last) = operations.keys().next_back() {
            next_id = last.0.saturating_add(1);
        }

        Ok(Self {
            operations,
            store,
            epoch,
            next_id,
            next_index,
        })
    }

    pub fn record_plan(&mut self, plan: RestorePlan) -> RsfResult<RestoreOperationId> {
        validate_plan(&plan)?;
        let id = RestoreOperationId(self.next_id);
        self.append_record(&BackupWalRecord::Planned {
            id,
            plan: plan.clone(),
        })?;
        self.operations.insert(
            id,
            RestoreOperation {
                id,
                plan,
                state: RestoreOperationState::Planned,
                execution: None,
                failure: None,
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
    }

    pub fn mark_started(&mut self, id: RestoreOperationId) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != RestoreOperationState::Planned {
            return Err(invalid(format!(
                "restore operation {} cannot start from {:?}",
                id.0, operation.state
            )));
        }
        operation.state = RestoreOperationState::Started;
        self.append_record(&BackupWalRecord::Started { id })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_completed(
        &mut self,
        id: RestoreOperationId,
        execution: RestoreExecution,
    ) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != RestoreOperationState::Started {
            return Err(invalid(format!(
                "restore operation {} cannot complete from {:?}",
                id.0, operation.state
            )));
        }
        validate_execution(&operation.plan, &execution)?;
        operation.state = RestoreOperationState::Completed;
        operation.execution = Some(execution.clone());
        operation.failure = None;
        self.append_record(&BackupWalRecord::Completed { id, execution })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        id: RestoreOperationId,
        reason: impl Into<String>,
    ) -> RsfResult<()> {
        let reason = reason.into();
        validate_failure(&reason)?;
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state == RestoreOperationState::Completed {
            return Err(invalid(format!(
                "restore operation {} cannot fail after completion",
                id.0
            )));
        }
        operation.state = RestoreOperationState::Failed;
        operation.failure = Some(reason.clone());
        operation.execution = None;
        self.append_record(&BackupWalRecord::Failed { id, reason })?;
        self.operations = operations;
        Ok(())
    }

    pub fn operation(&self, id: RestoreOperationId) -> Option<&RestoreOperation> {
        self.operations.get(&id)
    }

    pub fn operations(&self) -> Vec<RestoreOperation> {
        self.operations.values().cloned().collect()
    }

    pub fn pending(&self) -> Vec<RestoreOperation> {
        self.operations
            .values()
            .filter(|operation| {
                matches!(
                    operation.state,
                    RestoreOperationState::Planned | RestoreOperationState::Started
                )
            })
            .cloned()
            .collect()
    }

    fn append_record(&mut self, record: &BackupWalRecord) -> RsfResult<()> {
        let payload = encode_backup_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(BACKUP_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: BACKUP_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Backup,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BackupWalRecord {
    Planned {
        id: RestoreOperationId,
        plan: RestorePlan,
    },
    Started {
        id: RestoreOperationId,
    },
    Completed {
        id: RestoreOperationId,
        execution: RestoreExecution,
    },
    Failed {
        id: RestoreOperationId,
        reason: String,
    },
}

fn apply_backup_record(
    operations: &mut BTreeMap<RestoreOperationId, RestoreOperation>,
    record: BackupWalRecord,
) -> RsfResult<()> {
    match record {
        BackupWalRecord::Planned { id, plan } => {
            if operations.contains_key(&id) {
                return Err(corrupt(format!(
                    "duplicate restore operation id {} in WAL",
                    id.0
                )));
            }
            validate_plan(&plan)?;
            operations.insert(
                id,
                RestoreOperation {
                    id,
                    plan,
                    state: RestoreOperationState::Planned,
                    execution: None,
                    failure: None,
                },
            );
        }
        BackupWalRecord::Started { id } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != RestoreOperationState::Planned {
                return Err(corrupt(format!(
                    "restore operation {} cannot replay start from {:?}",
                    id.0, operation.state
                )));
            }
            operation.state = RestoreOperationState::Started;
        }
        BackupWalRecord::Completed { id, execution } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != RestoreOperationState::Started {
                return Err(corrupt(format!(
                    "restore operation {} cannot replay completion from {:?}",
                    id.0, operation.state
                )));
            }
            validate_execution(&operation.plan, &execution)?;
            operation.state = RestoreOperationState::Completed;
            operation.execution = Some(execution);
            operation.failure = None;
        }
        BackupWalRecord::Failed { id, reason } => {
            validate_failure(&reason)?;
            let operation = operation_mut(operations, id)?;
            if operation.state == RestoreOperationState::Completed {
                return Err(corrupt(format!(
                    "restore operation {} cannot replay failure after completion",
                    id.0
                )));
            }
            operation.state = RestoreOperationState::Failed;
            operation.failure = Some(reason);
            operation.execution = None;
        }
    }
    Ok(())
}

fn validate_plan(plan: &RestorePlan) -> RsfResult<()> {
    if plan.manifest_id.is_empty() {
        return Err(invalid("restore plan manifest id must not be empty"));
    }
    plan.manifest_checksum.validate()?;
    if plan.item_count == 0 || plan.steps.is_empty() {
        return Err(invalid("restore plan must include steps"));
    }
    if plan.item_count != plan.steps.len() {
        return Err(invalid("restore plan item count must match steps"));
    }
    if plan.total_bytes != plan.steps.iter().map(|step| step.bytes).sum::<u64>() {
        return Err(invalid("restore plan total bytes must match steps"));
    }
    if !plan.checks.iter().all(|check| check.passed) {
        return Err(invalid("restore plan has failed checks"));
    }
    for check in &plan.checks {
        if check.name.is_empty() {
            return Err(invalid("restore check name must not be empty"));
        }
    }
    for step in &plan.steps {
        validate_step(step)?;
    }
    Ok(())
}

fn validate_execution(plan: &RestorePlan, execution: &RestoreExecution) -> RsfResult<()> {
    if !execution.covers_plan(plan) {
        return Err(invalid("restore execution does not cover restore plan"));
    }
    execution.manifest_checksum.validate()?;
    execution.execution_checksum.validate()?;
    if execution.restored_kinds.is_empty() {
        return Err(invalid("restore execution must include restored kinds"));
    }
    for item in &execution.applied {
        validate_applied(item)?;
    }
    Ok(())
}

fn validate_step(step: &RestoreStep) -> RsfResult<()> {
    if step.namespace.is_empty() || step.name.is_empty() {
        return Err(invalid("restore step namespace and name must not be empty"));
    }
    if step.bytes == 0 {
        return Err(invalid("restore step bytes must be non-zero"));
    }
    step.checksum.validate()
}

fn validate_applied(applied: &RestoreApplied) -> RsfResult<()> {
    if applied.namespace.is_empty() || applied.name.is_empty() {
        return Err(invalid(
            "restore applied namespace and name must not be empty",
        ));
    }
    if applied.bytes == 0 {
        return Err(invalid("restore applied bytes must be non-zero"));
    }
    applied.checksum.validate()
}

fn operation_mut(
    operations: &mut BTreeMap<RestoreOperationId, RestoreOperation>,
    id: RestoreOperationId,
) -> RsfResult<&mut RestoreOperation> {
    operations
        .get_mut(&id)
        .ok_or_else(|| invalid(format!("unknown restore operation {}", id.0)))
}

fn validate_failure(reason: &str) -> RsfResult<()> {
    if reason.trim().is_empty() {
        return Err(invalid("restore failure reason must not be empty"));
    }
    Ok(())
}

fn encode_backup_wal_payload(record: &BackupWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(BACKUP_WAL_MAGIC);
    out.push(BACKUP_WAL_VERSION);
    match record {
        BackupWalRecord::Planned { id, plan } => {
            validate_plan(plan)?;
            out.push(1);
            write_u64(&mut out, id.0);
            encode_plan(&mut out, plan)?;
        }
        BackupWalRecord::Started { id } => {
            out.push(2);
            write_u64(&mut out, id.0);
        }
        BackupWalRecord::Completed { id, execution } => {
            out.push(3);
            write_u64(&mut out, id.0);
            encode_execution(&mut out, execution)?;
        }
        BackupWalRecord::Failed { id, reason } => {
            validate_failure(reason)?;
            out.push(4);
            write_u64(&mut out, id.0);
            write_string(&mut out, reason)?;
        }
    }
    Ok(out)
}

fn decode_backup_wal_payload(bytes: &[u8]) -> RsfResult<BackupWalRecord> {
    if bytes.len() < BACKUP_WAL_MAGIC.len() + 2 {
        return Err(corrupt("backup WAL payload is too short"));
    }
    if &bytes[..BACKUP_WAL_MAGIC.len()] != BACKUP_WAL_MAGIC {
        return Err(corrupt("backup WAL magic mismatch"));
    }
    let mut offset = BACKUP_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != BACKUP_WAL_VERSION {
        return Err(corrupt("unsupported backup WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let id = RestoreOperationId(read_u64(bytes, &mut offset)?);
    let record = match tag {
        1 => BackupWalRecord::Planned {
            id,
            plan: decode_plan(bytes, &mut offset)?,
        },
        2 => BackupWalRecord::Started { id },
        3 => BackupWalRecord::Completed {
            id,
            execution: decode_execution(bytes, &mut offset)?,
        },
        4 => BackupWalRecord::Failed {
            id,
            reason: read_string(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("unknown backup WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in backup WAL payload"));
    }
    Ok(record)
}

fn encode_plan(out: &mut Vec<u8>, plan: &RestorePlan) -> RsfResult<()> {
    write_version(out, plan.checkpoint);
    write_string(out, &plan.manifest_id)?;
    write_digest(out, plan.manifest_checksum);
    write_usize(out, plan.item_count);
    write_u64(out, plan.total_bytes);
    write_usize(out, plan.checks.len());
    for check in &plan.checks {
        write_string(out, &check.name)?;
        write_bool(out, check.passed);
    }
    write_usize(out, plan.steps.len());
    for step in &plan.steps {
        encode_step(out, step)?;
    }
    Ok(())
}

fn decode_plan(bytes: &[u8], offset: &mut usize) -> RsfResult<RestorePlan> {
    let checkpoint = read_version(bytes, offset)?;
    let manifest_id = read_string(bytes, offset)?;
    let manifest_checksum = read_digest(bytes, offset)?;
    let item_count = read_usize(bytes, offset)?;
    let total_bytes = read_u64(bytes, offset)?;
    let check_len = read_usize(bytes, offset)?;
    let mut checks = Vec::with_capacity(check_len);
    for _ in 0..check_len {
        checks.push(RestoreCheck {
            name: read_string(bytes, offset)?,
            passed: read_bool(bytes, offset)?,
        });
    }
    let step_len = read_usize(bytes, offset)?;
    let mut steps = Vec::with_capacity(step_len);
    for _ in 0..step_len {
        steps.push(decode_step(bytes, offset)?);
    }
    Ok(RestorePlan {
        checkpoint,
        manifest_id,
        manifest_checksum,
        item_count,
        total_bytes,
        checks,
        steps,
    })
}

fn encode_execution(out: &mut Vec<u8>, execution: &RestoreExecution) -> RsfResult<()> {
    write_version(out, execution.checkpoint);
    write_string(out, &execution.manifest_id)?;
    write_digest(out, execution.manifest_checksum);
    write_usize(out, execution.restored_items);
    write_u64(out, execution.restored_bytes);
    write_usize(out, execution.restored_kinds.len());
    for kind in &execution.restored_kinds {
        encode_kind(out, kind);
    }
    write_usize(out, execution.applied.len());
    for applied in &execution.applied {
        encode_applied(out, applied)?;
    }
    write_digest(out, execution.execution_checksum);
    Ok(())
}

fn decode_execution(bytes: &[u8], offset: &mut usize) -> RsfResult<RestoreExecution> {
    let checkpoint = read_version(bytes, offset)?;
    let manifest_id = read_string(bytes, offset)?;
    let manifest_checksum = read_digest(bytes, offset)?;
    let restored_items = read_usize(bytes, offset)?;
    let restored_bytes = read_u64(bytes, offset)?;
    let kind_len = read_usize(bytes, offset)?;
    let mut restored_kinds = Vec::with_capacity(kind_len);
    for _ in 0..kind_len {
        restored_kinds.push(decode_kind(bytes, offset)?);
    }
    let applied_len = read_usize(bytes, offset)?;
    let mut applied = Vec::with_capacity(applied_len);
    for _ in 0..applied_len {
        applied.push(decode_applied(bytes, offset)?);
    }
    let execution_checksum = read_digest(bytes, offset)?;
    Ok(RestoreExecution {
        checkpoint,
        manifest_id,
        manifest_checksum,
        restored_items,
        restored_bytes,
        restored_kinds,
        applied,
        execution_checksum,
    })
}

fn encode_step(out: &mut Vec<u8>, step: &RestoreStep) -> RsfResult<()> {
    encode_kind(out, &step.kind);
    write_string(out, &step.namespace)?;
    write_string(out, &step.name)?;
    write_version(out, step.version);
    write_u64(out, step.bytes);
    write_digest(out, step.checksum);
    Ok(())
}

fn decode_step(bytes: &[u8], offset: &mut usize) -> RsfResult<RestoreStep> {
    Ok(RestoreStep {
        kind: decode_kind(bytes, offset)?,
        namespace: read_string(bytes, offset)?,
        name: read_string(bytes, offset)?,
        version: read_version(bytes, offset)?,
        bytes: read_u64(bytes, offset)?,
        checksum: read_digest(bytes, offset)?,
    })
}

fn encode_applied(out: &mut Vec<u8>, applied: &RestoreApplied) -> RsfResult<()> {
    encode_kind(out, &applied.kind);
    write_string(out, &applied.namespace)?;
    write_string(out, &applied.name)?;
    write_version(out, applied.version);
    write_u64(out, applied.bytes);
    write_digest(out, applied.checksum);
    Ok(())
}

fn decode_applied(bytes: &[u8], offset: &mut usize) -> RsfResult<RestoreApplied> {
    Ok(RestoreApplied {
        kind: decode_kind(bytes, offset)?,
        namespace: read_string(bytes, offset)?,
        name: read_string(bytes, offset)?,
        version: read_version(bytes, offset)?,
        bytes: read_u64(bytes, offset)?,
        checksum: read_digest(bytes, offset)?,
    })
}

fn encode_kind(out: &mut Vec<u8>, kind: &BackupItemKind) {
    out.push(match kind {
        BackupItemKind::CatalogRoot => 1,
        BackupItemKind::TransactionLog => 2,
        BackupItemKind::KvRange => 3,
        BackupItemKind::SqlTable => 4,
        BackupItemKind::ObjectManifest => 5,
        BackupItemKind::BlobChunk => 6,
        BackupItemKind::ChunkIndex => 7,
    });
}

fn decode_kind(bytes: &[u8], offset: &mut usize) -> RsfResult<BackupItemKind> {
    match read_u8(bytes, offset)? {
        1 => Ok(BackupItemKind::CatalogRoot),
        2 => Ok(BackupItemKind::TransactionLog),
        3 => Ok(BackupItemKind::KvRange),
        4 => Ok(BackupItemKind::SqlTable),
        5 => Ok(BackupItemKind::ObjectManifest),
        6 => Ok(BackupItemKind::BlobChunk),
        7 => Ok(BackupItemKind::ChunkIndex),
        _ => Err(corrupt("unknown backup item kind tag")),
    }
}

fn write_version(out: &mut Vec<u8>, value: CommitVersion) {
    write_u64(out, value.0);
}

fn write_usize(out: &mut Vec<u8>, value: usize) {
    write_u64(out, value as u64);
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn write_digest(out: &mut Vec<u8>, digest: Digest) {
    out.push(match digest.algorithm {
        DigestAlgorithm::Blake3 => 1,
        DigestAlgorithm::Sha256 => 2,
    });
    out.extend_from_slice(&digest.bytes);
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len()).map_err(|_| invalid("backup string is too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_version(bytes: &[u8], offset: &mut usize) -> RsfResult<CommitVersion> {
    Ok(CommitVersion(read_u64(bytes, offset)?))
}

fn read_usize(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    usize::try_from(read_u64(bytes, offset)?).map_err(|_| corrupt("usize field overflow"))
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> RsfResult<bool> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(corrupt("invalid backup bool field")),
    }
}

fn read_digest(bytes: &[u8], offset: &mut usize) -> RsfResult<Digest> {
    let algorithm = match read_u8(bytes, offset)? {
        1 => DigestAlgorithm::Blake3,
        2 => DigestAlgorithm::Sha256,
        _ => return Err(corrupt("invalid backup digest algorithm")),
    };
    if bytes.len().saturating_sub(*offset) < 32 {
        return Err(corrupt("unexpected end of backup digest"));
    }
    let mut value = [0_u8; 32];
    value.copy_from_slice(&bytes[*offset..*offset + 32]);
    *offset += 32;
    Digest::new(algorithm, value)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(corrupt("unexpected end of backup string length"));
    }
    let len = u32::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    let len = usize::try_from(len).map_err(|_| corrupt("backup string length overflow"))?;
    if bytes.len().saturating_sub(*offset) < len {
        return Err(corrupt("unexpected end of backup string"));
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + len])
        .map_err(|_| corrupt("backup string is not utf-8"))?
        .to_owned();
    *offset += len;
    Ok(value)
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    if *offset >= bytes.len() {
        return Err(corrupt("unexpected end of backup WAL payload"));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(corrupt("unexpected end of backup WAL payload"));
    }
    let value = u64::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
        bytes[*offset + 4],
        bytes[*offset + 5],
        bytes[*offset + 6],
        bytes[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests;
