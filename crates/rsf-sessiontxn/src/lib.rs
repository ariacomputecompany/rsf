#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_core::{
    Bytes, CommitEvidence, CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError,
    RsfResult,
    storage::WalRecordKind,
    transaction::{CommitIntent, KeyRange, Mutation, TransactionId},
};
use rsf_kv::{KvBatch, KvEngine, KvMutation, ReadSnapshot};
use rsf_protocol::{ClientRequestId, FabricRouterPort};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};
use rsf_txn::{DurableTransactionCoordinator, ReadVersion};

const SESSION_TXN_PG: PlacementGroupId = PlacementGroupId(0);
const SESSION_TXN_WAL_MAGIC: &[u8; 8] = b"RSFSTXN!";
const SESSION_TXN_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTransaction {
    pub session_id: String,
    pub snapshot: ReadSnapshot,
    read_conflicts: BTreeSet<Bytes>,
    pending: BTreeMap<Bytes, PendingValue>,
    savepoints: BTreeMap<String, BTreeMap<Bytes, PendingValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionRead {
    Overlay {
        key: Bytes,
        value: Option<Bytes>,
    },
    Snapshot {
        key: Bytes,
        version: Option<CommitVersion>,
        value: Option<Bytes>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionTransactionDecision {
    Began {
        session_id: String,
        snapshot: CommitVersion,
    },
    StagedWrite {
        session_id: String,
        key: Bytes,
    },
    SavepointCreated {
        session_id: String,
        name: String,
        pending_writes: usize,
    },
    SavepointRolledBack {
        session_id: String,
        name: String,
        rewound_writes: usize,
        pending_writes: usize,
    },
    OverlayRead {
        session_id: String,
        key: Bytes,
        value: Option<Bytes>,
    },
    SnapshotRead {
        session_id: String,
        key: Bytes,
        version: Option<CommitVersion>,
        value: Option<Bytes>,
    },
    Committed {
        session_id: String,
        version: CommitVersion,
        evidence: CommitEvidence,
        writes: usize,
    },
    RolledBack {
        session_id: String,
        discarded_writes: usize,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionTransactionReport {
    pub sessions: usize,
    pub pending_writes: usize,
    pub overlay_reads: usize,
    pub snapshot_reads: usize,
    pub commits: usize,
    pub rollbacks: usize,
    pub savepoints: usize,
    pub savepoint_rewinds: usize,
    pub rejections: usize,
}

#[derive(Clone, Debug, Default)]
pub struct SessionTransactionEngine {
    active: BTreeMap<String, SessionTransaction>,
    seen_sessions: BTreeSet<String>,
    pending_writes: usize,
    overlay_reads: usize,
    snapshot_reads: usize,
    commits: usize,
    rollbacks: usize,
    savepoints: usize,
    savepoint_rewinds: usize,
    rejections: usize,
}

#[derive(Debug)]
pub struct DurableSessionTransactionEngine {
    engine: SessionTransactionEngine,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PendingValue {
    Set(Bytes),
    Clear,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SessionTxnWalRecord {
    Begin {
        session_id: String,
        snapshot: ReadSnapshot,
    },
    StageSet {
        session_id: String,
        key: Bytes,
        value: Bytes,
    },
    StageClear {
        session_id: String,
        key: Bytes,
    },
    Savepoint {
        session_id: String,
        name: String,
    },
    RollbackToSavepoint {
        session_id: String,
        name: String,
    },
    SnapshotRead {
        session_id: String,
        key: Bytes,
    },
    Commit {
        session_id: String,
    },
    Rollback {
        session_id: String,
    },
}

impl SessionTransactionEngine {
    pub fn begin(
        &mut self,
        session_id: impl Into<String>,
        kv: &KvEngine,
        router: &impl FabricRouterPort,
    ) -> RsfResult<SessionTransactionDecision> {
        let session_id = session_id.into();
        validate_session_id(&session_id)?;
        if self.active.contains_key(&session_id) {
            return self.reject(format!(
                "session {session_id} already has an active transaction"
            ));
        }

        let snapshot = kv.snapshot(router);
        self.seen_sessions.insert(session_id.clone());
        self.active.insert(
            session_id.clone(),
            SessionTransaction {
                session_id: session_id.clone(),
                snapshot,
                read_conflicts: BTreeSet::new(),
                pending: BTreeMap::new(),
                savepoints: BTreeMap::new(),
            },
        );
        Ok(SessionTransactionDecision::Began {
            session_id,
            snapshot: snapshot.version,
        })
    }

    pub fn stage_set(
        &mut self,
        session_id: &str,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> RsfResult<SessionTransactionDecision> {
        let key = key.into();
        if key.is_empty() {
            return self.reject("session transaction key must not be empty");
        }
        let Some(session) = self.active.get_mut(session_id) else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        if !session.pending.contains_key(&key) {
            self.pending_writes += 1;
        }
        session
            .pending
            .insert(key.clone(), PendingValue::Set(value.into()));
        Ok(SessionTransactionDecision::StagedWrite {
            session_id: session_id.to_string(),
            key,
        })
    }

    pub fn savepoint(
        &mut self,
        session_id: &str,
        name: impl Into<String>,
    ) -> RsfResult<SessionTransactionDecision> {
        let name = name.into();
        validate_savepoint_name(&name)?;
        let Some(session) = self.active.get_mut(session_id) else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        session
            .savepoints
            .insert(name.clone(), session.pending.clone());
        self.savepoints += 1;
        Ok(SessionTransactionDecision::SavepointCreated {
            session_id: session_id.to_string(),
            name,
            pending_writes: session.pending.len(),
        })
    }

    pub fn rollback_to_savepoint(
        &mut self,
        session_id: &str,
        name: &str,
    ) -> RsfResult<SessionTransactionDecision> {
        validate_savepoint_name(name)?;
        let Some(session) = self.active.get_mut(session_id) else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        let Some(saved_pending) = session.savepoints.get(name).cloned() else {
            return self.reject(format!("session {session_id} has no savepoint {name}"));
        };
        let rewound_writes = rewound_write_count(&session.pending, &saved_pending);
        session.pending = saved_pending;
        let pending_writes = session.pending.len();
        self.savepoint_rewinds += 1;
        Ok(SessionTransactionDecision::SavepointRolledBack {
            session_id: session_id.to_string(),
            name: name.to_string(),
            rewound_writes,
            pending_writes,
        })
    }

    pub fn stage_clear(
        &mut self,
        session_id: &str,
        key: impl Into<Bytes>,
    ) -> RsfResult<SessionTransactionDecision> {
        let key = key.into();
        if key.is_empty() {
            return self.reject("session transaction key must not be empty");
        }
        let Some(session) = self.active.get_mut(session_id) else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        if !session.pending.contains_key(&key) {
            self.pending_writes += 1;
        }
        session.pending.insert(key.clone(), PendingValue::Clear);
        Ok(SessionTransactionDecision::StagedWrite {
            session_id: session_id.to_string(),
            key,
        })
    }

    pub fn read(
        &mut self,
        session_id: &str,
        kv: &KvEngine,
        key: impl Into<Bytes>,
    ) -> RsfResult<SessionTransactionDecision> {
        let key = key.into();
        let Some(session) = self.active.get_mut(session_id) else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        if let Some(pending) = session.pending.get(&key) {
            self.overlay_reads += 1;
            return Ok(SessionTransactionDecision::OverlayRead {
                session_id: session_id.to_string(),
                key,
                value: pending.value(),
            });
        }

        self.snapshot_reads += 1;
        let value = kv.get_at(&key, session.snapshot);
        session.read_conflicts.insert(key.clone());
        Ok(SessionTransactionDecision::SnapshotRead {
            session_id: session_id.to_string(),
            key,
            version: value.map(|value| value.version),
            value: value.map(|value| value.value.clone()),
        })
    }

    pub fn commit(
        &mut self,
        session_id: &str,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
    ) -> RsfResult<SessionTransactionDecision> {
        let Some(session) = self.active.remove(session_id) else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        if session.pending.is_empty() {
            return self.reject(format!("session {session_id} has no writes to commit"));
        }

        let writes = session.pending.len();
        let mutations = session
            .pending
            .into_iter()
            .map(|(key, value)| match value {
                PendingValue::Set(value) => KvMutation::Set { key, value },
                PendingValue::Clear => KvMutation::Clear { key },
            })
            .collect::<Vec<_>>();
        let request = session_commit_request_id(session_id, &mutations);
        let commit = kv.commit_batch_with_evidence(
            router,
            pg,
            KvBatch {
                client_request: Some(request),
                snapshot: session.snapshot,
                mutations,
            },
        )?;
        self.commits += 1;
        Ok(SessionTransactionDecision::Committed {
            session_id: session_id.to_string(),
            version: commit.version,
            evidence: commit.evidence,
            writes,
        })
    }

    pub fn commit_with_coordinator(
        &mut self,
        session_id: &str,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        transactions: &mut DurableTransactionCoordinator,
    ) -> RsfResult<SessionTransactionDecision> {
        let Some(session) = self.active.get(session_id).cloned() else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        if session.pending.is_empty() {
            return self.reject(format!("session {session_id} has no writes to commit"));
        }

        let writes = session.pending.len();
        let kv_mutations = session
            .pending
            .iter()
            .map(|(key, value)| match value {
                PendingValue::Set(value) => KvMutation::Set {
                    key: key.clone(),
                    value: value.clone(),
                },
                PendingValue::Clear => KvMutation::Clear { key: key.clone() },
            })
            .collect::<Vec<_>>();
        let intent = CommitIntent {
            id: session_transaction_id(session_id, &kv_mutations),
            read_conflicts: session
                .read_conflicts
                .iter()
                .map(|key| exact_key_range(key))
                .collect(),
            write_conflicts: kv_mutations
                .iter()
                .map(|mutation| exact_key_range(mutation.key()))
                .collect(),
            mutations: kv_mutations
                .iter()
                .map(|mutation| match mutation {
                    KvMutation::Set { key, value } => Mutation::Set {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    KvMutation::Clear { key } => Mutation::Clear { key: key.clone() },
                })
                .collect(),
        };
        let prepared = transactions.prepare(
            ReadVersion {
                version: session.snapshot.version,
            },
            intent,
        )?;
        let commit = transactions.commit(router, pg, prepared)?;
        kv.apply_committed_batch(commit.version, kv_mutations)?;
        self.active.remove(session_id);
        self.commits += 1;
        Ok(SessionTransactionDecision::Committed {
            session_id: session_id.to_string(),
            version: commit.version,
            evidence: commit.evidence,
            writes,
        })
    }

    pub fn rollback(&mut self, session_id: &str) -> RsfResult<SessionTransactionDecision> {
        let Some(session) = self.active.remove(session_id) else {
            return self.reject(format!("session {session_id} has no active transaction"));
        };
        let discarded_writes = session.pending.len();
        self.rollbacks += 1;
        Ok(SessionTransactionDecision::RolledBack {
            session_id: session_id.to_string(),
            discarded_writes,
        })
    }

    pub fn report(&self) -> SessionTransactionReport {
        SessionTransactionReport {
            sessions: self.seen_sessions.len(),
            pending_writes: self.pending_writes,
            overlay_reads: self.overlay_reads,
            snapshot_reads: self.snapshot_reads,
            commits: self.commits,
            rollbacks: self.rollbacks,
            savepoints: self.savepoints,
            savepoint_rewinds: self.savepoint_rewinds,
            rejections: self.rejections,
        }
    }

    fn reject<T>(&mut self, reason: impl Into<String>) -> RsfResult<T> {
        self.rejections += 1;
        Err(RsfError::new(ErrorCode::InvalidInput, reason.into()))
    }
}

impl PendingValue {
    fn value(&self) -> Option<Bytes> {
        match self {
            Self::Set(value) => Some(value.clone()),
            Self::Clear => None,
        }
    }
}

impl DurableSessionTransactionEngine {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(SESSION_TXN_PG),
        })?;
        let mut engine = SessionTransactionEngine::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::SessionTransaction)
        {
            apply_recovered_session_record(&mut engine, decode_session_txn_wal(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            engine,
            store,
            epoch,
            next_index,
        })
    }

    pub fn begin(
        &mut self,
        session_id: impl Into<String>,
        kv: &KvEngine,
        router: &impl FabricRouterPort,
    ) -> RsfResult<SessionTransactionDecision> {
        let session_id = session_id.into();
        let decision = self.engine.begin(session_id.clone(), kv, router)?;
        let SessionTransactionDecision::Began { snapshot, .. } = decision else {
            unreachable!("begin returns Began on success");
        };
        self.append_record(&SessionTxnWalRecord::Begin {
            session_id,
            snapshot: ReadSnapshot { version: snapshot },
        })?;
        Ok(decision)
    }

    pub fn stage_set(
        &mut self,
        session_id: &str,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> RsfResult<SessionTransactionDecision> {
        let key = key.into();
        let value = value.into();
        let decision = self
            .engine
            .stage_set(session_id, key.clone(), value.clone())?;
        self.append_record(&SessionTxnWalRecord::StageSet {
            session_id: session_id.to_string(),
            key,
            value,
        })?;
        Ok(decision)
    }

    pub fn stage_clear(
        &mut self,
        session_id: &str,
        key: impl Into<Bytes>,
    ) -> RsfResult<SessionTransactionDecision> {
        let key = key.into();
        let decision = self.engine.stage_clear(session_id, key.clone())?;
        self.append_record(&SessionTxnWalRecord::StageClear {
            session_id: session_id.to_string(),
            key,
        })?;
        Ok(decision)
    }

    pub fn savepoint(
        &mut self,
        session_id: &str,
        name: impl Into<String>,
    ) -> RsfResult<SessionTransactionDecision> {
        let name = name.into();
        let decision = self.engine.savepoint(session_id, name.clone())?;
        self.append_record(&SessionTxnWalRecord::Savepoint {
            session_id: session_id.to_string(),
            name,
        })?;
        Ok(decision)
    }

    pub fn rollback_to_savepoint(
        &mut self,
        session_id: &str,
        name: &str,
    ) -> RsfResult<SessionTransactionDecision> {
        let decision = self.engine.rollback_to_savepoint(session_id, name)?;
        self.append_record(&SessionTxnWalRecord::RollbackToSavepoint {
            session_id: session_id.to_string(),
            name: name.to_string(),
        })?;
        Ok(decision)
    }

    pub fn read(
        &mut self,
        session_id: &str,
        kv: &KvEngine,
        key: impl Into<Bytes>,
    ) -> RsfResult<SessionTransactionDecision> {
        let key = key.into();
        let decision = self.engine.read(session_id, kv, key.clone())?;
        if matches!(decision, SessionTransactionDecision::SnapshotRead { .. }) {
            self.append_record(&SessionTxnWalRecord::SnapshotRead {
                session_id: session_id.to_string(),
                key,
            })?;
        }
        Ok(decision)
    }

    pub fn commit(
        &mut self,
        session_id: &str,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
    ) -> RsfResult<SessionTransactionDecision> {
        let decision = self.engine.commit(session_id, kv, router, pg)?;
        self.append_record(&SessionTxnWalRecord::Commit {
            session_id: session_id.to_string(),
        })?;
        Ok(decision)
    }

    pub fn commit_with_coordinator(
        &mut self,
        session_id: &str,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        transactions: &mut DurableTransactionCoordinator,
    ) -> RsfResult<SessionTransactionDecision> {
        let decision =
            self.engine
                .commit_with_coordinator(session_id, kv, router, pg, transactions)?;
        self.append_record(&SessionTxnWalRecord::Commit {
            session_id: session_id.to_string(),
        })?;
        Ok(decision)
    }

    pub fn rollback(&mut self, session_id: &str) -> RsfResult<SessionTransactionDecision> {
        let decision = self.engine.rollback(session_id)?;
        self.append_record(&SessionTxnWalRecord::Rollback {
            session_id: session_id.to_string(),
        })?;
        Ok(decision)
    }

    pub fn report(&self) -> SessionTransactionReport {
        self.engine.report()
    }

    fn append_record(&mut self, record: &SessionTxnWalRecord) -> RsfResult<()> {
        let payload = encode_session_txn_wal(record)?;
        let previous_hash = self
            .store
            .recover_current()?
            .recovery
            .pg_tail(SESSION_TXN_PG)
            .map_or([0; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: SESSION_TXN_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::SessionTransaction,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn validate_session_id(session_id: &str) -> RsfResult<()> {
    if session_id.trim().is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "session id must not be empty",
        ));
    }
    Ok(())
}

fn apply_recovered_session_record(
    engine: &mut SessionTransactionEngine,
    record: SessionTxnWalRecord,
) -> RsfResult<()> {
    match record {
        SessionTxnWalRecord::Begin {
            session_id,
            snapshot,
        } => {
            validate_session_id(&session_id)?;
            if engine.active.contains_key(&session_id) {
                return Err(corrupt(
                    "session begin replay found duplicate active session",
                ));
            }
            engine.seen_sessions.insert(session_id.clone());
            engine.active.insert(
                session_id.clone(),
                SessionTransaction {
                    session_id,
                    snapshot,
                    read_conflicts: BTreeSet::new(),
                    pending: BTreeMap::new(),
                    savepoints: BTreeMap::new(),
                },
            );
        }
        SessionTxnWalRecord::StageSet {
            session_id,
            key,
            value,
        } => {
            if key.is_empty() {
                return Err(corrupt("session set replay has empty key"));
            }
            let Some(session) = engine.active.get_mut(&session_id) else {
                return Err(corrupt("session set replay found no active session"));
            };
            if !session.pending.contains_key(&key) {
                engine.pending_writes += 1;
            }
            session.pending.insert(key, PendingValue::Set(value));
        }
        SessionTxnWalRecord::StageClear { session_id, key } => {
            if key.is_empty() {
                return Err(corrupt("session clear replay has empty key"));
            }
            let Some(session) = engine.active.get_mut(&session_id) else {
                return Err(corrupt("session clear replay found no active session"));
            };
            if !session.pending.contains_key(&key) {
                engine.pending_writes += 1;
            }
            session.pending.insert(key, PendingValue::Clear);
        }
        SessionTxnWalRecord::Savepoint { session_id, name } => {
            validate_savepoint_name(&name)?;
            let Some(session) = engine.active.get_mut(&session_id) else {
                return Err(corrupt("session savepoint replay found no active session"));
            };
            session.savepoints.insert(name, session.pending.clone());
            engine.savepoints += 1;
        }
        SessionTxnWalRecord::RollbackToSavepoint { session_id, name } => {
            validate_savepoint_name(&name)?;
            let Some(session) = engine.active.get_mut(&session_id) else {
                return Err(corrupt(
                    "session savepoint rollback replay found no active session",
                ));
            };
            let Some(saved_pending) = session.savepoints.get(&name).cloned() else {
                return Err(corrupt(
                    "session savepoint rollback replay found no savepoint",
                ));
            };
            session.pending = saved_pending;
            engine.savepoint_rewinds += 1;
        }
        SessionTxnWalRecord::SnapshotRead { session_id, key } => {
            if key.is_empty() {
                return Err(corrupt("session snapshot read replay has empty key"));
            }
            let Some(session) = engine.active.get_mut(&session_id) else {
                return Err(corrupt(
                    "session snapshot read replay found no active session",
                ));
            };
            session.read_conflicts.insert(key);
            engine.snapshot_reads += 1;
        }
        SessionTxnWalRecord::Commit { session_id } => {
            let Some(_session) = engine.active.remove(&session_id) else {
                return Err(corrupt("session commit replay found no active session"));
            };
            engine.commits += 1;
        }
        SessionTxnWalRecord::Rollback { session_id } => {
            let Some(_session) = engine.active.remove(&session_id) else {
                return Err(corrupt("session rollback replay found no active session"));
            };
            engine.rollbacks += 1;
        }
    }
    Ok(())
}

fn encode_session_txn_wal(record: &SessionTxnWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(SESSION_TXN_WAL_MAGIC);
    out.push(SESSION_TXN_WAL_VERSION);
    match record {
        SessionTxnWalRecord::Begin {
            session_id,
            snapshot,
        } => {
            out.push(1);
            write_string(session_id, &mut out)?;
            out.extend_from_slice(&snapshot.version.0.to_le_bytes());
        }
        SessionTxnWalRecord::StageSet {
            session_id,
            key,
            value,
        } => {
            out.push(2);
            write_string(session_id, &mut out)?;
            write_bytes(key, &mut out)?;
            write_bytes(value, &mut out)?;
        }
        SessionTxnWalRecord::StageClear { session_id, key } => {
            out.push(3);
            write_string(session_id, &mut out)?;
            write_bytes(key, &mut out)?;
        }
        SessionTxnWalRecord::Savepoint { session_id, name } => {
            out.push(4);
            write_string(session_id, &mut out)?;
            write_string(name, &mut out)?;
        }
        SessionTxnWalRecord::RollbackToSavepoint { session_id, name } => {
            out.push(5);
            write_string(session_id, &mut out)?;
            write_string(name, &mut out)?;
        }
        SessionTxnWalRecord::SnapshotRead { session_id, key } => {
            out.push(8);
            write_string(session_id, &mut out)?;
            write_bytes(key, &mut out)?;
        }
        SessionTxnWalRecord::Commit { session_id } => {
            out.push(6);
            write_string(session_id, &mut out)?;
        }
        SessionTxnWalRecord::Rollback { session_id } => {
            out.push(7);
            write_string(session_id, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_session_txn_wal(bytes: &[u8]) -> RsfResult<SessionTxnWalRecord> {
    if !bytes.starts_with(SESSION_TXN_WAL_MAGIC) {
        return Err(corrupt("session transaction wal has invalid magic"));
    }
    let mut offset = SESSION_TXN_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != SESSION_TXN_WAL_VERSION {
        return Err(corrupt("session transaction wal has unsupported version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => SessionTxnWalRecord::Begin {
            session_id: read_string(bytes, &mut offset)?,
            snapshot: ReadSnapshot {
                version: CommitVersion(read_u64(bytes, &mut offset)?),
            },
        },
        2 => SessionTxnWalRecord::StageSet {
            session_id: read_string(bytes, &mut offset)?,
            key: read_bytes(bytes, &mut offset)?,
            value: read_bytes(bytes, &mut offset)?,
        },
        3 => SessionTxnWalRecord::StageClear {
            session_id: read_string(bytes, &mut offset)?,
            key: read_bytes(bytes, &mut offset)?,
        },
        4 => SessionTxnWalRecord::Savepoint {
            session_id: read_string(bytes, &mut offset)?,
            name: read_string(bytes, &mut offset)?,
        },
        5 => SessionTxnWalRecord::RollbackToSavepoint {
            session_id: read_string(bytes, &mut offset)?,
            name: read_string(bytes, &mut offset)?,
        },
        8 => SessionTxnWalRecord::SnapshotRead {
            session_id: read_string(bytes, &mut offset)?,
            key: read_bytes(bytes, &mut offset)?,
        },
        6 => SessionTxnWalRecord::Commit {
            session_id: read_string(bytes, &mut offset)?,
        },
        7 => SessionTxnWalRecord::Rollback {
            session_id: read_string(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("session transaction wal has unknown tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("session transaction wal has trailing bytes"));
    }
    Ok(record)
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    write_bytes(value.as_bytes(), out)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    String::from_utf8(read_bytes(bytes, offset)?)
        .map_err(|_| corrupt("session transaction string is not utf-8"))
}

fn write_bytes(bytes: &[u8], out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "session transaction byte field does not fit in record",
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> RsfResult<Bytes> {
    let len = read_u32(bytes, offset)? as usize;
    Ok(read_exact(bytes, offset, len)?.to_vec())
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("session transaction wal ended early"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = read_exact(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(u64::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = (*offset)
        .checked_add(len)
        .ok_or_else(|| corrupt("session transaction wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("session transaction wal ended early"))?;
    *offset = end;
    Ok(raw)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

fn validate_savepoint_name(name: &str) -> RsfResult<()> {
    if name.trim().is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "savepoint name must not be empty",
        ));
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "savepoint name must be ascii alphanumeric, underscore, or hyphen",
        ));
    }
    Ok(())
}

fn rewound_write_count(
    before: &BTreeMap<Bytes, PendingValue>,
    after: &BTreeMap<Bytes, PendingValue>,
) -> usize {
    before
        .keys()
        .chain(after.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| before.get(*key) != after.get(*key))
        .count()
}

fn exact_key_range(key: &[u8]) -> KeyRange {
    let mut end = key.to_vec();
    end.push(0);
    KeyRange {
        start: key.to_vec(),
        end,
    }
}

fn session_transaction_id(session_id: &str, mutations: &[KvMutation]) -> TransactionId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"rsf-session-transaction-v1");
    hasher.update(&(session_id.len() as u64).to_le_bytes());
    hasher.update(session_id.as_bytes());
    for mutation in mutations {
        match mutation {
            KvMutation::Set { key, value } => {
                hasher.update(b"set");
                hasher.update(&(key.len() as u64).to_le_bytes());
                hasher.update(key);
                hasher.update(&(value.len() as u64).to_le_bytes());
                hasher.update(value);
            }
            KvMutation::Clear { key } => {
                hasher.update(b"clear");
                hasher.update(&(key.len() as u64).to_le_bytes());
                hasher.update(key);
            }
        }
    }
    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    TransactionId(id)
}

fn session_commit_request_id(session_id: &str, mutations: &[KvMutation]) -> ClientRequestId {
    let mut hash = FNV_OFFSET;
    hash_bytes(&mut hash, session_id.as_bytes());
    for mutation in mutations {
        match mutation {
            KvMutation::Set { key, value } => {
                hash_bytes(&mut hash, b"set");
                hash_bytes(&mut hash, key);
                hash_bytes(&mut hash, value);
            }
            KvMutation::Clear { key } => {
                hash_bytes(&mut hash, b"clear");
                hash_bytes(&mut hash, key);
            }
        }
    }
    ClientRequestId::new(format!("sessiontxn:{session_id}:{hash}"), 0)
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    hash_len(hash, bytes.len() as u64);
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn hash_len(hash: &mut u64, len: u64) {
    for byte in len.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use rsf_core::{Epoch, NodeId};
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn pending_write_is_visible_before_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"old".to_vec(),
        )
        .unwrap();
        let mut sessions = SessionTransactionEngine::default();

        sessions.begin("s1", &kv, &router).unwrap();
        sessions
            .stage_set("s1", b"user/1".to_vec(), b"new".to_vec())
            .unwrap();
        let read = sessions.read("s1", &kv, b"user/1".to_vec()).unwrap();

        assert!(matches!(
            read,
            SessionTransactionDecision::OverlayRead {
                value: Some(value),
                ..
            } if value == b"new".to_vec()
        ));
        assert_eq!(kv.get(b"user/1").unwrap().value, b"old".to_vec());
        sessions
            .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
            .unwrap();
        assert_eq!(kv.get(b"user/1").unwrap().value, b"new".to_vec());
    }

    #[test]
    fn commit_uses_vsr_backed_router_and_recovers_certificate() {
        let path = test_path("rsf-sessiontxn-vsr-router");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            sessions.begin("s1", &kv, &router).unwrap();
            sessions
                .stage_set("s1", b"user/1".to_vec(), b"durable".to_vec())
                .unwrap();
            let committed = sessions
                .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
                .unwrap();
            assert!(matches!(
                committed,
                SessionTransactionDecision::Committed {
                    version: CommitVersion(1),
                    writes: 1,
                    ..
                }
            ));
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1), NodeId(0), voters)
                .unwrap()
                .recovered_certificates(PlacementGroupId(1))
                .expect("session transaction commit certificate recovers");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].version, CommitVersion(1));
    }

    #[test]
    fn commit_retry_replays_vsr_request_certificate_without_new_commit() {
        let path = test_path("rsf-sessiontxn-vsr-request-replay");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            let mut kv = KvEngine::default();
            let mut sessions = SessionTransactionEngine::default();

            sessions.begin("s1", &kv, &router).unwrap();
            sessions
                .stage_set("s1", b"user/1".to_vec(), b"durable".to_vec())
                .unwrap();
            let committed = sessions
                .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
                .unwrap();

            assert!(matches!(
                committed,
                SessionTransactionDecision::Committed {
                    version: CommitVersion(1),
                    writes: 1,
                    ..
                }
            ));
            assert_eq!(router.latest_version(), CommitVersion(1));
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            let mut kv = KvEngine::default();
            let mut sessions = SessionTransactionEngine::default();

            sessions.begin("s1", &kv, &router).unwrap();
            sessions
                .stage_set("s1", b"user/1".to_vec(), b"durable".to_vec())
                .unwrap();
            let committed = sessions
                .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
                .unwrap();

            assert!(matches!(
                committed,
                SessionTransactionDecision::Committed {
                    version: CommitVersion(1),
                    writes: 1,
                    ..
                }
            ));
            assert_eq!(router.latest_version(), CommitVersion(1));
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1), NodeId(0), voters)
                .unwrap()
                .recovered_certificates(PlacementGroupId(1))
                .expect("session transaction commit certificate recovers");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].version, CommitVersion(1));
    }

    #[test]
    fn durable_session_commit_uses_transaction_coordinator_and_applies_kv_once() {
        let router_path = test_path("rsf-sessiontxn-coordinator-router");
        let session_path = test_path("rsf-sessiontxn-coordinator-session");
        let txn_path = test_path("rsf-sessiontxn-coordinator-txn");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let mut kv = KvEngine::default();

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            let mut sessions =
                DurableSessionTransactionEngine::open(&session_path, Epoch(1)).unwrap();
            let mut transactions =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).unwrap();

            sessions.begin("s1", &kv, &router).unwrap();
            sessions
                .stage_set("s1", b"user/1".to_vec(), b"coordinated".to_vec())
                .unwrap();
            let committed = sessions
                .commit_with_coordinator(
                    "s1",
                    &mut kv,
                    &mut router,
                    PlacementGroupId(1),
                    &mut transactions,
                )
                .unwrap();
            assert!(matches!(
                committed,
                SessionTransactionDecision::Committed {
                    version: CommitVersion(1),
                    writes: 1,
                    ..
                }
            ));
            assert_eq!(kv.get(b"user/1").unwrap().value, b"coordinated".to_vec());

            kv.apply_committed_batch(
                CommitVersion(1),
                vec![KvMutation::Set {
                    key: b"user/1".to_vec(),
                    value: b"coordinated".to_vec(),
                }],
            )
            .unwrap();
            assert_eq!(kv.changes_since(CommitVersion(0)).len(), 1);
        }

        {
            let transactions = DurableTransactionCoordinator::open(&txn_path, Epoch(1)).unwrap();
            let sessions = DurableSessionTransactionEngine::open(&session_path, Epoch(1)).unwrap();
            assert_eq!(transactions.committed().len(), 1);
            assert_eq!(sessions.report().sessions, 1);
            assert_eq!(sessions.report().commits, 1);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(session_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn recovered_session_read_set_rejects_write_skew_through_coordinator() {
        let router_path = test_path("rsf-sessiontxn-recovered-skew-router");
        let session_path = test_path("rsf-sessiontxn-recovered-skew-session");
        let txn_path = test_path("rsf-sessiontxn-recovered-skew-txn");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let mut kv = KvEngine::default();

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            kv.set(
                &mut router,
                PlacementGroupId(1),
                b"account/a".to_vec(),
                b"open".to_vec(),
            )
            .unwrap();
            kv.set(
                &mut router,
                PlacementGroupId(1),
                b"account/b".to_vec(),
                b"open".to_vec(),
            )
            .unwrap();
            let mut sessions =
                DurableSessionTransactionEngine::open(&session_path, Epoch(1)).unwrap();
            sessions.begin("s1", &kv, &router).unwrap();
            sessions.read("s1", &kv, b"account/a".to_vec()).unwrap();
            sessions
                .stage_set("s1", b"account/b".to_vec(), b"closed".to_vec())
                .unwrap();
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            let mut transactions =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).unwrap();
            let read = transactions.begin(&router);
            let prepared = transactions
                .prepare(
                    read,
                    rsf_core::transaction::CommitIntent {
                        id: TransactionId([91; 16]),
                        read_conflicts: vec![exact_key_range(b"account/b")],
                        write_conflicts: vec![exact_key_range(b"account/a")],
                        mutations: vec![Mutation::Set {
                            key: b"account/a".to_vec(),
                            value: b"closed".to_vec(),
                        }],
                    },
                )
                .unwrap();
            transactions
                .commit(&mut router, PlacementGroupId(1), prepared)
                .unwrap();

            let mut sessions =
                DurableSessionTransactionEngine::open(&session_path, Epoch(1)).unwrap();
            let err = sessions
                .commit_with_coordinator(
                    "s1",
                    &mut kv,
                    &mut router,
                    PlacementGroupId(1),
                    &mut transactions,
                )
                .expect_err("recovered session read set must reject write skew");

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(err.message.contains("serialization cycle"));
            assert_eq!(router.latest_version(), CommitVersion(3));
            assert_eq!(transactions.committed().len(), 1);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(session_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn non_overlay_read_uses_begin_snapshot() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"old".to_vec(),
        )
        .unwrap();
        let mut sessions = SessionTransactionEngine::default();
        sessions.begin("s1", &kv, &router).unwrap();
        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"new".to_vec(),
        )
        .unwrap();

        let read = sessions.read("s1", &kv, b"user/1".to_vec()).unwrap();

        assert!(matches!(
            read,
            SessionTransactionDecision::SnapshotRead {
                value: Some(value),
                ..
            } if value == b"old".to_vec()
        ));
    }

    #[test]
    fn unknown_session_is_rejected() {
        let router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        assert!(sessions.begin("s1", &kv, &router).is_ok());
        assert!(sessions.begin("s1", &kv, &router).is_err());
        assert!(sessions.read("missing", &kv, b"user/1".to_vec()).is_err());
        assert_eq!(sessions.report().rejections, 2);
    }

    #[test]
    fn pending_clear_masks_snapshot_value_until_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"cart/1".to_vec(),
            b"present".to_vec(),
        )
        .unwrap();
        let mut sessions = SessionTransactionEngine::default();

        sessions.begin("s1", &kv, &router).unwrap();
        sessions.stage_clear("s1", b"cart/1".to_vec()).unwrap();
        let read = sessions.read("s1", &kv, b"cart/1".to_vec()).unwrap();

        assert!(matches!(
            read,
            SessionTransactionDecision::OverlayRead { value: None, .. }
        ));
        sessions
            .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
            .unwrap();
        assert!(kv.get(b"cart/1").is_none());
    }

    #[test]
    fn rollback_discards_pending_writes_without_global_visibility() {
        let router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        sessions.begin("s1", &kv, &router).unwrap();
        sessions
            .stage_set("s1", b"user/2".to_vec(), b"pending".to_vec())
            .unwrap();
        assert!(matches!(
            sessions.read("s1", &kv, b"user/2".to_vec()).unwrap(),
            SessionTransactionDecision::OverlayRead {
                value: Some(value),
                ..
            } if value == b"pending".to_vec()
        ));

        let rollback = sessions.rollback("s1").unwrap();

        assert!(matches!(
            rollback,
            SessionTransactionDecision::RolledBack {
                discarded_writes: 1,
                ..
            }
        ));
        assert!(kv.get(b"user/2").is_none());
        assert!(sessions.read("s1", &kv, b"user/2".to_vec()).is_err());
        assert_eq!(sessions.report().rollbacks, 1);
    }

    #[test]
    fn rollback_to_savepoint_discards_only_later_pending_writes() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        sessions.begin("s1", &kv, &router).unwrap();
        sessions
            .stage_set("s1", b"user/1".to_vec(), b"keep".to_vec())
            .unwrap();
        let savepoint = sessions.savepoint("s1", "after_keep").unwrap();
        assert!(matches!(
            savepoint,
            SessionTransactionDecision::SavepointCreated {
                pending_writes: 1,
                ..
            }
        ));

        sessions
            .stage_set("s1", b"user/2".to_vec(), b"discard".to_vec())
            .unwrap();
        let rewind = sessions.rollback_to_savepoint("s1", "after_keep").unwrap();
        assert!(matches!(
            rewind,
            SessionTransactionDecision::SavepointRolledBack {
                rewound_writes: 1,
                pending_writes: 1,
                ..
            }
        ));

        let keep = sessions.read("s1", &kv, b"user/1".to_vec()).unwrap();
        assert!(matches!(
            keep,
            SessionTransactionDecision::OverlayRead {
                value: Some(value),
                ..
            } if value == b"keep".to_vec()
        ));
        let discard = sessions.read("s1", &kv, b"user/2".to_vec()).unwrap();
        assert!(matches!(
            discard,
            SessionTransactionDecision::SnapshotRead { value: None, .. }
        ));

        sessions
            .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
            .unwrap();
        assert!(kv.get(b"user/1").is_some());
        assert!(kv.get(b"user/2").is_none());
        assert_eq!(sessions.report().savepoints, 1);
        assert_eq!(sessions.report().savepoint_rewinds, 1);
    }

    #[test]
    fn durable_session_overlay_resumes_after_restart() {
        let path = test_path("rsf-sessiontxn-durable-overlay");
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"old".to_vec(),
        )
        .unwrap();

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            sessions.begin("s1", &kv, &router).unwrap();
            sessions
                .stage_set("s1", b"user/1".to_vec(), b"new".to_vec())
                .unwrap();
        }

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            let read = sessions.read("s1", &kv, b"user/1".to_vec()).unwrap();
            assert!(matches!(
                read,
                SessionTransactionDecision::OverlayRead {
                    value: Some(value),
                    ..
                } if value == b"new".to_vec()
            ));
            sessions
                .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
                .unwrap();
            assert_eq!(kv.get(b"user/1").unwrap().value, b"new".to_vec());
        }

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            assert!(sessions.read("s1", &kv, b"user/1".to_vec()).is_err());
            assert_eq!(sessions.report().commits, 1);
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_session_savepoint_rollback_survives_restart() {
        let path = test_path("rsf-sessiontxn-durable-savepoint");
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            sessions.begin("s1", &kv, &router).unwrap();
            sessions
                .stage_set("s1", b"user/1".to_vec(), b"keep".to_vec())
                .unwrap();
            sessions.savepoint("s1", "after_keep").unwrap();
            sessions
                .stage_set("s1", b"user/2".to_vec(), b"discard".to_vec())
                .unwrap();
        }

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            let keep = sessions.read("s1", &kv, b"user/1".to_vec()).unwrap();
            assert!(matches!(
                keep,
                SessionTransactionDecision::OverlayRead {
                    value: Some(value),
                    ..
                } if value == b"keep".to_vec()
            ));
            let discard_before = sessions.read("s1", &kv, b"user/2".to_vec()).unwrap();
            assert!(matches!(
                discard_before,
                SessionTransactionDecision::OverlayRead {
                    value: Some(value),
                    ..
                } if value == b"discard".to_vec()
            ));

            let rewind = sessions
                .rollback_to_savepoint("s1", "after_keep")
                .expect("rollback to recovered savepoint");
            assert!(matches!(
                rewind,
                SessionTransactionDecision::SavepointRolledBack {
                    rewound_writes: 1,
                    pending_writes: 1,
                    ..
                }
            ));
        }

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            let discard_after = sessions.read("s1", &kv, b"user/2".to_vec()).unwrap();
            assert!(matches!(
                discard_after,
                SessionTransactionDecision::SnapshotRead { value: None, .. }
            ));
            sessions
                .commit("s1", &mut kv, &mut router, PlacementGroupId(1))
                .unwrap();
            assert!(kv.get(b"user/1").is_some());
            assert!(kv.get(b"user/2").is_none());
            assert_eq!(sessions.report().savepoint_rewinds, 1);
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_session_rollback_removes_recovered_overlay() {
        let path = test_path("rsf-sessiontxn-durable-rollback");
        let router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let kv = KvEngine::default();

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            sessions.begin("s1", &kv, &router).unwrap();
            sessions
                .stage_set("s1", b"user/1".to_vec(), b"pending".to_vec())
                .unwrap();
            sessions.rollback("s1").unwrap();
        }

        {
            let mut sessions = DurableSessionTransactionEngine::open(&path, Epoch(1)).unwrap();
            assert!(sessions.read("s1", &kv, b"user/1".to_vec()).is_err());
            assert_eq!(sessions.report().rollbacks, 1);
        }

        let _ = std::fs::remove_dir_all(path);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock moves forward")
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{}-{unique}", std::process::id()))
    }
}
