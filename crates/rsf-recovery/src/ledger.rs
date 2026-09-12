use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{
    DatabaseRestartCertificate, DatabaseRestartPlan, RecoveryPlanner, RecoveryRoot,
    RecoveryRootKind,
};

const RECOVERY_PG: PlacementGroupId = PlacementGroupId(61);
const RECOVERY_WAL_MAGIC: &[u8; 7] = b"RSFRCV1";
const RECOVERY_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecoveryOperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryOperationState {
    Planned,
    Certified,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryOperation {
    pub id: RecoveryOperationId,
    pub plan: DatabaseRestartPlan,
    pub state: RecoveryOperationState,
    pub certificate: Option<DatabaseRestartCertificate>,
    pub failure: Option<String>,
}

#[derive(Debug)]
pub struct DurableRecoveryLedger {
    operations: BTreeMap<RecoveryOperationId, RecoveryOperation>,
    store: SegmentStore,
    epoch: Epoch,
    next_id: u64,
    next_index: u64,
}

impl DurableRecoveryLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(RECOVERY_PG),
        })?;
        let mut operations = BTreeMap::new();
        let mut next_id = 1;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Recovery)
        {
            apply_recovery_record(
                &mut operations,
                decode_recovery_wal_payload(&record.payload)?,
            )?;
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

    pub fn record_plan(&mut self, plan: DatabaseRestartPlan) -> RsfResult<RecoveryOperationId> {
        validate_restart_plan(&plan)?;
        let id = RecoveryOperationId(self.next_id);
        self.append_record(&RecoveryWalRecord::Planned {
            id,
            checkpoint: plan.checkpoint,
            roots: plan.recovery.roots.clone(),
        })?;
        self.operations.insert(
            id,
            RecoveryOperation {
                id,
                plan,
                state: RecoveryOperationState::Planned,
                certificate: None,
                failure: None,
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
    }

    pub fn mark_certified(
        &mut self,
        id: RecoveryOperationId,
        certificate: DatabaseRestartCertificate,
    ) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != RecoveryOperationState::Planned {
            return Err(invalid(format!(
                "recovery operation {} cannot certify from {:?}",
                id.0, operation.state
            )));
        }
        validate_certificate_matches_plan(&operation.plan, &certificate)?;
        operation.state = RecoveryOperationState::Certified;
        operation.certificate = Some(certificate.clone());
        operation.failure = None;
        self.append_record(&RecoveryWalRecord::Certified { id, certificate })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        id: RecoveryOperationId,
        reason: impl Into<String>,
    ) -> RsfResult<()> {
        let reason = reason.into();
        validate_failure(&reason)?;
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state == RecoveryOperationState::Certified {
            return Err(invalid(format!(
                "recovery operation {} cannot fail after certification",
                id.0
            )));
        }
        operation.state = RecoveryOperationState::Failed;
        operation.failure = Some(reason.clone());
        operation.certificate = None;
        self.append_record(&RecoveryWalRecord::Failed { id, reason })?;
        self.operations = operations;
        Ok(())
    }

    pub fn operation(&self, id: RecoveryOperationId) -> Option<&RecoveryOperation> {
        self.operations.get(&id)
    }

    pub fn operations(&self) -> Vec<RecoveryOperation> {
        self.operations.values().cloned().collect()
    }

    pub fn pending(&self) -> Vec<RecoveryOperation> {
        self.operations
            .values()
            .filter(|operation| operation.state == RecoveryOperationState::Planned)
            .cloned()
            .collect()
    }

    fn append_record(&mut self, record: &RecoveryWalRecord) -> RsfResult<()> {
        let payload = encode_recovery_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let previous_hash = recovery
            .pg_tail(RECOVERY_PG)
            .map_or([0_u8; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: RECOVERY_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::Recovery,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RecoveryWalRecord {
    Planned {
        id: RecoveryOperationId,
        checkpoint: CommitVersion,
        roots: Vec<RecoveryRoot>,
    },
    Certified {
        id: RecoveryOperationId,
        certificate: DatabaseRestartCertificate,
    },
    Failed {
        id: RecoveryOperationId,
        reason: String,
    },
}

fn apply_recovery_record(
    operations: &mut BTreeMap<RecoveryOperationId, RecoveryOperation>,
    record: RecoveryWalRecord,
) -> RsfResult<()> {
    match record {
        RecoveryWalRecord::Planned {
            id,
            checkpoint,
            roots,
        } => {
            if operations.contains_key(&id) {
                return Err(corrupt(format!(
                    "duplicate recovery operation id {} in WAL",
                    id.0
                )));
            }
            let plan = RecoveryPlanner.plan_restart(checkpoint, roots)?;
            validate_restart_plan(&plan)?;
            operations.insert(
                id,
                RecoveryOperation {
                    id,
                    plan,
                    state: RecoveryOperationState::Planned,
                    certificate: None,
                    failure: None,
                },
            );
        }
        RecoveryWalRecord::Certified { id, certificate } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != RecoveryOperationState::Planned {
                return Err(corrupt(format!(
                    "recovery operation {} cannot replay certification from {:?}",
                    id.0, operation.state
                )));
            }
            validate_certificate_matches_plan(&operation.plan, &certificate)?;
            operation.state = RecoveryOperationState::Certified;
            operation.certificate = Some(certificate);
            operation.failure = None;
        }
        RecoveryWalRecord::Failed { id, reason } => {
            validate_failure(&reason)?;
            let operation = operation_mut(operations, id)?;
            if operation.state == RecoveryOperationState::Certified {
                return Err(corrupt(format!(
                    "recovery operation {} cannot replay failure after certification",
                    id.0
                )));
            }
            operation.state = RecoveryOperationState::Failed;
            operation.failure = Some(reason);
            operation.certificate = None;
        }
    }
    Ok(())
}

fn validate_restart_plan(plan: &DatabaseRestartPlan) -> RsfResult<()> {
    let recomputed = RecoveryPlanner.plan_restart(plan.checkpoint, plan.recovery.roots.clone())?;
    if &recomputed != plan {
        return Err(invalid(
            "database restart plan does not match recoverable root inventory",
        ));
    }
    if plan.steps.is_empty() {
        return Err(invalid("database restart plan must include steps"));
    }
    Ok(())
}

fn validate_certificate_matches_plan(
    plan: &DatabaseRestartPlan,
    certificate: &DatabaseRestartCertificate,
) -> RsfResult<()> {
    if !plan.opens_for_traffic {
        return Err(invalid(
            "database restart certificate requires a traffic-openable plan",
        ));
    }
    if certificate.checkpoint != plan.checkpoint {
        return Err(invalid(
            "restart certificate checkpoint does not match plan",
        ));
    }
    if certificate.snapshot_id.trim().is_empty() {
        return Err(invalid("restart certificate snapshot id must not be empty"));
    }
    if certificate.restore_manifest_id.trim().is_empty() {
        return Err(invalid(
            "restart certificate restore manifest id must not be empty",
        ));
    }
    if certificate.restored_items == 0 {
        return Err(invalid(
            "restart certificate must prove at least one restored item",
        ));
    }
    if certificate.restart_steps != plan.step_count() {
        return Err(invalid(
            "restart certificate step count does not match plan",
        ));
    }
    if certificate.replay_roots != plan.replay_roots {
        return Err(invalid(
            "restart certificate replay count does not match plan",
        ));
    }
    if certificate.reusable_roots != plan.reusable_roots {
        return Err(invalid(
            "restart certificate reusable count does not match plan",
        ));
    }
    Ok(())
}

fn operation_mut(
    operations: &mut BTreeMap<RecoveryOperationId, RecoveryOperation>,
    id: RecoveryOperationId,
) -> RsfResult<&mut RecoveryOperation> {
    operations
        .get_mut(&id)
        .ok_or_else(|| invalid(format!("unknown recovery operation {}", id.0)))
}

fn validate_failure(reason: &str) -> RsfResult<()> {
    if reason.trim().is_empty() {
        return Err(invalid("recovery failure reason must not be empty"));
    }
    Ok(())
}

fn encode_recovery_wal_payload(record: &RecoveryWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(RECOVERY_WAL_MAGIC);
    out.push(RECOVERY_WAL_VERSION);
    match record {
        RecoveryWalRecord::Planned {
            id,
            checkpoint,
            roots,
        } => {
            let plan = RecoveryPlanner.plan_restart(*checkpoint, roots.clone())?;
            validate_restart_plan(&plan)?;
            out.push(1);
            write_u64(&mut out, id.0);
            write_u64(&mut out, checkpoint.0);
            write_len(&mut out, roots.len())?;
            for root in roots {
                encode_root(&mut out, root)?;
            }
        }
        RecoveryWalRecord::Certified { id, certificate } => {
            out.push(2);
            write_u64(&mut out, id.0);
            encode_certificate(&mut out, certificate)?;
        }
        RecoveryWalRecord::Failed { id, reason } => {
            validate_failure(reason)?;
            out.push(3);
            write_u64(&mut out, id.0);
            write_string(&mut out, reason)?;
        }
    }
    Ok(out)
}

fn decode_recovery_wal_payload(bytes: &[u8]) -> RsfResult<RecoveryWalRecord> {
    if bytes.len() < RECOVERY_WAL_MAGIC.len() + 2 {
        return Err(corrupt("recovery WAL payload is too short"));
    }
    if &bytes[..RECOVERY_WAL_MAGIC.len()] != RECOVERY_WAL_MAGIC {
        return Err(corrupt("recovery WAL magic mismatch"));
    }
    let mut offset = RECOVERY_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != RECOVERY_WAL_VERSION {
        return Err(corrupt("unsupported recovery WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let id = RecoveryOperationId(read_u64(bytes, &mut offset)?);
    let record = match tag {
        1 => {
            let checkpoint = CommitVersion(read_u64(bytes, &mut offset)?);
            let root_len = read_len(bytes, &mut offset)?;
            let mut roots = Vec::with_capacity(root_len);
            for _ in 0..root_len {
                roots.push(decode_root(bytes, &mut offset)?);
            }
            RecoveryWalRecord::Planned {
                id,
                checkpoint,
                roots,
            }
        }
        2 => RecoveryWalRecord::Certified {
            id,
            certificate: decode_certificate(bytes, &mut offset)?,
        },
        3 => RecoveryWalRecord::Failed {
            id,
            reason: read_string(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("unknown recovery WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in recovery WAL payload"));
    }
    Ok(record)
}

fn encode_root(out: &mut Vec<u8>, root: &RecoveryRoot) -> RsfResult<()> {
    out.push(recovery_root_kind_tag(root.kind));
    write_string(out, &root.namespace)?;
    write_string(out, &root.name)?;
    write_u64(out, root.durable_version.0);
    write_bool(out, root.present);
    write_bool(out, root.checksum_ok);
    Ok(())
}

fn decode_root(bytes: &[u8], offset: &mut usize) -> RsfResult<RecoveryRoot> {
    let kind = recovery_root_kind_from_tag(read_u8(bytes, offset)?)?;
    Ok(RecoveryRoot {
        kind,
        namespace: read_string(bytes, offset)?,
        name: read_string(bytes, offset)?,
        durable_version: CommitVersion(read_u64(bytes, offset)?),
        present: read_bool(bytes, offset)?,
        checksum_ok: read_bool(bytes, offset)?,
    })
}

fn encode_certificate(
    out: &mut Vec<u8>,
    certificate: &DatabaseRestartCertificate,
) -> RsfResult<()> {
    write_u64(out, certificate.checkpoint.0);
    write_string(out, &certificate.snapshot_id)?;
    write_string(out, &certificate.restore_manifest_id)?;
    write_len(out, certificate.restored_items)?;
    write_len(out, certificate.restart_steps)?;
    write_len(out, certificate.replay_roots)?;
    write_len(out, certificate.reusable_roots)?;
    Ok(())
}

fn decode_certificate(bytes: &[u8], offset: &mut usize) -> RsfResult<DatabaseRestartCertificate> {
    Ok(DatabaseRestartCertificate {
        checkpoint: CommitVersion(read_u64(bytes, offset)?),
        snapshot_id: read_string(bytes, offset)?,
        restore_manifest_id: read_string(bytes, offset)?,
        restored_items: read_len(bytes, offset)?,
        restart_steps: read_len(bytes, offset)?,
        replay_roots: read_len(bytes, offset)?,
        reusable_roots: read_len(bytes, offset)?,
    })
}

fn recovery_root_kind_tag(kind: RecoveryRootKind) -> u8 {
    match kind {
        RecoveryRootKind::Catalog => 1,
        RecoveryRootKind::TransactionLog => 2,
        RecoveryRootKind::Kv => 3,
        RecoveryRootKind::Sql => 4,
        RecoveryRootKind::Object => 5,
        RecoveryRootKind::Blob => 6,
        RecoveryRootKind::ChunkIndex => 7,
    }
}

fn recovery_root_kind_from_tag(tag: u8) -> RsfResult<RecoveryRootKind> {
    match tag {
        1 => Ok(RecoveryRootKind::Catalog),
        2 => Ok(RecoveryRootKind::TransactionLog),
        3 => Ok(RecoveryRootKind::Kv),
        4 => Ok(RecoveryRootKind::Sql),
        5 => Ok(RecoveryRootKind::Object),
        6 => Ok(RecoveryRootKind::Blob),
        7 => Ok(RecoveryRootKind::ChunkIndex),
        _ => Err(corrupt("unknown recovery root kind tag")),
    }
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> RsfResult<bool> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(corrupt("invalid boolean tag in recovery WAL payload")),
    }
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("unexpected end of recovery WAL payload"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let end = offset.saturating_add(4);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of recovery WAL payload"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("slice length already checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let end = offset.saturating_add(8);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of recovery WAL payload"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("slice length already checked"),
    ))
}

fn write_len(out: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| invalid("recovery WAL collection length exceeds encoding limit"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    usize::try_from(read_u32(bytes, offset)?)
        .map_err(|_| corrupt("recovery WAL collection length is unsupported"))
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    write_len(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_len(bytes, offset)?;
    let end = offset.saturating_add(len);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of recovery WAL string"))?;
    *offset = end;
    String::from_utf8(slice.to_vec()).map_err(|_| corrupt("recovery WAL string is not valid UTF-8"))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_storage::SegmentStore;

    #[test]
    fn durable_recovery_ledger_recovers_certified_restart_after_reopen() {
        let path = test_path("recovers_certified_restart_after_reopen");
        let plan = restart_plan(CommitVersion(20));
        let certificate = certificate_for_plan(&plan);
        let id;
        {
            let mut ledger = DurableRecoveryLedger::open(&path, Epoch(4)).expect("ledger");
            id = ledger.record_plan(plan.clone()).expect("record plan");
            ledger
                .mark_certified(id, certificate.clone())
                .expect("certify");
        }

        let reopened = DurableRecoveryLedger::open(&path, Epoch(4)).expect("reopen");
        let operation = reopened.operation(id).expect("operation");

        assert_eq!(operation.state, RecoveryOperationState::Certified);
        assert_eq!(operation.plan, plan);
        assert_eq!(operation.certificate.as_ref(), Some(&certificate));
        assert!(operation.failure.is_none());
        assert!(reopened.pending().is_empty());
    }

    #[test]
    fn durable_recovery_ledger_rejected_certificate_does_not_persist() {
        let path = test_path("rejected_certificate_does_not_persist");
        let plan = restart_plan(CommitVersion(20));
        let mut certificate = certificate_for_plan(&plan);
        certificate.replay_roots += 1;
        let id;
        {
            let mut ledger = DurableRecoveryLedger::open(&path, Epoch(4)).expect("ledger");
            id = ledger.record_plan(plan.clone()).expect("record plan");
            let err = ledger
                .mark_certified(id, certificate)
                .expect_err("mismatched certificate rejected");
            assert_eq!(err.code, ErrorCode::InvalidInput);
        }

        let reopened = DurableRecoveryLedger::open(&path, Epoch(4)).expect("reopen");
        let operation = reopened.operation(id).expect("operation");

        assert_eq!(operation.state, RecoveryOperationState::Planned);
        assert!(operation.certificate.is_none());
        assert_eq!(reopened.pending().len(), 1);
    }

    #[test]
    fn durable_recovery_ledger_recovers_failed_restart_after_reopen() {
        let path = test_path("recovers_failed_restart_after_reopen");
        let plan = restart_plan(CommitVersion(20));
        let id;
        {
            let mut ledger = DurableRecoveryLedger::open(&path, Epoch(4)).expect("ledger");
            id = ledger.record_plan(plan).expect("record plan");
            ledger
                .mark_failed(id, "operator aborted corrupted restore media")
                .expect("fail");
        }

        let reopened = DurableRecoveryLedger::open(&path, Epoch(4)).expect("reopen");
        let operation = reopened.operation(id).expect("operation");

        assert_eq!(operation.state, RecoveryOperationState::Failed);
        assert_eq!(
            operation.failure.as_deref(),
            Some("operator aborted corrupted restore media")
        );
        assert!(operation.certificate.is_none());
    }

    #[test]
    fn durable_recovery_ledger_replay_rejects_malformed_payload() {
        let path = test_path("replay_rejects_malformed_payload");
        append_raw_recovery_record(&path, b"not-a-recovery-payload".to_vec(), 1, [0; 32]);

        let err = DurableRecoveryLedger::open(&path, Epoch(4))
            .expect_err("malformed payload rejected during replay");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("magic"));
    }

    #[test]
    fn durable_recovery_ledger_replay_rejects_certificate_without_plan() {
        let path = test_path("replay_rejects_certificate_without_plan");
        let certificate = certificate_for_plan(&restart_plan(CommitVersion(20)));
        let payload = encode_recovery_wal_payload(&RecoveryWalRecord::Certified {
            id: RecoveryOperationId(9),
            certificate,
        })
        .expect("encode");
        append_raw_recovery_record(&path, payload, 1, [0; 32]);

        let err = DurableRecoveryLedger::open(&path, Epoch(4))
            .expect_err("orphan certification rejected during replay");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("unknown recovery operation"));
    }

    fn restart_plan(checkpoint: CommitVersion) -> DatabaseRestartPlan {
        RecoveryPlanner
            .plan_restart(checkpoint, roots(checkpoint))
            .expect("restart plan")
    }

    fn certificate_for_plan(plan: &DatabaseRestartPlan) -> DatabaseRestartCertificate {
        DatabaseRestartCertificate {
            checkpoint: plan.checkpoint,
            snapshot_id: "snapshot-app-0001".to_string(),
            restore_manifest_id: "snapshot-app-0001".to_string(),
            restored_items: 7,
            restart_steps: plan.step_count(),
            replay_roots: plan.replay_roots,
            reusable_roots: plan.reusable_roots,
        }
    }

    fn roots(checkpoint: CommitVersion) -> Vec<RecoveryRoot> {
        vec![
            root(RecoveryRootKind::Catalog, "catalog/root", CommitVersion(18)),
            root(RecoveryRootKind::TransactionLog, "fabric/wal", checkpoint),
            root(RecoveryRootKind::Kv, "kv/app/config", CommitVersion(19)),
            root(RecoveryRootKind::Sql, "sql/app/users", CommitVersion(19)),
            root(
                RecoveryRootKind::Object,
                "object/media/hero.png",
                CommitVersion(17),
            ),
            root(RecoveryRootKind::Blob, "blob/snapshot-chunk", checkpoint),
            root(RecoveryRootKind::ChunkIndex, "chunk/index/main", checkpoint),
        ]
    }

    fn root(kind: RecoveryRootKind, name: &str, durable_version: CommitVersion) -> RecoveryRoot {
        RecoveryRoot {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            durable_version,
            present: true,
            checksum_ok: true,
        }
    }

    fn append_raw_recovery_record(
        path: &Path,
        payload: Vec<u8>,
        index: u64,
        previous_hash: [u8; 32],
    ) {
        let mut store = SegmentStore::open(path).expect("segment store");
        store
            .append_wal(WalAppend {
                pg: RECOVERY_PG,
                epoch: Epoch(4),
                index: LogIndex(index),
                kind: WalRecordKind::Recovery,
                previous_hash,
                payload: &payload,
            })
            .expect("append raw recovery record");
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-recovery-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
