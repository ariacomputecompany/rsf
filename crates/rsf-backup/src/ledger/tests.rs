use rsf_core::{CommitVersion, Epoch, LogIndex};
use rsf_storage::{SegmentStore, WalAppend};

use super::*;
use crate::{BackupItem, BackupPlanner, SnapshotManifest};

#[test]
fn durable_restore_recovers_completed_execution_after_restart() {
    let path = test_path("durable_restore_recovers_completed_execution_after_restart");
    let (_, plan, execution) = restore_fixture();

    {
        let mut ledger = DurableRestoreLedger::open(&path, Epoch(6)).expect("open ledger");
        let id = ledger.record_plan(plan.clone()).expect("persist plan");
        ledger.mark_started(id).expect("persist start");
        ledger
            .mark_completed(id, execution.clone())
            .expect("persist completion");
    }

    let recovered = DurableRestoreLedger::open(&path, Epoch(6)).expect("recover ledger");
    let operation = recovered
        .operation(RestoreOperationId(1))
        .expect("operation recovered");

    assert_eq!(operation.state, RestoreOperationState::Completed);
    assert_eq!(operation.plan, plan);
    assert_eq!(operation.execution, Some(execution));
    assert!(recovered.pending().is_empty());
}

#[test]
fn durable_restore_recovers_started_operation_as_pending() {
    let path = test_path("durable_restore_recovers_started_operation_as_pending");
    let (_, plan, _) = restore_fixture();
    {
        let mut ledger = DurableRestoreLedger::open(&path, Epoch(6)).expect("open ledger");
        let id = ledger.record_plan(plan).expect("persist plan");
        ledger.mark_started(id).expect("persist start");
    }

    let recovered = DurableRestoreLedger::open(&path, Epoch(6)).expect("recover ledger");

    assert_eq!(recovered.pending().len(), 1);
    assert_eq!(
        recovered
            .operation(RestoreOperationId(1))
            .map(|op| op.state),
        Some(RestoreOperationState::Started)
    );
}

#[test]
fn durable_restore_rejects_execution_that_does_not_cover_plan() {
    let path = test_path("durable_restore_rejects_execution_that_does_not_cover_plan");
    let (_, plan, mut execution) = restore_fixture();
    execution.restored_bytes += 1;
    let mut ledger = DurableRestoreLedger::open(&path, Epoch(6)).expect("open ledger");
    let id = ledger.record_plan(plan).expect("persist plan");
    ledger.mark_started(id).expect("persist start");

    let err = ledger
        .mark_completed(id, execution)
        .expect_err("mismatched execution must reject");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("does not cover"));
}

#[test]
fn backup_wal_decode_rejects_trailing_bytes() {
    let payload = encode_backup_wal_payload(&BackupWalRecord::Started {
        id: RestoreOperationId(9),
    })
    .expect("encode");
    let mut payload = payload;
    payload.push(99);

    let err = decode_backup_wal_payload(&payload).expect_err("trailing bytes reject");

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
    assert!(err.message.contains("trailing bytes"));
}

#[test]
fn durable_restore_replay_rejects_completion_without_start() {
    let path = test_path("durable_restore_replay_rejects_completion_without_start");
    let (_, plan, execution) = restore_fixture();
    let mut store = SegmentStore::open(&path).expect("segment store");
    append_raw(
        &mut store,
        1,
        &BackupWalRecord::Planned {
            id: RestoreOperationId(1),
            plan,
        },
    );
    append_raw(
        &mut store,
        2,
        &BackupWalRecord::Completed {
            id: RestoreOperationId(1),
            execution,
        },
    );

    let err = DurableRestoreLedger::open(&path, Epoch(6))
        .expect_err("replay must fail closed for invalid lifecycle");

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
    assert!(err.message.contains("cannot replay completion"));
}

fn restore_fixture() -> (SnapshotManifest, RestorePlan, RestoreExecution) {
    let planner = BackupPlanner;
    let manifest = planner
        .create_manifest("snapshot-1", CommitVersion(10), 1, full_items())
        .expect("manifest");
    let plan = planner.plan_restore(&manifest).expect("restore plan");
    let execution = planner
        .execute_restore(&manifest, &plan)
        .expect("restore execution");
    (manifest, plan, execution)
}

fn full_items() -> Vec<BackupItem> {
    vec![
        item(BackupItemKind::CatalogRoot, "system", "catalog", 10, 128),
        item(BackupItemKind::TransactionLog, "system", "txlog", 10, 256),
        item(BackupItemKind::KvRange, "app", "range-a", 9, 512),
        item(BackupItemKind::SqlTable, "app", "users", 9, 128),
        item(BackupItemKind::ObjectManifest, "app", "objects", 9, 64),
        item(BackupItemKind::BlobChunk, "app", "chunk-a", 8, 256),
        item(BackupItemKind::ChunkIndex, "app", "chunk-index", 10, 32),
    ]
}

fn item(kind: BackupItemKind, namespace: &str, name: &str, version: u64, bytes: u64) -> BackupItem {
    BackupItem {
        kind,
        namespace: namespace.to_string(),
        name: name.to_string(),
        version: CommitVersion(version),
        checksum: Digest::blake3_bytes(format!("backup:{name}:{version}:{bytes}").as_bytes()),
        bytes,
    }
}

fn append_raw(store: &mut SegmentStore, index: u64, record: &BackupWalRecord) {
    let payload = encode_backup_wal_payload(record).expect("encode backup record");
    let recovery = store.recover_current().expect("recover").recovery;
    let tail = recovery.pg_tail(BACKUP_PG);
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    store
        .append_wal(WalAppend {
            pg: BACKUP_PG,
            epoch: Epoch(6),
            index: LogIndex(index),
            kind: WalRecordKind::Backup,
            previous_hash,
            payload: &payload,
        })
        .expect("append raw backup record");
}

fn test_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rsf-backup-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}
