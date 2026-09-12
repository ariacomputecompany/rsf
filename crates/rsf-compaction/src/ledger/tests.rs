use std::path::PathBuf;

use rsf_core::{ErrorCode, PlacementGroupId};
use rsf_range::RangeId;

use super::*;
use crate::{CompactionEntry, CompactionExecutor, CompactionRun};

#[test]
fn durable_compaction_recovers_completed_output_after_restart() {
    let path = test_path("completed_output_after_restart");
    let (plan, runs) = fixture();
    let output = CompactionExecutor {
        gc_before: CommitVersion(21),
    }
    .compact(&plan, &runs)
    .expect("compact");

    {
        let mut ledger = DurableCompactionLedger::open(&path, Epoch(8)).expect("open");
        let id = ledger.record_plan(plan.clone()).expect("plan");
        ledger.mark_started(id).expect("start");
        ledger.mark_completed(id, output.clone()).expect("complete");
    }

    let recovered = DurableCompactionLedger::open(&path, Epoch(8)).expect("recover");
    let operation = recovered
        .operation(CompactionOperationId(1))
        .expect("operation");
    assert_eq!(operation.state, CompactionOperationState::Completed);
    assert_eq!(operation.plan, plan);
    assert_eq!(operation.output, Some(output));
    assert!(recovered.pending().is_empty());
}

#[test]
fn durable_compaction_recovers_started_operation_as_pending() {
    let path = test_path("started_operation_as_pending");
    let (plan, _) = fixture();

    {
        let mut ledger = DurableCompactionLedger::open(&path, Epoch(8)).expect("open");
        let id = ledger.record_plan(plan.clone()).expect("plan");
        ledger.mark_started(id).expect("start");
    }

    let recovered = DurableCompactionLedger::open(&path, Epoch(8)).expect("recover");
    let pending = recovered.pending();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].state, CompactionOperationState::Started);
    assert_eq!(pending[0].plan, plan);
}

#[test]
fn durable_compaction_rejects_completion_that_does_not_match_plan() {
    let path = test_path("rejects_bad_completion");
    let (plan, runs) = fixture();
    let mut output = CompactionExecutor {
        gc_before: CommitVersion(21),
    }
    .compact(&plan, &runs)
    .expect("compact");
    output.manifest.level = plan.output_level.saturating_add(1);

    let mut ledger = DurableCompactionLedger::open(&path, Epoch(8)).expect("open");
    let id = ledger.record_plan(plan).expect("plan");
    ledger.mark_started(id).expect("start");
    let err = ledger
        .mark_completed(id, output)
        .expect_err("bad output rejected");

    assert_eq!(err.code, ErrorCode::InvalidInput);
}

#[test]
fn compaction_wal_decode_rejects_trailing_bytes() {
    let (plan, _) = fixture();
    let mut payload = encode_compaction_wal_payload(&CompactionWalRecord::Planned {
        id: CompactionOperationId(1),
        plan,
    })
    .expect("encode");
    payload.push(99);

    let err = decode_compaction_wal_payload(&payload).expect_err("trailing bytes rejected");
    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

#[test]
fn durable_compaction_replay_rejects_completion_without_start() {
    let path = test_path("completion_without_start");
    let (plan, runs) = fixture();
    let output = CompactionExecutor {
        gc_before: CommitVersion(21),
    }
    .compact(&plan, &runs)
    .expect("compact");
    append_raw(
        &path,
        &CompactionWalRecord::Planned {
            id: CompactionOperationId(1),
            plan,
        },
        1,
    );
    append_raw(
        &path,
        &CompactionWalRecord::Completed {
            id: CompactionOperationId(1),
            output,
        },
        2,
    );

    let err = DurableCompactionLedger::open(&path, Epoch(8))
        .expect_err("completion without start must fail closed");
    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

fn fixture() -> (CompactionPlan, Vec<CompactionRun>) {
    let old = manifest(1, b"app/users/1", b"app/users/9", 21, 160);
    let new = manifest(2, b"app/users/1", b"app/users/9", 22, 224);
    (
        CompactionPlan {
            range: RangeId(7),
            inputs: vec![old.clone(), new.clone()],
            output_level: 1,
            estimated_bytes: 384,
        },
        vec![
            CompactionRun {
                manifest: old,
                entries: vec![
                    entry(b"app/users/1", Some(b"ada"), 21),
                    entry(b"app/users/2", Some(b"grace"), 21),
                ],
            },
            CompactionRun {
                manifest: new,
                entries: vec![
                    entry(b"app/users/1", Some(b"ada-lovelace"), 22),
                    entry(b"app/users/2", None, 22),
                ],
            },
        ],
    )
}

fn manifest(id: u8, first: &[u8], last: &[u8], version: u64, bytes: u64) -> SstableManifest {
    SstableManifest {
        range: RangeId(7),
        level: 0,
        first_key: first.to_vec(),
        last_key: last.to_vec(),
        version: CommitVersion(version),
        bytes,
        checksum: Digest::blake3_bytes(
            format!(
                "durable-compaction-input:{id}:{}:{}:{version}:{bytes}",
                String::from_utf8_lossy(first),
                String::from_utf8_lossy(last)
            )
            .as_bytes(),
        ),
    }
}

fn entry(key: &[u8], value: Option<&[u8]>, version: u64) -> CompactionEntry {
    CompactionEntry {
        key: key.to_vec(),
        value: value.map(|value| value.to_vec()),
        version: CommitVersion(version),
    }
}

fn append_raw(path: &PathBuf, record: &CompactionWalRecord, index: u64) {
    let mut store = SegmentStore::open(path).expect("store");
    let payload = encode_compaction_wal_payload(record).expect("encode");
    let recovery = store.recover_current().expect("recover").recovery;
    let tail = recovery.pg_tail(PlacementGroupId(56));
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    store
        .append_wal(WalAppend {
            pg: PlacementGroupId(56),
            epoch: Epoch(8),
            index: LogIndex(index),
            kind: WalRecordKind::Compaction,
            previous_hash,
            payload: &payload,
        })
        .expect("append raw compaction wal");
}

fn test_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rsf-compaction-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}
