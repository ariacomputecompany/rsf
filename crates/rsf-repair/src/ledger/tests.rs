use rsf_core::{
    Digest, Epoch,
    placement::{ActingSet, PlacementGroup, PlacementPolicy},
};
use rsf_storage::{SegmentStore, WalAppend};

use super::*;
use crate::{RepairPlanner, ReplicaState};

fn group() -> PlacementGroup {
    PlacementGroup {
        id: PlacementGroupId(7),
        acting_set: ActingSet {
            epoch: Epoch(3),
            primary: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        },
        policy: PlacementPolicy::Replicated { replicas: 3 },
    }
}

fn healthy(node: u64) -> ReplicaState {
    ReplicaState {
        node: NodeId(node),
        present: true,
        digest_ok: true,
        lag_bytes: 0,
        durable_bytes: 4096,
        checksum: Digest::blake3_bytes(format!("replica-{node}").as_bytes()),
    }
}

#[test]
fn durable_repair_recovers_completed_copy_after_restart() {
    let path = test_path("durable_repair_recovers_completed_copy_after_restart");
    let planner = RepairPlanner::default();
    let action = RepairAction::CopyReplica {
        pg: PlacementGroupId(7),
        from: NodeId(1),
        to: NodeId(3),
    };
    let result = planner
        .execute_copy(&group(), &[healthy(1), healthy(2)], &action)
        .expect("copy result");

    {
        let mut ledger = DurableRepairLedger::open(&path, Epoch(4)).expect("open ledger");
        let id = ledger.record_plan(action).expect("persist plan");
        ledger.mark_started(id).expect("persist start");
        ledger
            .mark_completed(id, RepairCompletion::Copy(result.clone()))
            .expect("persist completion");
    }

    let recovered = DurableRepairLedger::open(&path, Epoch(4)).expect("recover ledger");
    let operation = recovered
        .operation(RepairOperationId(1))
        .expect("operation recovered");
    assert_eq!(operation.state, RepairOperationState::Completed);
    assert_eq!(operation.completion, Some(RepairCompletion::Copy(result)));
    assert!(recovered.pending().is_empty());
}

#[test]
fn durable_repair_recovers_started_operation_as_pending() {
    let path = test_path("durable_repair_recovers_started_operation_as_pending");
    {
        let mut ledger = DurableRepairLedger::open(&path, Epoch(4)).expect("open ledger");
        let id = ledger
            .record_plan(RepairAction::DemoteSlowReplica {
                pg: PlacementGroupId(7),
                node: NodeId(2),
            })
            .expect("persist plan");
        ledger.mark_started(id).expect("persist start");
    }

    let recovered = DurableRepairLedger::open(&path, Epoch(4)).expect("recover ledger");
    assert_eq!(recovered.pending().len(), 1);
    assert_eq!(
        recovered.operation(RepairOperationId(1)).map(|op| op.state),
        Some(RepairOperationState::Started)
    );
}

#[test]
fn durable_repair_rejects_mismatched_completion() {
    let path = test_path("durable_repair_rejects_mismatched_completion");
    let mut ledger = DurableRepairLedger::open(&path, Epoch(4)).expect("open ledger");
    let id = ledger
        .record_plan(RepairAction::RecheckDigest {
            pg: PlacementGroupId(7),
            node: NodeId(2),
        })
        .expect("persist plan");
    ledger.mark_started(id).expect("persist start");

    let err = ledger
        .mark_completed(
            id,
            RepairCompletion::DemoteSlowReplica {
                pg: PlacementGroupId(7),
                node: NodeId(2),
            },
        )
        .expect_err("wrong completion kind must reject");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("does not match"));
}

#[test]
fn repair_wal_decode_rejects_trailing_bytes() {
    let payload = encode_repair_wal_payload(&RepairWalRecord::Started {
        id: RepairOperationId(9),
    })
    .expect("encode");
    let mut payload = payload;
    payload.push(99);

    let err = decode_repair_wal_payload(&payload).expect_err("trailing bytes reject");

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
    assert!(err.message.contains("trailing bytes"));
}

#[test]
fn durable_repair_replay_rejects_completion_without_start() {
    let path = test_path("durable_repair_replay_rejects_completion_without_start");
    let mut store = SegmentStore::open(&path).expect("segment store");
    append_raw(
        &mut store,
        1,
        &RepairWalRecord::Planned {
            id: RepairOperationId(1),
            action: RepairAction::DemoteSlowReplica {
                pg: PlacementGroupId(7),
                node: NodeId(2),
            },
        },
    );
    append_raw(
        &mut store,
        2,
        &RepairWalRecord::Completed {
            id: RepairOperationId(1),
            completion: RepairCompletion::DemoteSlowReplica {
                pg: PlacementGroupId(7),
                node: NodeId(2),
            },
        },
    );

    let err = DurableRepairLedger::open(&path, Epoch(4))
        .expect_err("replay must fail closed for invalid lifecycle");

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
    assert!(err.message.contains("cannot replay completion"));
}

fn append_raw(store: &mut SegmentStore, index: u64, record: &RepairWalRecord) {
    let payload = encode_repair_wal_payload(record).expect("encode repair record");
    let recovery = store.recover_current().expect("recover").recovery;
    let tail = recovery.pg_tail(REPAIR_PG);
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    store
        .append_wal(WalAppend {
            pg: REPAIR_PG,
            epoch: Epoch(4),
            index: LogIndex(index),
            kind: WalRecordKind::Repair,
            previous_hash,
            payload: &payload,
        })
        .expect("append raw repair record");
}

fn test_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rsf-repair-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}
