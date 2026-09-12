use rsf_core::{
    Epoch, FailureDomainId, LogIndex, RegionId,
    cluster::{ClusterMap, NodeDescriptor, NodeRole},
    placement::PlacementPolicy,
};
use rsf_storage::{SegmentStore, WalAppend};

use super::*;
use crate::{NodeLoad, RebalancePlanner, RebalancePolicy};

#[test]
fn durable_rebalance_recovers_completed_move_after_restart() {
    let path = test_path("durable_rebalance_recovers_completed_move_after_restart");
    let (action, proof, result) = planned_move();

    {
        let mut ledger = DurableRebalanceLedger::open(&path, Epoch(5)).expect("open ledger");
        let id = ledger
            .record_plan(action, proof)
            .expect("persist rebalance plan");
        ledger.mark_started(id).expect("persist start");
        ledger
            .mark_completed(id, RebalanceCompletion::Move(result.clone()))
            .expect("persist completion");
    }

    let recovered = DurableRebalanceLedger::open(&path, Epoch(5)).expect("recover ledger");
    let operation = recovered
        .operation(RebalanceOperationId(1))
        .expect("operation recovered");

    assert_eq!(operation.state, RebalanceOperationState::Completed);
    assert_eq!(
        operation.completion,
        Some(RebalanceCompletion::Move(result))
    );
    assert!(recovered.pending().is_empty());
}

#[test]
fn durable_rebalance_recovers_started_move_as_pending() {
    let path = test_path("durable_rebalance_recovers_started_move_as_pending");
    let (action, proof, _) = planned_move();
    {
        let mut ledger = DurableRebalanceLedger::open(&path, Epoch(5)).expect("open ledger");
        let id = ledger
            .record_plan(action, proof)
            .expect("persist rebalance plan");
        ledger.mark_started(id).expect("persist start");
    }

    let recovered = DurableRebalanceLedger::open(&path, Epoch(5)).expect("recover ledger");

    assert_eq!(recovered.pending().len(), 1);
    assert_eq!(
        recovered
            .operation(RebalanceOperationId(1))
            .map(|op| op.state),
        Some(RebalanceOperationState::Started)
    );
}

#[test]
fn durable_rebalance_rejects_completion_that_does_not_match_action() {
    let path = test_path("durable_rebalance_rejects_completion_that_does_not_match_action");
    let (action, proof, mut result) = planned_move();
    result.to = NodeId(2);
    let mut ledger = DurableRebalanceLedger::open(&path, Epoch(5)).expect("open ledger");
    let id = ledger
        .record_plan(action, proof)
        .expect("persist rebalance plan");
    ledger.mark_started(id).expect("persist start");

    let err = ledger
        .mark_completed(id, RebalanceCompletion::Move(result))
        .expect_err("mismatched completion must reject");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("does not match action"));
}

#[test]
fn rebalance_wal_decode_rejects_trailing_bytes() {
    let payload = encode_rebalance_wal_payload(&RebalanceWalRecord::Started {
        id: RebalanceOperationId(9),
    })
    .expect("encode");
    let mut payload = payload;
    payload.push(99);

    let err = decode_rebalance_wal_payload(&payload).expect_err("trailing bytes reject");

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
    assert!(err.message.contains("trailing bytes"));
}

#[test]
fn durable_rebalance_replay_rejects_completion_without_start() {
    let path = test_path("durable_rebalance_replay_rejects_completion_without_start");
    let (action, proof, result) = planned_move();
    let mut store = SegmentStore::open(&path).expect("segment store");
    append_raw(
        &mut store,
        1,
        &RebalanceWalRecord::Planned {
            id: RebalanceOperationId(1),
            action,
            proof,
        },
    );
    append_raw(
        &mut store,
        2,
        &RebalanceWalRecord::Completed {
            id: RebalanceOperationId(1),
            completion: RebalanceCompletion::Move(result),
        },
    );

    let err = DurableRebalanceLedger::open(&path, Epoch(5))
        .expect_err("replay must fail closed for invalid lifecycle");

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
    assert!(err.message.contains("cannot replay completion"));
}

fn planned_move() -> (RebalanceAction, PlacementPolicyProof, PlacementMoveResult) {
    let planner = RebalancePlanner;
    let cluster = cluster();
    let group = PlacementGroup {
        id: PlacementGroupId(1),
        acting_set: ActingSet {
            epoch: Epoch(1),
            primary: NodeId(0),
            replicas: vec![NodeId(0), NodeId(1), NodeId(2)],
        },
        policy: PlacementPolicy::Replicated { replicas: 3 },
    };
    let plan = planner
        .plan(
            &cluster,
            std::slice::from_ref(&group),
            &[load(0, 900), load(1, 500), load(2, 500), load(3, 100)],
            &RebalancePolicy {
                high_watermark_bps: 8_000,
                low_watermark_bps: 2_000,
                min_replicas: 3,
                max_moves: 1,
            },
        )
        .expect("plan move");
    let action = plan.actions[0].clone();
    let proof = plan.policy_proofs[0].clone();
    let result = planner
        .execute_move(&cluster, &group, &action, &proof)
        .expect("execute move");
    (action, proof, result)
}

fn cluster() -> ClusterMap {
    ClusterMap {
        epoch: Epoch(1),
        nodes: (0..4)
            .map(|id| NodeDescriptor {
                id: NodeId(id),
                region: RegionId::new("iad").expect("region"),
                failure_domain: FailureDomainId::new(format!("iad-{id}")).expect("domain"),
                capacity_bytes: 1_000,
                roles: vec![NodeRole::Storage],
            })
            .collect(),
    }
}

fn load(node: u64, used_bps: u64) -> NodeLoad {
    NodeLoad {
        node: NodeId(node),
        used_bytes: used_bps,
        capacity_bytes: 1_000,
    }
}

fn append_raw(store: &mut SegmentStore, index: u64, record: &RebalanceWalRecord) {
    let payload = encode_rebalance_wal_payload(record).expect("encode rebalance record");
    let recovery = store.recover_current().expect("recover").recovery;
    let tail = recovery.pg_tail(REBALANCE_PG);
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    store
        .append_wal(WalAppend {
            pg: REBALANCE_PG,
            epoch: Epoch(5),
            index: LogIndex(index),
            kind: WalRecordKind::Rebalance,
            previous_hash,
            payload: &payload,
        })
        .expect("append raw rebalance record");
}

fn test_path(name: &str) -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rsf-rebalance-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}
