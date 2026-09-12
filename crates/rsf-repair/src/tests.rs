use rsf_core::{
    Epoch,
    placement::{ActingSet, PlacementGroup},
};

use super::*;

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
fn healthy_replicas_need_no_repair() {
    let report = RepairPlanner::default()
        .plan(&group(), &[healthy(1), healthy(2), healthy(3)])
        .unwrap();

    assert!(report.findings.is_empty());
    assert!(report.actions.is_empty());
}

#[test]
fn scrub_reports_replica_inventory_counts() {
    let mut corrupt = healthy(2);
    corrupt.digest_ok = false;
    let mut slow = healthy(3);
    slow.lag_bytes = 32;

    let scrub = RepairPlanner::new(16)
        .scrub(&group(), &[healthy(1), corrupt, slow])
        .unwrap();

    assert_eq!(scrub.pg, PlacementGroupId(7));
    assert_eq!(scrub.expected_replicas, 3);
    assert_eq!(scrub.scanned_replicas, 3);
    assert_eq!(scrub.present_replicas, 3);
    assert_eq!(scrub.verified_replicas, 1);
    assert_eq!(scrub.missing_replicas, 0);
    assert_eq!(scrub.corrupt_replicas, 1);
    assert_eq!(scrub.slow_replicas, 1);
}

#[test]
fn scrub_rejects_duplicate_replica_inventory() {
    let err = RepairPlanner::default()
        .scrub(&group(), &[healthy(1), healthy(1), healthy(2)])
        .expect_err("duplicate scrub entries must reject");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("duplicates"));
}

#[test]
fn scrub_rejects_non_acting_replica_inventory() {
    let err = RepairPlanner::default()
        .scrub(&group(), &[healthy(1), healthy(2), healthy(9)])
        .expect_err("non-acting scrub entries must reject");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("non-acting"));
}

#[test]
fn missing_replica_is_copied_from_healthy_source() {
    let report = RepairPlanner::default()
        .plan(&group(), &[healthy(1), healthy(2)])
        .unwrap();

    assert!(report.findings.iter().any(|finding| {
        finding.kind == RepairFindingKind::MissingReplica && finding.node == Some(NodeId(3))
    }));
    assert!(report.actions.contains(&RepairAction::CopyReplica {
        pg: PlacementGroupId(7),
        from: NodeId(1),
        to: NodeId(3)
    }));
}

#[test]
fn copy_execution_restores_missing_replica_from_healthy_source() {
    let planner = RepairPlanner::default();
    let states = [healthy(1), healthy(2)];
    let action = RepairAction::CopyReplica {
        pg: PlacementGroupId(7),
        from: NodeId(1),
        to: NodeId(3),
    };

    let result = planner.execute_copy(&group(), &states, &action).unwrap();

    assert_eq!(result.pg, PlacementGroupId(7));
    assert_eq!(result.from, NodeId(1));
    assert_eq!(result.to, NodeId(3));
    assert_eq!(result.bytes, 4096);
    assert_eq!(result.checksum, states[0].checksum);
    assert_eq!(
        result.restored,
        ReplicaState {
            node: NodeId(3),
            present: true,
            digest_ok: true,
            lag_bytes: 0,
            durable_bytes: states[0].durable_bytes,
            checksum: states[0].checksum,
        }
    );
}

#[test]
fn copy_execution_rejects_unhealthy_source() {
    let mut source = healthy(1);
    source.digest_ok = false;
    let action = RepairAction::CopyReplica {
        pg: PlacementGroupId(7),
        from: NodeId(1),
        to: NodeId(3),
    };

    let err = RepairPlanner::default()
        .execute_copy(&group(), &[source, healthy(2)], &action)
        .expect_err("unhealthy source must reject copy");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("not healthy"));
}

#[test]
fn copy_execution_rejects_already_healthy_target() {
    let action = RepairAction::CopyReplica {
        pg: PlacementGroupId(7),
        from: NodeId(1),
        to: NodeId(3),
    };

    let err = RepairPlanner::default()
        .execute_copy(&group(), &[healthy(1), healthy(2), healthy(3)], &action)
        .expect_err("already healthy target must reject copy");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("already healthy"));
}

#[test]
fn corrupt_replica_is_rechecked_and_repaired() {
    let mut corrupt = healthy(2);
    corrupt.digest_ok = false;

    let report = RepairPlanner::default()
        .plan(&group(), &[healthy(1), corrupt, healthy(3)])
        .unwrap();

    assert!(report.actions.contains(&RepairAction::RecheckDigest {
        pg: PlacementGroupId(7),
        node: NodeId(2)
    }));
    assert!(report.actions.contains(&RepairAction::QuarantineReplica {
        pg: PlacementGroupId(7),
        node: NodeId(2)
    }));
    assert!(report.actions.contains(&RepairAction::CopyReplica {
        pg: PlacementGroupId(7),
        from: NodeId(1),
        to: NodeId(2)
    }));
}

#[test]
fn quarantine_execution_isolates_corrupt_replica() {
    let mut corrupt = healthy(2);
    corrupt.digest_ok = false;
    let action = RepairAction::QuarantineReplica {
        pg: PlacementGroupId(7),
        node: NodeId(2),
    };

    let result = RepairPlanner::default()
        .execute_quarantine(
            &group(),
            &[healthy(1), corrupt.clone(), healthy(3)],
            &action,
        )
        .unwrap();

    assert_eq!(result.pg, PlacementGroupId(7));
    assert_eq!(result.node, NodeId(2));
    assert_eq!(result.bytes, corrupt.durable_bytes);
    assert_eq!(result.checksum, corrupt.checksum);
    assert!(result.reason.contains("digest verification"));
    assert_eq!(
        result.quarantined,
        ReplicaState {
            node: NodeId(2),
            present: false,
            digest_ok: false,
            lag_bytes: 0,
            durable_bytes: 0,
            checksum: corrupt.checksum,
        }
    );
}

#[test]
fn quarantine_execution_rejects_healthy_replica() {
    let action = RepairAction::QuarantineReplica {
        pg: PlacementGroupId(7),
        node: NodeId(2),
    };

    let err = RepairPlanner::default()
        .execute_quarantine(&group(), &[healthy(1), healthy(2), healthy(3)], &action)
        .expect_err("healthy replica must not be quarantined");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("not corrupt"));
}

#[test]
fn quarantine_execution_rejects_missing_replica() {
    let action = RepairAction::QuarantineReplica {
        pg: PlacementGroupId(7),
        node: NodeId(3),
    };

    let err = RepairPlanner::default()
        .execute_quarantine(&group(), &[healthy(1), healthy(2)], &action)
        .expect_err("missing replica must not be quarantined");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("missing"));
}

#[test]
fn repair_requires_a_healthy_source() {
    let states = [
        ReplicaState {
            node: NodeId(1),
            present: true,
            digest_ok: false,
            lag_bytes: 0,
            durable_bytes: 4096,
            checksum: Digest::blake3_bytes(b"corrupt-source"),
        },
        ReplicaState {
            node: NodeId(2),
            present: false,
            digest_ok: false,
            lag_bytes: 0,
            durable_bytes: 0,
            checksum: Digest::blake3_bytes(b"missing-target"),
        },
    ];

    let err = RepairPlanner::default()
        .plan(&group(), &states)
        .expect_err("unsafe repair must fail");

    assert_eq!(err.code, ErrorCode::QuorumUnavailable);
}

#[test]
fn slow_replica_is_marked_for_demote() {
    let report = RepairPlanner::new(16)
        .plan(
            &group(),
            &[
                healthy(1),
                ReplicaState {
                    node: NodeId(2),
                    present: true,
                    digest_ok: true,
                    lag_bytes: 64,
                    durable_bytes: 4096,
                    checksum: Digest::blake3_bytes(b"slow-replica"),
                },
                healthy(3),
            ],
        )
        .unwrap();

    assert!(report.findings.iter().any(|finding| {
        finding.kind == RepairFindingKind::SlowReplica && finding.node == Some(NodeId(2))
    }));
    assert!(report.actions.contains(&RepairAction::DemoteSlowReplica {
        pg: PlacementGroupId(7),
        node: NodeId(2)
    }));
    assert!(report.actions.contains(&RepairAction::CopyReplica {
        pg: PlacementGroupId(7),
        from: NodeId(1),
        to: NodeId(2)
    }));
}

#[test]
fn under_replication_repair_proves_planned_copies_restore_target() {
    let planner = RepairPlanner::default();
    let states = [healthy(1), healthy(2)];
    let report = planner.plan(&group(), &states).unwrap();
    let action = report
        .actions
        .iter()
        .find(|action| matches!(action, RepairAction::RestoreReplication { .. }))
        .expect("under-replicated group must plan restore action");

    let result = planner
        .execute_under_replication_repair(&group(), &states, action, &report)
        .unwrap();

    assert_eq!(result.pg, PlacementGroupId(7));
    assert_eq!(result.healthy_before, 2);
    assert_eq!(result.expected, 3);
    assert_eq!(result.deficit, 1);
    assert_eq!(result.copy_repairs, 1);
    assert_eq!(result.quarantine_repairs, 0);
    assert_eq!(result.demote_repairs, 0);
    assert_eq!(result.projected_healthy_replicas, 3);
}

#[test]
fn under_replication_repair_accounts_for_corrupt_and_slow_remediation() {
    let planner = RepairPlanner::new(16);
    let mut corrupt = healthy(2);
    corrupt.digest_ok = false;
    let mut slow = healthy(3);
    slow.lag_bytes = 64;
    let states = [healthy(1), corrupt, slow];
    let report = planner.plan(&group(), &states).unwrap();
    let action = report
        .actions
        .iter()
        .find(|action| matches!(action, RepairAction::RestoreReplication { .. }))
        .expect("corrupt and slow replicas must plan restore action");

    let result = planner
        .execute_under_replication_repair(&group(), &states, action, &report)
        .unwrap();

    assert_eq!(result.healthy_before, 1);
    assert_eq!(result.expected, 3);
    assert_eq!(result.deficit, 2);
    assert_eq!(result.copy_repairs, 2);
    assert_eq!(result.quarantine_repairs, 1);
    assert_eq!(result.demote_repairs, 1);
    assert_eq!(result.projected_healthy_replicas, 3);
}

#[test]
fn under_replication_repair_rejects_insufficient_actions() {
    let planner = RepairPlanner::default();
    let states = [healthy(1), healthy(2)];
    let action = RepairAction::RestoreReplication {
        pg: PlacementGroupId(7),
        healthy: 2,
        expected: 3,
        deficit: 1,
    };
    let report = RepairReport {
        findings: Vec::new(),
        actions: vec![action.clone()],
    };

    let err = planner
        .execute_under_replication_repair(&group(), &states, &action, &report)
        .expect_err("restore action without planned copy cannot close deficit");

    assert_eq!(err.code, ErrorCode::QuorumUnavailable);
    assert!(err.message.contains("only projects"));
}

#[test]
fn under_replication_repair_rejects_healthy_group() {
    let planner = RepairPlanner::default();
    let action = RepairAction::RestoreReplication {
        pg: PlacementGroupId(7),
        healthy: 3,
        expected: 3,
        deficit: 0,
    };
    let report = RepairReport {
        findings: Vec::new(),
        actions: vec![action.clone()],
    };

    let err = planner
        .execute_under_replication_repair(
            &group(),
            &[healthy(1), healthy(2), healthy(3)],
            &action,
            &report,
        )
        .expect_err("healthy group cannot execute under-replication repair");

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert!(err.message.contains("real replica deficit"));
}
