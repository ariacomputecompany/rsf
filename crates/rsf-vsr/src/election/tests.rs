use super::*;
use std::path::{Path, PathBuf};

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-election-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn open(path: &Path) -> DurableVsrCell {
    DurableVsrCell::open(
        path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}
fn request() -> VsrViewChangeRequest {
    VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    }
}
fn operation() -> VsrOperation {
    VsrOperation::ReplicatedCommand {
        operation_id: OperationId(1),
        payload: b"value".to_vec(),
    }
}
fn prepare(cell: &mut DurableVsrCell) -> VsrPrepare {
    let VsrDecision::Prepared(value) = cell.prepare(NodeId(0), operation(), 5).unwrap() else {
        panic!("prepare");
    };
    value
}

#[test]
fn promise_survives_restart_and_fences_all_old_view_mutation_paths() {
    let path = root("fence");
    let mut cell = open(&path);
    let prepared = prepare(&mut cell);
    cell.acknowledge(NodeId(2), Epoch(1), prepared.index);
    let snapshot = cell
        .begin_view_change(NodeId(1), NodeId(1), request())
        .unwrap();
    assert_eq!(snapshot.report.accepted_tail, LogIndex(1));
    assert_eq!(snapshot.report.history.probe.committed_tail, LogIndex(0));
    assert!(
        snapshot
            .records
            .iter()
            .any(|record| record.payload == prepared.encode().unwrap())
    );
    let frozen = snapshot.report;
    drop(cell);
    let mut cell = open(&path);
    assert_eq!(cell.view_change_promise(), Some(&request()));
    assert_eq!(
        cell.begin_view_change(NodeId(1), NodeId(1), request())
            .unwrap()
            .report,
        frozen
    );
    assert!(cell.prepare(NodeId(0), operation(), 5).is_err());
    assert!(matches!(
        cell.acknowledge(NodeId(2), Epoch(1), prepared.index),
        VsrDecision::Rejected { .. }
    ));
    assert!(cell.commit(NodeId(0), prepared.index).is_err());
    assert!(
        cell.receive_prepare(NodeId(1), NodeId(0), prepared)
            .is_err()
    );
    assert!(cell.reprepare(NodeId(0), LogIndex(1)).is_err());
    assert!(cell.primary_probe().is_err());
    assert!(cell.history_snapshot().is_err());
    assert!(matches!(
        cell.view_change(Epoch(3), NodeId(2)),
        VsrDecision::Rejected { .. }
    ));
    assert!(matches!(
        cell.reconfigure(Epoch(3), NodeId(2), [NodeId(0), NodeId(1), NodeId(2)]),
        VsrDecision::Rejected { .. }
    ));
    assert_eq!(
        cell.frozen_view_change_snapshot(NodeId(1), &request())
            .unwrap()
            .report,
        frozen
    );
    assert_eq!(cell.view(), Epoch(1));
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn malformed_spoofed_stale_and_conflicting_promises_are_side_effect_free() {
    let path = root("reject");
    let mut cell = open(&path);
    let before = cell.history_snapshot().unwrap().descriptor;
    assert!(
        cell.begin_view_change(NodeId(1), NodeId(0), request())
            .is_err()
    );
    assert!(
        cell.begin_view_change(NodeId(9), NodeId(1), request())
            .is_err()
    );
    for request in [
        VsrViewChangeRequest {
            target_view: Epoch(1),
            ..request()
        },
        VsrViewChangeRequest {
            previous_view: Epoch(2),
            target_view: Epoch(3),
            ..request()
        },
        VsrViewChangeRequest {
            pg: PlacementGroupId(2),
            ..request()
        },
        VsrViewChangeRequest {
            voters: vec![NodeId(0), NodeId(1)],
            ..request()
        },
        VsrViewChangeRequest {
            voters: vec![NodeId(1), NodeId(0), NodeId(2)],
            ..request()
        },
    ] {
        assert!(
            cell.begin_view_change(NodeId(1), NodeId(1), request)
                .is_err()
        );
    }
    assert_eq!(cell.history_snapshot().unwrap().descriptor, before);
    cell.begin_view_change(NodeId(1), NodeId(1), request())
        .unwrap();
    let conflict = VsrViewChangeRequest {
        candidate: NodeId(2),
        ..request()
    };
    assert!(
        cell.begin_view_change(NodeId(2), NodeId(2), conflict)
            .is_err()
    );
    let higher = VsrViewChangeRequest {
        target_view: Epoch(3),
        candidate: NodeId(2),
        ..request()
    };
    cell.begin_view_change(NodeId(2), NodeId(2), higher.clone())
        .unwrap();
    assert!(
        cell.begin_view_change(NodeId(1), NodeId(1), request())
            .is_err()
    );
    assert!(
        cell.frozen_view_change_snapshot(NodeId(1), &request())
            .is_err()
    );
    drop(cell);
    assert_eq!(open(&path).view_change_promise(), Some(&higher));
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn codecs_reject_truncation_and_snapshot_integrity_binds_the_fence_and_history() {
    let path = root("codec");
    let mut cell = open(&path);
    let mut snapshot = cell
        .begin_view_change(NodeId(1), NodeId(1), request())
        .unwrap();
    let encoded = snapshot.report.encode().unwrap();
    assert_eq!(
        VsrViewChangeReport::decode(&encoded).unwrap(),
        snapshot.report
    );
    for end in 0..encoded.len() {
        assert!(VsrViewChangeReport::decode(&encoded[..end]).is_err());
    }
    let request = request().encode().unwrap();
    for end in 0..request.len() {
        assert!(VsrViewChangeRequest::decode(&request[..end]).is_err());
    }
    snapshot.records[0].payload[0] ^= 1;
    assert!(snapshot.validate_integrity().is_err());
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn replay_rejects_writes_or_conflicting_promises_after_a_persisted_fence() {
    for conflict in [false, true] {
        let path = root("corrupt");
        let mut cell = open(&path);
        let prepared = prepare(&mut cell);
        cell.begin_view_change(NodeId(1), NodeId(1), request())
            .unwrap();
        drop(cell);
        let mut store = SegmentStore::open(&path).unwrap();
        let (kind, bytes) = if conflict {
            (
                WalRecordKind::ViewPromise,
                VsrViewChangeRequest {
                    candidate: NodeId(2),
                    ..request()
                }
                .encode()
                .unwrap(),
            )
        } else {
            (WalRecordKind::Prepare, prepared.encode().unwrap())
        };
        store
            .append_next_wal(PlacementGroupId(1), Epoch(1), kind, &bytes)
            .unwrap();
        drop(store);
        assert!(DurableVsrCell::open_existing(&path, PlacementGroupId(1)).is_err());
        let _ = std::fs::remove_dir_all(path);
    }
}

#[test]
fn every_fenced_majority_prevents_an_old_primary_quorum_even_after_restart() {
    for count in [3usize, 5] {
        let voters = (0..count as u64).map(NodeId).collect::<Vec<_>>();
        let quorum = count / 2 + 1;
        for mask in 0u32..1 << count {
            if mask.count_ones() as usize != quorum {
                continue;
            }
            let path = root("intersection");
            let request = VsrViewChangeRequest {
                voters: voters.clone(),
                ..request()
            };
            let prepared = VsrPrepare {
                pg: PlacementGroupId(1),
                view: Epoch(1),
                index: LogIndex(1),
                primary: NodeId(0),
                operation: operation(),
                bytes: 5,
            };
            let mut old_acknowledgements = 0;
            for replica in &voters {
                let directory = path.join(replica.0.to_string());
                let mut cell = DurableVsrCell::open(
                    &directory,
                    PlacementGroupId(1),
                    Epoch(1),
                    NodeId(0),
                    voters.clone(),
                )
                .unwrap();
                if mask & (1 << replica.0) != 0 {
                    cell.begin_view_change(*replica, NodeId(1), request.clone())
                        .unwrap();
                }
                drop(cell);
                let mut cell =
                    DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).unwrap();
                let acknowledged = if *replica == NodeId(0) {
                    cell.prepare(NodeId(0), operation(), 5).is_ok()
                } else {
                    cell.receive_prepare(*replica, NodeId(0), prepared.clone())
                        .is_ok()
                };
                old_acknowledgements += usize::from(acknowledged);
            }
            assert!(
                old_acknowledgements < quorum,
                "voters={count} fenced={mask:b} old acknowledgements={old_acknowledgements}"
            );
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

#[test]
fn a_certificate_assembled_before_the_promise_cannot_be_applied_after_it() {
    let path = root("delayed-certificate");
    let mut primary = open(&path.join("primary"));
    let prepared = prepare(&mut primary);
    let mut follower = open(&path.join("follower"));
    follower
        .receive_prepare(NodeId(2), NodeId(0), prepared.clone())
        .unwrap();
    primary.acknowledge(NodeId(2), Epoch(1), prepared.index);
    let VsrDecision::Committed(commit) = primary.commit(NodeId(0), prepared.index).unwrap() else {
        panic!("certificate");
    };
    follower
        .begin_view_change(NodeId(2), NodeId(1), request())
        .unwrap();
    assert!(
        follower
            .receive_commit(NodeId(2), NodeId(0), commit.clone())
            .is_err()
    );
    assert!(
        follower
            .catch_up_from(&[VsrCommittedEntry {
                prepare: prepared,
                commit
            }])
            .is_err()
    );
    assert_eq!(follower.committed_tail(), LogIndex(0));
    assert_eq!(
        follower
            .frozen_view_change_snapshot(NodeId(2), &request())
            .unwrap()
            .report
            .accepted_tail,
        LogIndex(1)
    );
    assert!(
        follower
            .remember_client_request(VsrClientRequestId::new("old", 1), LogIndex(1))
            .is_err()
    );
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn tail_truncation_preserves_the_fence_and_accepted_history_before_it() {
    let path = root("truncate");
    let mut cell = open(&path);
    prepare(&mut cell);
    let expected = cell
        .begin_view_change(NodeId(1), NodeId(1), request())
        .unwrap()
        .report;
    drop(cell);
    let mut store = SegmentStore::open(&path).unwrap();
    let report = store.truncate_uncommitted_tails().unwrap();
    assert_eq!(report.dropped_wal_records, 0);
    drop(store);
    let mut cell = open(&path);
    assert_eq!(
        cell.frozen_view_change_snapshot(NodeId(1), &request())
            .unwrap()
            .report,
        expected
    );
    assert!(cell.primary_probe().is_err());
    assert_eq!(WalRecordKind::ViewPromise.to_u8(), 39);
    assert_eq!(WalRecordKind::from_u8(39), Some(WalRecordKind::ViewPromise));
    let _ = std::fs::remove_dir_all(path);
}
