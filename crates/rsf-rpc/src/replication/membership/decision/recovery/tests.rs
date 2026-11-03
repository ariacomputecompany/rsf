use super::*;
use rsf_core::{Epoch, OperationId};
use rsf_vsr::{VsrOperation, VsrViewChangeRequest};

fn peer(now: Instant) -> Peer {
    Peer {
        next: now,
        upload: None,
        require_stage: false,
        query_next: false,
        failures: 0,
        provisioned: false,
        refresh: true,
        repair: None,
    }
}

#[test]
fn recovery_rotates_ready_groups_and_peers_without_waiting_for_backoff() {
    let now = Instant::now();
    let timing = PeerMembershipRecoveryTiming {
        interval: Duration::from_millis(1),
    };
    let mut groups = BTreeMap::from([
        (PlacementGroupId(1), Group::new(timing, now)),
        (PlacementGroupId(2), Group::new(timing, now)),
        (PlacementGroupId(3), Group::new(timing, now)),
    ]);
    groups.get_mut(&PlacementGroupId(2)).unwrap().next = now + Duration::from_secs(60);
    let mut last = None;
    let selected = (0..4)
        .map(|_| {
            let pg = rotated(groups.keys().copied().collect(), last)
                .into_iter()
                .find(|pg| groups[pg].next <= now)
                .unwrap();
            last = Some(pg);
            pg
        })
        .collect::<Vec<_>>();
    assert_eq!(
        selected,
        vec![
            PlacementGroupId(1),
            PlacementGroupId(3),
            PlacementGroupId(1),
            PlacementGroupId(3)
        ]
    );
    let group = groups.get_mut(&PlacementGroupId(1)).unwrap();
    group.peers = [
        (NodeId(1), peer(now)),
        (NodeId(2), peer(now)),
        (NodeId(3), peer(now)),
    ]
    .into();
    group.peers.get_mut(&NodeId(2)).unwrap().next = now + Duration::from_secs(60);
    assert_eq!(
        (0..4)
            .map(|_| group.due_peer(now).unwrap())
            .collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(3), NodeId(1), NodeId(3)]
    );
}

#[test]
fn staged_rejection_after_one_completed_provision_does_not_loop_upload_or_claim_repair() {
    let now = Instant::now();
    let interval = Duration::from_millis(2);
    let mut peer = peer(now);
    peer.failed(worker::Kind::Stage, interval, now);
    assert_eq!(peer.upload, Some(Upload::Begin));
    // Successful inbox completion is not a successful decision import or stage.
    peer.provisioned = true;
    peer.upload = None;
    peer.require_stage = true;
    for _ in 0..20 {
        peer.failed(worker::Kind::Stage, interval, now);
        assert_eq!(peer.upload, None);
        assert_eq!(peer.repair, Some(choice::Repair::Inspect));
        assert!(peer.require_stage);
        assert!(!peer.query_next);
        assert!(peer.next <= now + Duration::from_secs(60));
    }
    peer.query_next = true;
    peer.failed(worker::Kind::Query, interval, now);
    assert!(!peer.query_next);
    assert_eq!(peer.upload, None);
}

#[test]
fn incremental_authority_batches_preserve_exact_cross_snapshot_order_and_byte_bounds() {
    let root = std::env::temp_dir().join(format!(
        "rsf-recovery-batches-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let proposal = VsrMembershipProposal {
        election: VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(1),
            voters: vec![NodeId(0), NodeId(1), NodeId(2)],
        },
        old_primary: NodeId(0),
        new_voters: vec![NodeId(1), NodeId(2), NodeId(3)],
    };
    let mut snapshots = Vec::new();
    for node in 0..3 {
        let mut cell = DurableVsrCell::open(
            root.join(node.to_string()),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        for index in 0..40 {
            let payload = vec![index as u8; if index == 7 { 128 * 1024 } else { 32 }];
            cell.prepare(
                NodeId(0),
                VsrOperation::ReplicatedCommand {
                    operation_id: OperationId(index + 1),
                    payload: payload.clone(),
                },
                payload.len() as u32,
            )
            .unwrap();
        }
        snapshots.push(
            cell.begin_membership(NodeId(node), NodeId(0), proposal.clone())
                .unwrap(),
        );
    }
    let approval = VsrMembershipApproval::from_snapshots(snapshots).unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let expected = input.flattened_records(&approval).unwrap();
    let mut actual = Vec::new();
    let mut saw_large = false;
    while actual.len() < expected.len() {
        let batch = progress::approval_batch(&input, &approval, actual.len() as u64).unwrap();
        assert!(!batch.records.is_empty());
        assert!(batch.records.len() <= rsf_vsr::MAX_HISTORY_BATCH_RECORDS);
        if batch.encode().unwrap().len() > rsf_vsr::HISTORY_BATCH_TARGET_BYTES {
            assert_eq!(batch.records.len(), 1);
            saw_large = true;
        }
        actual.extend(batch.records);
    }
    assert!(saw_large);
    assert_eq!(actual, expected);
    assert!(progress::approval_batch(&input, &approval, actual.len() as u64).is_err());
    std::fs::remove_dir_all(root).unwrap();
}
