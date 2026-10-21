use super::*;
#[test]
fn membership_approval_recovers_from_real_old_voters_after_primary_loss() {
    let root = root("membership-approval");
    provision(&root);
    let first = PeerProcess::spawn_with_learner(&root, 1, true);
    let second = PeerProcess::spawn_with_learner(&root, 2, true);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let prepare = prepare(&mut leader, b"membership retained complete payload");
    leader
        .replicate_with_transport(NodeId(0), prepare.index, &mut network)
        .unwrap();
    let proposal = rsf_vsr::VsrMembershipProposal {
        election: rsf_vsr::VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(1),
            voters: vec![NodeId(0), NodeId(1), NodeId(2)],
        },
        old_primary: NodeId(0),
        new_voters: vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
    };
    let approval = network.approve_membership(&mut leader, &proposal).unwrap();
    assert_eq!(approval.snapshots().len(), 3);
    assert!(leader.primary_probe().is_err());
    for replica in [NodeId(1), NodeId(2)] {
        let snapshot = network
            .fetch_membership_snapshot(replica, PlacementGroupId(1))
            .unwrap();
        assert_eq!(snapshot.report.proposal, proposal);
        snapshot.validate().unwrap();
    }
    // Repeated authenticated initiation is an exact fence retry, not another vote.
    let before = approval.snapshots()[0].records.clone();
    assert_eq!(
        network
            .approve_membership(&mut leader, &proposal)
            .unwrap()
            .snapshots()[0]
            .records,
        before
    );
    drop(approval);
    drop(network);
    drop(leader);
    drop(first);
    drop(second);
    std::fs::remove_dir_all(root.join("store-0")).unwrap();
    let first = PeerProcess::spawn_with_learner(&root, 1, true);
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    let mut survivor = cell(&root, 2);
    let network = NetworkVsrTransport::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        peers.clone(),
        Duration::from_millis(300),
    )
    .unwrap();
    let discovered = network
        .fetch_membership_snapshot(NodeId(1), PlacementGroupId(1))
        .unwrap()
        .report
        .proposal;
    let recovered = network
        .recover_membership_approval(&mut survivor, &discovered)
        .unwrap();
    assert_eq!(recovered.proposal(), &proposal);
    assert_eq!(recovered.snapshots().len(), 2);
    assert_eq!(
        survivor.committed_prefix().unwrap()[0].prepare.operation,
        prepare.operation
    );
    assert_eq!(survivor.replica_count(), 3);
    assert!(!survivor.contains_replica(NodeId(3)));
    assert!(survivor.primary_probe().is_err());
    let mut endpoints = peers;
    endpoints.insert(
        NodeId(3),
        PeerEndpoint {
            address: "127.0.0.1:1".parse().unwrap(),
            server_name: "localhost".into(),
            fingerprint: identity(&root, 3).fingerprint,
        },
    );
    let learner = NetworkVsrTransport::new(
        NodeId(3),
        &cluster(),
        identity(&root, 3),
        endpoints,
        Duration::from_millis(300),
    )
    .unwrap();
    let rejected = learner
        .fetch_membership_snapshot(NodeId(1), PlacementGroupId(1))
        .unwrap_err();
    assert!(rejected.reason.contains("old voter"), "{}", rejected.reason);
    drop(survivor);
    drop(first);
    std::fs::remove_dir_all(root).unwrap();
}
