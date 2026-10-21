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

#[test]
fn membership_minority_continuation_completes_over_tls_after_initiator_loss() {
    for coordinator in [1, 2] {
        let root = root(&format!("membership-minority-{coordinator}"));
        provision(&root);
        let first = PeerProcess::spawn_with_learner(&root, 1, true);
        let second = PeerProcess::spawn_with_learner(&root, 2, true);
        let mut peers = endpoints(&root);
        peers.get_mut(&NodeId(1)).unwrap().address = first.address;
        peers.get_mut(&NodeId(2)).unwrap().address = second.address;
        let mut network = transport(&root, peers.clone());
        let mut leader = cell(&root, 0);
        let prepared = prepare(&mut leader, b"minority continuation preserves payload");
        leader
            .replicate_with_transport(NodeId(0), prepared.index, &mut network)
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
        // Only voter 1 receives the original primary's proposal: no old quorum.
        network
            .exchange_io(NodeId(1), MEMBERSHIP_BEGIN, &proposal.encode().unwrap())
            .unwrap();
        assert!(
            network
                .fetch_membership_snapshot(NodeId(2), PlacementGroupId(1))
                .is_err()
        );
        drop(network);
        drop(leader);
        drop(first);
        drop(second);
        std::fs::remove_dir_all(root.join("store-0")).unwrap();
        let remote_node = 3 - coordinator;
        let mut remote = PeerProcess::spawn_with_learner(&root, remote_node, true);
        peers.get_mut(&NodeId(remote_node)).unwrap().address = remote.address;
        let mut local = cell(&root, coordinator);
        assert_eq!(local.membership_fence().is_some(), coordinator == 1);
        let mut network = NetworkVsrTransport::new(
            NodeId(coordinator),
            &cluster(),
            identity(&root, coordinator),
            peers.clone(),
            Duration::from_millis(300),
        )
        .unwrap();
        let approval = network
            .resume_membership_approval(&mut local, &proposal)
            .unwrap();
        assert_eq!(approval.snapshots().len(), 2);
        assert_eq!(approval.proposal(), &proposal);
        assert!(local.primary_probe().is_err());
        assert_eq!(
            local.committed_prefix().unwrap()[0].prepare.operation,
            prepared.operation
        );
        let before = local
            .membership_snapshot(NodeId(coordinator), NodeId(coordinator))
            .unwrap()
            .records;
        let mut conflict = proposal.clone();
        conflict.new_voters.push(NodeId(4));
        assert!(
            network
                .resume_membership_approval(&mut local, &conflict)
                .is_err()
        );
        assert_eq!(
            local
                .membership_snapshot(NodeId(coordinator), NodeId(coordinator))
                .unwrap()
                .records,
            before
        );
        let own = local
            .membership_snapshot(NodeId(coordinator), NodeId(coordinator))
            .unwrap()
            .report;
        // Report identity is bound to the actual TLS sender, including on retry.
        let mut spoofed = own.clone();
        spoofed.voter.replica = NodeId(remote_node);
        assert!(
            network
                .exchange_io(
                    NodeId(remote_node),
                    MEMBERSHIP_RESUME,
                    &spoofed.encode().unwrap()
                )
                .is_err()
        );
        drop(remote);
        remote = PeerProcess::spawn_with_learner(&root, remote_node, true);
        peers.get_mut(&NodeId(remote_node)).unwrap().address = remote.address;
        network = NetworkVsrTransport::new(
            NodeId(coordinator),
            &cluster(),
            identity(&root, coordinator),
            peers.clone(),
            Duration::from_millis(300),
        )
        .unwrap();
        assert_eq!(
            network
                .resume_membership_approval(&mut local, &proposal)
                .unwrap()
                .snapshots()
                .len(),
            2
        );
        assert_eq!(
            local
                .membership_snapshot(NodeId(coordinator), NodeId(coordinator))
                .unwrap()
                .records,
            before
        );
        peers.insert(
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
            peers,
            Duration::from_millis(300),
        )
        .unwrap();
        assert!(
            learner
                .exchange_io(
                    NodeId(remote_node),
                    MEMBERSHIP_RESUME,
                    &own.encode().unwrap()
                )
                .is_err()
        );
        assert!(
            learner
                .resume_membership_approval(&mut local, &proposal)
                .is_err()
        );
        assert_eq!(local.replica_count(), 3);
        assert!(!local.contains_replica(NodeId(3)));
        drop(local);
        drop(remote);
        let mut reopened = cell(&root, coordinator);
        assert_eq!(reopened.membership_fence(), Some(&proposal));
        assert_eq!(
            reopened
                .membership_snapshot(NodeId(coordinator), NodeId(coordinator))
                .unwrap()
                .records,
            before
        );
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn membership_continuation_without_surviving_report_never_initiates_proposal() {
    let root = root("membership-resume-no-report");
    provision(&root);
    let remote = PeerProcess::spawn(&root, 1);
    let mut peers = endpoints(&root);
    peers.get_mut(&NodeId(1)).unwrap().address = remote.address;
    let network = NetworkVsrTransport::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        peers,
        Duration::from_millis(100),
    )
    .unwrap();
    let mut local = cell(&root, 2);
    let before = local.history_snapshot().unwrap().records;
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
    assert!(
        network
            .resume_membership_approval(&mut local, &proposal)
            .unwrap_err()
            .reason
            .contains("no surviving durable report")
    );
    assert!(local.membership_fence().is_none());
    assert_eq!(local.history_snapshot().unwrap().records, before);
    assert!(
        network
            .fetch_membership_snapshot(NodeId(1), PlacementGroupId(1))
            .is_err()
    );
    drop(local);
    drop(remote);
    std::fs::remove_dir_all(root).unwrap();
}
