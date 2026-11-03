use super::*;

fn uploaded_b_selected_a(fixture: &Accepted) -> (PeerProcess, NetworkVsrTransport) {
    let peer = PeerProcess::spawn_staging(&fixture.root, 2, false);
    let mut peers = all_endpoints(&fixture.root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let network = decision_transport(&fixture.root, 3, peers);
    // All three are real authenticated routes. A completed B upload must stay
    // usable if another valid same-proposal upload selects A before STAGE(B).
    network
        .provision_membership_authority(NodeId(2), fixture.plan.approval())
        .unwrap();
    let a = VsrMembershipInput::from_approval(&fixture.authority).unwrap();
    network
        .request_io(NodeId(2), MEMBERSHIP_AUTH_BEGIN, &a.encode().unwrap())
        .unwrap();
    let mut query = PlacementGroupId(1).0.to_le_bytes().to_vec();
    query.extend_from_slice(&fixture.plan.chosen().input_id());
    assert!(
        network
            .exchange_io(NodeId(2), MEMBERSHIP_AUTH_FINISH, &query)
            .is_err()
    );
    (peer, network)
}

#[test]
fn real_tls_stage_uses_finished_inbox_b_after_another_upload_selects_a() {
    let fixture = fixture_for_receiver(false);
    let root = &fixture.root;
    let decision = MembershipDecisionStore::open_existing(
        root.join("membership-2/decision"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert!(decision.accepted().is_none());
    assert!(
        decision
            .retained_approval(fixture.plan.chosen().input_id())
            .unwrap()
            .is_none()
    );
    drop(decision);
    let (peer, network) = uploaded_b_selected_a(&fixture);
    let before = files(&root.join("membership-2/inbox"));
    let report = network.stage_membership(NodeId(2), &fixture.plan).unwrap();
    assert_eq!(report.replica, NodeId(2));
    assert_eq!(report.plan, fixture.plan.id());
    assert_eq!(files(&root.join("membership-2/inbox")), before);
    assert_eq!(
        std::fs::read(root.join("materialized-2/1")).unwrap(),
        fixture.committed.encode().unwrap()
    );
    assert!(!root.join("store-2/VSR_ACTIVE").exists());
    assert_eq!(
        network.stage_membership(NodeId(2), &fixture.plan).unwrap(),
        report
    );
    assert_eq!(files(&root.join("membership-2/inbox")), before);
    drop(peer);
    let decision = MembershipDecisionStore::open_existing(
        root.join("membership-2/decision"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(decision.chosen(), Some(fixture.plan.chosen()));
    drop(decision);
    let inbox = MembershipAuthorityInbox::open_existing(
        root.join("membership-2/inbox"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(
        inbox.selected_input().unwrap().id().unwrap(),
        VsrMembershipInput::from_approval(&fixture.authority)
            .unwrap()
            .id()
            .unwrap()
    );
    assert!(
        inbox
            .finish(NodeId(3), fixture.plan.chosen().input_id())
            .is_err()
    );
    drop(inbox);
}

#[test]
fn real_tls_stage_does_not_repair_lost_or_corrupt_nonselected_inbox_b() {
    for fault in ["missing", "corrupt"] {
        let fixture = fixture_for_receiver(false);
        let root = &fixture.root;
        let (peer, network) = uploaded_b_selected_a(&fixture);
        let directory = root
            .join("membership-2/inbox/inputs")
            .join(id_name(fixture.plan.chosen().input_id()));
        if fault == "missing" {
            std::fs::remove_dir_all(&directory).unwrap();
        } else {
            std::fs::write(directory.join("DESCRIPTOR"), b"damaged nonselected proof").unwrap();
        }
        let before = files(&root.join("membership-2"));
        for _ in 0..2 {
            assert!(network.stage_membership(NodeId(2), &fixture.plan).is_err());
            assert_eq!(files(&root.join("membership-2")), before);
            assert!(!root.join("membership-2/staging").exists());
            assert!(!root.join("materialized-2").exists());
        }
        drop(peer);
        let decision = MembershipDecisionStore::open_existing(
            root.join("membership-2/decision"),
            PlacementGroupId(1),
            NodeId(2),
        )
        .unwrap();
        assert!(decision.chosen().is_none());
        assert!(decision.accepted().is_none());
        assert!(
            decision
                .retained_approval(fixture.plan.chosen().input_id())
                .unwrap()
                .is_none()
        );
        drop(decision);
        assert_eq!(files(&root.join("membership-2")), before);
    }
}
