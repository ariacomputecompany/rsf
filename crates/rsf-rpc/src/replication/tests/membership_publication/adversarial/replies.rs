use super::*;

fn gated_recovery(ready: &mut Ready, corrupt: bool) -> (PeerReplicaServer, GatedPeer) {
    ready.publish();
    drop(ready.remote.take());
    let remote = server(&ready.root, 3, true, ready.peers.clone()).unwrap();
    let peer = GatedPeer::start(remote, ready.root.clone(), true, corrupt);
    ready.peers.get_mut(&NodeId(3)).unwrap().address = peer.address;
    let mut local = ready.local.take().unwrap();
    local.control_transport = ready.network();
    let mut local = managed(local, &[PG]);
    until(&mut local, &ready.root, |update| {
        update.phase == "publishing" && update.replica == Some(NodeId(3))
    });
    peer.gated.recv_timeout(RPC).unwrap();
    assert!(poll(&mut local, &ready.root).is_none());
    (local, peer)
}

#[test]
fn managed_recovery_accepts_delayed_historical_reply_after_real_tls_view_and_promise() {
    let mut ready = fixture();
    let (mut local, peer) = gated_recovery(&mut ready, false);
    let retained = std::fs::read(ready.root.join("membership-2/staging/PUBLICATION")).unwrap();
    let remote_retained =
        std::fs::read(ready.root.join("membership-3/staging/PUBLICATION")).unwrap();
    // Deliver the historical result while its completion remains unpolled.
    // The owning scheduler must revalidate it after subsequent inbound state
    // changes; a full installation need not fit inside an artificially held
    // response deadline for this acceptance-boundary test.
    peer.release();
    let network = ready.network();
    let request = VsrViewChangeRequest {
        pg: PG,
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        voters: proposal().new_voters,
    };
    let cell = local.cells.get_mut(&PG).unwrap();
    let (input, snapshots) = network.prepare_installation_input(cell, &request).unwrap();
    network
        .stage_installation_input(cell, &input, &snapshots)
        .unwrap();
    network
        .resume_view_installation(cell, &mut |entry| apply(&ready.root, 2, entry))
        .unwrap();
    let promise = VsrViewChangeRequest {
        previous_view: Epoch(3),
        target_view: Epoch(4),
        ..request
    };
    cell.begin_view_change(NodeId(2), NodeId(2), promise.clone())
        .unwrap();
    network.fetch_view_change(NodeId(3), &promise).unwrap();
    let before = cell.recovery_state_token().unwrap();
    let accepted = until(&mut local, &ready.root, |update| {
        update.publication_quorum.is_some()
    });
    assert_eq!(accepted.replica, Some(NodeId(3)));
    assert_eq!(
        accepted.publication_quorum.unwrap().plan_id(),
        ready.quorum.plan_id()
    );
    let cell = &local.cells[&PG];
    assert_eq!(cell.recovery_state_token().unwrap(), before);
    assert_eq!(cell.view(), Epoch(3));
    assert_eq!(cell.view_change_promise(), Some(&promise));
    assert!(cell.primary_probe().is_err());
    assert!(local.membership_admission_required().unwrap());
    assert_eq!(
        std::fs::read(ready.root.join("membership-2/staging/PUBLICATION")).unwrap(),
        retained
    );
    assert_eq!(
        std::fs::read(ready.root.join("membership-3/staging/PUBLICATION")).unwrap(),
        remote_retained
    );
    drop(local);
    drop(peer);
}

#[test]
fn managed_recovery_discards_real_delayed_reply_after_active_custody_or_context_loss() {
    for fault in ["active-pointer", "bound-proposal"] {
        let mut ready = fixture();
        let (mut local, peer) = gated_recovery(&mut ready, false);
        let retained = std::fs::read(ready.root.join("membership-2/staging/PUBLICATION")).unwrap();
        if fault == "active-pointer" {
            std::fs::remove_file(ready.root.join("store-2/VSR_ACTIVE")).unwrap();
        } else {
            // Fault the test-owned scheduling context while its real TLS worker
            // is blocked. No production hook or durable journal is rewritten.
            let mut other = proposal();
            other.election.target_view = Epoch(3);
            local.membership_recovery_bindings.insert(PG, other);
        }
        peer.release();
        let discarded = until(&mut local, &ready.root, |update| {
            matches!(update.phase, "local-error" | "stale-reply")
        });
        assert!(discarded.publication_quorum.is_none());
        if fault == "active-pointer" {
            assert_eq!(discarded.phase, "local-error");
            assert!(discarded.detail.contains("custody failed validation"));
        } else {
            assert_eq!(discarded.phase, "stale-reply");
        }
        assert!(local.membership_admission_required().unwrap());
        for _ in 0..3 {
            let update = until(&mut local, &ready.root, |_| true);
            assert_eq!(update.phase, "local-error");
            assert!(update.publication_quorum.is_none());
            assert!(update.replica.is_none());
        }
        assert_eq!(
            std::fs::read(ready.root.join("membership-2/staging/PUBLICATION")).unwrap(),
            retained
        );
        drop(local);
        drop(peer);
    }
}

#[test]
fn managed_recovery_rejects_authenticated_reply_for_another_input_then_recovers() {
    let mut ready = fixture();
    let (mut local, peer) = gated_recovery(&mut ready, true);
    let before = local.cells[&PG].recovery_state_token().unwrap();
    let retained = std::fs::read(ready.root.join("membership-2/staging/PUBLICATION")).unwrap();
    peer.release();
    let rejected = until(&mut local, &ready.root, |update| {
        update.replica == Some(NodeId(3)) && update.detail.contains("peer recovery failed")
    });
    assert!(rejected.publication_quorum.is_none());
    assert!(rejected.detail.contains("canonical transition"));
    assert_eq!(local.cells[&PG].recovery_state_token().unwrap(), before);
    let accepted = until(&mut local, &ready.root, |update| {
        update.publication_quorum.is_some()
    });
    assert_eq!(
        accepted.publication_quorum.unwrap().plan_id(),
        ready.quorum.plan_id()
    );
    assert!(local.membership_admission_required().unwrap());
    assert_eq!(
        std::fs::read(ready.root.join("membership-2/staging/PUBLICATION")).unwrap(),
        retained
    );
    drop(local);
    drop(peer);
}
