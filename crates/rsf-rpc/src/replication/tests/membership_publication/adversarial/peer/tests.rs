use super::*;

#[test]
fn real_tls_expired_ordinary_publication_reply_keeps_peer_and_exact_receipt_live() {
    let mut ready = fixture();
    drop(ready.remote.take());
    let root = ready.root.clone();
    let before_applies = std::fs::read(root.join("materialized-3/.applies")).unwrap();
    let remote = server(&root, 3, true, ready.peers.clone()).unwrap();
    let peer = GatedPeer::start_inner(remote, root.clone(), false, false, true);
    ready.peers.get_mut(&NodeId(3)).unwrap().address = peer.address;
    let network = ready.network();

    // The first real dispatcher call finishes publication and application. Its
    // reply then expires at the original 800 ms deadline. Only the transport's
    // existing one lost-reply retry can obtain this result; no test retry loop.
    let report = network
        .publish_membership(NodeId(3), &ready.quorum)
        .expect("the same peer must serve the existing bounded publication retry");
    let expired = peer.expired.recv_timeout(RPC).unwrap();
    let failure = peer.failures.recv_timeout(RPC).unwrap();
    assert_eq!(failure.node, NodeId(3));
    assert_eq!(failure.sender, Some(NodeId(2)));
    assert_eq!(failure.opcode, Some(MEMBERSHIP_PUBLISH));
    assert_eq!(failure.phase, "reply");
    assert_eq!(failure.kind, io::ErrorKind::TimedOut);
    assert_eq!(failure.detail, "peer RPC deadline expired");
    assert_eq!(report.replica, NodeId(3));
    assert_eq!(report.plan, ready.quorum.plan_id());
    assert_eq!(report.input, ready.chosen.input_id());
    assert_eq!(report.proposal, *ready.quorum.proposal());

    // Publication already existed before the failed write. Retry cannot add a
    // generation, rewrite WAL/selected input, or replace original PUBLICATION.
    assert_eq!(inventory(&root, 3), expired.inventory);
    let publication = root.join("membership-3/staging/PUBLICATION");
    assert_eq!(std::fs::read(&publication).unwrap(), expired.retained);
    assert_eq!(
        std::fs::read(root.join("materialized-3/1")).unwrap(),
        ready.committed.encode().unwrap()
    );
    assert!(!root.join("materialized-3/2").exists());
    assert!(expired.applies.starts_with(&before_applies));
    let first_callbacks = &expired.applies[before_applies.len()..];
    assert!(
        !first_callbacks.is_empty(),
        "first publication must apply before replying"
    );
    assert!(first_callbacks.len().is_multiple_of(8));
    let index = 1_u64.to_le_bytes();
    assert!(first_callbacks.chunks_exact(8).all(|entry| entry == index));
    let mut expected_applies = expired.applies;
    expected_applies.extend_from_slice(&index);
    assert_eq!(
        std::fs::read(root.join("materialized-3/.applies")).unwrap(),
        expected_applies,
        "one historical retry must freshly apply the current one-entry prefix"
    );

    let queried = network
        .query_membership_publication(NodeId(3), PG, ready.quorum.plan_id())
        .expect("healthy peer must continue serving after the failed connection");
    assert_eq!(queried, report);
    expected_applies.extend_from_slice(&index);
    let status = network.replica_status(NodeId(3), PG).unwrap();
    assert_eq!(status.normal.view, Epoch(2));
    assert_eq!(status.normal.committed_tail, LogIndex(1));
    assert_eq!(inventory(&root, 3), expired.inventory);
    assert_eq!(std::fs::read(&publication).unwrap(), expired.retained);
    assert_eq!(
        std::fs::read(root.join("materialized-3/.applies")).unwrap(),
        expected_applies,
        "publication query reapplies the prefix; status has no callback"
    );
    drop(peer);

    let promoted = DurableVsrCell::open_existing(root.join("store-3"), PG).unwrap();
    assert_eq!(promoted.promoted_replica().unwrap(), Some(NodeId(3)));
    assert_eq!(promoted.view(), Epoch(2));
    assert_eq!(
        promoted.committed_operation(LogIndex(1)),
        Some(&ready.committed.operation)
    );
    assert_eq!(
        promoted.prepared(LogIndex(2)).unwrap().operation,
        ready.accepted.operation
    );
    assert_eq!(promoted.request_commit(&ready.request), Some(LogIndex(1)));
    assert!(
        promoted
            .membership_publication(ready.quorum.plan_id())
            .unwrap()
            .is_some()
    );
    drop(promoted);
}
