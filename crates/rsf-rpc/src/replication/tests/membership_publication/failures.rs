use super::*;

#[test]
fn real_tls_membership_publication_rejects_wrong_authority_missing_apply_and_corrupt_owner() {
    let mut r = fixture();
    let path = r.root.clone();
    let network = r.network();
    let old = transport_for(&path, 0, r.peers.clone());
    assert!(
        old.exchange_once(NodeId(3), MEMBERSHIP_PUBLISH, &r.quorum.encode().unwrap())
            .is_err()
    );
    for field in ["input", "plan"] {
        let mut reports = r.quorum.reports().to_vec();
        for report in &mut reports {
            if field == "input" {
                report.input[0] ^= 1;
            } else {
                report.plan[0] ^= 1;
            }
        }
        let invalid = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
        assert!(network.publish_membership(NodeId(3), &invalid).is_err());
    }
    assert!(!path.join("store-3").exists());
    assert!(!path.join("membership-3/staging/PUBLICATION").exists());
    r.restart_remote(false);
    let network = r.network();
    let error = network
        .publish_membership(NodeId(3), &r.quorum)
        .unwrap_err();
    assert!(error.reason.contains("materializer"), "{}", error.reason);
    let error = network
        .query_membership_publication(NodeId(3), PG, r.quorum.plan_id())
        .unwrap_err();
    assert!(error.reason.contains("materializer"), "{}", error.reason);
    assert!(!path.join("store-3").exists());
    r.restart_remote(true);
    std::fs::write(path.join("fail-apply-3"), b"fail cold materialization").unwrap();
    let error = r
        .network()
        .publish_membership(NodeId(3), &r.quorum)
        .unwrap_err();
    assert!(
        error
            .reason
            .contains("injected publication materializer failure"),
        "{}",
        error.reason
    );
    assert!(!path.join("membership-3/staging/PUBLICATION").exists());
    assert!(!path.join("store-3").exists());
    r.restart_local();
    let quorum = r.quorum.clone();
    assert!(
        r.local()
            .publish_membership_locally(&quorum, &mut |_| Err(
                "injected local publication materializer failure".into()
            ))
            .is_err()
    );
    assert!(!path.join("store-2/VSR_ACTIVE").exists());
    assert!(!path.join("membership-2/staging/PUBLICATION").exists());
    // Fail only the post-publication application pass: the first stage pass
    // completes, PUBLICATION is retained, and the active pointer is durable,
    // but no publication ACK is returned. Cold retry must not publish again.
    r.restart_local();
    let mut applies = 0;
    assert!(
        r.local()
            .publish_membership_locally(&quorum, &mut |entry| {
                applies += 1;
                if applies == 2 {
                    return Err("injected apply failure after durable publication".into());
                }
                persist_staged_entry(&path, 2, entry)
            })
            .is_err()
    );
    assert_eq!(applies, 2);
    let partial = inventory(&path, 2);
    assert!(!path.join("store-3").exists());
    r.restart_local();
    std::fs::remove_file(path.join("fail-apply-3")).unwrap();
    r.restart_remote(true);
    r.publish();
    assert_eq!(inventory(&path, 2), partial);
    let before = inventory(&path, 3);
    let mut unknown = r.quorum.plan_id();
    unknown[0] ^= 1;
    assert!(
        r.network()
            .query_membership_publication(NodeId(3), PG, unknown)
            .is_err()
    );
    assert!(
        r.network()
            .query_membership_publication(NodeId(3), PlacementGroupId(9), r.quorum.plan_id())
            .is_err()
    );
    let mut query = PG.0.to_le_bytes().to_vec();
    query.extend_from_slice(&r.quorum.plan_id());
    let old = transport_for(&path, 0, r.peers.clone());
    assert!(
        old.exchange_once(NodeId(3), MEMBERSHIP_PUBLICATION, &query)
            .is_err()
    );
    assert_eq!(inventory(&path, 3), before);
    r.restart_remote(false);
    let error = r
        .network()
        .query_membership_publication(NodeId(3), PG, r.quorum.plan_id())
        .unwrap_err();
    assert!(error.reason.contains("materializer"), "{}", error.reason);
    let error = r
        .network()
        .publish_membership(NodeId(3), &r.quorum)
        .unwrap_err();
    assert!(error.reason.contains("materializer"), "{}", error.reason);
    assert_eq!(inventory(&path, 3), before);
    drop(r.remote.take());
    let learner = std::fs::read(path.join("learner-3/ACTIVE")).unwrap();
    std::fs::write(
        path.join("store-3/VSR_PROMOTION"),
        b"corrupt owned identity",
    )
    .unwrap();
    assert!(PublicationPeer::start(&path, 3, true, true).is_err());
    assert_eq!(
        std::fs::read(path.join("store-3/VSR_PROMOTION")).unwrap(),
        b"corrupt owned identity"
    );
    assert_eq!(
        std::fs::read(path.join("learner-3/ACTIVE")).unwrap(),
        learner
    );
}
