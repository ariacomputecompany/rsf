use super::*;

#[cfg(unix)]
#[test]
fn managed_learner_poll_rejects_lost_default_manifest_before_dispatch_or_promise() {
    let fixture = accepted_fixture();
    let root = &fixture.root;
    let pg = PlacementGroupId(1);
    // A real learner can still be at the original published bootstrap when a
    // transition begins. The retained old approval contains its later prefix.
    let mut source = DurableVsrCell::open(
        root.join("learner-source"),
        pg,
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    let snapshot = source.history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let mut learner =
        rsf_vsr::LearnerHistoryStore::open(root.join("learner-3"), pg, NodeId(3), NodeId(0))
            .unwrap();
    assert_eq!(learner.begin(snapshot.descriptor).unwrap(), 0);
    for (ordinal, record) in snapshot.records.into_iter().enumerate() {
        learner.append(id, ordinal as u64, record).unwrap();
    }
    learner.finish(id).unwrap();
    drop(source);
    let mut server = PeerReplicaServer::with_learners(
        NodeId(3),
        &cluster(),
        identity(root, 3),
        all_endpoints(root),
        BTreeMap::new(),
        [(pg, learner)].into(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_membership_paths([(pg, root.join("membership-3"))].into())
    .unwrap()
    .with_promotion_paths([(pg, root.join("store-3"))].into())
    .unwrap();
    server
        .initialize_membership_decision(&fixture.authority)
        .unwrap();
    let mut server = server
        .with_membership_recovery(
            [(
                pg,
                crate::replication::PeerMembershipRecoveryTiming {
                    interval: Duration::from_millis(1),
                },
            )]
            .into(),
        )
        .unwrap();
    let before = server
        .with_membership_decision(pg, |store, _| store.decision_state().unwrap())
        .unwrap();
    let inner = root.join("learner-3").join(id_name(id)).join("cell");
    assert!(!inner.join("VSR_ACTIVE").exists());
    std::fs::remove_file(inner.join("MANIFEST")).unwrap();
    let update = server
        .poll_membership_recovery(&mut |_| panic!("no application work before custody validation"))
        .unwrap()
        .unwrap();
    assert_eq!(update.phase, "local-error");
    assert!(
        update.replica.is_none(),
        "no transport dispatch may precede this check"
    );
    let after = server
        .with_membership_decision(pg, |store, _| store.decision_state().unwrap())
        .unwrap();
    assert_eq!(after, before);
    assert!(server.membership_recovery_enabled());
    assert!(
        !inner.join("MANIFEST").exists(),
        "poll cannot repair lost published custody"
    );
}
