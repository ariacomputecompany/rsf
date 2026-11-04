use super::*;

#[test]
fn real_tls_invalid_control_commit_cannot_replace_published_active() {
    let mut f = Fixture::new("control-invalid-commit");
    genesis(&mut f);
    provision_control(&mut f);
    let original = snapshot(&mut f);
    let prepare = crate::replication::tests::prepare(
        f.control.get_mut(&NodeId(0)).unwrap(),
        b"ordinary-data-is-not-control",
    );
    commit_local(&mut f, prepare);
    let invalid = snapshot(&mut f);
    let id = invalid.descriptor.id().unwrap();
    let running = Running::start(control_server(&f, limits()).unwrap());
    let sender = sender(&f, &running);
    let active = std::fs::read(f.root.join("learner-0/ACTIVE")).unwrap();
    send_snapshot(&sender, &invalid).unwrap();
    rejected(
        sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id)),
        "invalid control command header or size",
    );
    assert_eq!(
        std::fs::read(f.root.join("learner-0/ACTIVE")).unwrap(),
        active
    );
    assert!(running.applied.lock().unwrap().is_empty());
    // A clean semantic refusal retains the healthy published observation.
    let server = running.finish();
    assert_eq!(
        server.learners[&CONTROL]
            .control_observation()
            .unwrap()
            .committed_tail(),
        LogIndex(1)
    );
    let running = Running::start(server);
    let sender = self::sender(&f, &running);
    send_snapshot(&sender, &original).unwrap();
    sender
        .exchange_io(
            NodeId(3),
            HISTORY_FINISH,
            &finish_bytes(original.descriptor.id().unwrap()),
        )
        .unwrap();
    assert!(running.applied.lock().unwrap().is_empty());
    drop(running.finish());
    f.cleanup();
}

#[test]
fn real_tls_accepted_only_foreign_command_is_rejected_before_control_publication() {
    let mut f = Fixture::new("control-invalid-accepted");
    genesis(&mut f);
    provision_control(&mut f);
    crate::replication::tests::prepare(
        f.control.get_mut(&NodeId(0)).unwrap(),
        b"foreign-accepted-tail",
    );
    let invalid = snapshot(&mut f);
    let id = invalid.descriptor.id().unwrap();
    assert_eq!(invalid.descriptor.probe.committed_tail, LogIndex(1));
    let running = Running::start(control_server(&f, limits()).unwrap());
    let sender = sender(&f, &running);
    let active = std::fs::read(f.root.join("learner-0/ACTIVE")).unwrap();
    send_snapshot(&sender, &invalid).unwrap();
    rejected(
        sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id)),
        "invalid control command header or size",
    );
    assert_eq!(
        std::fs::read(f.root.join("learner-0/ACTIVE")).unwrap(),
        active
    );
    assert!(running.applied.lock().unwrap().is_empty());
    drop(running.finish());
    // Negative legacy fixture: generic VSR can publish these valid WAL bytes.
    // A policy-owning cold control open must still reject accepted-only content.
    provision_control(&mut f);
    let before = tree(&f.root.join("learner-0"));
    assert_eq!(
        control_server(&f, limits()).unwrap_err(),
        "invalid control command header or size"
    );
    assert_eq!(tree(&f.root.join("learner-0")), before);
    f.cleanup();
}

#[test]
fn real_tls_canonical_rejected_command_and_accepted_receipt_conflict_remain_valid_history() {
    let mut f = Fixture::new("control-rejected-is-log-work");
    genesis(&mut f);
    provision_control(&mut f);
    let mut rejected_intent = f.intent(41);
    rejected_intent.parent = CatalogRoot([79; 32]);
    let rejected_request = rejected_intent.request;
    let prepare = prepare_command(
        f.control.get_mut(&NodeId(0)).unwrap(),
        ControlCommand::Begin(rejected_intent.clone()),
    );
    commit_local(&mut f, prepare);
    // Same request, changed operation: a deterministic receipt conflict, never
    // substituted as new authority and never treated as malformed history.
    rejected_intent.parent = CatalogRoot([80; 32]);
    prepare_command(
        f.control.get_mut(&NodeId(0)).unwrap(),
        ControlCommand::Begin(rejected_intent),
    );
    let snapshot = snapshot(&mut f);
    let id = snapshot.descriptor.id().unwrap();
    let running = Running::start(control_server(&f, limits()).unwrap());
    let sender = sender(&f, &running);
    send_snapshot(&sender, &snapshot).unwrap();
    for _ in 0..2 {
        assert_eq!(
            sender
                .exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id))
                .unwrap(),
            2u64.to_le_bytes()
        );
    }
    assert!(running.applied.lock().unwrap().is_empty());
    let server = running.finish();
    let observed = server.learners[&CONTROL].control_observation().unwrap();
    assert_eq!(observed.committed_tail(), LogIndex(2));
    assert_eq!(observed.catalog(), &f.catalog);
    assert!(observed.pending().is_none());
    assert!(matches!(
        observed.request_outcome(rejected_request),
        Some(ControlOutcome::Rejected(_))
    ));
    drop(server);
    // Both the committed receipt and accepted suffix are rechecked on cold open.
    let server = control_server(&f, limits()).unwrap();
    assert_eq!(
        server.learners[&CONTROL]
            .control_observation()
            .unwrap()
            .committed_tail(),
        LogIndex(2)
    );
    drop(server);
    f.cleanup();
}

#[test]
fn exact_published_prefix_retry_runs_authoritative_control_validator_again() {
    let mut f = Fixture::new("control-exact-retry-semantic");
    genesis(&mut f);
    provision_control(&mut f);
    let snapshot = snapshot(&mut f);
    let id = snapshot.descriptor.id().unwrap();
    let mut learner = rsf_vsr::LearnerHistoryStore::open_published_existing(
        f.root.join("learner-0"),
        CONTROL,
        NodeId(3),
        NodeId(0),
        limits(),
    )
    .unwrap();
    assert_eq!(
        learner.begin_prefix(snapshot.descriptor, limits()).unwrap(),
        snapshot.records.len() as u64
    );
    learner
        .finish_prefix(id, |preview| {
            ControlState::validate_import_preview(&f.anchor, preview)
        })
        .unwrap();
    let before = tree(&f.root.join("learner-0"));
    let mut foreign = f.anchor.clone();
    foreign.genesis_catalog = CatalogRoot([51; 32]);
    assert!(
        learner
            .finish_prefix(id, |preview| ControlState::validate_import_preview(
                &foreign, preview
            ))
            .is_err()
    );
    assert_eq!(tree(&f.root.join("learner-0")), before);
    assert_eq!(
        learner
            .finish_prefix(id, |preview| ControlState::validate_import_preview(
                &f.anchor, preview
            ))
            .unwrap(),
        LogIndex(1)
    );
    assert_eq!(
        PublishedControlState::recover(&f.anchor, &learner)
            .unwrap()
            .catalog(),
        &f.catalog
    );
    drop(learner);
    f.cleanup();
}
