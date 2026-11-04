use super::*;

#[test]
fn real_tls_control_cold_record_and_finish_require_begin_even_for_active_id() {
    let mut f = Fixture::new("control-cold-context");
    genesis(&mut f);
    provision_control(&mut f);
    let snapshot = snapshot(&mut f);
    let id = snapshot.descriptor.id().unwrap();
    let running = Running::start(control_server(&f, limits()).unwrap());
    let sender = sender(&f, &running);
    let before = tree(&f.root.join("learner-0"));
    begin_required(
        sender.exchange_io(NodeId(3), HISTORY_RECORD, &record_bytes(&snapshot, 0)),
        false,
        id,
    );
    begin_required(
        sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id)),
        true,
        id,
    );
    let mut malformed = finish_bytes(id);
    malformed.extend_from_slice(&0u64.to_le_bytes());
    malformed.push(0);
    begin_required(
        sender.exchange_io(NodeId(3), HISTORY_RECORD, &malformed),
        false,
        id,
    );
    assert_eq!(tree(&f.root.join("learner-0")), before);
    send_snapshot(&sender, &snapshot).unwrap();
    assert_eq!(
        sender
            .exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id))
            .unwrap(),
        1u64.to_le_bytes()
    );
    assert!(running.applied.lock().unwrap().is_empty());
    drop(running.finish());
    // Reopen reconstructs policy before any read; volatile context is not an ACK.
    let running = Running::start(control_server(&f, limits()).unwrap());
    let sender = self::sender(&f, &running);
    begin_required(
        sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id)),
        true,
        id,
    );
    send_snapshot(&sender, &snapshot).unwrap();
    sender
        .exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id))
        .unwrap();
    assert!(running.applied.lock().unwrap().is_empty());
    drop(running.finish());
    f.cleanup();
}

#[test]
fn control_policy_before_open_refuses_raw_enrollment_and_bounded_cold_history() {
    let mut f = Fixture::new("control-policy-before-open");
    genesis(&mut f);
    provision_control(&mut f);
    let before = tree(&f.root.join("learner-0"));
    let raw =
        rsf_vsr::LearnerHistoryStore::open(f.root.join("learner-0"), CONTROL, NodeId(3), NodeId(0))
            .unwrap();
    let data =
        rsf_vsr::LearnerHistoryStore::open(f.root.join("learner-1"), DATA, NodeId(3), NodeId(2))
            .unwrap();
    let raw_server = PeerReplicaServer::with_learners(
        NodeId(3),
        &cluster(),
        identity(&f.root, 3),
        f.peers.clone(),
        BTreeMap::new(),
        [(CONTROL, raw), (DATA, data)].into(),
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(
        raw_server
            .with_control_replication(f.anchor.clone(), f.catalog.clone())
            .unwrap_err()
            .contains("policy before bounded cold open")
    );
    let count = snapshot(&mut f).descriptor.records;
    let mut too_small = limits();
    too_small.max_records = count - 1;
    assert!(control_server(&f, too_small).is_err());
    let mut too_small = limits();
    too_small.max_encoded_bytes = 18;
    assert!(control_server(&f, too_small).is_err());
    assert_eq!(tree(&f.root.join("learner-0")), before);
    // Invalid immutable trust is rejected before even trying to open this path.
    let absent = f.root.join("must-not-open");
    let mut wrong = f.anchor.clone();
    wrong.genesis_catalog = CatalogRoot([91; 32]);
    assert!(
        unbound_server(&f)
            .with_control_learner_replication(
                wrong,
                f.catalog.clone(),
                &absent,
                NodeId(0),
                limits()
            )
            .is_err()
    );
    assert!(!absent.exists());
    assert!(control_server(&f, limits()).is_ok());
    f.cleanup();
}

#[test]
fn real_tls_control_bounded_begin_rejects_oversize_then_accepts_valid_descriptor() {
    let mut f = Fixture::new("control-begin-limits");
    genesis(&mut f);
    provision_control(&mut f);
    let snapshot = snapshot(&mut f);
    let id = snapshot.descriptor.id().unwrap();
    let mut bound = limits();
    bound.max_records = snapshot.descriptor.records;
    let running = Running::start(control_server(&f, bound).unwrap());
    let sender = sender(&f, &running);
    let before = tree(&f.root.join("learner-0"));
    let mut oversized = snapshot.descriptor.clone();
    oversized.records += 1;
    rejected(
        sender.exchange_io(NodeId(3), HISTORY_BEGIN, &oversized.encode().unwrap()),
        "record/byte budget",
    );
    begin_required(
        sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id)),
        true,
        id,
    );
    assert_eq!(tree(&f.root.join("learner-0")), before);
    send_snapshot(&sender, &snapshot).unwrap();
    sender
        .exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id))
        .unwrap();
    assert!(running.applied.lock().unwrap().is_empty());
    drop(running.finish());
    f.cleanup();
}

#[test]
fn real_tls_control_only_constructor_enforces_cold_context_and_semantic_policy() {
    let mut f = Fixture::unbound("control-only-learner");
    // The trusted catalog remains complete, including its data PG. This
    // dedicated constructor hosts only the reserved control learner locally.
    genesis(&mut f);
    provision_control(&mut f);
    let open = |f: &Fixture| {
        PeerReplicaServer::control_learner(
            NodeId(3),
            &cluster(),
            identity(&f.root, 3),
            f.peers.clone(),
            f.root.join("learner-0"),
            ControlLearnerPolicy {
                anchor: f.anchor.clone(),
                genesis: f.catalog.clone(),
                source: NodeId(0),
                limits: limits(),
            },
            Duration::from_millis(800),
        )
    };
    let server = open(&f).unwrap();
    assert!(server.cells.is_empty());
    assert_eq!(
        server.learners.keys().copied().collect::<Vec<_>>(),
        vec![CONTROL]
    );
    let snapshot = snapshot(&mut f);
    let id = snapshot.descriptor.id().unwrap();
    let running = Running::start(server);
    let sender = sender(&f, &running);
    let before = tree(&f.root.join("learner-0"));
    begin_required(
        sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id)),
        true,
        id,
    );
    assert_eq!(tree(&f.root.join("learner-0")), before);
    send_snapshot(&sender, &snapshot).unwrap();
    sender
        .exchange_io(NodeId(3), HISTORY_FINISH, &finish_bytes(id))
        .unwrap();
    assert!(running.applied.lock().unwrap().is_empty());
    drop(running.finish());
    crate::replication::tests::prepare(
        f.control.get_mut(&NodeId(0)).unwrap(),
        b"invalid-only-control-tail",
    );
    provision_control(&mut f);
    let before = tree(&f.root.join("learner-0"));
    assert_eq!(
        open(&f).unwrap_err(),
        "invalid control command header or size"
    );
    assert_eq!(tree(&f.root.join("learner-0")), before);
    // The ordinary constructor remains unable to manufacture an empty server.
    assert!(
        PeerReplicaServer::with_learners(
            NodeId(3),
            &cluster(),
            identity(&f.root, 3),
            f.peers.clone(),
            BTreeMap::new(),
            BTreeMap::new(),
            Duration::from_millis(800)
        )
        .is_err()
    );
    f.cleanup();
}

#[test]
fn control_only_constructor_rejects_invalid_policy_before_touching_absent_root() {
    let f = Fixture::unbound("control-only-invalid-policy");
    for fault in ["fingerprint", "limits"] {
        let path = f.root.join(format!("must-not-open-{fault}"));
        let mut policy = ControlLearnerPolicy {
            anchor: f.anchor.clone(),
            genesis: f.catalog.clone(),
            source: NodeId(0),
            limits: limits(),
        };
        let reason = match fault {
            "fingerprint" => {
                policy.genesis.nodes[3].certificate = [91; 32];
                policy.anchor.genesis_catalog = policy.genesis.root().unwrap();
                "certificate mapping"
            }
            "limits" => {
                policy.limits.max_records = 0;
                "finite history limits"
            }
            _ => unreachable!(),
        };
        let error = PeerReplicaServer::control_learner(
            NodeId(3),
            &cluster(),
            identity(&f.root, 3),
            f.peers.clone(),
            &path,
            policy,
            Duration::from_millis(800),
        )
        .unwrap_err();
        assert!(error.contains(reason), "{error}");
        assert!(
            !path.exists(),
            "pure policy rejection must precede cold open"
        );
    }
    f.cleanup();
}
