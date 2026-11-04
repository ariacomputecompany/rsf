use super::*;
mod fixture;
use fixture::*;
mod context;
mod semantics;
mod status;

#[test]
fn real_tls_control_learner_finish_is_observation_only_and_rechecks_same_id_custody() {
    let mut f = Fixture::new("control-domain-learner");
    let prepare = prepare_command(
        f.control.get_mut(&NodeId(0)).unwrap(),
        ControlCommand::Genesis(f.catalog.clone()),
    );
    f.control
        .get_mut(&NodeId(2))
        .unwrap()
        .receive_prepare(NodeId(2), NodeId(0), prepare.clone())
        .unwrap();
    f.control
        .get_mut(&NodeId(0))
        .unwrap()
        .acknowledge(NodeId(2), prepare.view, prepare.index);
    assert!(matches!(
        f.control
            .get_mut(&NodeId(0))
            .unwrap()
            .commit(NodeId(0), prepare.index)
            .unwrap(),
        VsrDecision::Committed(_)
    ));
    // Trusted test provisioning precedes the mandatory existing-anchor builder.
    // Production first Genesis publication remains a distinct prerequisite.
    provision_control(&mut f);
    let server = control_server(&f, limits()).unwrap();
    let running = Running::start(server);
    let mut peers = f.peers.clone();
    peers.get_mut(&NodeId(3)).unwrap().address = running.address;
    let sender = NetworkVsrTransport::new(
        NodeId(0),
        &cluster(),
        identity(&f.root, 0),
        peers,
        Duration::from_millis(800),
    )
    .unwrap();
    let snapshot = f
        .control
        .get_mut(&NodeId(0))
        .unwrap()
        .history_snapshot()
        .unwrap();
    let id = snapshot.descriptor.id().unwrap();
    send_snapshot(&sender, &snapshot).unwrap();
    let mut finish = CONTROL.0.to_le_bytes().to_vec();
    finish.extend_from_slice(&id);
    sender
        .exchange_io(NodeId(3), HISTORY_FINISH, &finish)
        .unwrap();
    sender
        .exchange_io(NodeId(3), HISTORY_FINISH, &finish)
        .unwrap();
    assert!(running.applied.lock().unwrap().is_empty());
    let mut server = running.finish();
    assert!(server.cells.is_empty());
    let observed = server.learners[&CONTROL].control_observation().unwrap();
    assert_eq!(observed.catalog(), &f.catalog);
    assert!(server.committed_application_entries().unwrap().is_empty());
    let active = f.root.join("learner-0/ACTIVE");
    let bytes = std::fs::read(&active).unwrap();
    let running = Running::start(server);
    let mut peers = f.peers.clone();
    peers.get_mut(&NodeId(3)).unwrap().address = running.address;
    let sender = NetworkVsrTransport::new(
        NodeId(0),
        &cluster(),
        identity(&f.root, 0),
        peers,
        Duration::from_millis(800),
    )
    .unwrap();
    std::fs::remove_file(&active).unwrap();
    let missing = tree(&f.root.join("learner-0"));
    rejected(sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish), "");
    assert!(running.applied.lock().unwrap().is_empty());
    assert_eq!(tree(&f.root.join("learner-0")), missing);
    assert!(!active.exists());
    // The queued learner custody latch makes restored bytes unable to revive
    // the already failed process-local owner or its prior observation.
    std::fs::write(&active, bytes).unwrap();
    let restored = tree(&f.root.join("learner-0"));
    rejected(
        sender.exchange_io(NodeId(3), HISTORY_FINISH, &finish),
        "learner durability failed",
    );
    assert!(running.applied.lock().unwrap().is_empty());
    assert_eq!(tree(&f.root.join("learner-0")), restored);
    let server = running.finish();
    assert!(server.learners[&CONTROL].control_observation().is_err());
    drop(server);
    f.cleanup();
}
