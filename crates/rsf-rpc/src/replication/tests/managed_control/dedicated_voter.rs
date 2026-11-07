//! Dedicated inventory still authenticates the complete catalog and actual cell.
use super::*;

#[test]
fn dedicated_control_voter_keeps_full_policy_with_exact_one_actual_owner() {
    let mut f = Fixture::unbound("dedicated-control-voter");
    let cell = f.control.remove(&NodeId(0)).unwrap();
    let mut server = PeerReplicaServer::control_voter(
        NodeId(0),
        &cluster(),
        identity(&f.root, 0),
        f.peers.clone(),
        cell,
        f.anchor.clone(),
        f.catalog.clone(),
        Duration::from_millis(800),
    )
    .unwrap();
    assert_eq!(
        server.cells.keys().copied().collect::<Vec<_>>(),
        vec![CONTROL]
    );
    assert!(server.learners.is_empty());
    assert_eq!(server.control_primary().unwrap(), NodeId(0));
    assert_eq!(server.cells[&CONTROL].next_index(), LogIndex(1));
    assert!(
        server
            .control_stage_supported(CONTROL)
            .unwrap_err()
            .contains("requires sealed control replay custody")
    );
    // Observe the full catalog through the actual writer and persisted command,
    // without exposing private Binding fields solely for this test.
    server.request_control_genesis().unwrap();
    assert!(matches!(
        server.poll_control_writer().unwrap(),
        ControlWriterProgress::Accepted { index: LogIndex(1) }
    ));
    let rsf_vsr::VsrOperation::ReplicatedCommand { payload, .. } = &server.cells[&CONTROL]
        .prepared(LogIndex(1))
        .unwrap()
        .operation
    else {
        panic!("dedicated control writer did not prepare a catalog command");
    };
    assert_eq!(
        ControlCommand::decode(payload).unwrap(),
        ControlCommand::Genesis(f.catalog.clone())
    );
    drop(server);
    // The preexisting mixed constructor still rejects a partial inventory.
    let cell = DurableVsrCell::open_existing(f.root.join("pg-0/node-0"), CONTROL).unwrap();
    let partial = PeerReplicaServer::new(
        NodeId(0),
        &cluster(),
        identity(&f.root, 0),
        f.peers.clone(),
        [(CONTROL, cell)].into(),
        Duration::from_millis(800),
    )
    .unwrap();
    assert_eq!(
        partial
            .with_control_replication(f.anchor.clone(), f.catalog.clone())
            .unwrap_err(),
        "control provisioning requires the exact hosted group inventory"
    );
}

#[test]
fn dedicated_control_voter_rejects_foreign_actual_owner_and_full_catalog_tls_mismatch() {
    for damage in ["pg", "tls", "role"] {
        let mut f = Fixture::unbound("dedicated-control-refusal");
        let mut peers = f.peers.clone();
        let local = if damage == "role" {
            NodeId(3)
        } else {
            NodeId(0)
        };
        let cell = if damage == "pg" {
            f.data.remove(&NodeId(0)).unwrap()
        } else {
            f.control.remove(&NodeId(0)).unwrap()
        };
        if damage == "tls" {
            peers.get_mut(&NodeId(2)).unwrap().fingerprint = [77; 32];
        }
        let failure = PeerReplicaServer::control_voter(
            local,
            &cluster(),
            identity(&f.root, local.0),
            peers,
            cell,
            f.anchor.clone(),
            f.catalog.clone(),
            Duration::from_millis(800),
        )
        .unwrap_err();
        if damage == "tls" {
            assert!(failure.contains("certificate"), "{failure}");
        } else {
            assert_eq!(
                failure,
                "dedicated control voter requires actual original voting owner"
            );
        }
    }
}
