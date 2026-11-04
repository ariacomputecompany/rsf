use super::*;

#[test]
fn ordinary_election_materializes_actual_control_owner_and_receiver_before_confirmation() {
    let mut f = Fixture::new("control-domain-install");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    let source = f.control.get_mut(&NodeId(0)).unwrap();
    let genesis = prepare_command(source, ControlCommand::Genesis(f.catalog.clone()));
    let genesis_entry = commit_tls(source, &mut sender, genesis);
    let mut peers = f.peers.clone();
    peers.get_mut(&NodeId(1)).unwrap().address = running.address;
    let cells = [
        (CONTROL, f.control.remove(&NodeId(0)).unwrap()),
        (DATA, f.data.remove(&NodeId(0)).unwrap()),
    ]
    .into_iter()
    .collect();
    let mut candidate = PeerReplicaServer::new(
        NodeId(0),
        &cluster(),
        identity(&f.root, 0),
        peers,
        cells,
        Duration::from_millis(800),
    )
    .unwrap()
    .with_control_replication(f.anchor.clone(), f.catalog.clone())
    .unwrap();
    let update = candidate
        .poll_elections(
            PeerElectionTiming {
                heartbeat: Duration::from_millis(20),
                timeout: Duration::from_millis(3260),
                rank_delay: Duration::from_millis(20),
            },
            &mut |entry| {
                assert_ne!(entry.prepare.pg, CONTROL);
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(update.pg, CONTROL);
    assert_eq!(update.view, Epoch(2));
    assert!(update.quorum_confirmed, "{}", update.detail);
    assert_eq!(genesis_entry.prepare.view, Epoch(1));
    sender.transfer_committed(NodeId(1), genesis_entry).unwrap();
    assert!(!candidate.installation_pending.contains(&CONTROL));
    assert_eq!(
        ControlState::recover(&f.anchor, &candidate.cells[&CONTROL])
            .unwrap()
            .catalog(),
        &f.catalog
    );
    assert!(running.applied.lock().unwrap().is_empty());
    let server = running.finish();
    assert_eq!(server.cells[&CONTROL].view(), Epoch(2));
    assert!(!server.installation_pending.contains(&CONTROL));
    assert_eq!(
        ControlState::recover(&f.anchor, &server.cells[&CONTROL])
            .unwrap()
            .catalog(),
        &f.catalog
    );
    drop(server);
    drop(candidate);
    f.cleanup();
}

#[test]
fn installed_invalid_control_prefix_stays_pending_and_cannot_ack_finish_or_probe() {
    let mut f = Fixture::new("control-domain-install-invalid");
    // A real VSR certificate can certify bytes that are not a control command.
    // The configured receiver must enforce its own semantic replay boundary.
    let source = f.control.get_mut(&NodeId(0)).unwrap();
    let bad = prepare(source, b"not-control");
    f.control
        .get_mut(&NodeId(2))
        .unwrap()
        .receive_prepare(NodeId(2), NodeId(0), bad.clone())
        .unwrap();
    f.control
        .get_mut(&NodeId(0))
        .unwrap()
        .acknowledge(NodeId(2), bad.view, bad.index);
    assert!(matches!(
        f.control
            .get_mut(&NodeId(0))
            .unwrap()
            .commit(NodeId(0), bad.index)
            .unwrap(),
        VsrDecision::Committed(_)
    ));
    let request = rsf_vsr::VsrViewChangeRequest {
        pg: CONTROL,
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(0),
        voters: (0..3).map(NodeId).collect(),
    };
    let snapshots = [NodeId(0), NodeId(2)]
        .into_iter()
        .map(|node| {
            f.control
                .get_mut(&node)
                .unwrap()
                .begin_view_change(node, NodeId(0), request.clone())
                .unwrap()
        })
        .collect::<Vec<_>>();
    let input = rsf_vsr::VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    let running = Running::start(f.server.take().unwrap());
    let sender = f.transport(0, running.address);
    sender
        .exchange_io(NodeId(1), INSTALL_BEGIN, &input.encode().unwrap())
        .unwrap();
    let records = input
        .reports()
        .iter()
        .flat_map(|report| {
            snapshots
                .iter()
                .find(|snapshot| snapshot.report.replica == report.replica)
                .unwrap()
                .records
                .clone()
        })
        .collect::<Vec<_>>();
    let mut prefix = CONTROL.0.to_le_bytes().to_vec();
    prefix.extend_from_slice(&input.id().unwrap());
    let mut count = 0;
    while count < records.len() {
        let batch = rsf_vsr::VsrHistoryBatch::pack(count as u64, &records[count..]).unwrap();
        count += batch.records.len();
        let mut payload = prefix.clone();
        payload.extend_from_slice(&batch.encode().unwrap());
        sender
            .exchange_io(NodeId(1), INSTALL_BATCH, &payload)
            .unwrap();
    }
    rejected(
        sender.exchange_io(NodeId(1), INSTALL_FINISH, &prefix),
        "control command",
    );
    rejected(
        sender.exchange_io(NodeId(1), INSTALL_FINISH, &prefix),
        "control command",
    );
    assert!(running.applied.lock().unwrap().is_empty());
    let mut server = running.finish();
    assert_eq!(server.cells[&CONTROL].view(), Epoch(2));
    assert!(server.installation_pending.contains(&CONTROL));
    assert!(server.committed_application_entries().is_err());
    let probe = server.cells[&CONTROL]
        .primary_probe()
        .unwrap()
        .encode()
        .unwrap();
    let running = Running::start(server);
    let sender = f.transport(0, running.address);
    rejected(
        sender.exchange_io(NodeId(1), PRIMARY_PROBE, &probe),
        "control command",
    );
    let server = running.finish();
    assert!(server.installation_pending.contains(&CONTROL));
    drop(server);
    f.cleanup();
}
