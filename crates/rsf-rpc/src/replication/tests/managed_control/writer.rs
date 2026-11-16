use super::*;
use crate::replication::managed_control::writer::{identity::Context, observation};

fn genesis(f: &mut Fixture, transport: &mut NetworkVsrTransport) {
    let prepare = prepare_command(
        f.control.get_mut(&NodeId(0)).unwrap(),
        ControlCommand::Genesis(f.catalog.clone()),
    );
    commit_tls(f.control.get_mut(&NodeId(0)).unwrap(), transport, prepare);
}
fn primary(f: &mut Fixture, address: SocketAddr) -> PeerReplicaServer {
    let mut peers = f.peers.clone();
    peers.get_mut(&NodeId(1)).unwrap().address = address;
    PeerReplicaServer::new(
        NodeId(0),
        &cluster(),
        identity(&f.root, 0),
        peers,
        [
            (CONTROL, f.control.remove(&NodeId(0)).unwrap()),
            (DATA, f.data.remove(&NodeId(0)).unwrap()),
        ]
        .into_iter()
        .collect(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_control_replication(f.anchor.clone(), f.catalog.clone())
    .unwrap()
}
fn observed(writer: &mut PeerReplicaServer) -> ObservedOldControlDelivery {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = String::new();
    while Instant::now() < deadline {
        match writer.poll_control_writer().unwrap() {
            ControlWriterProgress::ObservedOldControlDelivery(observed) => return observed,
            progress => last = format!("{progress:?}"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("bounded control writer did not observe delivery: {last}");
}
#[test]
fn real_tls_accepted_only_follower_cannot_report_local_primary_commit_then_exact_retry_repairs() {
    let mut f = Fixture::new("control-writer-accepted-only");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    genesis(&mut f, &mut sender);
    let intent = f.intent(71);
    let source = f.control.get_mut(&NodeId(0)).unwrap();
    let prepare = prepare_command(source, ControlCommand::Begin(intent.clone()));
    sender
        .acknowledge_prepare(VsrPrepareMessage {
            replica: NodeId(1),
            prepare: prepare.clone(),
        })
        .unwrap();
    source.acknowledge(NodeId(1), prepare.view, prepare.index);
    assert!(matches!(
        source.commit(NodeId(0), prepare.index).unwrap(),
        VsrDecision::Committed(_)
    ));
    let state = ControlState::recover(&f.anchor, source).unwrap();
    let context = Context::recover(&state, source).unwrap();
    let before = tree(&f.root.join("pg-0/node-1"));
    let rejection = observation::remote(&sender, NodeId(1), &context).unwrap_err();
    assert!(
        rejection.starts_with("peer rejected request: "),
        "{rejection}"
    );
    assert!(
        rejection.contains("no committed pending membership intent"),
        "{rejection}"
    );
    assert_eq!(tree(&f.root.join("pg-0/node-1")), before);
    let mut collector = observation::Collector::new(context.clone());
    collector
        .insert(
            observation::local(&state, source, NodeId(0), identity(&f.root, 0).fingerprint)
                .unwrap(),
        )
        .unwrap();
    assert!(
        collector.observed().is_none(),
        "local commit is not a delivered majority"
    );
    let mut writer = primary(&mut f, running.address);
    writer.request_membership_intent(intent.clone()).unwrap();
    let delivered = observed(&mut writer);
    assert_eq!(delivered.begin_index(), prepare.index);
    assert_eq!(delivered.reporters(), &[NodeId(0), NodeId(1)]);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(3));
    assert_eq!(
        writer.cells[&CONTROL]
            .prepared(prepare.index)
            .unwrap()
            .operation,
        prepare.operation
    );
    // Every original reply may be lost: discard volatile collection and reopen
    // the actual primary. Exact queries reconstruct, without another Begin.
    drop(writer);
    f.control.insert(
        NodeId(0),
        DurableVsrCell::open_existing(f.root.join("pg-0/node-0"), CONTROL).unwrap(),
    );
    f.data.insert(
        NodeId(0),
        DurableVsrCell::open_existing(f.root.join("pg-1/node-0"), DATA).unwrap(),
    );
    let mut writer = primary(&mut f, running.address);
    let recollected = observed(&mut writer);
    assert_eq!(recollected.intent(), delivered.intent());
    assert_eq!(recollected.begin_index(), delivered.begin_index());
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(3));
    assert!(running.applied.lock().unwrap().is_empty());
    drop(writer);
    drop(running.finish());
    f.cleanup();
}
#[test]
fn real_tls_writer_reprepares_earliest_stable_operation_before_new_request_and_keeps_origin() {
    let mut f = Fixture::new("control-writer-reprepare");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    genesis(&mut f, &mut sender);
    let intent = f.intent(72);
    let source = f.control.get_mut(&NodeId(0)).unwrap();
    let original = prepare_command(source, ControlCommand::Begin(intent.clone()));
    sender
        .acknowledge_prepare(VsrPrepareMessage {
            replica: NodeId(1),
            prepare: original.clone(),
        })
        .unwrap();
    let mut follower = running.finish();
    assert!(matches!(
        source.view_change(Epoch(2), NodeId(0)),
        VsrDecision::ViewChanged { .. }
    ));
    assert!(matches!(
        follower
            .cells
            .get_mut(&CONTROL)
            .unwrap()
            .view_change(Epoch(2), NodeId(0)),
        VsrDecision::ViewChanged { .. }
    ));
    let running = Running::start(follower);
    let mut writer = primary(&mut f, running.address);
    writer.request_membership_intent(intent.clone()).unwrap();
    assert!(
        matches!(writer.poll_control_writer().unwrap(), ControlWriterProgress::Accepted { index } if index == original.index)
    );
    assert_eq!(
        writer.cells[&CONTROL]
            .prepared(original.index)
            .unwrap()
            .operation,
        original.operation
    );
    assert_eq!(
        writer.cells[&CONTROL]
            .prepared(original.index)
            .unwrap()
            .view,
        Epoch(2)
    );
    let delivered = observed(&mut writer);
    assert_eq!(delivered.begin_index(), original.index);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(3));
    // Exact committed retry bypasses new-proposal view freshness; changed bytes
    // under the same request are a receipt conflict and cannot append.
    let mut changed = intent.clone();
    changed.groups[0].proposal.election.target_view = Epoch(3);
    assert!(
        writer
            .request_membership_intent(changed)
            .unwrap_err()
            .contains("conflicts with its retained canonical command")
    );
    writer.request_membership_intent(intent.clone()).unwrap();
    assert_eq!(observed(&mut writer).begin_index(), original.index);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(3));
    assert!(running.applied.lock().unwrap().is_empty());
    drop(writer);
    drop(running.finish());
    f.cleanup();
}
#[test]
fn real_tls_control_observation_uses_historical_custody_under_pending_ordinary_promise() {
    let mut f = Fixture::new("control-writer-promised-report");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    genesis(&mut f, &mut sender);
    let intent = f.intent(73);
    let source = f.control.get_mut(&NodeId(0)).unwrap();
    let prepare = prepare_command(source, ControlCommand::Begin(intent));
    commit_tls(source, &mut sender, prepare);
    let state = ControlState::recover(&f.anchor, source).unwrap();
    let context = Context::recover(&state, source).unwrap();
    let local_report =
        observation::local(&state, source, NodeId(0), identity(&f.root, 0).fingerprint).unwrap();
    let mut follower = running.finish();
    follower
        .cells
        .get_mut(&CONTROL)
        .unwrap()
        .begin_view_change(
            NodeId(1),
            NodeId(2),
            rsf_vsr::VsrViewChangeRequest {
                pg: CONTROL,
                previous_view: Epoch(1),
                target_view: Epoch(2),
                candidate: NodeId(2),
                voters: (0..3).map(NodeId).collect(),
            },
        )
        .unwrap();
    assert!(follower.cells[&CONTROL].primary_probe().is_err());
    let running = Running::start(follower);
    let sender = f.transport(0, running.address);
    let mut collector = observation::Collector::new(context.clone());
    collector.insert(local_report).unwrap();
    collector
        .insert(observation::remote(&sender, NodeId(1), &context).unwrap())
        .unwrap();
    let observed = collector.observed().unwrap();
    assert_eq!(observed.reporters(), &[NodeId(0), NodeId(1)]);
    let before = tree(&f.root.join("pg-0/node-1"));
    // Authenticated learner 3 is not an old-voter collector.
    let error =
        observation::remote(&f.transport(3, running.address), NodeId(1), &context).unwrap_err();
    assert!(
        error.starts_with("peer rejected request: ")
            && error.contains("not an original control voter"),
        "{error}"
    );
    assert_eq!(tree(&f.root.join("pg-0/node-1")), before);
    drop(running.finish());
    f.cleanup();
}

#[test]
fn real_tls_late_prepare_after_local_commit_delivers_to_surviving_majority() {
    let mut f = Fixture::new("control-writer-late-prepare");
    let one = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, one.address);
    genesis(&mut f, &mut sender);
    let prefix = f.control[&NodeId(0)].committed_prefix().unwrap();
    f.control
        .get_mut(&NodeId(2))
        .unwrap()
        .catch_up_from(&prefix)
        .unwrap();
    let two = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&f.root, 2),
        f.peers.clone(),
        [
            (CONTROL, f.control.remove(&NodeId(2)).unwrap()),
            (DATA, f.data.remove(&NodeId(2)).unwrap()),
        ]
        .into_iter()
        .collect(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_control_replication(f.anchor.clone(), f.catalog.clone())
    .unwrap();
    let two = Running::start(two);
    let intent = f.intent(74);
    let mut writer = primary(&mut f, one.address);
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(2))
        .unwrap()
        .address = two.address;
    writer.request_membership_intent(intent.clone()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while writer.cells[&CONTROL].committed_tail() == LogIndex(1) {
        assert!(
            Instant::now() < deadline,
            "initial prepare quorum did not commit"
        );
        writer.poll_control_writer().unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    // Fair peer order reaches both common tips before the first PREPARE. The
    // local commit used node1; node2 has not supplied a PREPARE ACK yet.
    let accepted = writer.cells[&CONTROL]
        .committed_prefix()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(
        accepted.commit.certificate.signers,
        [NodeId(0), NodeId(1)].into_iter().collect()
    );
    drop(one.finish());
    let delivered = observed(&mut writer);
    assert_eq!(delivered.reporters(), &[NodeId(0), NodeId(2)]);
    let two = two.finish();
    assert_eq!(two.cells[&CONTROL].committed_tail(), LogIndex(2));
    assert_eq!(
        two.cells[&CONTROL].committed_operation(LogIndex(2)),
        Some(&accepted.prepare.operation)
    );
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(3));
    drop(two);
    drop(writer);
    f.cleanup();
}
#[test]
fn real_tls_rejected_accepted_suffix_reconciles_before_new_begin_and_retries_remain_durable_receipts()
 {
    let mut f = Fixture::new("control-writer-rejected-tail");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    genesis(&mut f, &mut sender);
    let mut invalid_parent = f.intent(75);
    invalid_parent.parent = CatalogRoot([99; 32]);
    let retained = prepare_command(
        f.control.get_mut(&NodeId(0)).unwrap(),
        ControlCommand::Begin(invalid_parent.clone()),
    );
    sender
        .acknowledge_prepare(VsrPrepareMessage {
            replica: NodeId(1),
            prepare: retained.clone(),
        })
        .unwrap();
    let accepted = f.intent(76);
    let mut writer = primary(&mut f, running.address);
    writer.request_membership_intent(accepted.clone()).unwrap();
    assert!(
        matches!(writer.poll_control_writer().unwrap(), ControlWriterProgress::Accepted { index } if index == retained.index)
    );
    assert_eq!(
        writer.cells[&CONTROL].prepared(retained.index).unwrap(),
        &retained
    );
    let delivery = observed(&mut writer);
    assert_eq!(delivery.begin_index(), LogIndex(3));
    let state = ControlState::recover(&f.anchor, &writer.cells[&CONTROL]).unwrap();
    assert_eq!(
        state.intent_outcome(&invalid_parent).unwrap(),
        Some(ControlOutcome::Rejected(Rejection::StaleParent))
    );
    writer
        .request_membership_intent(invalid_parent.clone())
        .unwrap();
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::LocallyCommitted {
            begin: None,
            outcome: ControlOutcome::Rejected(Rejection::StaleParent),
            ..
        }
    ));
    let next = writer.cells[&CONTROL].next_index();
    invalid_parent.parent = f.catalog.root().unwrap();
    assert!(
        writer
            .request_membership_intent(invalid_parent)
            .unwrap_err()
            .contains("conflicts with its retained canonical command")
    );
    writer.request_membership_intent(accepted).unwrap();
    assert_eq!(observed(&mut writer).begin_index(), delivery.begin_index());
    assert_eq!(writer.cells[&CONTROL].next_index(), next);
    assert!(running.applied.lock().unwrap().is_empty());
    drop(writer);
    drop(running.finish());
    f.cleanup();
}
#[test]
fn real_tls_control_report_rejects_foreign_identity_mixed_key_and_noncanonical_frames() {
    let mut f = Fixture::new("control-writer-authenticated-report");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    genesis(&mut f, &mut sender);
    let intent = f.intent(77);
    let prepare = prepare_command(
        f.control.get_mut(&NodeId(0)).unwrap(),
        ControlCommand::Begin(intent),
    );
    commit_tls(f.control.get_mut(&NodeId(0)).unwrap(), &mut sender, prepare);
    let source = &f.control[&NodeId(0)];
    let state = ControlState::recover(&f.anchor, source).unwrap();
    let context = Context::recover(&state, source).unwrap();
    let mut collector = observation::Collector::new(context.clone());
    for _ in 0..2 {
        collector
            .insert(
                observation::local(&state, source, NodeId(0), identity(&f.root, 0).fingerprint)
                    .unwrap(),
            )
            .unwrap();
    }
    assert!(
        collector.observed().is_none(),
        "duplicate reporter was counted twice"
    );
    let mut mixed = context.clone();
    mixed.key.begin = LogIndex(context.key.begin.0 + 1);
    let mut wrong_collector = observation::Collector::new(mixed.clone());
    assert!(
        wrong_collector
            .insert(observation::remote(&sender, NodeId(1), &context).unwrap())
            .unwrap_err()
            .contains("mixed control delivery key")
    );
    rejected(
        sender.exchange_io(NodeId(1), CONTROL_COMMITTED, &mixed.key.encode()),
        "differs from exact pending committed Begin",
    );
    let encoded = context.key.encode();
    for malformed in [
        encoded[..encoded.len() - 1].to_vec(),
        [encoded.clone(), vec![0]].concat(),
    ] {
        rejected(
            sender.exchange_io(NodeId(1), CONTROL_COMMITTED, &malformed),
            "canonical size",
        );
    }
    let mut malformed = encoded.clone();
    malformed[189] = 0;
    malformed[190] = 1;
    rejected(
        sender.exchange_io(NodeId(1), CONTROL_COMMITTED, &malformed),
        "lineage is noncanonical",
    );
    let mut server = running.finish();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let mutator = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        for corruption in 0..3 {
            let socket = loop {
                assert!(
                    Instant::now() < deadline,
                    "authenticated report request never arrived"
                );
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("listener failed: {error}"),
                }
            };
            let mut stream = StreamOwned::new(
                ServerConnection::new(Arc::clone(&server.identity.server)).unwrap(),
                DeadlineStream::new(socket, Instant::now() + Duration::from_millis(800)).unwrap(),
            );
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock).unwrap();
            }
            let certificate = stream.conn.peer_certificates().unwrap().first().unwrap();
            let from = server.nodes[&fingerprint(certificate.as_ref())];
            let request = read_frame(&mut stream).unwrap();
            let (kind, payload) = parse(&request, &server.cluster, from, server.local).unwrap();
            assert_eq!(kind, CONTROL_COMMITTED);
            let mut report = server.dispatch_control_committed(from, payload).unwrap();
            // A real bound owner produced the original report. The authenticated
            // endpoint then lies about reporter, incarnation or stable operation.
            let length = report.len();
            match corruption {
                0 => report[length - 48..length - 40].copy_from_slice(&2_u64.to_le_bytes()),
                1 => report[length - 40..length - 32].copy_from_slice(&2_u64.to_le_bytes()),
                2 => report[125] ^= 1,
                _ => unreachable!(),
            }
            write_frame(
                &mut stream,
                &frame(&server.cluster, server.local, from, kind, &report).unwrap(),
            )
            .unwrap();
            stream.conn.send_close_notify();
            let _ = std::io::Write::flush(&mut stream);
        }
        server
    });
    let sender = f.transport(0, address);
    for _ in 0..3 {
        let error = observation::remote(&sender, NodeId(1), &context).unwrap_err();
        assert_eq!(
            error,
            "authenticated control report differs from requested key/reporter"
        );
    }
    drop(mutator.join().unwrap());
    f.cleanup();
}
#[test]
fn writer_checks_actual_control_primary_and_fresh_target_before_new_append() {
    let mut f = Fixture::new("control-writer-guards");
    let intent = f.intent(78);
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    genesis(&mut f, &mut sender);
    let mut follower = running.finish();
    let before = tree(&f.root.join("pg-0/node-1"));
    assert!(
        follower
            .request_membership_intent(intent.clone())
            .unwrap_err()
            .contains("actual current control primary")
    );
    assert_eq!(tree(&f.root.join("pg-0/node-1")), before);
    let mut writer = primary(&mut f, "127.0.0.1:1".parse().unwrap());
    assert!(matches!(
        writer
            .cells
            .get_mut(&DATA)
            .unwrap()
            .view_change(Epoch(2), NodeId(2)),
        VsrDecision::ViewChanged { .. }
    ));
    writer.request_membership_intent(intent.clone()).unwrap();
    let before = tree(&f.root.join("pg-0/node-0"));
    assert!(
        writer
            .poll_control_writer()
            .unwrap_err()
            .contains("target proposal differs")
    );
    assert_eq!(tree(&f.root.join("pg-0/node-0")), before);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(2));
    let mut corrected = intent;
    corrected.request = ControlRequestId([80; 32]);
    corrected.groups[1].proposal.election.previous_view = Epoch(2);
    corrected.groups[1].proposal.election.target_view = Epoch(3);
    writer.request_membership_intent(corrected).unwrap();
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::Accepted { index: LogIndex(2) }
    ));
    drop(writer);
    drop(follower);
    f.cleanup();
}
#[test]
fn writer_never_returns_cached_observation_after_live_control_custody_loss() {
    let mut f = Fixture::new("control-writer-custody");
    let running = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, running.address);
    genesis(&mut f, &mut sender);
    let intent = f.intent(79);
    let mut writer = primary(&mut f, running.address);
    writer.request_membership_intent(intent).unwrap();
    observed(&mut writer);
    let manifest = f.root.join("pg-0/node-0/MANIFEST");
    let bytes = std::fs::read(&manifest).unwrap();
    std::fs::remove_file(&manifest).unwrap();
    assert!(writer.poll_control_writer().is_err());
    assert!(
        !manifest.exists(),
        "writer repaired acknowledged custody loss"
    );
    std::fs::write(&manifest, bytes).unwrap();
    assert!(
        writer.poll_control_writer().is_err(),
        "restored path revived failed live owner"
    );
    assert!(running.applied.lock().unwrap().is_empty());
    drop(writer);
    drop(running.finish());
    f.cleanup();
}

mod network;

#[test]
fn real_tls_writer_recovers_commit_reply_lost_after_actual_durable_delivery() {
    let mut f = Fixture::new("control-writer-lost-commit-ack");
    let initial = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, initial.address);
    genesis(&mut f, &mut sender);
    let controlled = network::Controlled::start(initial.finish(), network::Fault::LoseCommitReply);
    let intent = f.intent(82);
    let mut writer = primary(&mut f, controlled.address);
    writer.request_membership_intent(intent.clone()).unwrap();
    let delivery = observed(&mut writer);
    controlled
        .entered
        .try_recv()
        .expect("fixture did not lose an actual successful COMMIT reply");
    assert_eq!(delivery.intent(), intent.id().unwrap());
    assert_eq!(delivery.begin_index(), LogIndex(2));
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(3));
    let follower = controlled.finish();
    assert_eq!(follower.cells[&CONTROL].committed_tail(), LogIndex(2));
    drop(follower);
    drop(writer);
    f.cleanup();
}
#[test]
fn real_tls_successful_prepare_reply_is_rejected_after_local_view_changes_before_consumption() {
    delayed_prepare(false);
}
#[test]
fn real_tls_successful_prepare_reply_never_revives_lost_then_restored_source_custody() {
    delayed_prepare(true);
}
fn delayed_prepare(lose_custody: bool) {
    let mut f = Fixture::new(if lose_custody {
        "control-writer-late-custody"
    } else {
        "control-writer-late-view"
    });
    let initial = Running::start(f.server.take().unwrap());
    let mut sender = f.transport(0, initial.address);
    genesis(&mut f, &mut sender);
    let controlled = network::Controlled::start(initial.finish(), network::Fault::SignalPrepare);
    let intent = f.intent(83);
    let mut writer = primary(&mut f, controlled.address);
    writer.request_membership_intent(intent).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if controlled.entered.try_recv().is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "successful PREPARE response never arrived"
        );
        writer.poll_control_writer().unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    controlled.flush_reply();
    // The responder flushed a real success. Do not poll the writer until the
    // local source changes; no network deadline is extended for this schedule.
    let operation = writer.cells[&CONTROL]
        .prepared(LogIndex(2))
        .unwrap()
        .operation
        .clone();
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(1));
    let manifest = f.root.join("pg-0/node-0/MANIFEST");
    let saved = if lose_custody {
        let bytes = std::fs::read(&manifest).unwrap();
        std::fs::remove_file(&manifest).unwrap();
        Some(bytes)
    } else {
        assert!(matches!(
            writer
                .cells
                .get_mut(&CONTROL)
                .unwrap()
                .view_change(Epoch(2), NodeId(0)),
            VsrDecision::ViewChanged { .. }
        ));
        None
    };
    let error = loop {
        assert!(Instant::now() < deadline, "late reply was never rejected");
        match writer.poll_control_writer() {
            Err(error) => break error,
            Ok(ControlWriterProgress::Waiting) => std::thread::sleep(Duration::from_millis(1)),
            Ok(other) => panic!("stale reply advanced writer: {other:?}"),
        }
    };
    if let Some(bytes) = saved {
        assert!(error.contains("VSR durability failed"), "{error}");
        assert!(
            !manifest.exists(),
            "writer repaired lost acknowledged manifest"
        );
        std::fs::write(&manifest, bytes).unwrap();
        assert!(
            writer.poll_control_writer().is_err(),
            "restored path revived failed owner"
        );
    } else {
        assert!(error.contains("source changed"), "{error}");
    }
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(1));
    assert_eq!(
        writer.cells[&CONTROL]
            .prepared(LogIndex(2))
            .unwrap()
            .operation,
        operation
    );
    drop(writer);
    drop(controlled.finish());
    f.cleanup();
}

mod genesis;
