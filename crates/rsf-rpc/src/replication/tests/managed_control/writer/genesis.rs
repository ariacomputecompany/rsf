//! Actual empty owners and TLS. No seeded Genesis certificate or fake ACK.
use super::*;

fn owner(f: &mut Fixture, node: u64) -> PeerReplicaServer {
    if node == 1 {
        return f.server.take().unwrap();
    }
    PeerReplicaServer::new(
        NodeId(node),
        &cluster(),
        identity(&f.root, node),
        f.peers.clone(),
        [
            (CONTROL, f.control.remove(&NodeId(node)).unwrap()),
            (DATA, f.data.remove(&NodeId(node)).unwrap()),
        ]
        .into_iter()
        .collect(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_control_replication(f.anchor.clone(), f.catalog.clone())
    .unwrap()
}
fn endpoint(writer: &mut PeerReplicaServer, node: u64, address: SocketAddr) {
    writer
        .control_transport
        .peers
        .get_mut(&NodeId(node))
        .unwrap()
        .address = address;
}
fn delivery(writer: &mut PeerReplicaServer) -> ObservedControlGenesisDelivery {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = String::new();
    while Instant::now() < deadline {
        match writer.poll_control_writer().unwrap() {
            ControlWriterProgress::ObservedGenesisDelivery(observed) => return observed,
            progress => last = format!("{progress:?}"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("Genesis did not reach actual delivery within unchanged budget: {last}");
}
fn local_commit(writer: &mut PeerReplicaServer) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            Instant::now() < deadline,
            "Genesis local commit never completed"
        );
        match writer.poll_control_writer().unwrap() {
            ControlWriterProgress::GenesisLocallyCommitted => break,
            ControlWriterProgress::ObservedGenesisDelivery(_) => {
                panic!("delivery preceded local commit progress")
            }
            _ => std::thread::sleep(Duration::from_millis(1)),
        }
    }
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(1));
}
fn reopen(f: &mut Fixture, node: u64) -> PeerReplicaServer {
    f.control.insert(
        NodeId(node),
        DurableVsrCell::open_existing(f.root.join(format!("pg-0/node-{node}")), CONTROL).unwrap(),
    );
    f.data.insert(
        NodeId(node),
        DurableVsrCell::open_existing(f.root.join(format!("pg-1/node-{node}")), DATA).unwrap(),
    );
    // node1 normally comes from Fixture.server, but strict reopen constructs a
    // fresh receiver without duplicating an owned cell or transition opener.
    PeerReplicaServer::new(
        NodeId(node),
        &cluster(),
        identity(&f.root, node),
        f.peers.clone(),
        [
            (CONTROL, f.control.remove(&NodeId(node)).unwrap()),
            (DATA, f.data.remove(&NodeId(node)).unwrap()),
        ]
        .into_iter()
        .collect(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_control_replication(f.anchor.clone(), f.catalog.clone())
    .unwrap()
}
fn exact_genesis(f: &Fixture, server: &PeerReplicaServer, expected: &VsrCommittedEntry) {
    let cell = &server.cells[&CONTROL];
    assert_eq!(cell.committed_prefix().unwrap(), vec![expected.clone()]);
    assert_eq!(cell.prepared(LogIndex(1)), Some(&expected.prepare));
    assert_eq!(cell.next_index(), LogIndex(2));
    assert_eq!(
        ControlState::recover(&f.anchor, cell).unwrap().catalog(),
        &f.catalog
    );
    assert_eq!(expected.prepare.index, LogIndex(1));
    assert_eq!(
        expected.commit.certificate.operation,
        expected.prepare.operation
    );
    assert!(expected.commit.certificate.signers.len() >= cell.quorum());
}

#[test]
fn real_tls_fresh_genesis_distinguishes_accepted_followers_then_cold_recollects_exact_three_roots()
{
    let mut f = Fixture::new("genesis-fresh-three");
    let one = Running::start(owner(&mut f, 1));
    let two = Running::start(owner(&mut f, 2));
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    endpoint(&mut writer, 2, two.address);
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::GenesisRequired
    ));
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(1));
    writer.request_control_genesis().unwrap();
    local_commit(&mut writer);
    let expected = writer.cells[&CONTROL].committed_prefix().unwrap().remove(0);
    assert_eq!(
        expected.commit.certificate.signers,
        [NodeId(0), NodeId(1)].into_iter().collect()
    );
    // Local commit returns before dispatching its COMMIT. Stop the real receiver
    // at this cut: its accepted vote alone must not have become delivery.
    assert!(one.applied.lock().unwrap().is_empty());
    let one = one.finish();
    assert_eq!(one.cells[&CONTROL].committed_tail(), LogIndex(0));
    assert_eq!(
        one.cells[&CONTROL].prepared(LogIndex(1)),
        Some(&expected.prepare)
    );
    let one = Running::start(one);
    endpoint(&mut writer, 1, one.address);
    let observed = delivery(&mut writer);
    assert_eq!(observed.pg(), CONTROL);
    assert_eq!(observed.genesis(), f.anchor.genesis_catalog);
    assert_eq!(observed.historical_primary(), NodeId(0));
    assert_eq!(observed.historical_view(), Epoch(1));
    assert_eq!(observed.operation_id(), OperationId(1));
    assert!(observed.reporters().contains(&NodeId(0)));
    assert!(observed.reporters().len() >= 2);
    assert!(one.applied.lock().unwrap().is_empty());
    drop(one.finish());
    // A new request must recollect through a different live receiver, never a
    // retained success bit. Node2 becomes an actual committed keeper too.
    endpoint(&mut writer, 1, "127.0.0.1:1".parse().unwrap());
    writer.request_control_genesis().unwrap();
    assert_eq!(delivery(&mut writer).reporters(), &[NodeId(0), NodeId(2)]);
    assert!(two.applied.lock().unwrap().is_empty());
    drop(two.finish());
    drop(writer);
    for node in 0..3 {
        let server = reopen(&mut f, node);
        exact_genesis(&f, &server, &expected);
        drop(server);
    }
    let one = Running::start(reopen(&mut f, 1));
    let mut writer = reopen(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    writer.request_control_genesis().unwrap();
    let cold = delivery(&mut writer);
    assert_eq!(cold.reporters(), &[NodeId(0), NodeId(1)]);
    assert_eq!(cold.operation_id(), observed.operation_id());
    exact_genesis(&f, &writer, &expected);
    drop(writer);
    drop(one.finish());
    f.cleanup();
}

#[test]
fn real_tls_genesis_request_repeats_preserve_flight_and_incompatible_goals_do_not_reset_it() {
    let mut f = Fixture::new("genesis-slot");
    let controlled = network::Controlled::start(owner(&mut f, 1), network::Fault::SignalPrepare);
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, controlled.address);
    writer.request_control_genesis().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while controlled.entered.try_recv().is_err() {
        assert!(Instant::now() < deadline);
        writer.poll_control_writer().unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    let before = tree(&f.root.join("pg-0/node-0"));
    writer.request_control_genesis().unwrap();
    writer.request_control_genesis().unwrap();
    assert_eq!(tree(&f.root.join("pg-0/node-0")), before);
    controlled.flush_reply();
    local_commit(&mut writer);
    let before = tree(&f.root.join("pg-0/node-0"));
    assert!(
        writer
            .request_membership_intent(f.intent(91))
            .unwrap_err()
            .contains("unresolved request")
    );
    assert_eq!(tree(&f.root.join("pg-0/node-0")), before);
    assert_eq!(delivery(&mut writer).operation_id(), OperationId(1));
    writer.request_membership_intent(f.intent(91)).unwrap();
    assert!(
        writer
            .request_control_genesis()
            .unwrap_err()
            .contains("unresolved request")
    );
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(2));
    let intent = observed(&mut writer);
    assert_eq!(intent.begin_index(), LogIndex(2));
    drop(writer);
    drop(controlled.finish());
    f.cleanup();
}

#[test]
fn real_tls_cold_accepted_genesis_resumes_without_request_and_empty_poll_never_appends() {
    let mut f = Fixture::new("genesis-cold-accepted");
    let one = Running::start(owner(&mut f, 1));
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::GenesisRequired
    ));
    writer.request_control_genesis().unwrap();
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::Accepted { index: LogIndex(1) }
    ));
    let original = writer.cells[&CONTROL]
        .prepared(LogIndex(1))
        .unwrap()
        .clone();
    drop(writer);
    let mut writer = reopen(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    // No explicit request: the actual accepted tail must still reconcile.
    local_commit(&mut writer);
    assert_eq!(
        writer.cells[&CONTROL].prepared(LogIndex(1)),
        Some(&original)
    );
    // Attach a goal while automatic reconciliation still owns its cycle.
    writer.request_control_genesis().unwrap();
    assert_eq!(delivery(&mut writer).operation_id(), OperationId(1));
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(2));
    drop(writer);
    drop(one.finish());
    f.cleanup();
}

#[test]
fn real_tls_genesis_lost_durable_commit_reply_recollects_without_duplicate_operation() {
    let mut f = Fixture::new("genesis-lost-commit");
    let controlled = network::Controlled::start(owner(&mut f, 1), network::Fault::LoseCommitReply);
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, controlled.address);
    writer.request_control_genesis().unwrap();
    let observed = delivery(&mut writer);
    controlled
        .entered
        .try_recv()
        .expect("no actual successful COMMIT reply was lost");
    let expected = writer.cells[&CONTROL].committed_prefix().unwrap().remove(0);
    let follower = controlled.finish();
    exact_genesis(&f, &follower, &expected);
    assert_eq!(observed.reporters(), &[NodeId(0), NodeId(1)]);
    drop(follower);
    drop(writer);
    for node in 0..2 {
        let server = reopen(&mut f, node);
        exact_genesis(&f, &server, &expected);
        drop(server);
    }
    f.cleanup();
}

#[test]
fn real_tls_genesis_late_prepare_reply_cannot_commit_after_source_view_changes() {
    delayed(false);
}
#[test]
fn real_tls_genesis_late_prepare_reply_cannot_revive_lost_then_restored_source() {
    delayed(true);
}
fn delayed(loss: bool) {
    let mut f = Fixture::new(if loss {
        "genesis-late-loss"
    } else {
        "genesis-late-view"
    });
    let controlled = network::Controlled::start(owner(&mut f, 1), network::Fault::SignalPrepare);
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, controlled.address);
    writer.request_control_genesis().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while controlled.entered.try_recv().is_err() {
        assert!(Instant::now() < deadline);
        writer.poll_control_writer().unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    controlled.flush_reply();
    let original = writer.cells[&CONTROL]
        .prepared(LogIndex(1))
        .unwrap()
        .clone();
    let manifest = f.root.join("pg-0/node-0/MANIFEST");
    let saved = if loss {
        let saved = manifest.with_extension("saved");
        assert!(!saved.exists());
        std::fs::rename(&manifest, &saved).unwrap();
        Some(saved)
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
        assert!(Instant::now() < deadline);
        match writer.poll_control_writer() {
            Err(error) => break error,
            Ok(ControlWriterProgress::Waiting) => std::thread::sleep(Duration::from_millis(1)),
            other => panic!("late reply advanced Genesis: {other:?}"),
        }
    };
    if let Some(saved) = saved {
        // The first check reports the missing file and records its reason.
        // Subsequent checks must retain that fence even after inode restoration.
        assert!(error.starts_with("storage Recover failed: "), "{error}");
        assert!(!manifest.exists());
        // Restore the original inode after the observed failure. A new inode
        // would merely trigger another physical mismatch instead of proving
        // that the already observed loss permanently fenced this live owner.
        std::fs::rename(saved, &manifest).unwrap();
        assert_eq!(
            writer.poll_control_writer().unwrap_err(),
            format!("VSR durability failed; reopen before further operations: {error}")
        );
    } else {
        assert!(error.contains("source changed"), "{error}");
    }
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(0));
    assert_eq!(
        writer.cells[&CONTROL].prepared(LogIndex(1)),
        Some(&original)
    );
    drop(writer);
    drop(controlled.finish());
    f.cleanup();
}

#[test]
fn real_tls_accepted_genesis_survives_original_primary_loss_and_certified_election() {
    primary_loss(false);
}
#[test]
fn real_tls_primary_only_genesis_commit_survives_original_source_loss_from_accepted_survivor() {
    primary_loss(true);
}
fn primary_loss(committed: bool) {
    let mut f = Fixture::new(if committed {
        "genesis-local-source-loss"
    } else {
        "genesis-accepted-source-loss"
    });
    let one = Running::start(owner(&mut f, 1));
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    writer.request_control_genesis().unwrap();
    if committed {
        local_commit(&mut writer);
    } else {
        assert!(matches!(
            writer.poll_control_writer().unwrap(),
            ControlWriterProgress::Accepted { index: LogIndex(1) }
        ));
        let prepare = writer.cells[&CONTROL]
            .prepared(LogIndex(1))
            .unwrap()
            .clone();
        // Actual second durable vote; stop before synthesizing any local ACK or
        // certificate. The new primary must discover retained history itself.
        let mut sender = writer.control_transport.clone();
        assert_eq!(
            sender
                .acknowledge_prepare(VsrPrepareMessage {
                    replica: NodeId(1),
                    prepare
                })
                .unwrap(),
            NodeId(1)
        );
    }
    let original = writer.cells[&CONTROL]
        .prepared(LogIndex(1))
        .unwrap()
        .clone();
    let mut candidate = one.finish();
    assert_eq!(candidate.cells[&CONTROL].committed_tail(), LogIndex(0));
    assert_eq!(
        candidate.cells[&CONTROL].prepared(LogIndex(1)),
        Some(&original)
    );
    drop(writer); // Original source is permanently unavailable for this schedule.
    let two = Running::start(owner(&mut f, 2));
    endpoint(&mut candidate, 2, two.address);
    let transport = candidate.control_transport.clone();
    let request = rsf_vsr::VsrViewChangeRequest {
        pg: CONTROL,
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: (0..3).map(NodeId).collect(),
    };
    let cell = candidate.cells.get_mut(&CONTROL).unwrap();
    let (input, snapshots) = transport
        .prepare_installation_input(cell, &request)
        .unwrap();
    assert_eq!(
        input
            .reports()
            .iter()
            .map(|report| report.replica)
            .collect::<BTreeSet<_>>(),
        [NodeId(1), NodeId(2)].into_iter().collect()
    );
    transport
        .stage_installation_input(cell, &input, &snapshots)
        .unwrap();
    assert_eq!(
        transport
            .resume_view_installation(cell, &mut |_| panic!(
                "accepted-only installation fabricated committed control data"
            ))
            .unwrap(),
        Epoch(2)
    );
    assert_eq!(cell.committed_tail(), LogIndex(0));
    assert_eq!(
        cell.prepared(LogIndex(1)).unwrap().operation,
        original.operation
    );
    candidate.request_control_genesis().unwrap();
    let observed = delivery(&mut candidate);
    assert_eq!(observed.reporters(), &[NodeId(1), NodeId(2)]);
    assert_eq!(observed.historical_primary(), NodeId(1));
    assert_eq!(observed.historical_view(), Epoch(2));
    let expected = candidate.cells[&CONTROL]
        .committed_prefix()
        .unwrap()
        .remove(0);
    assert_eq!(expected.prepare.operation, original.operation);
    assert_eq!(
        expected.commit.certificate.signers,
        [NodeId(1), NodeId(2)].into_iter().collect()
    );
    assert!(two.applied.lock().unwrap().is_empty());
    let follower = two.finish();
    exact_genesis(&f, &follower, &expected);
    drop(follower);
    drop(candidate);
    for node in [1, 2] {
        let server = reopen(&mut f, node);
        exact_genesis(&f, &server, &expected);
        drop(server);
    }
    f.cleanup();
}

#[test]
fn real_tls_genesis_conflicting_minority_requires_certified_installation_then_preserves_selected_operation()
 {
    let mut f = Fixture::new("genesis-minority");
    let mut zero = owner(&mut f, 0);
    zero.request_control_genesis().unwrap();
    assert!(matches!(
        zero.poll_control_writer().unwrap(),
        ControlWriterProgress::Accepted { index: LogIndex(1) }
    ));
    let selected = zero.cells[&CONTROL].prepared(LogIndex(1)).unwrap().clone();
    let mut minority = selected.clone();
    let VsrOperation::ReplicatedCommand { operation_id, .. } = &mut minority.operation else {
        unreachable!()
    };
    *operation_id = OperationId(99);
    let one = Running::start(owner(&mut f, 1));
    let two = Running::start(owner(&mut f, 2));
    endpoint(&mut zero, 1, one.address);
    endpoint(&mut zero, 2, two.address);
    let mut sender = zero.control_transport.clone();
    sender
        .acknowledge_prepare(VsrPrepareMessage {
            replica: NodeId(1),
            prepare: minority.clone(),
        })
        .unwrap();
    sender
        .acknowledge_prepare(VsrPrepareMessage {
            replica: NodeId(2),
            prepare: selected.clone(),
        })
        .unwrap();
    let error = sender
        .acknowledge_prepare(VsrPrepareMessage {
            replica: NodeId(1),
            prepare: selected.clone(),
        })
        .unwrap_err();
    assert!(
        error
            .reason
            .contains("initial control prepare replaces retained Genesis"),
        "{}",
        error.reason
    );
    let one = one.finish();
    assert_eq!(one.cells[&CONTROL].prepared(LogIndex(1)), Some(&minority));
    let mut candidate = two.finish();
    let zero = Running::start(zero);
    endpoint(&mut candidate, 0, zero.address);
    let mut transport = candidate.control_transport.clone();
    let request = rsf_vsr::VsrViewChangeRequest {
        pg: CONTROL,
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(2),
        voters: (0..3).map(NodeId).collect(),
    };
    let cell = candidate.cells.get_mut(&CONTROL).unwrap();
    let (input, snapshots) = transport
        .prepare_installation_input(cell, &request)
        .unwrap();
    assert_eq!(
        input
            .reports()
            .iter()
            .map(|report| report.replica)
            .collect::<BTreeSet<_>>(),
        [NodeId(0), NodeId(2)].into_iter().collect()
    );
    transport
        .stage_installation_input(cell, &input, &snapshots)
        .unwrap();
    drop(zero.finish()); // Keep the real persisted quorum input; lose source0.
    let one = Running::start(one);
    transport.peers.get_mut(&NodeId(0)).unwrap().address = "127.0.0.1:1".parse().unwrap();
    transport.peers.get_mut(&NodeId(1)).unwrap().address = one.address;
    transport
        .resume_view_installation(cell, &mut |_| {
            panic!("uncommitted Genesis became committed during installation")
        })
        .unwrap();
    endpoint(&mut candidate, 0, "127.0.0.1:1".parse().unwrap());
    endpoint(&mut candidate, 1, one.address);
    candidate.request_control_genesis().unwrap();
    let observed = delivery(&mut candidate);
    assert_eq!(observed.operation_id(), OperationId(1));
    assert_eq!(observed.reporters(), &[NodeId(1), NodeId(2)]);
    assert_eq!(candidate.cells[&CONTROL].next_index(), LogIndex(2));
    let expected = candidate.cells[&CONTROL]
        .committed_prefix()
        .unwrap()
        .remove(0);
    assert_eq!(expected.prepare.operation, selected.operation);
    let one = one.finish();
    exact_genesis(&f, &one, &expected);
    drop(one);
    drop(candidate);
    f.cleanup();
}

#[test]
fn real_tls_genesis_goal_reconciles_rejected_and_pending_suffix_before_delivery_without_duplicate_genesis()
 {
    let mut f = Fixture::new("genesis-retained-tail");
    let one = Running::start(owner(&mut f, 1));
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    writer.request_control_genesis().unwrap();
    delivery(&mut writer);
    let original = writer.cells[&CONTROL].committed_prefix().unwrap().remove(0);
    let mut stale = f.intent(92);
    stale.parent = CatalogRoot([99; 32]);
    let rejected = prepare_command(
        writer.cells.get_mut(&CONTROL).unwrap(),
        ControlCommand::Begin(stale.clone()),
    );
    let pending = f.intent(93);
    let accepted = prepare_command(
        writer.cells.get_mut(&CONTROL).unwrap(),
        ControlCommand::Begin(pending.clone()),
    );
    assert_eq!(rejected.index, LogIndex(2));
    assert_eq!(accepted.index, LogIndex(3));
    drop(writer);
    let mut writer = reopen(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    writer.request_control_genesis().unwrap();
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::Accepted { index: LogIndex(2) }
    ));
    assert_eq!(writer.cells[&CONTROL].committed_tail(), LogIndex(1));
    assert_eq!(
        writer.cells[&CONTROL].prepared(LogIndex(2)),
        Some(&rejected)
    );
    delivery(&mut writer);
    let state = ControlState::recover(&f.anchor, &writer.cells[&CONTROL]).unwrap();
    assert_eq!(
        state.intent_outcome(&stale).unwrap(),
        Some(ControlOutcome::Rejected(Rejection::StaleParent))
    );
    assert_eq!(state.pending(), Some(&pending));
    assert_eq!(
        state.request_begin_index(pending.request),
        Some(LogIndex(3))
    );
    assert_eq!(
        writer.cells[&CONTROL].prepared(LogIndex(3)),
        Some(&accepted)
    );
    assert_eq!(
        writer.cells[&CONTROL].committed_prefix().unwrap()[0],
        original
    );
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(4));
    // With no explicit request, polling starts an automatic observation of the
    // existing pending intent. A later Genesis goal must drain that real flight
    // and advance, retaining the pending Begin instead of returning old intent
    // observations forever or discarding an unconsumed transport reply.
    assert!(matches!(
        writer.poll_control_writer().unwrap(),
        ControlWriterProgress::LocallyCommitted { .. }
    ));
    writer.request_control_genesis().unwrap();
    assert_eq!(delivery(&mut writer).operation_id(), OperationId(1));
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(4));
    assert_eq!(
        ControlState::recover(&f.anchor, &writer.cells[&CONTROL])
            .unwrap()
            .pending(),
        Some(&pending)
    );
    assert!(one.applied.lock().unwrap().is_empty());
    let follower = one.finish();
    assert_eq!(
        follower.cells[&CONTROL].committed_prefix().unwrap(),
        writer.cells[&CONTROL].committed_prefix().unwrap()
    );
    drop(follower);
    drop(writer);
    let cold = reopen(&mut f, 0);
    let state = ControlState::recover(&f.anchor, &cold.cells[&CONTROL]).unwrap();
    assert_eq!(state.pending(), Some(&pending));
    assert_eq!(
        state.intent_outcome(&stale).unwrap(),
        Some(ControlOutcome::Rejected(Rejection::StaleParent))
    );
    drop(cold);
    f.cleanup();
}

#[test]
fn genesis_request_rejects_follower_pending_promise_and_foreign_provisioning_without_append() {
    let mut f = Fixture::new("genesis-role-guard");
    let mut follower = owner(&mut f, 1);
    let before = tree(&f.root.join("pg-0/node-1"));
    assert!(
        follower
            .request_control_genesis()
            .unwrap_err()
            .contains("actual current control primary")
    );
    assert_eq!(tree(&f.root.join("pg-0/node-1")), before);
    drop(follower);
    let mut writer = owner(&mut f, 0);
    writer
        .cells
        .get_mut(&CONTROL)
        .unwrap()
        .begin_view_change(
            NodeId(0),
            NodeId(1),
            rsf_vsr::VsrViewChangeRequest {
                pg: CONTROL,
                previous_view: Epoch(1),
                target_view: Epoch(2),
                candidate: NodeId(1),
                voters: (0..3).map(NodeId).collect(),
            },
        )
        .unwrap();
    let before = tree(&f.root.join("pg-0/node-0"));
    assert!(writer.request_control_genesis().is_err());
    assert_eq!(tree(&f.root.join("pg-0/node-0")), before);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(1));
    drop(writer);
    let mut wrong = f.anchor.clone();
    wrong.genesis_catalog = CatalogRoot([88; 32]);
    let cells = [
        (CONTROL, f.control.remove(&NodeId(2)).unwrap()),
        (DATA, f.data.remove(&NodeId(2)).unwrap()),
    ]
    .into_iter()
    .collect();
    let unbound = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&f.root, 2),
        f.peers.clone(),
        cells,
        Duration::from_millis(800),
    )
    .unwrap();
    let before = tree(&f.root.join("pg-0/node-2"));
    assert!(
        unbound
            .with_control_replication(wrong, f.catalog.clone())
            .is_err()
    );
    assert_eq!(tree(&f.root.join("pg-0/node-2")), before);
    f.cleanup();
}

#[test]
fn real_tls_genesis_lost_durable_prepare_reply_retries_same_operation_before_commit() {
    let mut f = Fixture::new("genesis-lost-prepare");
    let controlled = network::Controlled::start(owner(&mut f, 1), network::Fault::LosePrepareReply);
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, controlled.address);
    writer.request_control_genesis().unwrap();
    let observed = delivery(&mut writer);
    controlled
        .entered
        .try_recv()
        .expect("no actual successful PREPARE reply was lost");
    let expected = writer.cells[&CONTROL].committed_prefix().unwrap().remove(0);
    assert_eq!(observed.operation_id(), OperationId(1));
    assert_eq!(observed.reporters(), &[NodeId(0), NodeId(1)]);
    let follower = controlled.finish();
    exact_genesis(&f, &follower, &expected);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(2));
    drop(follower);
    drop(writer);
    f.cleanup();
}

#[test]
fn genesis_delivery_rejects_actual_certified_control_membership_change() {
    use rsf_vsr::{MembershipStagingStore, VsrMembershipStagedPlan, VsrMembershipStagedQuorum};
    let mut f = Fixture::new("genesis-changed-membership");
    let one = Running::start(owner(&mut f, 1));
    let mut writer = owner(&mut f, 0);
    endpoint(&mut writer, 1, one.address);
    writer.request_control_genesis().unwrap();
    delivery(&mut writer);
    let mut follower = one.finish();
    // Construct a real later kernel lineage as fixture history. This deliberately
    // bypasses no RPC guard: no managed control membership route is enabled by
    // this slice, and these reports come directly from these actual owners.
    let proposal = proposal(CONTROL, 0);
    let approval = VsrMembershipApproval::from_snapshots(vec![
        writer
            .cells
            .get_mut(&CONTROL)
            .unwrap()
            .begin_membership(NodeId(0), NodeId(0), proposal.clone())
            .unwrap(),
        follower
            .cells
            .get_mut(&CONTROL)
            .unwrap()
            .begin_membership(NodeId(1), NodeId(0), proposal)
            .unwrap(),
    ])
    .unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let mut zero =
        MembershipDecisionStore::open(f.root.join("genesis-decision-0"), NodeId(0), &approval)
            .unwrap();
    let mut one =
        MembershipDecisionStore::open(f.root.join("genesis-decision-1"), NodeId(1), &approval)
            .unwrap();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(0),
    };
    let decision = VsrMembershipDecision::from_promises(
        vec![
            zero.promise(NodeId(0), round).unwrap(),
            one.promise(NodeId(0), round).unwrap(),
        ],
        input,
    )
    .unwrap();
    let nomination = zero.nominate(&decision).unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![
        nomination.clone(),
        one.accept(NodeId(0), &decision, &nomination).unwrap(),
    ])
    .unwrap();
    let first = VsrMembershipStagedPlan::select(chosen.clone(), approval.clone()).unwrap();
    let second = VsrMembershipStagedPlan::select(chosen, approval).unwrap();
    let mut stages = [
        MembershipStagingStore::stage(
            f.root.join("genesis-stage-0"),
            NodeId(0),
            first,
            &writer.cells[&CONTROL],
        )
        .unwrap(),
        MembershipStagingStore::stage(
            f.root.join("genesis-stage-1"),
            NodeId(1),
            second,
            &follower.cells[&CONTROL],
        )
        .unwrap(),
    ];
    let reports = stages
        .iter_mut()
        .map(|stage| {
            stage
                .materialize(&mut |entry| {
                    assert_eq!(entry.prepare.pg, CONTROL);
                    assert_eq!(entry.prepare.index, LogIndex(1));
                    let VsrOperation::ReplicatedCommand { payload, .. } = &entry.prepare.operation
                    else {
                        panic!("non-control Genesis history")
                    };
                    assert_eq!(
                        ControlCommand::decode(payload).unwrap(),
                        ControlCommand::Genesis(f.catalog.clone())
                    );
                    Ok(())
                })
                .unwrap()
        })
        .collect();
    let quorum = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
    writer
        .cells
        .get_mut(&CONTROL)
        .unwrap()
        .install_membership(&stages[0], &quorum)
        .unwrap();
    let lineage = writer.cells[&CONTROL].membership_lineage().unwrap();
    assert!(lineage.plan.is_some());
    assert_eq!(lineage.voters, vec![NodeId(0), NodeId(1), NodeId(3)]);
    assert_eq!(
        writer.cells[&CONTROL].primary_probe().unwrap().primary,
        NodeId(0)
    );
    ControlState::recover(&f.anchor, &writer.cells[&CONTROL])
        .unwrap()
        .validate_retained_history(&writer.cells[&CONTROL])
        .unwrap();
    let before = tree(&f.root.join("pg-0/node-0"));
    assert_eq!(
        writer.request_control_genesis().unwrap_err(),
        "initial Genesis delivery requires the original provisioned control membership"
    );
    assert_eq!(tree(&f.root.join("pg-0/node-0")), before);
    assert_eq!(writer.cells[&CONTROL].next_index(), LogIndex(2));
    drop(stages);
    drop(zero);
    drop(one);
    drop(writer);
    drop(follower);
    f.cleanup();
}
