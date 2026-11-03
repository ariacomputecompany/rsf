use super::*;
use rsf_vsr::{VsrMembershipImportReply, VsrMembershipInputProgress, VsrMembershipPromiseReply};

fn bootstrap(fixture: &Accepted) -> [u8; 32] {
    rsf_core::Digest::blake3_bytes(
        &fixture.authority.snapshots()[0].records[0]
            .encode()
            .unwrap(),
    )
    .bytes
}
fn wire_request(
    tag: u8,
    proposal: &VsrMembershipProposal,
    bootstrap: [u8; 32],
    body: &[u8],
) -> Vec<u8> {
    let mut bytes = b"RSFMD1".to_vec();
    bytes.push(tag);
    bytes.extend_from_slice(&proposal.election.pg.0.to_le_bytes());
    bytes.extend_from_slice(&proposal.id().unwrap());
    bytes.extend_from_slice(&bootstrap);
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn real_tls_prechoice_recovers_highest_accepted_input_in_bounded_exchanges_after_coordinator_loss()
{
    let fixture = accepted_fixture();
    let root = &fixture.root;
    let peer = PeerProcess::spawn_decision(root, 2, false);
    let mut peers = all_endpoints(root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let network = decision_transport(root, 3, peers);
    let p = proposed();
    let bootstrap = bootstrap(&fixture);
    let state = network
        .membership_decision_state(NodeId(2), &p, bootstrap)
        .unwrap();
    assert_eq!(state.replica, NodeId(2));
    assert_eq!(
        state.accepted,
        Some((
            fixture.plan.chosen().round(),
            fixture.plan.chosen().input_id()
        ))
    );
    assert!(state.chosen.is_none());
    assert!(
        network
            .query_membership_chosen(NodeId(2), &p, bootstrap, fixture.plan.chosen().input_id())
            .unwrap()
            .is_none()
    );
    let mut local = MembershipDecisionStore::open(
        root.join("membership-3/decision"),
        NodeId(3),
        &fixture.authority,
    )
    .unwrap();
    let round = VsrMembershipRound::after(&p, NodeId(3), state.promised).unwrap();
    let VsrMembershipPromiseReply::Granted(remote) = network
        .promise_membership_recovery(NodeId(2), &p, bootstrap, round)
        .unwrap()
    else {
        panic!("promise")
    };
    let own = local.promise(NodeId(3), round).unwrap();
    assert_eq!(remote.accepted, state.accepted);
    assert_eq!(own.round, round);
    // An actually lower own ballot receives structured durable preemption.
    let higher = VsrMembershipRound {
        counter: round.counter + 1,
        coordinator: NodeId(3),
    };
    let _ = network
        .promise_membership_recovery(NodeId(2), &p, bootstrap, higher)
        .unwrap();
    assert!(
        matches!(network.promise_membership_recovery(NodeId(2),&p,bootstrap,round).unwrap(),VsrMembershipPromiseReply::Preempted(s) if s.promised == Some(higher))
    );
    let VsrMembershipPromiseReply::Granted(remote) = network
        .promise_membership_recovery(NodeId(2), &p, bootstrap, higher)
        .unwrap()
    else {
        panic!("retry promise")
    };
    let own = local.promise(NodeId(3), higher).unwrap();
    let VsrMembershipInputProgress::Complete { input } = network
        .membership_input_progress(NodeId(2), &p, bootstrap, fixture.plan.chosen().input_id())
        .unwrap()
    else {
        panic!("accepted input")
    };
    let decision = VsrMembershipDecision::from_promises(vec![own, remote], input.clone()).unwrap();
    assert_eq!(
        local.begin_input(NodeId(3), higher, input.clone()).unwrap(),
        0
    );
    let mut at = 0;
    let mut exchanges = 0;
    while at < input.records().unwrap() {
        let batch = network
            .membership_input_batch(NodeId(2), &input, bootstrap, at)
            .unwrap();
        at = local
            .append_input(NodeId(3), higher, input.id().unwrap(), batch)
            .unwrap();
        exchanges += 1;
    }
    assert!(exchanges > 1, "fixture must span bounded batches");
    let nomination = local.nominate(&decision).unwrap();
    assert_eq!(
        network
            .begin_membership_input(NodeId(2), higher, &input)
            .unwrap(),
        input.records().unwrap()
    );
    let (_, retained) = local.input(input.id().unwrap()).unwrap();
    assert_eq!(
        network
            .append_membership_input(
                NodeId(2),
                higher,
                &input,
                VsrHistoryBatch {
                    ordinal: 0,
                    records: retained[..1].to_vec()
                }
            )
            .unwrap(),
        input.records().unwrap()
    );
    let vote = network
        .accept_membership(NodeId(2), &decision, &nomination)
        .unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![nomination, vote]).unwrap();
    local.learn_chosen(chosen.clone()).unwrap();
    network.learn_membership_chosen(NodeId(2), &chosen).unwrap();
    assert_eq!(
        network
            .query_membership_chosen(NodeId(2), &p, bootstrap, chosen.input_id())
            .unwrap(),
        Some(chosen.clone())
    );
    assert_eq!(chosen.input_id(), fixture.plan.chosen().input_id());
    let plan =
        VsrMembershipStagedPlan::select(chosen, local.approval(input.id().unwrap()).unwrap())
            .unwrap();
    assert_eq!(
        plan.request_commit(&fixture.request),
        Some(fixture.committed.index)
    );
    assert!(!root.join("membership-2/staging").exists());
    assert!(!root.join("materialized-2").exists());
    assert!(!root.join("store-2/VSR_ACTIVE").exists());
    drop(local);
    drop(peer);
}

#[test]
fn real_tls_chosen_import_resumes_partial_unbound_input_after_receiver_restart() {
    let fixture = fixture_for_receiver(false);
    let root = &fixture.root;
    let mut peer = PeerProcess::spawn_decision(root, 2, false);
    let mut peers = all_endpoints(root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let mut network = decision_transport(root, 3, peers.clone());
    let p = proposed();
    let bootstrap = bootstrap(&fixture);
    let input = fixture.plan.input();
    let chosen = fixture.plan.chosen();
    let id = input.id().unwrap();
    let records = input.flattened_records(fixture.plan.approval()).unwrap();
    assert_eq!(
        network
            .membership_input_progress(NodeId(2), &p, bootstrap, id)
            .unwrap(),
        VsrMembershipInputProgress::Missing
    );
    assert_eq!(
        network
            .begin_membership_chosen_input(NodeId(2), chosen, input, bootstrap)
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: 0
        }
    );
    let first = VsrHistoryBatch {
        ordinal: 0,
        records: records[..1].to_vec(),
    };
    assert_eq!(
        network
            .append_membership_chosen_input(NodeId(2), input, bootstrap, first.clone())
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: 1
        }
    );
    assert!(
        network
            .finish_membership_chosen_input(NodeId(2), input, bootstrap)
            .is_err()
    );
    let before = network
        .membership_decision_state(NodeId(2), &p, bootstrap)
        .unwrap();
    assert!(before.accepted.is_none());
    assert!(before.chosen.is_none());
    assert!(before.promised.is_none());
    drop(peer);
    peer = PeerProcess::spawn_decision(root, 2, false);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    network = decision_transport(root, 3, peers);
    assert_eq!(
        network
            .append_membership_chosen_input(NodeId(2), input, bootstrap, first)
            .unwrap(),
        VsrMembershipImportReply::BeginRequired
    );
    assert_eq!(
        network
            .membership_input_progress(NodeId(2), &p, bootstrap, id)
            .unwrap(),
        VsrMembershipInputProgress::Partial {
            input: input.clone(),
            records: 1
        }
    );
    assert_eq!(
        network
            .begin_membership_chosen_input(NodeId(2), chosen, input, bootstrap)
            .unwrap(),
        VsrMembershipImportReply::Retained {
            input: id,
            records: 1
        }
    );
    let mut at = 1;
    while at < records.len() {
        let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
        at += batch.records.len();
        assert_eq!(
            network
                .append_membership_chosen_input(NodeId(2), input, bootstrap, batch)
                .unwrap(),
            VsrMembershipImportReply::Retained {
                input: id,
                records: at as u64
            }
        );
    }
    assert_eq!(
        network
            .finish_membership_chosen_input(NodeId(2), input, bootstrap)
            .unwrap(),
        VsrMembershipImportReply::Chosen { input: id }
    );
    assert_eq!(
        network
            .finish_membership_chosen_input(NodeId(2), input, bootstrap)
            .unwrap(),
        VsrMembershipImportReply::Chosen { input: id }
    );
    assert_eq!(
        network
            .query_membership_chosen(NodeId(2), &p, bootstrap, id)
            .unwrap(),
        Some(chosen.clone())
    );
    assert!(!root.join("membership-2/staging").exists());
    drop(peer);
    let store = MembershipDecisionStore::open_existing(
        root.join("membership-2/decision"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(store.chosen(), Some(chosen));
}

#[test]
fn real_tls_prechoice_rejects_foreign_context_and_missing_acknowledged_custody_without_mutation() {
    let fixture = accepted_fixture();
    let root = &fixture.root;
    let peer = PeerProcess::spawn_decision(root, 2, false);
    let mut peers = all_endpoints(root);
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let network = decision_transport(root, 3, peers.clone());
    let removed = decision_transport(root, 0, peers);
    let p = proposed();
    let bootstrap = bootstrap(&fixture);
    let before = files(&root.join("membership-2"));
    let request = wire_request(0, &p, bootstrap, &[]);
    assert!(
        removed
            .exchange_io(NodeId(2), MEMBERSHIP_RECOVERY, &request)
            .is_err()
    );
    for n in [0, 5, 6, 7, 14, 15, 46, 47, 78] {
        assert!(
            network
                .exchange_io(NodeId(2), MEMBERSHIP_RECOVERY, &request[..n])
                .is_err()
        );
    }
    let mut foreign = p.clone();
    foreign.election.target_view = Epoch(3);
    assert!(
        network
            .membership_decision_state(NodeId(2), &foreign, bootstrap)
            .is_err()
    );
    let mut wrong_bootstrap = bootstrap;
    wrong_bootstrap[0] ^= 1;
    assert!(
        network
            .membership_decision_state(NodeId(2), &p, wrong_bootstrap)
            .is_err()
    );
    assert!(
        network
            .exchange_io(
                NodeId(2),
                MEMBERSHIP_RECOVERY,
                &wire_request(255, &p, bootstrap, &[])
            )
            .is_err()
    );
    assert!(
        network
            .exchange_io(
                NodeId(2),
                MEMBERSHIP_RECOVERY,
                &wire_request(0, &p, bootstrap, &[0])
            )
            .is_err()
    );
    assert_eq!(files(&root.join("membership-2")), before);
    let input = fixture.plan.input();
    let directory = root
        .join("membership-2/decision/inputs")
        .join(id_name(input.id().unwrap()));
    std::fs::remove_dir_all(&directory).unwrap();
    let broken = files(&root.join("membership-2"));
    assert!(
        network
            .membership_input_progress(NodeId(2), &p, bootstrap, input.id().unwrap())
            .is_err()
    );
    assert!(
        network
            .begin_membership_chosen_input(NodeId(2), fixture.plan.chosen(), input, bootstrap)
            .is_err()
    );
    assert_eq!(files(&root.join("membership-2")), broken);
    assert!(!directory.exists());
    drop(peer);
}

struct PrechoicePeer {
    stop: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Drop for PrechoicePeer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let joined = self.worker.take().unwrap().join();
        if !std::thread::panicking() {
            joined.unwrap();
        }
    }
}

#[test]
fn real_tls_prechoice_lost_promise_and_chosen_finish_replies_are_idempotent_and_metadata_binds_reporter()
 {
    let fixture = fixture_for_receiver(false);
    let root = &fixture.root;
    let mut peers = all_endpoints(root);
    let mut server = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(root, 2),
        peers.clone(),
        [(PlacementGroupId(1), cell(root, 2))].into(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_membership_paths([(PlacementGroupId(1), root.join("membership-2"))].into())
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    peers.get_mut(&NodeId(2)).unwrap().address = listener.local_addr().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopping = Arc::clone(&stop);
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&attempts);
    let path = root.clone();
    let worker = std::thread::spawn(move || {
        let mut completed = BTreeMap::new();
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            let socket = match listener.accept() {
                Ok((socket, _)) => socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("prechoice listener: {error}"),
            };
            let mut stream = StreamOwned::new(
                ServerConnection::new(Arc::clone(&server.identity.server)).unwrap(),
                DeadlineStream::new(socket, Instant::now() + Duration::from_millis(800)).unwrap(),
            );
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock).unwrap();
            }
            let certificate = stream.conn.peer_certificates().unwrap().first().unwrap();
            let sender = server.nodes[&fingerprint(certificate.as_ref())];
            let bytes = read_frame(&mut stream).unwrap();
            let (kind, payload) = parse(&bytes, &server.cluster, sender, server.local).unwrap();
            assert_eq!(kind, MEMBERSHIP_RECOVERY);
            let tag = payload[6];
            let result = server.dispatch_membership_recovery(sender, payload);
            if matches!(tag, 1 | 6) && result.is_ok() {
                let inventory = files(&path.join("membership-2"));
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if let Some(before) = completed.get(&tag) {
                    assert_eq!(*before, inventory, "retry rewrote durable decision");
                } else {
                    completed.insert(tag, inventory);
                    drop(stream);
                    continue;
                }
            }
            let (reply_kind, reply) = match result {
                Ok(mut value) if tag == 0 => {
                    let mut state =
                        rsf_vsr::VsrMembershipDecisionState::decode(&value[7..]).unwrap();
                    state.replica = NodeId(3);
                    value.truncate(7);
                    value.extend_from_slice(&state.encode().unwrap());
                    (kind, value)
                }
                Ok(value) => (kind, value),
                Err(error) => (255, error.to_string().into_bytes()),
            };
            write_frame(
                &mut stream,
                &frame(&server.cluster, server.local, sender, reply_kind, &reply).unwrap(),
            )
            .unwrap();
            stream.conn.send_close_notify();
            let _ = stream.flush();
        }
    });
    let peer = PrechoicePeer {
        stop,
        worker: Some(worker),
    };
    let network = decision_transport(root, 3, peers);
    let bootstrap = bootstrap(&fixture);
    let round = VsrMembershipRound {
        counter: 8,
        coordinator: NodeId(3),
    };
    assert!(matches!(
        network
            .promise_membership_recovery(NodeId(2), &proposed(), bootstrap, round)
            .unwrap(),
        VsrMembershipPromiseReply::Granted(_)
    ));
    let input = fixture.plan.input();
    network
        .begin_membership_chosen_input(NodeId(2), fixture.plan.chosen(), input, bootstrap)
        .unwrap();
    let records = input.flattened_records(fixture.plan.approval()).unwrap();
    let mut at = 0;
    while at < records.len() {
        let batch = VsrHistoryBatch::pack(at as u64, &records[at..]).unwrap();
        at += batch.records.len();
        network
            .append_membership_chosen_input(NodeId(2), input, bootstrap, batch)
            .unwrap();
    }
    assert_eq!(
        network
            .finish_membership_chosen_input(NodeId(2), input, bootstrap)
            .unwrap(),
        VsrMembershipImportReply::Chosen {
            input: input.id().unwrap()
        }
    );
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 4);
    let before = files(&root.join("membership-2"));
    assert!(
        network
            .membership_decision_state(NodeId(2), &proposed(), bootstrap)
            .is_err()
    );
    assert_eq!(files(&root.join("membership-2")), before);
    drop(peer);
    let store = MembershipDecisionStore::open_existing(
        root.join("membership-2/decision"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert_eq!(store.decision_state().unwrap().promised, Some(round));
    assert_eq!(store.chosen(), Some(fixture.plan.chosen()));
}

#[path = "prechoice/active_read.rs"]
mod active_read;

#[path = "prechoice/custody.rs"]
mod custody;

#[path = "prechoice/journal_custody.rs"]
mod journal_custody;

#[path = "prechoice/inbound_custody.rs"]
mod inbound_custody;

#[cfg(unix)]
#[path = "prechoice/startup_anchor.rs"]
mod startup_anchor;
