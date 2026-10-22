use super::*;
use rsf_vsr::{
    MembershipDecisionStore, VsrMembershipInput, VsrMembershipProposal, VsrMembershipRound,
};
fn proposed() -> VsrMembershipProposal {
    VsrMembershipProposal {
        election: rsf_vsr::VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(1),
            target_view: Epoch(2),
            candidate: NodeId(1),
            voters: vec![NodeId(0), NodeId(1), NodeId(2)],
        },
        old_primary: NodeId(0),
        new_voters: vec![NodeId(1), NodeId(2), NodeId(3)],
    }
}
fn decision_transport(
    root: &Path,
    local: u64,
    peers: BTreeMap<NodeId, PeerEndpoint>,
) -> NetworkVsrTransport {
    NetworkVsrTransport::new(
        NodeId(local),
        &cluster(),
        identity(root, local),
        peers,
        Duration::from_millis(800),
    )
    .unwrap()
}
fn all_endpoints(root: &Path) -> BTreeMap<NodeId, PeerEndpoint> {
    let mut peers = endpoints(root);
    peers.insert(
        NodeId(3),
        PeerEndpoint {
            address: "127.0.0.1:1".parse().unwrap(),
            server_name: "localhost".into(),
            fingerprint: identity(root, 3).fingerprint,
        },
    );
    peers
}
#[test]
fn real_tls_membership_plan_choice_survives_old_voter_and_coordinator_data_loss() {
    let root = root("decision-peer-recovery");
    provision(&root);
    let first = PeerProcess::spawn_decision(&root, 1, false);
    let second = PeerProcess::spawn_staging(&root, 2, false);
    let learner = PeerProcess::spawn_decision(&root, 3, true);
    let mut peers = all_endpoints(&root);
    for (node, address) in [
        (1, first.address),
        (2, second.address),
        (3, learner.address),
    ] {
        peers.get_mut(&NodeId(node)).unwrap().address = address;
    }
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let prepared = prepare(
        &mut leader,
        b"full membership transition payload remains intact",
    );
    leader
        .replicate_with_transport(NodeId(0), prepared.index, &mut network)
        .unwrap();
    let request = rsf_vsr::VsrClientRequestId::new("membership-original", 7);
    leader
        .remember_client_request(request.clone(), prepared.index)
        .unwrap();
    network.sync_learner(NodeId(3), &mut leader).unwrap();
    let approval = network
        .approve_membership(&mut leader, &proposed())
        .unwrap();
    let id = VsrMembershipInput::from_approval(&approval)
        .unwrap()
        .id()
        .unwrap();
    drop(network);
    drop(leader);
    drop(first);
    drop(learner);
    std::fs::remove_dir_all(root.join("store-0")).unwrap();
    let local_cell = cell(&root, 1);
    local_cell.validate_membership_authority(&approval).unwrap();
    let mut local =
        MembershipDecisionStore::open(root.join("membership-1/decision"), NodeId(1), &approval)
            .unwrap();
    let network = decision_transport(&root, 1, peers.clone());
    // Learner 3 is offline: only 1 and 2 accept the original chosen input.
    let chosen = network
        .choose_membership_plan(
            &mut local,
            VsrMembershipRound {
                counter: 1,
                coordinator: NodeId(1),
            },
            &approval,
        )
        .unwrap();
    assert_eq!(chosen.input_id(), id);
    assert_eq!(chosen.accepted().len(), 2);
    assert!(local_cell.primary_probe().is_err());
    assert!(!local_cell.contains_replica(NodeId(3)));
    drop(local_cell);
    drop(local);
    drop(network);
    std::fs::remove_dir_all(root.join("store-1")).unwrap();
    std::fs::remove_dir_all(root.join("membership-1")).unwrap();
    let network = decision_transport(&root, 3, peers.clone());
    let retained = network
        .fetch_membership_input(NodeId(2), PlacementGroupId(1), id)
        .unwrap();
    // A different valid old-quorum subset is a different candidate input. The
    // replacement must fetch/retain the accepted input from node 2 instead.
    let candidate =
        rsf_vsr::VsrMembershipApproval::from_snapshots(retained.snapshots()[1..].to_vec()).unwrap();
    assert_ne!(
        VsrMembershipInput::from_approval(&candidate)
            .unwrap()
            .id()
            .unwrap(),
        id
    );
    let anchor = rsf_vsr::LearnerHistoryStore::open(
        root.join("learner-3"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap();
    anchor.validate_membership_authority(&candidate).unwrap();
    let mut survivor =
        MembershipDecisionStore::open(root.join("membership-3/decision"), NodeId(3), &candidate)
            .unwrap();
    let recovered = network
        .choose_membership_plan(
            &mut survivor,
            VsrMembershipRound {
                counter: 2,
                coordinator: NodeId(3),
            },
            &candidate,
        )
        .unwrap();
    assert_eq!(recovered.input_id(), id);
    assert!(
        recovered
            .accepted()
            .iter()
            .all(|r| [NodeId(2), NodeId(3)].contains(&r.replica))
    );
    assert_eq!(
        anchor.committed_entries().unwrap()[0].prepare.operation,
        prepared.operation
    );
    let proof = survivor.approval(id).unwrap();
    let staged_plan = rsf_vsr::VsrMembershipStagedPlan::select(
        recovered.clone(),
        rsf_vsr::VsrMembershipApproval::from_snapshots(proof.snapshots().to_vec()).unwrap(),
    )
    .unwrap();
    let mut staged = anchor
        .stage_membership(root.join("membership-3/staging"), staged_plan)
        .unwrap();
    let local_receipt = staged
        .materialize(&mut |entry| {
            persist_staged_entry(&root, 3, entry).map_err(|reason| {
                rsf_core::RsfError::new(rsf_core::ErrorCode::InvalidInput, reason)
            })
        })
        .unwrap();
    let staged_quorum = network
        .collect_membership_stages(local_receipt, staged.plan())
        .unwrap();
    assert_eq!(staged_quorum.proposal().election.candidate, NodeId(1));
    assert!(
        staged_quorum
            .reports()
            .iter()
            .all(|r| [NodeId(2), NodeId(3)].contains(&r.replica))
    );
    drop(staged);
    let original = proof
        .snapshots()
        .iter()
        .find(|s| s.report.voter.replica == NodeId(0))
        .unwrap();
    let mut restored = rsf_storage::SegmentStore::open(root.join("verified-old-proof")).unwrap();
    for record in &original.records {
        restored
            .append_next_wal(
                PlacementGroupId(1),
                record.epoch,
                record.kind,
                &record.payload,
            )
            .unwrap();
    }
    drop(restored);
    let restored = DurableVsrCell::open(
        root.join("verified-old-proof"),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap();
    assert_eq!(restored.request_commit(&request), Some(prepared.index));
    assert_eq!(
        restored.committed_prefix().unwrap()[0].prepare.operation,
        prepared.operation
    );
    drop(restored);
    drop(survivor);
    drop(anchor);
    drop(network);
    let learner = PeerProcess::spawn_decision(&root, 3, true);
    peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
    let network = decision_transport(&root, 2, peers);
    assert_eq!(
        network
            .fetch_membership_input(NodeId(3), PlacementGroupId(1), id)
            .unwrap()
            .proposal(),
        &proposed()
    );
    // Persisted transition choice still does not promote the learner.
    assert!(
        network
            .request_io(NodeId(3), TAIL, &PlacementGroupId(1).0.to_le_bytes())
            .is_err()
    );
    let mut spoofed = PlacementGroupId(1).0.to_le_bytes().to_vec();
    spoofed.extend_from_slice(&3u64.to_le_bytes());
    spoofed.extend_from_slice(&3u64.to_le_bytes());
    spoofed.extend_from_slice(&proposed().id().unwrap());
    assert!(
        network
            .exchange_io(NodeId(3), MEMBERSHIP_PROMISE, &spoofed)
            .is_err()
    );
    drop(second);
    drop(learner);
    std::fs::remove_dir_all(root).unwrap();
}
fn empty_approval(root: &Path, name: &str, bootstrap: Epoch) -> rsf_vsr::VsrMembershipApproval {
    let mut snapshots = Vec::new();
    for node in 0..2 {
        let mut cell = DurableVsrCell::open(
            root.join(format!("{name}-{node}")),
            PlacementGroupId(1),
            bootstrap,
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        if bootstrap < Epoch(1) {
            cell.view_change(Epoch(1), NodeId(0));
            assert_eq!(cell.view(), Epoch(1));
        }
        snapshots.push(
            cell.begin_membership(NodeId(node), NodeId(0), proposed())
                .unwrap(),
        );
    }
    rsf_vsr::VsrMembershipApproval::from_snapshots(snapshots).unwrap()
}
#[test]
fn real_tls_membership_authority_rejects_unpublished_learners_and_foreign_bootstrap() {
    let root = root("decision-authority-rejection");
    provision(&root);
    let second = PeerProcess::spawn_decision(&root, 2, false);
    let learner = PeerProcess::spawn_decision(&root, 3, true);
    let mut peers = all_endpoints(&root);
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
    let network = decision_transport(&root, 1, peers);
    let proof = empty_approval(&root, "normal", Epoch(1));
    let rejected = network
        .provision_membership_authority(NodeId(3), &proof)
        .unwrap_err();
    assert!(
        rejected.reason.contains("published authority anchor"),
        "{}",
        rejected.reason
    );
    assert!(!root.join("membership-3/decision").exists());
    let foreign = empty_approval(&root, "foreign", Epoch(0));
    let rejected = network
        .provision_membership_authority(NodeId(2), &foreign)
        .unwrap_err();
    assert!(rejected.reason.contains("bootstrap"), "{}", rejected.reason);
    assert!(!root.join("membership-2/decision").exists());
    // A failed upload cannot leave a usable round promise behind.
    let mut query = PlacementGroupId(1).0.to_le_bytes().to_vec();
    query.extend_from_slice(&1u64.to_le_bytes());
    query.extend_from_slice(&1u64.to_le_bytes());
    query.extend_from_slice(&proposed().id().unwrap());
    assert!(
        network
            .exchange_io(NodeId(2), MEMBERSHIP_PROMISE, &query)
            .is_err()
    );
    assert!(
        network
            .exchange_io(NodeId(3), MEMBERSHIP_PROMISE, &query)
            .is_err()
    );
    drop(second);
    drop(learner);
    std::fs::remove_dir_all(root).unwrap();
}

struct DecisionReplyLossPeer {
    address: SocketAddr,
    stop: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Drop for DecisionReplyLossPeer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
    }
}
#[test]
fn membership_acceptance_resolves_one_lost_tls_reply_without_duplicate_vote() {
    let root = root("decision-lost-acceptance");
    provision(&root);
    let approval = empty_approval(&root, "authority", Epoch(1));
    let mut peers = all_endpoints(&root);
    let mut server = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        peers.clone(),
        [(PlacementGroupId(1), cell(&root, 2))].into(),
        Duration::from_millis(800),
    )
    .unwrap()
    .with_membership_paths([(PlacementGroupId(1), root.join("membership-2"))].into())
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopping = Arc::clone(&stop);
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts = Arc::clone(&accepted);
    let path = root.clone();
    let worker = std::thread::spawn(move || {
        let mut first_count = None;
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            let socket = match listener.accept() {
                Ok((s, _)) => s,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(e) => panic!("decision listener: {e}"),
            };
            // Authenticate the normal TLS/frame envelope, invoke the production
            // dispatcher, then suppress exactly one already-durable acceptance.
            let mut stream = StreamOwned::new(
                ServerConnection::new(Arc::clone(&server.identity.server)).unwrap(),
                DeadlineStream::new(socket, Instant::now() + Duration::from_millis(800)).unwrap(),
            );
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock).unwrap();
            }
            let cert = stream.conn.peer_certificates().unwrap().first().unwrap();
            let sender = server.nodes[&fingerprint(cert.as_ref())];
            let request = read_frame(&mut stream).unwrap();
            let (kind, payload) = parse(&request, &server.cluster, sender, server.local).unwrap();
            let response = server.dispatch_membership_decision(sender, kind, payload);
            if kind == MEMBERSHIP_ACCEPT && response.is_ok() {
                let count = rsf_storage::SegmentStore::recover_existing(
                    path.join("membership-2/decision/journal"),
                )
                .unwrap()
                .recovery
                .wal_records
                .len();
                let prior = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if prior == 0 {
                    first_count = Some(count);
                    drop(stream);
                    continue;
                }
                assert_eq!(prior, 1);
                assert_eq!(
                    Some(count),
                    first_count,
                    "retry appended another acceptance"
                );
            }
            let (reply_kind, bytes) = match response {
                Ok(bytes) => (kind, bytes),
                Err(e) => (255, e.to_string().into_bytes()),
            };
            write_frame(
                &mut stream,
                &frame(&server.cluster, server.local, sender, reply_kind, &bytes).unwrap(),
            )
            .unwrap();
            stream.conn.send_close_notify();
            let _ = stream.flush();
        }
    });
    let peer = DecisionReplyLossPeer {
        address,
        stop,
        worker: Some(worker),
    };
    peers.get_mut(&NodeId(2)).unwrap().address = peer.address;
    let network = decision_transport(&root, 1, peers);
    let mut local =
        MembershipDecisionStore::open(root.join("membership-1/decision"), NodeId(1), &approval)
            .unwrap();
    let chosen = network
        .choose_membership_plan(
            &mut local,
            VsrMembershipRound {
                counter: 1,
                coordinator: NodeId(1),
            },
            &approval,
        )
        .unwrap();
    assert_eq!(chosen.accepted().len(), 2);
    assert_eq!(accepted.load(std::sync::atomic::Ordering::SeqCst), 2);
    drop(local);
    drop(peer);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn real_tls_staged_membership_requires_materializer_and_survives_peer_restart() {
    let root = root("staged-peer-materialization");
    provision(&root);
    let first = PeerProcess::spawn_staging(&root, 1, false);
    let mut second = PeerProcess::spawn_staging(&root, 2, false);
    let mut learner = PeerProcess::spawn_decision(&root, 3, true);
    let mut peers = all_endpoints(&root);
    for (node, address) in [
        (1, first.address),
        (2, second.address),
        (3, learner.address),
    ] {
        peers.get_mut(&NodeId(node)).unwrap().address = address;
    }
    let mut network = transport(&root, peers.clone());
    let mut leader = cell(&root, 0);
    let payload = (0..128 * 1024).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let prepared = prepare(&mut leader, &payload);
    leader
        .replicate_with_transport(NodeId(0), prepared.index, &mut network)
        .unwrap();
    network.sync_learner(NodeId(3), &mut leader).unwrap();
    let approval = network
        .approve_membership(&mut leader, &proposed())
        .unwrap();
    drop(network);
    drop(first);
    let local_cell = cell(&root, 1);
    local_cell.validate_membership_authority(&approval).unwrap();
    let mut decision =
        MembershipDecisionStore::open(root.join("membership-1/decision"), NodeId(1), &approval)
            .unwrap();
    let network = decision_transport(&root, 1, peers.clone());
    let chosen = network
        .choose_membership_plan(
            &mut decision,
            VsrMembershipRound {
                counter: 1,
                coordinator: NodeId(1),
            },
            &approval,
        )
        .unwrap();
    let plan = rsf_vsr::VsrMembershipStagedPlan::select(chosen, approval).unwrap();
    let mut staged = rsf_vsr::MembershipStagingStore::stage(
        root.join("membership-1/staging"),
        NodeId(1),
        plan,
        &local_cell,
    )
    .unwrap();
    let local = staged
        .materialize(&mut |entry| {
            persist_staged_entry(&root, 1, entry).map_err(|reason| {
                rsf_core::RsfError::new(rsf_core::ErrorCode::InvalidInput, reason)
            })
        })
        .unwrap();
    let quorum = network
        .collect_membership_stages(local.clone(), staged.plan())
        .unwrap();
    assert_eq!(quorum.reports().len(), 2);
    assert!(quorum.reports().iter().all(|r| r.replica != NodeId(3)));
    assert!(
        !root.join("membership-3/staging/META").exists(),
        "a peer with no explicit materializer acknowledged staging"
    );
    let previous = quorum
        .reports()
        .iter()
        .find(|r| r.replica == NodeId(2))
        .unwrap()
        .clone();
    let expected = prepared.encode().unwrap();
    assert_eq!(
        std::fs::read(root.join("materialized-2/1")).unwrap(),
        expected
    );
    drop(second);
    std::fs::remove_dir_all(root.join("materialized-2")).unwrap();
    second = PeerProcess::spawn_staging(&root, 2, false);
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let network = decision_transport(&root, 1, peers.clone());
    let recovered = network
        .collect_membership_stages(local.clone(), staged.plan())
        .unwrap();
    assert_eq!(
        recovered.reports().iter().find(|r| r.replica == NodeId(2)),
        Some(&previous)
    );
    assert_eq!(
        std::fs::read(root.join("materialized-2/1")).unwrap(),
        expected
    );
    let applied_before_retry = std::fs::read(root.join("materialized-2/.applies")).unwrap();
    drop(learner);
    learner = PeerProcess::spawn_staging(&root, 3, true);
    peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
    let network = decision_transport(&root, 1, peers);
    let complete = network
        .collect_membership_stages(local, staged.plan())
        .unwrap();
    assert_eq!(complete.reports().len(), 3);
    assert_eq!(
        std::fs::read(root.join("materialized-2/.applies")).unwrap(),
        applied_before_retry,
        "warm staging retry repeated application work"
    );
    assert_eq!(
        std::fs::read(root.join("materialized-3/1")).unwrap(),
        expected
    );
    assert!(
        network
            .request_io(NodeId(3), TAIL, &PlacementGroupId(1).0.to_le_bytes())
            .is_err()
    );
    assert!(local_cell.primary_probe().is_err());
    assert!(!local_cell.contains_replica(NodeId(3)));
    drop(staged);
    drop(decision);
    drop(local_cell);
    drop(leader);
    drop(second);
    drop(learner);
    std::fs::remove_dir_all(root).unwrap();
}
