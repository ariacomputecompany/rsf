use super::*;

const SECOND: PlacementGroupId = PlacementGroupId(2);

fn seed_second(root: &Path) {
    let root = root.join("group-2");
    let mut cells = (0..3)
        .map(|node| {
            DurableVsrCell::open(
                root.join(format!("store-{node}")),
                SECOND,
                Epoch(1),
                NodeId(0),
                (0..3).map(NodeId),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let payload = (0..128 * 1024).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    let first = prepare(&mut cells[0], &payload);
    for node in [1, 2] {
        cells[node]
            .receive_prepare(NodeId(node as u64), NodeId(0), first.clone())
            .unwrap();
        cells[0].acknowledge(NodeId(node as u64), Epoch(1), first.index);
    }
    let VsrDecision::Committed(commit) = cells[0].commit(NodeId(0), first.index).unwrap() else {
        panic!("commit")
    };
    for node in [1, 2] {
        cells[node]
            .receive_commit(NodeId(node as u64), NodeId(0), commit.clone())
            .unwrap();
    }
    cells[0]
        .remember_client_request(
            VsrClientRequestId::new("second-group-original", 1),
            first.index,
        )
        .unwrap();
    let accepted = prepare(&mut cells[0], b"second group accepted tail");
    for node in [1, 2] {
        cells[node]
            .receive_prepare(NodeId(node as u64), NodeId(0), accepted.clone())
            .unwrap();
    }
    let mut learner =
        LearnerHistoryStore::open(root.join("learner-3"), SECOND, NodeId(3), NodeId(0)).unwrap();
    let history = cells[0].history_snapshot().unwrap();
    let id = history.descriptor.id().unwrap();
    learner.begin(history.descriptor).unwrap();
    for (ordinal, record) in history.records.into_iter().enumerate() {
        learner.append(id, ordinal as u64, record).unwrap();
    }
    learner.finish(id).unwrap();
    let mut proposal = proposal();
    proposal.election.pg = SECOND;
    let approval = VsrMembershipApproval::from_snapshots(
        cells
            .iter_mut()
            .enumerate()
            .map(|(node, cell)| {
                cell.begin_membership(NodeId(node as u64), NodeId(0), proposal.clone())
                    .unwrap()
            })
            .collect(),
    )
    .unwrap();
    for node in [2, 3] {
        drop(
            rsf_vsr::MembershipAuthorityInbox::open(
                root.join(format!("membership-{node}/inbox")),
                SECOND,
                NodeId(node),
            )
            .unwrap(),
        );
    }
    let mut decisions = [1, 2, 3].map(|node| {
        MembershipDecisionStore::open(
            root.join(format!("membership-{node}/decision")),
            NodeId(node),
            &approval,
        )
        .unwrap()
    });
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: NodeId(2),
    };
    let promises = decisions
        .iter_mut()
        .map(|decision| decision.promise(NodeId(2), round).unwrap())
        .collect();
    let decision = rsf_vsr::VsrMembershipDecision::from_promises(
        promises,
        VsrMembershipInput::from_approval(&approval).unwrap(),
    )
    .unwrap();
    let nomination = decisions[1].nominate(&decision).unwrap();
    let first = decisions[0]
        .accept(NodeId(2), &decision, &nomination)
        .unwrap();
    let third = decisions[2]
        .accept(NodeId(2), &decision, &nomination)
        .unwrap();
    let chosen = VsrMembershipChosen::from_accepted(vec![first, nomination, third]).unwrap();
    for decision in &mut decisions {
        decision.learn_chosen(chosen.clone()).unwrap();
    }
    let plan = || {
        VsrMembershipStagedPlan::select(
            chosen.clone(),
            VsrMembershipApproval::from_snapshots(approval.snapshots().to_vec()).unwrap(),
        )
        .unwrap()
    };
    let mut local = MembershipStagingStore::stage(
        root.join("membership-2/staging"),
        NodeId(2),
        plan(),
        &cells[2],
    )
    .unwrap();
    let mut remote = learner
        .stage_membership(root.join("membership-3/staging"), plan())
        .unwrap();
    for (node, stage) in [(2, &mut local), (3, &mut remote)] {
        stage
            .materialize(&mut |entry| {
                persist_staged_entry(&root, node, entry).map_err(|reason| {
                    rsf_core::RsfError::new(rsf_core::ErrorCode::InvalidInput, reason)
                })
            })
            .unwrap();
    }
}

fn combined(root: &Path, node: u64, peers: BTreeMap<NodeId, PeerEndpoint>) -> PeerReplicaServer {
    let mut cells = BTreeMap::new();
    let mut learners = BTreeMap::new();
    let mut paths = BTreeMap::new();
    let mut targets = BTreeMap::new();
    for (pg, directory) in [(PG, root.to_path_buf()), (SECOND, root.join("group-2"))] {
        paths.insert(pg, directory.join(format!("membership-{node}")));
        if node == 3 {
            learners.insert(
                pg,
                LearnerHistoryStore::open(directory.join("learner-3"), pg, NodeId(3), NodeId(0))
                    .unwrap(),
            );
            targets.insert(pg, directory.join("store-3"));
        } else {
            cells.insert(
                pg,
                DurableVsrCell::open_existing(directory.join(format!("store-{node}")), pg).unwrap(),
            );
        }
    }
    PeerReplicaServer::with_learners(
        NodeId(node),
        &cluster(),
        identity(root, node),
        peers,
        cells,
        learners,
        RPC,
    )
    .unwrap()
    .with_existing_membership_paths(paths)
    .unwrap()
    .with_promotion_paths(targets)
    .unwrap()
}

#[test]
fn real_tls_two_group_recovery_serves_inbound_and_rotates_past_a_blackhole() {
    let mut ready = fixture();
    drop(ready.remote.take());
    drop(ready.local.take());
    seed_second(&ready.root);
    let blackhole = Blackhole::start(&ready.root);
    ready.peers.get_mut(&NodeId(1)).unwrap().address = blackhole.address;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    ready.peers.get_mut(&NodeId(2)).unwrap().address = listener.local_addr().unwrap();
    let remote = GatedPeer::start(
        combined(&ready.root, 3, ready.peers.clone()),
        ready.root.clone(),
        false,
        false,
    );
    ready.peers.get_mut(&NodeId(3)).unwrap().address = remote.address;
    let mut local = managed(combined(&ready.root, 2, ready.peers.clone()), &[PG, SECOND]);
    let initial = until(&mut local, &ready.root, |update| {
        update.replica == Some(NodeId(1))
    });
    assert_eq!(initial.pg, PG);
    blackhole.seen.recv_timeout(RPC).unwrap();
    assert!(poll(&mut local, &ready.root).is_none());
    let observer = transport_for(&ready.root, 1, ready.peers.clone());
    let (status_send, status) = channel();
    let client = Joined(Some(std::thread::spawn(move || {
        status_send
            .send(observer.replica_status(NodeId(2), SECOND))
            .unwrap();
    })));
    let mut progress = Vec::new();
    let mut inbound = false;
    let deadline = Instant::now() + RPC;
    let observed = loop {
        match listener.accept() {
            Ok((socket, _)) => {
                local
                    .serve_connection_with_apply(socket, &mut |entry| apply(&ready.root, 2, entry))
                    .unwrap();
                inbound = true;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("inbound accept: {error}"),
        }
        if let Ok(status) = status.try_recv() {
            break status.unwrap();
        }
        if let Some(update) = poll(&mut local, &ready.root) {
            progress.push(update);
        }
        assert!(
            Instant::now() < deadline,
            "inbound service waited for the outbound blackhole"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    client.join();
    assert!(inbound);
    assert_eq!(observed.normal.pg, SECOND);
    assert_eq!(observed.normal.committed_tail, LogIndex(1));
    assert!(
        poll(&mut local, &ready.root).is_none(),
        "inbound status must finish while the flight is still pending"
    );
    let deadline = Instant::now() + HARNESS;
    let mut published = BTreeSet::new();
    while published.len() < 2 {
        if let Some(update) = poll(&mut local, &ready.root) {
            if update.publication_quorum.is_some() {
                published.insert(update.pg);
            }
            progress.push(update);
        }
        assert!(
            Instant::now() < deadline,
            "one blackhole starved a managed placement group"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let first_dispatch = progress
        .iter()
        .find(|update| update.detail.contains("exchange dispatched"))
        .unwrap();
    assert_eq!(
        first_dispatch.pg, SECOND,
        "after PG1's bounded flight, the next ready group must run"
    );
    assert_eq!(published, [PG, SECOND].into());
    assert!(local.membership_admission_required().unwrap());
    for pg in [PG, SECOND] {
        let cell = &local.cells[&pg];
        assert_eq!(cell.view(), Epoch(2));
        assert_eq!(cell.committed_tail(), LogIndex(1));
        assert!(cell.prepared(LogIndex(2)).is_some());
        let VsrOperation::ReplicatedCommand { payload, .. } =
            cell.committed_operation(LogIndex(1)).unwrap()
        else {
            panic!("payload")
        };
        assert_eq!(payload.len(), 128 * 1024);
    }
    assert_eq!(
        local.cells[&PG].request_commit(&ready.request),
        Some(LogIndex(1))
    );
    assert_eq!(
        local.cells[&SECOND].request_commit(&VsrClientRequestId::new("second-group-original", 1)),
        Some(LogIndex(1))
    );
    drop(local);
    drop(remote);
    drop(blackhole);
}
