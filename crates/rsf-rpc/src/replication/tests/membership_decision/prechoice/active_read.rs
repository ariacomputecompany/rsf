use super::*;

#[test]
fn real_tls_round_bound_copy_activity_rejects_stale_reads_and_keeps_observers_inert() {
    let fixture = accepted_fixture();
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
    .with_existing_membership_paths([(PlacementGroupId(1), root.join("membership-2"))].into())
    .unwrap()
    .with_membership_recovery(
        [(
            PlacementGroupId(1),
            crate::replication::PeerMembershipRecoveryTiming {
                interval: Duration::from_millis(20),
            },
        )]
        .into(),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    peers.get_mut(&NodeId(2)).unwrap().address = listener.local_addr().unwrap();
    let (snapshot_send, snapshots) = std::sync::mpsc::channel();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopping = Arc::clone(&stop);
    let worker = std::thread::spawn(move || {
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            let socket = match listener.accept() {
                Ok((socket, _)) => socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("active read listener: {error}"),
            };
            // Use the production TLS authentication/dispatch path. Recovery is
            // configured but deliberately unpolled so only inbound operations
            // can change this test-owned detector snapshot.
            let _ = server.serve_connection_with_apply(socket, &mut |_| Ok(()));
            snapshot_send
                .send(format!("{:?}", server.membership_recovery))
                .unwrap();
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
    network
        .promise_membership_recovery(NodeId(2), &proposed(), bootstrap, round)
        .unwrap();
    let promised = snapshots.recv_timeout(Duration::from_secs(2)).unwrap();
    let inventory = files(&root.join("membership-2"));
    network
        .membership_decision_state(NodeId(2), &proposed(), bootstrap)
        .unwrap();
    assert_eq!(
        snapshots.recv_timeout(Duration::from_secs(2)).unwrap(),
        promised
    );
    network
        .membership_input_progress(
            NodeId(2),
            &proposed(),
            bootstrap,
            fixture.plan.chosen().input_id(),
        )
        .unwrap();
    assert_eq!(
        snapshots.recv_timeout(Duration::from_secs(2)).unwrap(),
        promised
    );
    network
        .membership_input_batch(NodeId(2), fixture.plan.input(), bootstrap, 0)
        .unwrap();
    assert_eq!(
        snapshots.recv_timeout(Duration::from_secs(2)).unwrap(),
        promised
    );
    assert_eq!(files(&root.join("membership-2")), inventory);
    network
        .membership_round_input_batch(NodeId(2), fixture.plan.input(), bootstrap, round, 0)
        .unwrap();
    let copied = snapshots.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_ne!(copied, promised);
    network
        .membership_round_input_batch(NodeId(2), fixture.plan.input(), bootstrap, round, 0)
        .unwrap();
    assert_eq!(
        snapshots.recv_timeout(Duration::from_secs(2)).unwrap(),
        copied,
        "repeated exact range is not fresh progress"
    );
    let newer = VsrMembershipRound {
        counter: 9,
        ..round
    };
    network
        .promise_membership_recovery(NodeId(2), &proposed(), bootstrap, newer)
        .unwrap();
    let advanced = snapshots.recv_timeout(Duration::from_secs(2)).unwrap();
    let before = files(&root.join("membership-2"));
    assert!(
        network
            .membership_round_input_batch(NodeId(2), fixture.plan.input(), bootstrap, round, 0)
            .is_err()
    );
    assert_eq!(
        snapshots.recv_timeout(Duration::from_secs(2)).unwrap(),
        advanced
    );
    assert_eq!(files(&root.join("membership-2")), before);
    let mut body = fixture.plan.chosen().input_id().to_vec();
    body.extend_from_slice(&0u64.to_le_bytes());
    body.extend_from_slice(&newer.counter.to_le_bytes());
    body.extend_from_slice(&newer.coordinator.0.to_le_bytes());
    for length in [41, 47, 48, 55, 57] {
        let mut malformed = body.clone();
        malformed.resize(length, 0);
        assert!(
            network
                .exchange_io(
                    NodeId(2),
                    MEMBERSHIP_RECOVERY,
                    &wire_request(7, &proposed(), bootstrap, &malformed)
                )
                .is_err()
        );
        assert_eq!(
            snapshots.recv_timeout(Duration::from_secs(2)).unwrap(),
            advanced
        );
    }
    let mut wrong = bootstrap;
    wrong[0] ^= 1;
    assert!(
        network
            .membership_round_input_batch(NodeId(2), fixture.plan.input(), wrong, newer, 0)
            .is_err()
    );
    assert_eq!(
        snapshots.recv_timeout(Duration::from_secs(2)).unwrap(),
        advanced
    );
    assert_eq!(files(&root.join("membership-2")), before);
    drop(peer);
}
