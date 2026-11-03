use super::*;

fn rejected<T>(result: Result<T, VsrTransportError>) {
    match result {
        Ok(_) => panic!("damaged decision journal returned a successful authenticated reply"),
        Err(error) => assert!(
            error.reason.contains("peer rejected request:"),
            "expected an actual peer refusal, not a transport timeout: {error:?}"
        ),
    }
}

#[test]
fn real_tls_managed_decision_retries_reject_warm_journal_loss_before_durable_ack() {
    for missing in ["manifest", "segment"] {
        let fixture = accepted_fixture();
        let root = &fixture.root;
        let pg = PlacementGroupId(1);
        let p = proposed();
        let bootstrap = bootstrap(&fixture);
        let mut peers = all_endpoints(root);
        let mut server = PeerReplicaServer::new(
            NodeId(2),
            &cluster(),
            identity(root, 2),
            peers.clone(),
            [(pg, cell(root, 2))].into(),
            Duration::from_millis(800),
        )
        .unwrap()
        .with_existing_membership_paths([(pg, root.join("membership-2"))].into())
        .unwrap()
        .with_membership_recovery(
            [(
                pg,
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
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let worker = std::thread::spawn(move || {
            while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        // Production TLS authentication, managed guard, and
                        // dispatch; no synthetic metadata or reply DTOs.
                        let _ = server.serve_connection_with_apply(socket, &mut |_| {
                            panic!("consensus retries do not materialize application state")
                        });
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("journal custody listener: {error}"),
                }
            }
        });
        let peer = PrechoicePeer {
            stop,
            worker: Some(worker),
        };
        let network = decision_transport(root, 3, peers);
        let input = fixture.plan.input();
        let id = input.id().unwrap();
        let r = VsrMembershipRound {
            counter: 8,
            coordinator: NodeId(3),
        };
        let mut local = MembershipDecisionStore::open(
            root.join("coordinator-3"),
            NodeId(3),
            fixture.plan.approval(),
        )
        .unwrap();
        let VsrMembershipPromiseReply::Granted(remote) = network
            .promise_membership_recovery(NodeId(2), &p, bootstrap, r)
            .unwrap()
        else {
            panic!("promise");
        };
        let own = local.promise(NodeId(3), r).unwrap();
        let decision =
            VsrMembershipDecision::from_promises(vec![own, remote], input.clone()).unwrap();
        let nomination = local.nominate(&decision).unwrap();
        let accepted = network
            .accept_membership(NodeId(2), &decision, &nomination)
            .unwrap();
        let chosen =
            VsrMembershipChosen::from_accepted(vec![nomination.clone(), accepted.clone()]).unwrap();
        network.learn_membership_chosen(NodeId(2), &chosen).unwrap();
        assert_eq!(
            network
                .finish_membership_chosen_input(NodeId(2), input, bootstrap)
                .unwrap(),
            VsrMembershipImportReply::Chosen { input: id }
        );
        assert_eq!(
            network
                .accept_membership(NodeId(2), &decision, &nomination)
                .unwrap(),
            accepted
        );
        assert!(matches!(
            network
                .promise_membership_recovery(NodeId(2), &p, bootstrap, r)
                .unwrap(),
            VsrMembershipPromiseReply::Granted(_)
        ));
        let state = network
            .membership_decision_state(NodeId(2), &p, bootstrap)
            .unwrap();
        assert_eq!(state.chosen, Some((r, id)));
        let journal = root.join("membership-2/decision/journal");
        let lost = if missing == "manifest" {
            journal.join("MANIFEST")
        } else {
            let recovered = rsf_storage::SegmentStore::recover_existing(&journal).unwrap();
            journal
                .join("segments")
                .join(&recovered.segments.last().unwrap().file_name)
        };
        let inputs = files(&root.join("membership-2/decision/inputs"));
        std::fs::remove_file(&lost).unwrap();
        let damaged = files(&journal);
        // Equal-round promise was already cached and successfully acknowledged.
        // It must now return an actual authenticated refusal before any write.
        rejected(network.promise_membership_recovery(NodeId(2), &p, bootstrap, r));
        rejected(network.membership_decision_state(NodeId(2), &p, bootstrap));
        rejected(network.accept_membership(NodeId(2), &decision, &nomination));
        rejected(network.learn_membership_chosen(NodeId(2), &chosen));
        rejected(network.finish_membership_chosen_input(NodeId(2), input, bootstrap));
        rejected(network.query_membership_chosen(NodeId(2), &p, bootstrap, id));
        rejected(network.membership_input_progress(NodeId(2), &p, bootstrap, id));
        // The original PROMISE wire path has the same custody boundary.
        let mut context = pg.0.to_le_bytes().to_vec();
        context.extend_from_slice(&r.counter.to_le_bytes());
        context.extend_from_slice(&r.coordinator.0.to_le_bytes());
        context.extend_from_slice(&p.id().unwrap());
        let error = network
            .exchange_io(NodeId(2), MEMBERSHIP_PROMISE, &context)
            .unwrap_err();
        assert!(error.to_string().contains("peer rejected request:"));
        assert!(!lost.exists());
        assert_eq!(files(&journal), damaged);
        assert_eq!(files(&root.join("membership-2/decision/inputs")), inputs);
        assert!(!root.join("membership-2/staging").exists());
        assert!(!root.join("store-2/VSR_ACTIVE").exists());
        drop(peer);
        drop(local);
        assert!(
            MembershipDecisionStore::open_existing(
                root.join("membership-2/decision"),
                pg,
                NodeId(2)
            )
            .is_err()
        );
        assert!(!lost.exists());
    }
}
