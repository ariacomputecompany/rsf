use super::*;

#[test]
fn managed_recovery_drop_joins_live_tls_flight_before_releasing_transport_custody() {
    let mut ready = fixture();
    drop(ready.local.take());
    let blackhole = Blackhole::start(&ready.root);
    ready.peers.get_mut(&NodeId(1)).unwrap().address = blackhole.address;
    for _ in 0..3 {
        let mut local = managed(
            server(&ready.root, 2, false, ready.peers.clone()).unwrap(),
            &[PG],
        );
        // Source0's unique receipt belongs to retained chosen input B. Until
        // publication, the old authoritative cell has never contained it.
        assert_eq!(local.cells[&PG].request_commit(&ready.request), None);
        assert_eq!(
            local.membership_staging[&PG]
                .plan()
                .request_commit(&ready.request),
            Some(LogIndex(1))
        );
        // These client-config Arcs are unique to this freshly opened server and
        // its transport clones. A detached flight would keep the Weak alive.
        let transport_custody = Arc::downgrade(&local.identity.client);
        let before = blackhole.attempts.load(Ordering::SeqCst);
        let dispatched = until(&mut local, &ready.root, |update| {
            update.replica == Some(NodeId(1))
        });
        assert_eq!(dispatched.phase, "staging");
        blackhole.seen.recv_timeout(RPC).unwrap();
        assert!(poll(&mut local, &ready.root).is_none());
        let (started_send, started) = channel();
        let (done_send, done) = channel();
        let dropper = Joined(Some(std::thread::spawn(move || {
            started_send.send(()).unwrap();
            drop(local);
            done_send.send(()).unwrap();
        })));
        started.recv_timeout(RPC).unwrap();
        // The request was observed pending before destruction. Check that no
        // transport ownership survives completion, without assuming the host
        // schedules this assertion a fixed time before the RPC deadline.
        // Both attempts retain their real 800 ms deadline. The outer bound only
        // allows host scheduling overhead while proving join is finite.
        done.recv_timeout(RPC * 2 + Duration::from_secs(1)).unwrap();
        dropper.join();
        assert!(
            transport_custody.upgrade().is_none(),
            "transport work outlived server destruction"
        );
        assert_eq!(
            blackhole.attempts.load(Ordering::SeqCst) - before,
            2,
            "one bounded lost-reply retry per flight"
        );
        blackhole.seen.recv_timeout(RPC).unwrap(); // Consume the retry signal.
        let cell = DurableVsrCell::open_existing(ready.root.join("store-2"), PG).unwrap();
        assert_eq!(
            cell.committed_operation(LogIndex(1)),
            Some(&ready.committed.operation)
        );
        assert_eq!(
            cell.prepared(LogIndex(2)).unwrap().operation,
            ready.accepted.operation
        );
        assert_eq!(cell.request_commit(&ready.request), None);
        assert_eq!(cell.view(), Epoch(1), "shutdown must not publish a stage");
        drop(cell);
        let stage = MembershipStagingStore::open_existing(
            ready.root.join("membership-2/staging"),
            PG,
            NodeId(2),
        )
        .unwrap();
        assert_eq!(stage.plan().id(), ready.quorum.plan_id());
        assert_eq!(
            stage.plan().request_commit(&ready.request),
            Some(LogIndex(1))
        );
        drop(stage);
    }
    drop(blackhole);
}
