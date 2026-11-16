use super::*;
use crate::replication::PeerMembershipRecoveryTiming;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn real_tls_managed_recovery_polls_both_survivors_while_transport_waits() {
    let mut ready = fixture();
    drop(ready.remote.take());
    drop(ready.local.take());
    let first = TcpListener::bind("127.0.0.1:0").unwrap();
    let second = TcpListener::bind("127.0.0.1:0").unwrap();
    let unavailable = TcpListener::bind("127.0.0.1:0").unwrap();
    ready.peers.get_mut(&NodeId(1)).unwrap().address = unavailable.local_addr().unwrap();
    ready.peers.get_mut(&NodeId(2)).unwrap().address = first.local_addr().unwrap();
    ready.peers.get_mut(&NodeId(3)).unwrap().address = second.local_addr().unwrap();
    // A peer can close before completing TLS (for example after its own
    // bounded attempt expires). Such a connection must not kill the listener.
    // Queue a real closed connection on each survivor before either polls.
    for listener in [&first, &second] {
        let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        socket.shutdown(std::net::Shutdown::Both).unwrap();
        drop(socket);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = Arc::clone(&stop);
    unavailable.set_nonblocking(true).unwrap();
    let blackhole = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut sockets = Vec::new();
        while !stopped.load(Ordering::SeqCst) && Instant::now() < deadline {
            match unavailable.accept() {
                Ok((socket, _)) => sockets.push(socket),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("blackhole accept: {e}"),
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let barrier = Arc::new(Barrier::new(3));
    let majorities = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();
    for (node, listener) in [(2, first), (3, second)] {
        let root = ready.root.clone();
        let peers = ready.peers.clone();
        let barrier = Arc::clone(&barrier);
        let majorities = Arc::clone(&majorities);
        threads.push(std::thread::spawn(move || {
            let mut server = server(&root, node, node == 3, peers)
                .unwrap()
                .with_membership_recovery(
                    [(
                        PG,
                        PeerMembershipRecoveryTiming {
                            interval: Duration::from_millis(1),
                        },
                    )]
                    .into(),
                )
                .unwrap();
            listener.set_nonblocking(true).unwrap();
            barrier.wait();
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut reported = false;
            let mut empty_polls = 0;
            let mut inbound = 0;
            let mut disconnected = 0;
            while Instant::now() < deadline && majorities.load(Ordering::SeqCst) < 2 {
                for _ in 0..8 {
                    match listener.accept() {
                        Ok((socket, _)) => {
                            let started = Instant::now();
                            let mut applied = 0;
                            // Exact implementation of serve_connection_with_apply,
                            // retaining ErrorKind before that wrapper erases it.
                            let result = server.serve_io(socket, &mut |entry| {
                                applied += 1;
                                persist_staged_entry(&root, node, entry)
                            }, true);
                            match result {
                                Ok(()) => inbound += 1,
                                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                                    assert_eq!(applied, 0, "EOF connection invoked materialization");
                                    disconnected += 1;
                                    eprintln!(
                                        "recovery peer {node} rejected disconnected connection after {:?}: {error}",
                                        started.elapsed()
                                    );
                                }
                                Err(error) => panic!("recovery peer {node} inbound RPC failed: {error}"),
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => panic!("recovery accept: {e}"),
                    }
                }
                match server
                    .poll_membership_recovery(&mut |entry| persist_staged_entry(&root, node, entry))
                    .unwrap()
                {
                    Some(update) if update.publication_quorum.is_some() => {
                        assert!(!reported);
                        reported = true;
                        majorities.fetch_add(1, Ordering::SeqCst);
                    }
                    None => empty_polls += 1,
                    Some(_) => {}
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(reported, "node {node} did not recover publication majority");
            assert!(
                empty_polls > 10,
                "node {node} blocked instead of yielding pending transport"
            );
            assert!(inbound > 0, "node {node} did not serve its surviving peer");
            assert!(disconnected > 0, "node {node} did not observe its queued EOF connection");
            assert!(server.membership_recovery_enabled());
            assert!(server.membership_admission_required().unwrap());
            let plan = ready_plan(&server);
            let report = server
                .query_membership_publication(PG, plan, &mut |entry| {
                    persist_staged_entry(&root, node, entry)
                })
                .unwrap();
            assert_eq!(report.replica, NodeId(node));
            drop(server);
        }));
    }
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    blackhole.join().unwrap();
    assert_eq!(majorities.load(Ordering::SeqCst), 2);
    for node in [2, 3] {
        let stage = MembershipStagingStore::open_existing(
            ready.root.join(format!("membership-{node}/staging")),
            PG,
            NodeId(node),
        )
        .unwrap();
        assert_eq!(
            stage.publication_quorum().unwrap().unwrap().plan_id(),
            ready.quorum.plan_id()
        );
        let cell =
            DurableVsrCell::open_existing(ready.root.join(format!("store-{node}")), PG).unwrap();
        assert_eq!(
            cell.request_commit(&ready.request),
            Some(ready.committed.index)
        );
    }
}

fn ready_plan(server: &PeerReplicaServer) -> [u8; 32] {
    server.membership_staging[&PG].plan().id()
}

#[test]
fn managed_recovery_keeps_exact_proposal_guard_after_caught_materializer_unwind() {
    let mut ready = fixture();
    let mut server = ready
        .local
        .take()
        .unwrap()
        .with_membership_recovery(
            [(
                PG,
                PeerMembershipRecoveryTiming {
                    interval: Duration::from_millis(1),
                },
            )]
            .into(),
        )
        .unwrap();
    let mut different = proposal();
    different.election.target_view = Epoch(3);
    different.election.candidate = NodeId(2);
    let before = server.cells[&PG].membership_fence().cloned();
    assert!(
        server
            .guard_membership_recovery(MEMBERSHIP_BEGIN, &different.encode().unwrap())
            .is_err()
    );
    assert_eq!(server.cells[&PG].membership_fence().cloned(), before);
    // Force a cold materialization callback rather than a warm receipt shortcut.
    server.membership_stage_voter_tokens.clear();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = server.poll_membership_recovery(&mut |_| panic!("materializer unwind"));
    }));
    assert!(result.is_err());
    assert!(server.membership_recovery_enabled());
    assert!(server.membership_admission_required().unwrap());
    assert!(
        server
            .guard_membership_recovery(MEMBERSHIP_BEGIN, &different.encode().unwrap())
            .is_err()
    );
    assert!(
        server
            .poll_membership_recovery(&mut |_| Ok(()))
            .unwrap_err()
            .contains("lost custody")
    );
}

#[test]
fn strict_membership_recovery_paths_do_not_create_a_missing_decision() {
    let root = root("strict-membership-recovery");
    provision(&root);
    let inbox =
        rsf_vsr::MembershipAuthorityInbox::open(root.join("transition/inbox"), PG, NodeId(2))
            .unwrap();
    drop(inbox);
    let cells = [(PG, cell(&root, 2))].into();
    let server = PeerReplicaServer::new(
        NodeId(2),
        &cluster(),
        identity(&root, 2),
        peers(&root),
        cells,
        Duration::from_millis(800),
    )
    .unwrap();
    assert!(
        server
            .with_existing_membership_paths([(PG, root.join("transition"))].into())
            .is_err()
    );
    assert!(!root.join("transition/decision").exists());
    std::fs::remove_dir_all(root).unwrap();
}
