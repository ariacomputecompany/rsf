use super::*;

struct ReplyLossPeer {
    stop: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Drop for ReplyLossPeer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let joined = self.worker.take().unwrap().join();
        if !std::thread::panicking() {
            joined.unwrap();
        }
    }
}
#[test]
fn real_tls_membership_publication_resolves_one_lost_reply_and_binds_reporter() {
    let mut r = fixture();
    let path = r.root.clone();
    let quorum = r.quorum.clone();
    r.local()
        .publish_membership_locally(&quorum, &mut |entry| persist_staged_entry(&path, 2, entry))
        .unwrap();
    drop(r.remote.take());
    let mut server = server(&path, 3, true, r.peers.clone()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    r.peers.get_mut(&NodeId(3)).unwrap().address = listener.local_addr().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopping = Arc::clone(&stop);
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&attempts);
    let root = path.clone();
    let worker = std::thread::spawn(move || {
        let mut published = None;
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            let socket = match listener.accept() {
                Ok((socket, _)) => socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("publication listener: {error}"),
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
            let request = read_frame(&mut stream).unwrap();
            let (kind, payload) = parse(&request, &server.cluster, sender, server.local).unwrap();
            // The real dispatcher returns only after durable publication and
            // current-prefix application. Suppress that completed reply once.
            let response =
                server.dispatch_membership_publication(sender, kind, payload, &mut |entry| {
                    persist_staged_entry(&root, 3, entry)
                });
            if kind == MEMBERSHIP_PUBLISH && response.is_ok() {
                let current = inventory(&root, 3);
                let count = seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count == 0 {
                    published = Some(current);
                    drop(stream);
                    continue;
                }
                assert_eq!(count, 1, "more than one recovery retry");
                assert_eq!(
                    Some(current),
                    published,
                    "lost reply caused new authority or WAL writes"
                );
            }
            let (reply_kind, bytes) = match response {
                Ok(bytes) if kind == MEMBERSHIP_PUBLICATION => {
                    // A valid report cannot borrow another voter's identity,
                    // even inside a frame authenticated as the real node 3.
                    let mut report = rsf_vsr::VsrMembershipPublishedReport::decode(&bytes).unwrap();
                    report.replica = NodeId(2);
                    (kind, report.encode().unwrap())
                }
                Ok(bytes) => (kind, bytes),
                Err(error) => (255, error.to_string().into_bytes()),
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
    let peer = ReplyLossPeer {
        stop,
        worker: Some(worker),
    };
    let report = r.network().publish_membership(NodeId(3), &quorum).unwrap();
    assert_eq!(report.replica, NodeId(3));
    assert_eq!(report.plan, quorum.plan_id());
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    let before_query = inventory(&path, 3);
    let error = r
        .network()
        .query_membership_publication(NodeId(3), PG, quorum.plan_id())
        .unwrap_err();
    assert!(
        error.reason.contains("another reporter"),
        "{}",
        error.reason
    );
    assert_eq!(inventory(&path, 3), before_query);
    drop(peer);
    let promoted = DurableVsrCell::open_existing(path.join("store-3"), PG).unwrap();
    assert_eq!(
        promoted.committed_operation(LogIndex(1)),
        Some(&r.committed.operation)
    );
    assert_eq!(
        promoted.prepared(LogIndex(2)).unwrap().operation,
        r.accepted.operation
    );
    assert_eq!(promoted.request_commit(&r.request), Some(LogIndex(1)));
    assert_eq!(promoted.promoted_replica().unwrap(), Some(NodeId(3)));
    drop(promoted);
}
