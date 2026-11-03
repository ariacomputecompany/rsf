use super::*;

type TlsStream = StreamOwned<ServerConnection, DeadlineStream>;

fn request(
    socket: TcpStream,
    identity: &PeerIdentity,
) -> io::Result<(TlsStream, Vec<u8>, [u8; 32])> {
    let mut stream = StreamOwned::new(
        ServerConnection::new(Arc::clone(&identity.server)).map_err(|e| invalid(e.to_string()))?,
        DeadlineStream::new(socket, Instant::now() + RPC)?,
    );
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock)?;
    }
    let fingerprint = fingerprint(
        stream
            .conn
            .peer_certificates()
            .unwrap()
            .first()
            .unwrap()
            .as_ref(),
    );
    let bytes = read_frame(&mut stream)?;
    Ok((stream, bytes, fingerprint))
}

pub(super) struct Blackhole {
    pub address: SocketAddr,
    pub seen: Receiver<()>,
    pub attempts: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Blackhole {
    pub fn start(root: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let expected = identity(root, 2).fingerprint;
        let identity = identity(root, 1);
        let cluster = cluster_digest(&cluster());
        let (send, seen) = channel();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&attempts);
        let thread = std::thread::spawn(move || {
            let mut retained = Vec::new();
            while !stopping.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let (stream, bytes, fingerprint) = request(socket, &identity).unwrap();
                        assert_eq!(fingerprint, expected);
                        let (kind, _) = parse(&bytes, &cluster, NodeId(2), NodeId(1)).unwrap();
                        assert!(matches!(
                            kind,
                            MEMBERSHIP_STAGE
                                | MEMBERSHIP_AUTH_BEGIN
                                | MEMBERSHIP_PUBLISH
                                | MEMBERSHIP_PUBLICATION
                                | MEMBERSHIP_RECOVERY
                        ));
                        retained.push(stream); // The authenticated request receives no reply.
                        count.fetch_add(1, Ordering::SeqCst);
                        send.send(()).unwrap();
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("blackhole accept: {error}"),
                }
            }
        });
        Self {
            address,
            seen,
            attempts,
            stop,
            thread: Some(thread),
        }
    }
}
impl Drop for Blackhole {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let joined = self.thread.take().unwrap().join();
        if !std::thread::panicking() {
            joined.unwrap();
        }
    }
}

enum Command {
    Release,
    Stop,
}
pub(super) struct GatedPeer {
    pub address: SocketAddr,
    pub gated: Receiver<()>,
    pub delivered: Receiver<()>,
    commands: Sender<Command>,
    thread: Option<std::thread::JoinHandle<PeerReplicaServer>>,
}
impl GatedPeer {
    pub fn start(mut server: PeerReplicaServer, root: PathBuf, hold: bool, corrupt: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let (commands, receive) = channel();
        let (gated_send, gated) = channel();
        let (delivered_send, delivered) = channel();
        let thread = std::thread::spawn(move || {
            let mut held: Option<(TlsStream, Vec<u8>)> = None;
            let mut gate_once = hold;
            loop {
                while let Ok(command) = receive.try_recv() {
                    match command {
                        Command::Stop => return server,
                        Command::Release => {
                            let (mut stream, bytes) = held
                                .take()
                                .expect("release requires a completed held reply");
                            write_frame(&mut stream, &bytes)
                                .expect("held response exceeded unchanged RPC deadline");
                            stream.flush().unwrap();
                            delivered_send.send(()).unwrap();
                        }
                    }
                }
                let socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("gated peer accept: {error}"),
                };
                let (mut stream, bytes, fingerprint) = request(socket, &server.identity).unwrap();
                let sender = server.nodes[&fingerprint];
                let (kind, payload) = parse(&bytes, &server.cluster, sender, server.local).unwrap();
                server.guard_membership_recovery(kind, payload).unwrap();
                let local = server.local.0;
                let response = match kind {
                    MEMBERSHIP_RECOVERY => server.dispatch_membership_recovery(sender, payload),
                    MEMBERSHIP_AUTH_BEGIN
                    | MEMBERSHIP_AUTH_BATCH
                    | MEMBERSHIP_AUTH_FINISH
                    | MEMBERSHIP_PROMISE
                    | MEMBERSHIP_INPUT_BEGIN
                    | MEMBERSHIP_INPUT_BATCH
                    | MEMBERSHIP_ACCEPT
                    | MEMBERSHIP_CHOSEN
                    | MEMBERSHIP_INPUT_DESCRIPTOR
                    | MEMBERSHIP_INPUT_READ => {
                        server.dispatch_membership_decision(sender, kind, payload)
                    }
                    MEMBERSHIP_STAGE => {
                        server.dispatch_membership_stage(sender, payload, &mut |entry| {
                            apply(&root, local, entry)
                        })
                    }
                    MEMBERSHIP_PUBLISH | MEMBERSHIP_PUBLICATION => server
                        .dispatch_membership_publication(sender, kind, payload, &mut |entry| {
                            apply(&root, local, entry)
                        }),
                    VIEW_CHANGE_BEGIN | VIEW_CHANGE_RECORD | VIEW_CHANGE_BATCH => {
                        server.dispatch_election(sender, kind, payload)
                    }
                    INSTALL_BEGIN | INSTALL_RECORD | INSTALL_BATCH | INSTALL_FINISH => server
                        .dispatch_installation(sender, kind, payload, &mut |entry| {
                            apply(&root, local, entry)
                        }),
                    REPLICA_STATUS => server.dispatch_status(sender, payload),
                    _ => panic!("unexpected gated-peer request {kind}"),
                };
                let (reply_kind, mut reply) = match response {
                    Ok(bytes) => (kind, bytes),
                    Err(error) => (255, error.to_string().into_bytes()),
                };
                let gate = gate_once
                    && reply_kind != 255
                    && matches!(kind, MEMBERSHIP_PUBLISH | MEMBERSHIP_PUBLICATION);
                if gate && corrupt {
                    let mut report = rsf_vsr::VsrMembershipPublishedReport::decode(&reply).unwrap();
                    report.input[0] ^= 1;
                    reply = report.encode().unwrap();
                }
                let bytes =
                    frame(&server.cluster, server.local, sender, reply_kind, &reply).unwrap();
                if gate {
                    gate_once = false;
                    held = Some((stream, bytes));
                    gated_send.send(()).unwrap();
                } else {
                    write_frame(&mut stream, &bytes).unwrap();
                    let _ = stream.flush();
                }
            }
        });
        Self {
            address,
            gated,
            delivered,
            commands,
            thread: Some(thread),
        }
    }
    pub fn release(&self) {
        self.commands.send(Command::Release).unwrap();
        self.delivered.recv_timeout(RPC).unwrap();
    }
}
impl Drop for GatedPeer {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Stop);
        let joined = self.thread.take().unwrap().join();
        if !std::thread::panicking() {
            drop(joined.unwrap());
        }
    }
}
