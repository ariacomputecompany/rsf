use super::*;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

#[derive(Clone, Copy)]
pub(super) enum Fault {
    SignalPrepare,
    LoseCommitReply,
    LosePrepareReply,
}
pub(super) struct Controlled {
    pub address: SocketAddr,
    pub entered: Receiver<()>,
    release: SyncSender<()>,
    flushed: Receiver<()>,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<PeerReplicaServer>>,
}
impl Controlled {
    pub fn start(mut server: PeerReplicaServer, fault: Fault) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::clone(&stop);
        let (entered_tx, entered) = sync_channel(1);
        let (release, release_rx) = sync_channel(1);
        let (flushed_tx, flushed) = sync_channel(1);
        let worker = std::thread::spawn(move || {
            let mut injected = false;
            while !done.load(Ordering::Acquire) {
                let socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("controlled listener: {error}"),
                };
                let mut stream = StreamOwned::new(
                    ServerConnection::new(Arc::clone(&server.identity.server)).unwrap(),
                    DeadlineStream::new(socket, Instant::now() + Duration::from_millis(800))
                        .unwrap(),
                );
                while stream.conn.is_handshaking() {
                    stream.conn.complete_io(&mut stream.sock).unwrap();
                }
                let certificate = stream.conn.peer_certificates().unwrap().first().unwrap();
                let sender = server.nodes[&fingerprint(certificate.as_ref())];
                let bytes = read_frame(&mut stream).unwrap();
                let (kind, payload) = parse(&bytes, &server.cluster, sender, server.local).unwrap();
                let result = if kind == CONTROL_COMMITTED {
                    server.dispatch_control_committed(sender, payload)
                } else {
                    server
                        .dispatch(sender, kind, payload, &mut |entry| {
                            panic!(
                                "control escaped into application callback at {:?}",
                                entry.prepare.index
                            )
                        })
                        .map(|index| index.0.to_le_bytes().to_vec())
                };
                let target = matches!(
                    (&fault, kind),
                    (Fault::SignalPrepare | Fault::LosePrepareReply, PREPARE)
                        | (Fault::LoseCommitReply, COMMIT)
                );
                let inject = target && !injected && result.is_ok();
                if inject {
                    injected = true;
                    if matches!(fault, Fault::LoseCommitReply | Fault::LosePrepareReply) {
                        entered_tx.send(()).unwrap();
                        // The actual durable handler already succeeded. Lose only
                        // its network response; do not fabricate a stored ACK.
                        drop(stream);
                        continue;
                    }
                    entered_tx.send(()).unwrap();
                    if release_rx.recv_timeout(Duration::from_millis(800)).is_err() {
                        break;
                    }
                }
                let (kind, response) = match result {
                    Ok(response) => (kind, response),
                    Err(error) => (255, error.to_string().into_bytes()),
                };
                write_frame(
                    &mut stream,
                    &frame(&server.cluster, server.local, sender, kind, &response).unwrap(),
                )
                .unwrap();
                stream.conn.send_close_notify();
                std::io::Write::flush(&mut stream).unwrap();
                if inject && matches!(fault, Fault::SignalPrepare) {
                    // Reply has genuinely left the authenticated socket. The
                    // harness changes source custody before consuming it.
                    flushed_tx.send(()).unwrap();
                }
            }
            server
        });
        Self {
            address,
            entered,
            release,
            flushed,
            stop,
            worker: Some(worker),
        }
    }
    pub fn flush_reply(&self) {
        self.release.send(()).unwrap();
        self.flushed
            .recv_timeout(Duration::from_millis(800))
            .expect("real TLS success was not flushed within its original budget");
    }
    pub fn finish(mut self) -> PeerReplicaServer {
        self.stop.store(true, Ordering::Release);
        let _ = self.release.try_send(());
        self.worker.take().unwrap().join().unwrap()
    }
}
impl Drop for Controlled {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.release.try_send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
