//! Test-only mutation encoder; no production initial sender or authority grant.
use super::*;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;
const TIMEOUT: Duration = Duration::from_millis(800);
struct Deadline {
    socket: TcpStream,
    until: Instant,
}
impl Deadline {
    fn remaining(&self) -> std::io::Result<Duration> {
        self.until
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "fixture deadline"))
    }
}
impl Read for Deadline {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        self.socket.set_read_timeout(Some(self.remaining()?))?;
        self.socket.read(b)
    }
}
impl Write for Deadline {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        self.socket.write(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.socket.flush()
    }
}
fn policy(f: &Fixture) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"RSFCI1");
    h.update(Sha256::digest(f.policy.anchor.cluster.as_str().as_bytes()));
    h.update(f.policy.anchor.control_pg.0.to_le_bytes());
    h.update(f.policy.anchor.control_bootstrap);
    h.update(f.policy.anchor.genesis_catalog.0);
    h.update(3u64.to_le_bytes());
    h.update(f.policy.limits.max_records.to_le_bytes());
    h.update(f.policy.limits.max_encoded_bytes.to_le_bytes());
    h.finalize().into()
}
pub fn initial(f: &Fixture, kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut b = b"RSFIC1".to_vec();
    b.extend(policy(f));
    b.push(kind);
    b.extend(payload);
    b
}
pub fn selection(s: VsrInitialSelection) -> Vec<u8> {
    let mut b = s.generation().to_le_bytes().to_vec();
    b.extend(s.history_id());
    b
}
pub struct Reply {
    kind: u8,
    body: Vec<u8>,
    server: Result<(), String>,
}
impl Reply {
    pub fn success(self) -> Vec<u8> {
        assert!(self.server.is_ok(), "{:?}", self.server);
        assert_ne!(self.kind, 255, "{}", String::from_utf8_lossy(&self.body));
        self.body
    }
    pub fn rejected(self, reason: &str) {
        assert!(self.server.is_err());
        assert_eq!(
            self.kind, 255,
            "must receive authenticated rejection, not timeout"
        );
        let text = String::from_utf8(self.body).unwrap();
        assert!(text.contains(reason), "expected {reason}: {text}");
    }
    pub fn cursor(self) -> (VsrInitialSelection, u64) {
        let b = self.success();
        assert_eq!(b.len(), 119);
        assert_eq!(b[70], 1);
        (
            VsrInitialSelection::new(
                u64::from_le_bytes(b[71..79].try_into().unwrap()),
                b[79..111].try_into().unwrap(),
            )
            .unwrap(),
            u64::from_le_bytes(b[111..119].try_into().unwrap()),
        )
    }
}
pub fn exchange(service: &mut PeerService, f: &Fixture, kind: u8, payload: Vec<u8>) -> Reply {
    exchange_with(service, f, kind, payload, |_, _| Ok(()))
}
pub fn exchange_with(
    service: &mut PeerService,
    f: &Fixture,
    kind: u8,
    payload: Vec<u8>,
    hook: impl FnMut(Boundary, &Path) -> Result<(), String> + Send + 'static,
) -> Reply {
    exchange_from(service, f, 0, kind, payload, hook)
}
pub fn exchange_from(
    service: &mut PeerService,
    f: &Fixture,
    sender: u64,
    kind: u8,
    payload: Vec<u8>,
    hook: impl FnMut(Boundary, &Path) -> Result<(), String> + Send + 'static,
) -> Reply {
    let address = service.listener.local_addr().unwrap();
    let socket = TcpStream::connect_timeout(&address, TIMEOUT).unwrap();
    let cluster: [u8; 32] = Sha256::digest(f.policy.anchor.cluster.as_str().as_bytes()).into();
    let mut request = b"RSFR1".to_vec();
    request.extend(cluster);
    request.extend(sender.to_le_bytes());
    request.extend(3u64.to_le_bytes());
    request.push(kind);
    request.extend(&payload);
    std::thread::scope(|scope| {
        let server = scope.spawn(move || {
            let (socket, _) = service.listener.accept().unwrap();
            let control = service.control.as_ref().unwrap();
            with_hook(hook, || {
                service.server.serve_connection_with_native_control(
                    socket,
                    &mut |entry| {
                        if let Some(command) = decode_application_entry(entry)? {
                            service.materializer.apply(&command)?;
                        }
                        Ok(())
                    },
                    &mut control::Adapter {
                        identity: &mut service.state_identity,
                        control,
                    },
                )
            })
        });
        let connection = rustls::ClientConnection::new(
            f.clients[&sender].clone(),
            rustls::pki_types::ServerName::try_from("localhost")
                .unwrap()
                .to_owned(),
        )
        .unwrap();
        let mut stream = rustls::StreamOwned::new(
            connection,
            Deadline {
                socket,
                until: Instant::now() + TIMEOUT,
            },
        );
        stream
            .write_all(&(request.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(&request).unwrap();
        stream.flush().unwrap();
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let size = u32::from_le_bytes(size) as usize;
        assert!((54..=2 * rsf_vsr::MAX_REPLICATED_COMMAND_BYTES + 128 * 1024).contains(&size));
        let mut response = vec![0; size];
        stream.read_exact(&mut response).unwrap();
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(
                stream.conn.peer_certificates().unwrap()[0].as_ref()
            )),
            f.receiver_fingerprint
        );
        assert_eq!(&response[..5], b"RSFR1");
        assert_eq!(&response[5..37], cluster);
        assert_eq!(u64::from_le_bytes(response[37..45].try_into().unwrap()), 3);
        assert_eq!(
            u64::from_le_bytes(response[45..53].try_into().unwrap()),
            sender
        );
        let response_kind = response[53];
        assert!(response_kind == kind || response_kind == 255);
        let body = response[54..].to_vec();
        if kind == 37 && response_kind == 37 {
            assert!(body.len() >= 71);
            assert_eq!(&body[..6], b"RSFIR1");
            assert_eq!(&body[6..38], policy(f));
            assert_eq!(&body[38..70], Sha256::digest(&payload).as_slice());
        }
        Reply {
            kind: response_kind,
            body,
            server: server.join().unwrap(),
        }
    })
}
pub fn copy(
    service: &mut PeerService,
    f: &Fixture,
    snapshot: &VsrHistorySnapshot,
) -> VsrInitialSelection {
    let (selection, count) = exchange(
        service,
        f,
        37,
        initial(f, 1, &snapshot.descriptor.encode().unwrap()),
    )
    .cursor();
    copy_selected(service, f, snapshot, selection, count);
    selection
}
pub fn copy_selected(
    service: &mut PeerService,
    f: &Fixture,
    snapshot: &VsrHistorySnapshot,
    selected: VsrInitialSelection,
    mut count: u64,
) {
    while count < snapshot.descriptor.records {
        let batch = VsrHistoryBatch::pack(count, &snapshot.records[count as usize..]).unwrap();
        let expected = count + batch.records.len() as u64;
        let mut body = selection(selected);
        body.extend(batch.encode().unwrap());
        let (actual, next) = exchange(service, f, 37, initial(f, 4, &body)).cursor();
        assert_eq!(actual, selected);
        assert_eq!(next, expected);
        count = next;
    }
}

pub fn history(service: &mut PeerService, f: &Fixture, sender: u64, snapshot: &VsrHistorySnapshot) {
    let reply = exchange_from(
        service,
        f,
        sender,
        6,
        snapshot.descriptor.encode().unwrap(),
        |_, _| Ok(()),
    )
    .success();
    assert_eq!(reply.len(), 8);
    let start = u64::from_le_bytes(reply.try_into().unwrap());
    for (ordinal, record) in snapshot.records.iter().enumerate().skip(start as usize) {
        let mut p = snapshot.descriptor.probe.pg.0.to_le_bytes().to_vec();
        p.extend(snapshot.descriptor.id().unwrap());
        p.extend((ordinal as u64).to_le_bytes());
        p.extend(record.encode().unwrap());
        assert_eq!(
            exchange_from(service, f, sender, 7, p, |_, _| Ok(())).success(),
            (ordinal as u64 + 1).to_le_bytes()
        );
    }
    let mut p = snapshot.descriptor.probe.pg.0.to_le_bytes().to_vec();
    p.extend(snapshot.descriptor.id().unwrap());
    assert_eq!(
        exchange_from(service, f, sender, 8, p, |_, _| Ok(())).success(),
        snapshot.descriptor.probe.committed_tail.0.to_le_bytes()
    );
}
