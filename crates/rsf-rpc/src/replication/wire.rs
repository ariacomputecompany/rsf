use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use rsf_core::{ClusterId, NodeId};
use sha2::{Digest, Sha256};

pub(super) const MAX_FRAME: usize = 2 * rsf_vsr::MAX_REPLICATED_COMMAND_BYTES + 128 * 1024;
pub(super) const HEADER: usize = 5 + 32 + 8 + 8 + 1;
pub(super) const PREPARE: u8 = 1;
pub(super) const COMMIT: u8 = 2;
pub(super) const TAIL: u8 = 3;
pub(super) const TRANSFER: u8 = 4;
pub(super) const PRIMARY_PROBE: u8 = 5;
pub(super) const HISTORY_BEGIN: u8 = 6;
pub(super) const HISTORY_RECORD: u8 = 7;
pub(super) const HISTORY_FINISH: u8 = 8;
pub(super) const VIEW_CHANGE_BEGIN: u8 = 9;
pub(super) const VIEW_CHANGE_RECORD: u8 = 10;
pub(super) const INSTALL_BEGIN: u8 = 11;
pub(super) const INSTALL_RECORD: u8 = 12;
pub(super) const INSTALL_FINISH: u8 = 13;
pub(super) const REPLICA_STATUS: u8 = 14;
pub(super) const HEARTBEAT: u8 = 15;
pub(super) const INSTALL_BATCH: u8 = 16;
pub(super) const VIEW_CHANGE_BATCH: u8 = 17;

pub(super) fn cluster_digest(cluster: &ClusterId) -> [u8; 32] {
    Sha256::digest(cluster.as_str().as_bytes()).into()
}

pub(super) fn frame(
    cluster: &[u8; 32],
    from: NodeId,
    to: NodeId,
    kind: u8,
    payload: &[u8],
) -> io::Result<Vec<u8>> {
    if payload.len() > MAX_FRAME - HEADER {
        return Err(invalid("peer payload exceeds frame limit"));
    }
    let mut bytes = Vec::with_capacity(HEADER + payload.len());
    bytes.extend_from_slice(b"RSFR1");
    bytes.extend_from_slice(cluster);
    bytes.extend_from_slice(&from.0.to_le_bytes());
    bytes.extend_from_slice(&to.0.to_le_bytes());
    bytes.push(kind);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

pub(super) fn parse<'a>(
    bytes: &'a [u8],
    cluster: &[u8; 32],
    from: NodeId,
    to: NodeId,
) -> io::Result<(u8, &'a [u8])> {
    if bytes.len() < HEADER
        || &bytes[..5] != b"RSFR1"
        || &bytes[5..37] != cluster
        || u64_at(bytes, 37)? != from.0
        || u64_at(bytes, 45)? != to.0
    {
        return Err(invalid(
            "peer frame version, cluster, or authenticated node binding mismatch",
        ));
    }
    Ok((bytes[53], &bytes[HEADER..]))
}

pub(super) fn u64_at(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let slice = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid("truncated peer integer"))?;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

pub(super) fn read_frame(stream: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    if !(HEADER..=MAX_FRAME).contains(&length) {
        return Err(invalid("peer frame length is outside bounds"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn write_frame(stream: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    if !(HEADER..=MAX_FRAME).contains(&bytes.len()) {
        return Err(invalid("peer frame length is outside bounds"));
    }
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()
}

pub(super) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Every TLS read/write shares one absolute deadline, including handshaking.
/// A peer trickling bytes cannot reset the timeout indefinitely.
pub(super) struct DeadlineStream {
    socket: TcpStream,
    deadline: Instant,
}
impl DeadlineStream {
    pub fn new(socket: TcpStream, deadline: Instant) -> io::Result<Self> {
        socket.set_nonblocking(false)?;
        socket.set_nodelay(true)?;
        Ok(Self { socket, deadline })
    }
    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "peer RPC deadline expired"))
    }
}
impl Read for DeadlineStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.socket.set_read_timeout(Some(self.remaining()?))?;
        self.socket.read(bytes)
    }
}
impl Write for DeadlineStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.socket.set_write_timeout(Some(self.remaining()?))?;
        self.socket.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}
