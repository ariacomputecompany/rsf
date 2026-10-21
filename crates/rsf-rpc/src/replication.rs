//! Authenticated peer RPCs for durable VSR logs. TLS authenticates the leaf;
//! the configured fingerprint binds it to a node, and each frame binds the
//! cluster, sender, destination, and protocol version before touching storage.
mod control;
mod election;
mod membership;
pub use control::{PeerElectionProgress, PeerElectionTiming, PeerElectionUpdate};
mod history;
mod installation;
mod recovery_retry;
mod tls;
mod wire;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rsf_core::{ClusterId, LogIndex, NodeId, PlacementGroupId};
use rsf_vsr::{
    DurableVsrCell, VsrCommit, VsrCommittedEntry, VsrPrepare, VsrPrepareMessage, VsrTransport,
    VsrTransportError,
};
use rustls::pki_types::ServerName;
use rustls::{ClientConnection, ServerConnection, StreamOwned};

pub use tls::{PeerIdentity, fingerprint};
use wire::*;

#[derive(Clone, Debug)]
pub struct PeerEndpoint {
    pub address: SocketAddr,
    pub server_name: String,
    pub fingerprint: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct NetworkVsrTransport {
    local: NodeId,
    cluster: [u8; 32],
    identity: PeerIdentity,
    peers: BTreeMap<NodeId, PeerEndpoint>,
    timeout: Duration,
}

impl NetworkVsrTransport {
    pub fn new(
        local: NodeId,
        cluster: &ClusterId,
        identity: PeerIdentity,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        timeout: Duration,
    ) -> Result<Self, String> {
        validate_configuration(local, &identity, &peers, timeout)?;
        Ok(Self {
            local,
            cluster: cluster_digest(cluster),
            identity,
            peers,
            timeout,
        })
    }

    fn request(
        &self,
        replica: NodeId,
        kind: u8,
        payload: &[u8],
    ) -> Result<LogIndex, VsrTransportError> {
        self.request_io(replica, kind, payload)
            .map(LogIndex)
            .map_err(|err| VsrTransportError {
                replica,
                reason: err.to_string(),
            })
    }

    fn request_io(&self, replica: NodeId, kind: u8, payload: &[u8]) -> io::Result<u64> {
        let payload = self.exchange_io(replica, kind, payload)?;
        if payload.len() != 8 {
            return Err(invalid("peer acknowledgement has wrong size"));
        }
        u64_at(&payload, 0)
    }

    fn exchange_io(&self, replica: NodeId, kind: u8, payload: &[u8]) -> io::Result<Vec<u8>> {
        recovery_retry::exchange(kind, || self.exchange_once(replica, kind, payload))
    }

    fn exchange_once(&self, replica: NodeId, kind: u8, payload: &[u8]) -> io::Result<Vec<u8>> {
        let peer = self
            .peers
            .get(&replica)
            .ok_or_else(|| invalid("replica has no authenticated endpoint"))?;
        if replica == self.local {
            return Err(invalid("peer transport cannot acknowledge itself"));
        }
        let deadline = Instant::now() + self.timeout;
        let socket = TcpStream::connect_timeout(&peer.address, self.timeout)?;
        let name = ServerName::try_from(peer.server_name.clone())
            .map_err(|_| invalid("invalid peer server name"))?;
        let mut stream = StreamOwned::new(
            ClientConnection::new(Arc::clone(&self.identity.client), name)
                .map_err(|err| invalid(err.to_string()))?,
            DeadlineStream::new(socket, deadline)?,
        );
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock)?;
        }
        let certificate = stream
            .conn
            .peer_certificates()
            .and_then(|chain| chain.first())
            .ok_or_else(|| invalid("peer omitted certificate"))?;
        if fingerprint(certificate.as_ref()) != peer.fingerprint {
            return Err(invalid(
                "peer certificate fingerprint does not match target node",
            ));
        }
        write_frame(
            &mut stream,
            &frame(&self.cluster, self.local, replica, kind, payload)?,
        )?;
        let response = read_frame(&mut stream)?;
        let (response_kind, payload) = parse(&response, &self.cluster, replica, self.local)?;
        if response_kind == 255 {
            return Err(invalid(format!(
                "peer rejected request: {}",
                String::from_utf8_lossy(payload)
            )));
        }
        if response_kind != kind {
            return Err(invalid("peer acknowledgement has wrong type"));
        }
        Ok(payload.to_vec())
    }

    fn expect_index(
        &self,
        replica: NodeId,
        kind: u8,
        payload: &[u8],
        expected: LogIndex,
    ) -> Result<NodeId, VsrTransportError> {
        let index = self.request(replica, kind, payload)?;
        if index != expected {
            return Err(VsrTransportError {
                replica,
                reason: "peer acknowledged a different log index".into(),
            });
        }
        Ok(replica)
    }
}

impl VsrTransport for NetworkVsrTransport {
    fn confirm_primary(
        &mut self,
        replica: NodeId,
        probe: &rsf_vsr::VsrPrimaryProbe,
    ) -> Result<(), VsrTransportError> {
        let bytes = probe.encode().map_err(|err| VsrTransportError {
            replica,
            reason: err.message,
        })?;
        self.expect_index(replica, PRIMARY_PROBE, &bytes, probe.committed_tail)
            .map(|_| ())
    }

    fn acknowledge_prepare(
        &mut self,
        message: VsrPrepareMessage,
    ) -> Result<NodeId, VsrTransportError> {
        let bytes = message.prepare.encode().map_err(|err| VsrTransportError {
            replica: message.replica,
            reason: err.message,
        })?;
        self.expect_index(message.replica, PREPARE, &bytes, message.prepare.index)
    }
    fn acknowledge_commit(
        &mut self,
        replica: NodeId,
        commit: VsrCommit,
    ) -> Result<NodeId, VsrTransportError> {
        let bytes = commit.encode().map_err(|err| VsrTransportError {
            replica,
            reason: err.message,
        })?;
        self.expect_index(replica, COMMIT, &bytes, commit.index)
    }
    fn committed_tail(
        &mut self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<LogIndex, VsrTransportError> {
        self.request(replica, TAIL, &pg.0.to_le_bytes())
    }
    fn transfer_committed(
        &mut self,
        replica: NodeId,
        entry: VsrCommittedEntry,
    ) -> Result<(), VsrTransportError> {
        let prepare = entry.prepare.encode().map_err(|err| VsrTransportError {
            replica,
            reason: err.message,
        })?;
        let commit = entry.commit.encode().map_err(|err| VsrTransportError {
            replica,
            reason: err.message,
        })?;
        let mut bytes = Vec::with_capacity(4 + prepare.len() + commit.len());
        bytes.extend_from_slice(&(prepare.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&prepare);
        bytes.extend_from_slice(&commit);
        self.expect_index(replica, TRANSFER, &bytes, entry.prepare.index)
            .map(|_| ())
    }
}

#[derive(Debug)]
pub struct PeerReplicaServer {
    local: NodeId,
    cluster: [u8; 32],
    identity: PeerIdentity,
    nodes: BTreeMap<[u8; 32], NodeId>,
    cells: BTreeMap<PlacementGroupId, DurableVsrCell>,
    timeout: Duration,
    learners: BTreeMap<PlacementGroupId, rsf_vsr::LearnerHistoryStore>,
    election_snapshots: BTreeMap<PlacementGroupId, rsf_vsr::VsrViewChangeSnapshot>,
    membership_snapshots: BTreeMap<PlacementGroupId, rsf_vsr::VsrMembershipSnapshot>,
    learner_materialized: BTreeMap<PlacementGroupId, [u8; 32]>,
    installation_pending: BTreeSet<PlacementGroupId>,
    control_transport: NetworkVsrTransport,
    election_clock: BTreeMap<PlacementGroupId, control::Clock>,
    pending_contact: Option<PlacementGroupId>,
    last_control_group: Option<PlacementGroupId>,
}

impl PeerReplicaServer {
    /// Serialize a client coordinator with peer traffic over the same live cells.
    /// The callback must return cell custody before returning; protocol's scoped
    /// installed-cell API provides restoration on errors and unwinding. Mark the
    /// prefix pending before lending so even a failed client operation is applied
    /// before any subsequent peer acknowledgement or heartbeat succeeds. The
    /// callback returns true only after applying every committed prefix and all
    /// successful client mutations through that same serialized materializer.
    pub fn with_voting_cells<R>(
        &mut self,
        operation: impl FnOnce(
            NodeId,
            &mut BTreeMap<PlacementGroupId, DurableVsrCell>,
            NetworkVsrTransport,
        ) -> (R, bool),
    ) -> R {
        self.installation_pending.extend(self.cells.keys().copied());
        let (result, materialized) =
            operation(self.local, &mut self.cells, self.control_transport.clone());
        if materialized {
            self.installation_pending.clear();
        }
        result
    }

    pub fn new(
        local: NodeId,
        cluster: &ClusterId,
        identity: PeerIdentity,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        cells: BTreeMap<PlacementGroupId, DurableVsrCell>,
        timeout: Duration,
    ) -> Result<Self, String> {
        Self::with_learners(
            local,
            cluster,
            identity,
            peers,
            cells,
            BTreeMap::new(),
            timeout,
        )
    }

    pub fn with_learners(
        local: NodeId,
        cluster: &ClusterId,
        identity: PeerIdentity,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        cells: BTreeMap<PlacementGroupId, DurableVsrCell>,
        learners: BTreeMap<PlacementGroupId, rsf_vsr::LearnerHistoryStore>,
        timeout: Duration,
    ) -> Result<Self, String> {
        validate_configuration(local, &identity, &peers, timeout)?;
        if (cells.is_empty() && learners.is_empty())
            || cells.values().any(|cell| {
                !cell.contains_replica(local)
                    || cell.replicas().iter().any(|node| !peers.contains_key(node))
            })
        {
            return Err(
                "peer server requires mapped local membership and endpoints for every voter".into(),
            );
        }
        if learners.iter().any(|(pg, learner)| {
            cells.contains_key(pg)
                || learner.placement_group() != *pg
                || learner.local() != local
                || !peers.contains_key(&learner.source())
        }) {
            return Err("learner requires a mapped remote source, matching identity, and no voting cell for its group".into());
        }
        let control_transport =
            NetworkVsrTransport::new(local, cluster, identity.clone(), peers.clone(), timeout)?;
        let election_clock = cells
            .keys()
            .map(|pg| (*pg, control::Clock::new(Instant::now())))
            .collect();
        let installation_pending = cells
            .iter()
            .filter_map(|(pg, cell)| cell.has_installed_view().then_some(*pg))
            .collect();
        Ok(Self {
            local,
            cluster: cluster_digest(cluster),
            identity,
            installation_pending,
            control_transport,
            election_clock,
            pending_contact: None,
            last_control_group: None,
            nodes: peers
                .into_iter()
                .map(|(node, peer)| (peer.fingerprint, node))
                .collect(),
            cells,
            timeout,
            learners,
            election_snapshots: BTreeMap::new(),
            membership_snapshots: BTreeMap::new(),
            learner_materialized: BTreeMap::new(),
        })
    }

    /// Serve one bounded RPC. Caller owns listener scheduling and records errors.
    /// No cell is created from untrusted message contents.
    pub fn serve_connection(&mut self, socket: TcpStream) -> Result<(), String> {
        self.serve_connection_with_apply(socket, &mut |_| Ok(()))
    }

    /// Run a trusted materializer after durable commit and before acknowledging
    /// it. Retries invoke the materializer again, so it must be idempotent.
    pub fn serve_connection_with_apply(
        &mut self,
        socket: TcpStream,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<(), String> {
        self.serve_io(socket, apply).map_err(|err| err.to_string())
    }

    /// Recovery input for a materializer before the peer starts accepting traffic.
    pub fn committed_entries(&self) -> Result<Vec<VsrCommittedEntry>, String> {
        let mut entries = Vec::new();
        for cell in self.cells.values() {
            entries.extend(cell.committed_prefix().map_err(|err| err.message)?);
        }
        for learner in self.learners.values() {
            entries.extend(learner.committed_entries().map_err(|err| err.message)?);
        }
        Ok(entries)
    }

    fn serve_io(
        &mut self,
        socket: TcpStream,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<()> {
        let mut stream = StreamOwned::new(
            ServerConnection::new(Arc::clone(&self.identity.server))
                .map_err(|err| invalid(err.to_string()))?,
            DeadlineStream::new(socket, Instant::now() + self.timeout)?,
        );
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock)?;
        }
        let certificate = stream
            .conn
            .peer_certificates()
            .and_then(|chain| chain.first())
            .ok_or_else(|| invalid("peer omitted client certificate"))?;
        let sender = *self
            .nodes
            .get(&fingerprint(certificate.as_ref()))
            .ok_or_else(|| invalid("client certificate is not bound to a peer node"))?;
        let request = read_frame(&mut stream)?;
        let (kind, payload) = parse(&request, &self.cluster, sender, self.local)?;
        self.pending_contact = None;
        let response = if matches!(
            kind,
            MEMBERSHIP_BEGIN | MEMBERSHIP_SNAPSHOT | MEMBERSHIP_BATCH | MEMBERSHIP_RESUME
        ) {
            self.dispatch_membership(sender, kind, payload)
        } else if kind == REPLICA_STATUS {
            self.dispatch_status(sender, payload)
        } else if kind == HEARTBEAT {
            self.dispatch_heartbeat(sender, payload, apply)
        } else if matches!(
            kind,
            INSTALL_BEGIN | INSTALL_RECORD | INSTALL_FINISH | INSTALL_BATCH
        ) {
            self.dispatch_installation(sender, kind, payload, apply)
        } else if matches!(
            kind,
            VIEW_CHANGE_BEGIN | VIEW_CHANGE_RECORD | VIEW_CHANGE_BATCH
        ) {
            self.dispatch_election(sender, kind, payload)
        } else {
            self.dispatch(sender, kind, payload, apply)
                .map(|index| index.0.to_le_bytes().to_vec())
        };
        if response.is_ok()
            && let Some(pg) = self.pending_contact.take()
        {
            self.record_primary_contact(pg, sender, Instant::now());
        }
        match response {
            Ok(payload) => write_frame(
                &mut stream,
                &frame(&self.cluster, self.local, sender, kind, &payload)?,
            ),
            Err(err) => {
                let message = err.to_string();
                let bounded = &message.as_bytes()[..message.len().min(1024)];
                write_frame(
                    &mut stream,
                    &frame(&self.cluster, self.local, sender, 255, bounded)?,
                )?;
                Err(err)
            }
        }
    }

    fn cell(
        &mut self,
        sender: NodeId,
        pg: PlacementGroupId,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<&mut DurableVsrCell> {
        let local = self.local;
        let cell = self
            .cells
            .get_mut(&pg)
            .ok_or_else(|| invalid("placement group is not hosted here"))?;
        if sender == local || sender != cell.primary() || !cell.contains_replica(sender) {
            return Err(invalid("sender is not the active remote primary"));
        }
        self.pending_contact = Some(pg);
        if self.installation_pending.contains(&pg) {
            for entry in cell
                .committed_prefix()
                .map_err(|err| invalid(err.message))?
            {
                apply(&entry).map_err(invalid)?;
            }
            self.installation_pending.remove(&pg);
        }
        Ok(cell)
    }

    fn dispatch(
        &mut self,
        sender: NodeId,
        kind: u8,
        payload: &[u8],
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<LogIndex> {
        let local = self.local;
        let rsf = |err: rsf_core::RsfError| invalid(err.message);
        match kind {
            HISTORY_BEGIN | HISTORY_RECORD | HISTORY_FINISH => {
                self.dispatch_history(sender, kind, payload, apply)
            }
            PRIMARY_PROBE => {
                // Comparison with the complete canonical encoding rejects wrong
                // views, membership, tips, malformed fields and trailing bytes.
                let pg = PlacementGroupId(u64_at(payload, 5)?);
                let probe = self.cell(sender, pg, apply)?.primary_probe().map_err(rsf)?;
                if payload != probe.encode().map_err(rsf)? {
                    return Err(invalid(
                        "primary probe differs from current view, membership or committed tip",
                    ));
                }
                Ok(probe.committed_tail)
            }
            PREPARE => {
                let prepare = VsrPrepare::decode(payload).map_err(rsf)?;
                let index = prepare.index;
                self.cell(sender, prepare.pg, apply)?
                    .receive_prepare(local, sender, prepare)
                    .map_err(rsf)?;
                Ok(index)
            }
            COMMIT => {
                let commit = VsrCommit::decode(payload).map_err(rsf)?;
                let index = commit.index;
                let cell = self.cell(sender, commit.pg, apply)?;
                cell.receive_commit(local, sender, commit.clone())
                    .map_err(rsf)?;
                let prepare = cell
                    .prepared(index)
                    .cloned()
                    .ok_or_else(|| invalid("committed command has no prepare"))?;
                let pg = commit.pg;
                self.installation_pending.insert(pg);
                apply(&VsrCommittedEntry { prepare, commit }).map_err(invalid)?;
                self.installation_pending.remove(&pg);
                Ok(index)
            }
            TAIL if payload.len() == 8 => Ok(self
                .cell(sender, PlacementGroupId(u64_at(payload, 0)?), apply)?
                .committed_tail()),
            TRANSFER => {
                let length = payload
                    .get(..4)
                    .ok_or_else(|| invalid("truncated prefix entry"))?;
                let end = 4_usize
                    .checked_add(u32::from_le_bytes(length.try_into().unwrap()) as usize)
                    .ok_or_else(|| invalid("prefix entry length overflow"))?;
                let prepare = VsrPrepare::decode(
                    payload
                        .get(4..end)
                        .ok_or_else(|| invalid("truncated prefix prepare"))?,
                )
                .map_err(rsf)?;
                let commit = VsrCommit::decode(
                    payload
                        .get(end..)
                        .ok_or_else(|| invalid("truncated prefix commit"))?,
                )
                .map_err(rsf)?;
                let index = prepare.index;
                let entry = VsrCommittedEntry { prepare, commit };
                self.cell(sender, entry.prepare.pg, apply)?
                    .catch_up_from(std::slice::from_ref(&entry))
                    .map_err(rsf)?;
                self.installation_pending.insert(entry.prepare.pg);
                apply(&entry).map_err(invalid)?;
                self.installation_pending.remove(&entry.prepare.pg);
                Ok(index)
            }
            _ => Err(invalid("unsupported peer request or invalid payload size")),
        }
    }
}

fn validate_configuration(
    local: NodeId,
    identity: &PeerIdentity,
    peers: &BTreeMap<NodeId, PeerEndpoint>,
    timeout: Duration,
) -> Result<(), String> {
    if timeout.is_zero() || timeout > Duration::from_secs(30) {
        return Err("peer RPC timeout must be greater than zero and at most 30 seconds".into());
    }
    if peers
        .get(&local)
        .is_none_or(|peer| peer.fingerprint != identity.fingerprint)
    {
        return Err("local TLS identity does not match configured node fingerprint".into());
    }
    let mut fingerprints = BTreeSet::new();
    for peer in peers.values() {
        if !fingerprints.insert(peer.fingerprint) {
            return Err("each peer node requires a distinct certificate fingerprint".into());
        }
        ServerName::try_from(peer.server_name.clone()).map_err(|_| "invalid peer server name")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
