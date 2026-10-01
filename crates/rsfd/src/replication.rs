use rsf_admin::AdminPlane;
use rsf_core::{ClusterId, LogIndex, NodeId, PlacementGroupId};
use rsf_rpc::replication::{NetworkVsrTransport, PeerEndpoint, PeerIdentity};
use rsf_vsr::{VsrCommit, VsrCommittedEntry, VsrPrepareMessage, VsrTransport, VsrTransportError};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct ReplicationTransport(Option<NetworkVsrTransport>);
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    cluster: String,
    node: u64,
    tls: PathBuf,
    timeout_ms: u64,
    peers: Vec<Peer>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Peer {
    node: u64,
    address: SocketAddr,
    server_name: String,
    fingerprint: String,
}
impl ReplicationTransport {
    pub(crate) fn load(state: &Path, admin: &AdminPlane, primary: NodeId) -> Result<Self, String> {
        let path = state.join("replication.json");
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let voters = admin
                    .membership_view()
                    .ok_or("missing membership")?
                    .voter_nodes();
                if voters == vec![primary] {
                    return Ok(Self(None));
                }
                return Err("multi-node data execution requires authenticated replication.json; local quorum simulation is disabled".into());
            }
            Err(err) => return Err(format!("cannot read replication configuration: {err}")),
        };
        if raw.len() > 1024 * 1024 {
            return Err("replication configuration exceeds 1 MiB".into());
        }
        let config: Config = serde_json::from_slice(&raw)
            .map_err(|err| format!("invalid replication configuration: {err}"))?;
        if NodeId(config.node) != primary {
            return Err(
                "data execution requires the configured local node to be the primary".into(),
            );
        }
        let cluster = ClusterId::new(config.cluster).map_err(|err| err.message)?;
        let tls = if config.tls.is_absolute() {
            config.tls
        } else {
            state.join(config.tls)
        };
        let identity = PeerIdentity::load(&tls)?;
        let records = admin
            .records()
            .map(|record| {
                (
                    record.node.id,
                    record.identity.transport.certificate_fingerprint.bytes,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut peers = BTreeMap::new();
        for peer in config.peers {
            let fingerprint = crate::peer::decode_fingerprint(&peer.fingerprint)?;
            if records.get(&NodeId(peer.node)) != Some(&fingerprint) {
                return Err("peer certificate is not the admitted node identity".into());
            }
            if peers
                .insert(
                    NodeId(peer.node),
                    PeerEndpoint {
                        address: peer.address,
                        server_name: peer.server_name,
                        fingerprint,
                    },
                )
                .is_some()
            {
                return Err("duplicate replication peer".into());
            }
        }
        if admin
            .membership_view()
            .ok_or("missing membership")?
            .voter_nodes()
            .iter()
            .any(|node| !peers.contains_key(node))
        {
            return Err("replication configuration omits a voter".into());
        }
        let transport = NetworkVsrTransport::new(
            primary,
            &cluster,
            identity,
            peers,
            Duration::from_millis(config.timeout_ms),
        )?;
        let binding = state.join("replication-identity");
        std::fs::create_dir_all(&binding).map_err(|err| err.to_string())?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(binding.join(".peer.lock"))
            .map_err(|err| err.to_string())?;
        let _lock = rsf_storage::ExclusiveFileLock::acquire(lock)
            .map_err(|err| format!("replication identity is busy: {err}"))?;
        crate::peer::bind_state(&binding, &cluster, primary)?;
        Ok(Self(Some(transport)))
    }
    /// For inventory recovery only when configuration is invalid. This transport
    /// rejects every remote operation; callers must retain the configuration error.
    pub(crate) fn unavailable() -> Self {
        Self(None)
    }
    pub(crate) fn is_network(&self) -> bool {
        self.0.is_some()
    }
    fn network(&mut self, replica: NodeId) -> Result<&mut NetworkVsrTransport, VsrTransportError> {
        self.0.as_mut().ok_or_else(|| VsrTransportError {
            replica,
            reason: "standalone transport cannot acknowledge remote replicas".into(),
        })
    }
}
impl VsrTransport for ReplicationTransport {
    fn confirm_primary(
        &mut self,
        replica: NodeId,
        probe: &rsf_vsr::VsrPrimaryProbe,
    ) -> Result<(), VsrTransportError> {
        self.network(replica)?.confirm_primary(replica, probe)
    }

    fn acknowledge_prepare(
        &mut self,
        message: VsrPrepareMessage,
    ) -> Result<NodeId, VsrTransportError> {
        self.network(message.replica)?.acknowledge_prepare(message)
    }
    fn acknowledge_commit(
        &mut self,
        replica: NodeId,
        commit: VsrCommit,
    ) -> Result<NodeId, VsrTransportError> {
        self.network(replica)?.acknowledge_commit(replica, commit)
    }
    fn committed_tail(
        &mut self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<LogIndex, VsrTransportError> {
        self.network(replica)?.committed_tail(replica, pg)
    }
    fn transfer_committed(
        &mut self,
        replica: NodeId,
        entry: VsrCommittedEntry,
    ) -> Result<(), VsrTransportError> {
        self.network(replica)?.transfer_committed(replica, entry)
    }
}

/// A failed probe is readiness data, not a reason to stop the health listener.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicationStatus {
    pub mode: &'static str,
    pub quorum_available: bool,
    pub detail: String,
}

pub(crate) fn observe(
    state: &Path,
    admin: &AdminPlane,
) -> Result<
    (
        rsf_protocol::VsrBackedFabricRouter<ReplicationTransport>,
        ReplicationStatus,
    ),
    String,
> {
    let voters = admin
        .membership_view()
        .ok_or("missing membership")?
        .voter_nodes();
    let primary = *voters.first().ok_or("missing promoted voter")?;
    let (transport, configuration_error) = match ReplicationTransport::load(state, admin, primary) {
        Ok(transport) => (transport, None),
        Err(error) => (ReplicationTransport::unavailable(), Some(error)),
    };
    let mode = if configuration_error.is_some() {
        "unavailable"
    } else if transport.is_network() {
        "network"
    } else {
        "standalone"
    };
    let mut router = rsf_protocol::VsrBackedFabricRouter::open_with_transport(
        state.join("protocol"),
        [PlacementGroupId(1)],
        rsf_core::Epoch(admin.cluster_map().epoch.0.max(1)),
        primary,
        voters.clone(),
        transport,
    )
    .map_err(|err| err.message)?;
    let result = if let Some(error) = configuration_error {
        Err(error)
    } else if !crate::protocol_membership_matches(&router, &voters) {
        Err("protocol membership differs from the admin map; resume the pending join".into())
    } else {
        router.confirm_primary_quorum().map_err(|err| err.message)
    };
    let status = ReplicationStatus {
        mode,
        quorum_available: result.is_ok(),
        detail: match result {
            Ok(()) => "current primary, membership and committed tip confirmed by quorum".into(),
            Err(error) => error,
        },
    };
    Ok((router, status))
}
