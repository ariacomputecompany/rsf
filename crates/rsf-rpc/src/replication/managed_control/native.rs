//! Native identity persistence is distinct from actual control-history authority.
use super::*;

/// Borrow one independently owned native identity for a bounded connection.
/// These checks grant no voting, readiness, source or admission capability.
pub trait ControlNativePersistence {
    fn validate(&mut self) -> Result<(), String>;
    fn persist(&mut self, event: &initial::ControlInitialTransition<'_>) -> Result<(), String>;
}

/// Validate actual TLS/endpoint configuration and complete trusted provisioning
/// before persisting a restrictive identity. No file, owner or receipt is made.
pub fn validate_control_provisioning(
    cluster: &ClusterId,
    local: NodeId,
    identity: &PeerIdentity,
    peers: &BTreeMap<NodeId, PeerEndpoint>,
    anchor: &ControlBootstrap,
    genesis: &ControlCatalog,
    timeout: Duration,
) -> Result<(), String> {
    validate_configuration(local, identity, peers, timeout)?;
    let nodes = peers
        .iter()
        .map(|(node, peer)| (peer.fingerprint, *node))
        .collect();
    validate_policy(
        cluster_digest(cluster),
        local,
        identity,
        &nodes,
        anchor,
        genesis,
    )
}

impl initial::ControlInitialPolicy {
    /// Pure complete preflight for an initial learner. Caller-configured PG
    /// inventory is checked separately by dedicated/mixed owner constructors.
    pub fn validate_configuration(
        &self,
        cluster: &ClusterId,
        local: NodeId,
        identity: &PeerIdentity,
        peers: &BTreeMap<NodeId, PeerEndpoint>,
        timeout: Duration,
    ) -> Result<(), String> {
        self.limits.validate().map_err(|error| {
            format!("initial control requires finite limits: {}", error.message)
        })?;
        validate_control_provisioning(
            cluster,
            local,
            identity,
            peers,
            &self.anchor,
            &self.genesis,
            timeout,
        )?;
        self.validate_local(local)
    }
}

impl PeerReplicaServer {
    /// Serve with the same retained native identity checked before effects and
    /// before any response. Actual transition callbacks remain kernel-owned.
    pub fn serve_connection_with_native_control(
        &mut self,
        socket: TcpStream,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
        native: &mut impl ControlNativePersistence,
    ) -> Result<(), String> {
        self.serve_io_guarded(socket, apply, true, None, Some(native))
            .map_err(|error| error.to_string())
    }
}
