//! Dedicated control owner with full trusted catalog/TLS validation.
use super::*;

impl PeerReplicaServer {
    /// Bind exactly one actual control voter. The complete catalog remains the
    /// policy even when application groups are hosted elsewhere. This does not
    /// schedule Genesis, admit clients or authorize control membership work.
    #[expect(
        clippy::too_many_arguments,
        reason = "Authenticated peer construction also requires the complete control bootstrap policy"
    )]
    pub fn control_voter(
        local: NodeId,
        cluster: &ClusterId,
        identity: PeerIdentity,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        cell: DurableVsrCell,
        anchor: ControlBootstrap,
        genesis: ControlCatalog,
        timeout: Duration,
    ) -> Result<Self, String> {
        native::validate_control_provisioning(
            cluster, local, &identity, &peers, &anchor, &genesis, timeout,
        )?;
        let group = genesis
            .group(anchor.control_pg)
            .ok_or("control group absent")?;
        if !group.voters.contains(&local)
            || !genesis.nodes.iter().any(|node| {
                node.id == local && node.role == rsf_control_state::AdmissionRole::Voter
            })
            || cell.replica_status().map_err(|e| e.message)?.normal.pg != anchor.control_pg
        {
            return Err("dedicated control voter requires actual original voting owner".into());
        }
        let pg = anchor.control_pg;
        let mut server = Self::with_learners(
            local,
            cluster,
            identity,
            peers,
            [(pg, cell)].into(),
            BTreeMap::new(),
            timeout,
        )?;
        let mut binding = Binding {
            anchor,
            genesis,
            voter: None,
            learner: None,
            writer: writer::Writer::default(),
            initial_sender: None,
        };
        binding.refresh_voter(&server.cells[&pg])?;
        server.managed_control = Some(binding);
        Ok(server)
    }

    /// Read-only current-primary observation. A caller must still use the writer
    /// APIs, which independently validate actual custody before any operation.
    pub fn control_primary(&self) -> Result<NodeId, String> {
        let pg = self
            .control_placement_group()
            .ok_or("control replication is not configured")?;
        self.cells
            .get(&pg)
            .ok_or("control voter owner is absent")?
            .primary_probe()
            .map(|probe| probe.primary)
            .map_err(|e| e.message)
    }
}
