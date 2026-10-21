//! Client ingress serialized with peer consensus over exact live cell ownership.
use super::*;
use crate::data::{DataPlaneListener, DataPlaneResponse, MutationOutcome};
use rsf_core::{ErrorCode, RsfError, RsfResult};
use rsf_wire::{RequestEnvelope, WireOperationKind};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClientConfig {
    state: PathBuf,
    listen: SocketAddr,
    endpoints: Vec<ClientEndpoint>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientEndpoint {
    node: u64,
    address: SocketAddr,
}
pub(super) struct ClientService {
    listener: DataPlaneListener,
    state: PathBuf,
    coordination: PathBuf,
    endpoints: BTreeMap<NodeId, SocketAddr>,
    admitted: BTreeMap<NodeId, [u8; 32]>,
    coordinator: std::cell::RefCell<
        rsf_protocol::InstalledCoordinatorCache<rsf_rpc::replication::NetworkVsrTransport>,
    >,
    applied_version: std::cell::Cell<rsf_core::CommitVersion>,
}
impl ClientService {
    pub(super) fn open(
        config: &ClientConfig,
        base: &Path,
        peer_state: &Path,
        local: NodeId,
        peers: &BTreeMap<NodeId, PeerEndpoint>,
    ) -> Result<Self, String> {
        let state = if config.state.is_absolute() {
            config.state.clone()
        } else {
            base.join(&config.state)
        };
        let mut endpoints = BTreeMap::new();
        for endpoint in &config.endpoints {
            if endpoint.address.port() == 0
                || !peers.contains_key(&NodeId(endpoint.node))
                || endpoints
                    .insert(NodeId(endpoint.node), endpoint.address)
                    .is_some()
            {
                return Err(
                    "client endpoints require unique mapped nodes and nonzero ports".into(),
                );
            }
        }
        if endpoints.len() != peers.len() || endpoints.get(&local) != Some(&config.listen) {
            return Err(
                "client endpoints must cover every peer and match the local listener".into(),
            );
        }
        let admitted = peers
            .iter()
            .map(|(node, peer)| (*node, peer.fingerprint))
            .collect();
        let service = Self {
            listener: DataPlaneListener::bind(config.listen, &state)?,
            state,
            coordination: peer_state.join("client-protocol"),
            endpoints,
            admitted,
            coordinator: std::cell::RefCell::new(Default::default()),
            applied_version: std::cell::Cell::new(rsf_core::CommitVersion(0)),
        };
        // Require explicitly provisioned admission metadata. Opening a peer
        // listener never fabricates application grants or tenancy bindings.
        service.admission().map_err(|err| err.message)?;
        Ok(service)
    }

    fn admission(&self) -> RsfResult<(Epoch, Vec<NodeId>)> {
        let admin = rsf_admin::AdminPlane::recover(self.state.join("admin"))?;
        let voters = admin
            .membership_view()
            .ok_or_else(|| invalid("client admission requires promoted voters"))?
            .voter_nodes();
        for record in admin.records() {
            if self.admitted.get(&record.node.id)
                != Some(&record.identity.transport.certificate_fingerprint.bytes)
            {
                return Err(invalid(
                    "client admission node identity differs from configured peer",
                ));
            }
        }
        if voters.is_empty() || voters.iter().any(|node| !self.admitted.contains_key(node)) {
            return Err(invalid("client admission omits authenticated voters"));
        }
        Ok((Epoch(admin.cluster_map().epoch.0.max(1)), voters))
    }

    pub(super) fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
    }

    pub(super) fn serve_one(
        &self,
        server: &mut PeerReplicaServer,
        materializer: &mut ApplicationMaterializer,
    ) -> Result<(), String> {
        let served = self.listener.serve_one_with(
            &mut |envelope, fingerprint, output| {
                self.execute(server, materializer, envelope, fingerprint, output)
            },
            &mut std::io::stdout(),
        )?;
        if served != 0 {
            let stats = self.coordinator.borrow().stats();
            println!(
                "{}",
                serde_json::json!({"event":"peer.client_cache","full_recoveries":stats.full_recoveries,"reuses":stats.reuses})
            );
        }
        Ok(())
    }

    fn execute(
        &self,
        server: &mut PeerReplicaServer,
        materializer: &mut ApplicationMaterializer,
        envelope: RequestEnvelope,
        fingerprint: &[u8; 32],
        output: &mut dyn std::io::Write,
    ) -> DataPlaneResponse {
        let rejected = |mutation, reason: String| DataPlaneResponse::Rejected {
            protocol: envelope.protocol.as_str(),
            mutation,
            reason,
        };
        if let Err(error) = crate::security::authorize(&self.state, fingerprint, &envelope) {
            return rejected(MutationOutcome::NotAttempted, error.message);
        }
        let (epoch, admitted) = match self.admission() {
            Ok(admission) => admission,
            Err(error) => return rejected(MutationOutcome::NotAttempted, error.message),
        };
        server.with_voting_cells(|local, cells, transport| {
            let response = (|| {
                // Read-only status remains available while a promise fences normal
                // probes. No stale authority or volatile observation admits clients.
                let statuses = cells
                    .values()
                    .map(|cell| cell.primary_probe())
                    .collect::<RsfResult<Vec<_>>>();
                let statuses = match statuses {
                    Ok(statuses) if !statuses.is_empty() => statuses,
                    Ok(_) => {
                        return rejected(
                            MutationOutcome::NotAttempted,
                            "no hosted voting cells".into(),
                        );
                    }
                    Err(error) => return rejected(MutationOutcome::NotAttempted, error.message),
                };
                if statuses.iter().any(|status| status.voters != admitted) {
                    return rejected(
                        MutationOutcome::NotAttempted,
                        "installed membership differs from admitted voters".into(),
                    );
                }
                let primary = statuses[0].primary;
                if statuses.iter().any(|status| status.primary != primary) {
                    return rejected(
                        MutationOutcome::NotAttempted,
                        "placement groups require coordinated primary routing".into(),
                    );
                }
                if primary != local {
                    return DataPlaneResponse::Redirect {
                        protocol: envelope.protocol.as_str(),
                        primary: primary.0,
                        address: self.endpoints[&primary],
                    };
                }
                let result = self.coordinator.borrow_mut().with_cells(
                    &self.coordination,
                    epoch,
                    local,
                    cells,
                    transport,
                    |router, state| {
                        if state == rsf_protocol::CoordinatorState::Recovered {
                            self.applied_version.set(rsf_core::CommitVersion(0));
                            router.reconcile_installed_application_commands()?;
                        } else {
                            router.reconcile_application_commands()?;
                        }
                        for command in
                            router.committed_application_commands_since(self.applied_version.get())
                        {
                            materializer.apply(&command).map_err(invalid)?;
                            self.applied_version.set(command.commit.version);
                        }
                        // Even immutable reads through this primary endpoint wait
                        // for a fresh installed quorum; no election event is a lease.
                        router.confirm_read_quorum()?;
                        let response = materializer.execute(&envelope, router, output)?;
                        // Complete the prefix contract explicitly, including any
                        // secondary payload commands. Reapplying the selected
                        // engine's own command is idempotent; protocol-only pins
                        // never enter this application index.
                        for command in
                            router.committed_application_commands_since(self.applied_version.get())
                        {
                            materializer.apply(&command).map_err(invalid)?;
                            self.applied_version.set(command.commit.version);
                        }
                        self.applied_version.set(router.latest_version());
                        Ok(response)
                    },
                );
                result.unwrap_or_else(|error| {
                    rejected(
                        match envelope.operation_kind {
                            WireOperationKind::Read => MutationOutcome::NotAttempted,
                            _ => MutationOutcome::Unknown,
                        },
                        error.message,
                    )
                })
            })();
            let materialized = matches!(
                response,
                DataPlaneResponse::Executed { .. } | DataPlaneResponse::Streamed { .. }
            );
            (response, materialized)
        })
    }
}
fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
