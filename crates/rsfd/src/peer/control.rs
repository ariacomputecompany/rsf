//! Trusted native provisioning and policy-before-owner routing.
use super::*;
use rsf_control_state::{AdmissionRole, ControlBootstrap, ControlCommand};
use rsf_rpc::replication::{
    ControlInitialPolicy, ControlInitialTransition, ControlLearnerPolicy, ControlNativePersistence,
};
use state_identity::{ControlCustody, Reservation, StateIdentity};
mod bootstrap;
pub(super) use bootstrap::Scheduler;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    genesis: PathBuf,
    prefix_limits: Limits,
    provisioning: Provisioning,
    #[serde(default)]
    preferred_transport: Option<u64>,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    max_records: u64,
    max_encoded_bytes: u64,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Provisioning {
    Bootstrap,
    ExistingLearner { source: u64 },
    ExistingVoter,
}

pub(super) struct Trusted {
    policy: ControlInitialPolicy,
    provisioning: Provisioning,
    bootstrap: Option<rsf_vsr::VsrBootstrapSpec>,
}
impl Trusted {
    pub(super) fn load(
        config: &PeerConfig,
        base: &Path,
        cluster: &ClusterId,
        identity: &PeerIdentity,
        peers: &BTreeMap<NodeId, PeerEndpoint>,
    ) -> Result<Option<Self>, String> {
        let Some(control) = &config.control else {
            return Ok(None);
        };
        let path = if control.genesis.is_absolute() {
            control.genesis.clone()
        } else {
            base.join(&control.genesis)
        };
        let bytes = read_bounded(
            &path,
            rsf_control_state::MAX_COMMAND_BYTES as u64,
            "control Genesis provisioning",
        )?;
        let ControlCommand::Genesis(genesis) =
            ControlCommand::decode(&bytes).map_err(|e| e.message)?
        else {
            return Err("control provisioning must contain canonical Genesis".into());
        };
        let root = genesis.root().map_err(|e| e.message)?;
        let group = genesis
            .group(genesis.control_pg)
            .ok_or("control group absent")?;
        if group.lineage.plan.is_some() {
            return Err("native control provisioning requires original Genesis lineage".into());
        }
        let policy = ControlInitialPolicy {
            anchor: ControlBootstrap {
                cluster: cluster.clone(),
                control_pg: genesis.control_pg,
                control_bootstrap: group.lineage.bootstrap,
                genesis_catalog: root,
            },
            genesis,
            limits: rsf_vsr::VsrPrefixLimits {
                max_records: control.prefix_limits.max_records,
                max_encoded_bytes: control.prefix_limits.max_encoded_bytes,
            },
        };
        policy.limits.validate().map_err(|e| e.message)?;
        let local = NodeId(config.node);
        let timeout = Duration::from_millis(config.timeout_ms);
        rsf_rpc::replication::validate_control_provisioning(
            cluster,
            local,
            identity,
            peers,
            &policy.anchor,
            &policy.genesis,
            timeout,
        )?;
        let node = policy
            .genesis
            .nodes
            .iter()
            .find(|n| n.id == local)
            .ok_or("control local identity absent")?;
        let group = policy
            .genesis
            .group(policy.anchor.control_pg)
            .expect("validated catalog");
        match control.provisioning {
            Provisioning::Bootstrap => {
                if node.role == AdmissionRole::Voter {
                    if !group.voters.contains(&local) {
                        return Err("fresh control voter must be an original member".into());
                    }
                } else {
                    policy.validate_configuration(cluster, local, identity, peers, timeout)?;
                }
            }
            Provisioning::ExistingLearner { source } => {
                if node.role != AdmissionRole::Learner
                    || group.voters.contains(&local)
                    || source == local.0
                    || !group.voters.contains(&NodeId(source))
                {
                    return Err(
                        "existing control learner requires a provisioned historical source".into(),
                    );
                }
            }
            Provisioning::ExistingVoter => {
                if node.role != AdmissionRole::Voter || !group.voters.contains(&local) {
                    return Err(
                        "existing control voter requires provisioned voting identity".into(),
                    );
                }
            }
        }
        if let Some(preferred) = control.preferred_transport
            && (preferred == local.0 || !group.voters.contains(&NodeId(preferred)))
        {
            return Err("preferred control transport is not an original remote voter".into());
        }
        // The preference is a future scheduler input, never a historical source.
        // Genesis scheduling uses only an initialized actual control voter.
        let actual = config
            .cells
            .iter()
            .map(|c| PlacementGroupId(c.pg))
            .collect::<BTreeSet<_>>();
        let complete = policy
            .genesis
            .groups
            .iter()
            .map(|g| g.pg)
            .collect::<BTreeSet<_>>();
        if actual != complete && actual != [policy.anchor.control_pg].into() {
            return Err(
                "native control requires complete mixed inventory or dedicated control owner"
                    .into(),
            );
        }
        if matches!(control.provisioning, Provisioning::ExistingVoter) && actual != complete {
            return Err(
                "existing native control voter currently requires the complete catalog inventory"
                    .into(),
            );
        }
        let fresh_voter = matches!(control.provisioning, Provisioning::Bootstrap)
            && node.role == AdmissionRole::Voter;
        if fresh_voter && (config.cells.len() != 1 || actual != [policy.anchor.control_pg].into()) {
            return Err("fresh native voter requires exactly the dedicated control cell".into());
        }
        if config.client.is_some() {
            return Err(
                "native control cannot admit clients before replicated control admission".into(),
            );
        }
        let mut result = Self {
            policy,
            provisioning: control.provisioning,
            bootstrap: None,
        };
        let cell = config
            .cells
            .iter()
            .find(|c| c.pg == result.pg().0)
            .ok_or("native control cell is absent")?;
        result.validate_cell(cell)?;
        if fresh_voter {
            let spec = rsf_vsr::VsrBootstrapSpec::new(
                result.pg(),
                Epoch(cell.view),
                NodeId(cell.primary),
                cell.voters.iter().copied().map(NodeId),
            )
            .map_err(|e| e.message)?;
            spec.validate_limits(result.policy.limits)
                .map_err(|e| e.message)?;
            if spec.bootstrap_digest() != result.policy.anchor.control_bootstrap {
                return Err(
                    "fresh control bootstrap differs from canonical Genesis lineage".into(),
                );
            }
            result.bootstrap = Some(spec);
        }
        Ok(Some(result))
    }
    pub(super) fn pg(&self) -> PlacementGroupId {
        self.policy.anchor.control_pg
    }
    pub(super) fn voter(&self) -> bool {
        self.bootstrap.is_some() || matches!(self.provisioning, Provisioning::ExistingVoter)
    }
    fn validate_cell(&self, cell: &CellConfig) -> Result<(), String> {
        let group = self
            .policy
            .genesis
            .group(self.pg())
            .expect("validated catalog");
        if cell.learner_source.is_some()
            || cell.membership_recovery.is_some()
            || cell.view == 0
            || cell.voters.iter().copied().map(NodeId).collect::<Vec<_>>() != group.voters
            || !group.voters.contains(&NodeId(cell.primary))
        {
            return Err("reserved control cell cannot use raw learner, membership recovery or foreign bootstrap configuration".into());
        }
        Ok(())
    }
    pub(super) fn reservation(&self) -> Result<Reservation, String> {
        let custody = match self.provisioning {
            Provisioning::Bootstrap if self.bootstrap.is_some() => ControlCustody::ReservedVoter {},
            Provisioning::Bootstrap => ControlCustody::ReservedLearner {},
            Provisioning::ExistingLearner { source } => ControlCustody::ExistingLearner { source },
            Provisioning::ExistingVoter => ControlCustody::ExistingVoter {},
        };
        Reservation::new(&self.policy, custody)
    }
    pub(super) fn check_existing_policy(&self, identity: &mut StateIdentity) -> Result<(), String> {
        let stored = identity
            .reservation()?
            .ok_or("control reservation is absent")?;
        if !stored.compatible_provisioning(&self.reservation()?) {
            return Err("reserved control policy or owner kind cannot change".into());
        }
        Ok(())
    }
    /// Actual owners are supplied only for application groups. The reserved
    /// learner path is opened by its policy-owning constructor exactly once.
    pub(super) fn bind_server(
        &self,
        inputs: ServerInputs,
        identity: &mut StateIdentity,
    ) -> Result<PeerReplicaServer, String> {
        self.check_existing_policy(identity)?;
        let ServerInputs {
            local,
            cluster,
            tls,
            peers,
            mut cells,
            learners,
            state,
            timeout,
        } = inputs;
        let pg = self.pg();
        let path = state.join(format!("learner-{}", pg.0));
        if self.voter() {
            // This path was bounded before any ordinary role open.
            if !cells.contains_key(&pg) {
                return Err("reserved control voter owner is absent".into());
            }
            if self.bootstrap.is_some() {
                if cells.len() != 1
                    || !learners.is_empty()
                    || identity.reservation()?.map(Reservation::custody)
                        != Some(&ControlCustody::InitializedVoter {})
                {
                    return Err("fresh control voter requires one initialized native owner".into());
                }
                return PeerReplicaServer::control_voter(
                    local,
                    &cluster,
                    tls,
                    peers,
                    cells.remove(&pg).expect("checked owner"),
                    self.policy.anchor.clone(),
                    self.policy.genesis.clone(),
                    timeout,
                );
            }
            return PeerReplicaServer::with_learners(
                local, &cluster, tls, peers, cells, learners, timeout,
            )?
            .with_control_replication(self.policy.anchor.clone(), self.policy.genesis.clone());
        }
        if cells.contains_key(&pg) || learners.contains_key(&pg) {
            return Err("reserved control cannot borrow a raw owner".into());
        }
        let only = cells.is_empty() && learners.is_empty();
        match self.provisioning {
            Provisioning::Bootstrap => {
                let binding = identity.initial_binding(&self.policy)?;
                if only {
                    PeerReplicaServer::initial_control_learner(
                        local, &cluster, tls, peers, path, binding, timeout,
                    )
                } else {
                    PeerReplicaServer::with_learners(
                        local, &cluster, tls, peers, cells, learners, timeout,
                    )?
                    .with_initial_control_learner(path, binding)
                }
            }
            Provisioning::ExistingLearner { source } => {
                if only {
                    PeerReplicaServer::control_learner(
                        local,
                        &cluster,
                        tls,
                        peers,
                        path,
                        ControlLearnerPolicy {
                            anchor: self.policy.anchor.clone(),
                            genesis: self.policy.genesis.clone(),
                            source: NodeId(source),
                            limits: self.policy.limits,
                        },
                        timeout,
                    )
                } else {
                    PeerReplicaServer::with_learners(
                        local, &cluster, tls, peers, cells, learners, timeout,
                    )?
                    .with_control_learner_replication(
                        self.policy.anchor.clone(),
                        self.policy.genesis.clone(),
                        path,
                        NodeId(source),
                        self.policy.limits,
                    )
                }
            }
            Provisioning::ExistingVoter => unreachable!("handled actual bounded owner"),
        }
    }
}
pub(super) struct ServerInputs {
    pub local: NodeId,
    pub cluster: ClusterId,
    pub tls: PeerIdentity,
    pub peers: BTreeMap<NodeId, PeerEndpoint>,
    pub cells: BTreeMap<PlacementGroupId, DurableVsrCell>,
    pub learners: BTreeMap<PlacementGroupId, rsf_vsr::LearnerHistoryStore>,
    pub state: PathBuf,
    pub timeout: Duration,
}
pub(super) struct Adapter<'a> {
    pub identity: &'a mut StateIdentity,
    pub control: &'a Trusted,
}
impl ControlNativePersistence for Adapter<'_> {
    fn validate(&mut self) -> Result<(), String> {
        self.control.check_existing_policy(self.identity)
    }
    fn persist(&mut self, event: &ControlInitialTransition<'_>) -> Result<(), String> {
        self.identity.persist_initial(&self.control.policy, event)
    }
}
