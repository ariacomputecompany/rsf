//! Mandatory policy-owning control learner. No mutable/raw owner escape exists.
use super::*;
use rsf_core::{ErrorCode, RsfError, RsfResult};
use rsf_vsr::{
    LearnerHistoryStore, MembershipStagingStore, VsrHistoryBatch, VsrHistoryDescriptor,
    VsrHistoryRecord, VsrLearnerStateToken, VsrMembershipApproval, VsrPrefixLimits,
};
use std::path::Path;

/// Immutable trusted configuration supplied before a reserved owner is opened.
/// These values grant no publication, retention, voting, or client permission.
#[derive(Clone, Debug)]
pub struct ControlLearnerPolicy {
    pub anchor: ControlBootstrap,
    pub genesis: ControlCatalog,
    pub source: NodeId,
    pub limits: VsrPrefixLimits,
}
impl ControlLearnerPolicy {
    fn validate_local(&self, local: NodeId) -> Result<(), String> {
        let node = self
            .genesis
            .nodes
            .iter()
            .find(|node| node.id == local)
            .expect("configuration validated");
        let group = self
            .genesis
            .group(self.anchor.control_pg)
            .expect("configuration validated");
        if node.role != rsf_control_state::AdmissionRole::Learner
            || group.voters.contains(&local)
            || self.source == local
            || !group.voters.contains(&self.source)
            || self.limits.max_records == 0
            || self.limits.max_encoded_bytes == 0
        {
            return Err("control learner requires provisioned learner/source identities and finite history limits".into());
        }
        Ok(())
    }
}
#[derive(Debug)]
struct Context {
    descriptor: VsrHistoryDescriptor,
    sender: NodeId,
}
#[derive(Debug)]
pub(in crate::replication) struct ControlLearner {
    owner: LearnerHistoryStore,
    policy: ControlLearnerPolicy,
    context: Option<Context>,
}
impl ControlLearner {
    // The only constructor accepts trusted policy before opening the actual
    // existing anchor. There is deliberately no constructor from a raw learner.
    fn open(
        path: &Path,
        local: NodeId,
        policy: ControlLearnerPolicy,
    ) -> RsfResult<(Self, PublishedControlState)> {
        let owner = LearnerHistoryStore::open_published_existing(
            path,
            policy.anchor.control_pg,
            local,
            policy.source,
            policy.limits,
        )?;
        let state = PublishedControlState::recover(&policy.anchor, &owner)?;
        Ok((
            Self {
                owner,
                policy,
                context: None,
            },
            state,
        ))
    }
    pub(in crate::replication) fn placement_group(&self) -> PlacementGroupId {
        self.policy.anchor.control_pg
    }
    pub(in crate::replication) fn local(&self) -> NodeId {
        self.owner.local()
    }
    pub(in crate::replication) fn source(&self) -> NodeId {
        self.policy.source
    }
    pub(in crate::replication) fn published_history_id(&self) -> RsfResult<Option<[u8; 32]>> {
        self.owner.published_history_id()
    }
    pub(in crate::replication) fn published_state_token(&self) -> RsfResult<VsrLearnerStateToken> {
        self.owner.published_state_token()
    }
    pub(in crate::replication) fn committed_entries(&self) -> RsfResult<Vec<VsrCommittedEntry>> {
        self.owner.committed_entries()
    }
    pub(in crate::replication) fn observation(&self) -> RsfResult<PublishedControlState> {
        PublishedControlState::recover(&self.policy.anchor, &self.owner)
    }
    pub(in crate::replication) fn validate_membership_authority(
        &self,
        approval: &VsrMembershipApproval,
    ) -> RsfResult<()> {
        self.owner.validate_membership_authority(approval)
    }
    pub(in crate::replication) fn validate_membership_stage(
        &self,
        stage: &MembershipStagingStore,
    ) -> RsfResult<()> {
        self.owner.validate_membership_stage(stage)
    }

    pub(in crate::replication) fn begin(
        &mut self,
        sender: NodeId,
        descriptor: VsrHistoryDescriptor,
    ) -> io::Result<u64> {
        if sender != self.policy.source
            || descriptor.probe.pg != self.policy.anchor.control_pg
            || !self
                .policy
                .genesis
                .group(self.policy.anchor.control_pg)
                .expect("validated policy")
                .voters
                .contains(&sender)
        {
            return Err(invalid(
                "control history sender/PG differs from provisioned source",
            ));
        }
        let count = self
            .owner
            .begin_prefix(descriptor.clone(), self.policy.limits)
            .map_err(rsf)?;
        self.context = Some(Context { descriptor, sender });
        Ok(count)
    }
    pub(in crate::replication) fn require_context(
        &self,
        sender: NodeId,
        kind: u8,
        id: [u8; 32],
    ) -> io::Result<()> {
        // Physical failure must not be hidden as an absent volatile context.
        self.owner.published_state_token().map_err(rsf)?;
        let Some(context) = &self.context else {
            return Err(super::super::history::status::HistoryBeginRequired::error(
                kind,
                self.policy.anchor.control_pg,
                id,
            ));
        };
        if sender != context.sender
            || sender != self.policy.source
            || context.descriptor.id().map_err(rsf)? != id
        {
            return Err(invalid(
                "control history request differs from its bounded BEGIN context",
            ));
        }
        Ok(())
    }
    pub(in crate::replication) fn append(
        &mut self,
        sender: NodeId,
        id: [u8; 32],
        ordinal: u64,
        encoded: &[u8],
    ) -> io::Result<u64> {
        self.require_context(sender, HISTORY_RECORD, id)?;
        if encoded.len() as u64 > self.policy.limits.max_encoded_bytes {
            return Err(invalid(
                "control history record exceeds configured byte budget",
            ));
        }
        let record = VsrHistoryRecord::decode(encoded).map_err(rsf)?;
        self.owner
            .append_prefix_batch(
                id,
                &VsrHistoryBatch {
                    ordinal,
                    records: vec![record],
                },
            )
            .map_err(rsf)
    }
    pub(in crate::replication) fn finish(
        &mut self,
        sender: NodeId,
        id: [u8; 32],
    ) -> io::Result<(LogIndex, PublishedControlState)> {
        self.require_context(sender, HISTORY_FINISH, id)?;
        let anchor = &self.policy.anchor;
        let tail = self
            .owner
            .finish_prefix(id, |preview| {
                ControlState::validate_import_preview(anchor, preview)
            })
            .map_err(rsf)?;
        // Semantic validation precedes ACTIVE; this fresh observation follows
        // the real publication and cannot be replaced with candidate DTOs.
        let state = self.observation().map_err(rsf)?;
        Ok((tail, state))
    }
}
fn rsf(error: RsfError) -> io::Error {
    invalid(error.message)
}
pub(in crate::replication) fn unsupported() -> RsfError {
    RsfError::new(
        ErrorCode::InvalidInput,
        "configured control membership publication requires sealed control replay custody",
    )
}

impl PeerReplicaServer {
    /// Construct a server whose only hosted group is the reserved control
    /// learner. Policy and TLS identity are checked before bounded cold opening.
    pub fn control_learner(
        local: NodeId,
        cluster: &ClusterId,
        identity: PeerIdentity,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        path: impl AsRef<Path>,
        policy: ControlLearnerPolicy,
        timeout: Duration,
    ) -> Result<Self, String> {
        validate_configuration(local, &identity, &peers, timeout)?;
        let nodes = peers
            .iter()
            .map(|(node, peer)| (peer.fingerprint, *node))
            .collect();
        let pg = policy.anchor.control_pg;
        validate_policy(
            cluster_digest(cluster),
            local,
            &identity,
            &nodes,
            &policy.anchor,
            &policy.genesis,
        )?;
        policy.validate_local(local)?;
        let anchor = policy.anchor.clone();
        let genesis = policy.genesis.clone();
        let (owner, state) =
            ControlLearner::open(path.as_ref(), local, policy).map_err(|error| error.message)?;
        let mut server = Self::with_learner_owners(
            local,
            cluster,
            identity,
            peers,
            BTreeMap::new(),
            [(
                pg,
                super::super::learner::LearnerOwner::Control(Box::new(owner)),
            )]
            .into(),
            timeout,
        )?;
        server.managed_control = Some(Binding::for_learner(anchor, genesis, state));
        Ok(server)
    }

    /// Trusted policy is supplied before bounded cold opening. The reserved
    /// owner must not already be present as a generic learner or voter. Native
    /// monotonic configuration/provisioning remains a separate prerequisite.
    pub fn with_control_learner_replication(
        mut self,
        anchor: ControlBootstrap,
        genesis: ControlCatalog,
        path: impl AsRef<Path>,
        source: NodeId,
        limits: VsrPrefixLimits,
    ) -> Result<Self, String> {
        let pg = anchor.control_pg;
        if self.cells.contains_key(&pg) || self.learners.contains_key(&pg) {
            return Err(
                "reserved control learner must be opened under policy, not supplied as a raw owner"
                    .into(),
            );
        }
        let mut actual = self
            .cells
            .keys()
            .chain(self.learners.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        actual.insert(pg);
        self.validate_control_configuration(&anchor, &genesis, actual)?;
        let policy = ControlLearnerPolicy {
            anchor: anchor.clone(),
            genesis: genesis.clone(),
            source,
            limits,
        };
        policy.validate_local(self.local)?;
        // Kernel validates all limit arithmetic before inspecting/opening paths.
        let (owner, state) = ControlLearner::open(path.as_ref(), self.local, policy)
            .map_err(|error| error.message)?;
        self.learners.insert(
            pg,
            super::super::learner::LearnerOwner::Control(Box::new(owner)),
        );
        self.managed_control = Some(Binding::for_learner(anchor, genesis, state));
        Ok(self)
    }
}
