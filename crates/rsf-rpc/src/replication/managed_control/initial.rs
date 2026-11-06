//! Mandatory bounded initial-control owner and synchronous native restriction.
use super::*;
use rsf_core::{ErrorCode, RsfError, RsfResult};
use rsf_vsr::{
    InitialLearnerHistoryStore, VsrInitialBegin, VsrInitialError, VsrInitialPolicy,
    VsrInitialRestriction, VsrInitialSelection, VsrInitialTransition, VsrPrefixLimits,
};
use std::path::Path;
#[cfg(test)]
mod client;
mod source;
pub(in crate::replication) mod wire;
pub use rsf_vsr::VsrInitialState as ControlInitialState;
pub use source::{ControlInitialSource, ControlInitialSourceError};

/// Trusted provisioning before any provisional cold open. Source is unbound.
#[derive(Clone, Debug)]
pub struct ControlInitialPolicy {
    pub anchor: ControlBootstrap,
    pub genesis: ControlCatalog,
    pub limits: VsrPrefixLimits,
}
/// Caller-provided trusted native reservation, validated before owner open.
/// This configuration pair is not an opaque transition or completion capability.
#[derive(Clone, Debug)]
pub struct ControlInitialBinding {
    pub policy: ControlInitialPolicy,
    pub state: ControlInitialState,
}
impl ControlInitialPolicy {
    fn validate_local(&self, local: NodeId) -> Result<(), String> {
        let group = self
            .genesis
            .group(self.anchor.control_pg)
            .ok_or("control group absent")?;
        let node = self
            .genesis
            .nodes
            .iter()
            .find(|n| n.id == local)
            .ok_or("local identity absent")?;
        if node.role != rsf_control_state::AdmissionRole::Learner
            || group.voters.contains(&local)
            || group.lineage.plan.is_some()
            || self.limits.max_records == 0
            || self.limits.max_encoded_bytes < 18
        {
            return Err(
                "initial control requires provisioned learner, original voters and finite limits"
                    .into(),
            );
        }
        Ok(())
    }
    pub(super) fn authorize(&self, sender: NodeId, local: NodeId) -> io::Result<()> {
        if sender == local
            || !self
                .genesis
                .group(self.anchor.control_pg)
                .expect("validated policy")
                .voters
                .contains(&sender)
        {
            return Err(invalid(
                "initial control sender is not a provisioned original voter",
            ));
        }
        Ok(())
    }
}
/// Only actual kernel transition callbacks can construct this borrowed event.
/// Native persistence compares exact expected policy/state under its peer lock.
#[derive(Debug)]
pub struct ControlInitialTransition<'a> {
    policy: &'a ControlInitialPolicy,
    local: NodeId,
    expected: ControlInitialState,
    next: ControlInitialState,
}
impl ControlInitialTransition<'_> {
    pub fn policy(&self) -> &ControlInitialPolicy {
        self.policy
    }
    pub fn local(&self) -> NodeId {
        self.local
    }
    pub fn expected(&self) -> ControlInitialState {
        self.expected
    }
    pub fn next(&self) -> ControlInitialState {
        self.next
    }
}
pub(in crate::replication) type Persist<'a> =
    dyn FnMut(&ControlInitialTransition<'_>) -> Result<(), String> + 'a;
#[derive(Debug)]
pub(in crate::replication) struct InitialControlLearner {
    owner: InitialLearnerHistoryStore,
    policy: ControlInitialPolicy,
    local: NodeId,
}
impl InitialControlLearner {
    fn open(
        path: &Path,
        local: NodeId,
        policy: ControlInitialPolicy,
        state: ControlInitialState,
    ) -> RsfResult<Self> {
        let restriction = match state {
            ControlInitialState::Reserved => VsrInitialRestriction::Reserved,
            ControlInitialState::Selected(s) => VsrInitialRestriction::Selected(s),
            ControlInitialState::Publishing { selection, source } => {
                VsrInitialRestriction::Publishing { selection, source }
            }
            ControlInitialState::Published { .. } => {
                return Err(error("published control requires strict published owner"));
            }
        };
        let owner = InitialLearnerHistoryStore::open(
            path,
            VsrInitialPolicy {
                pg: policy.anchor.control_pg,
                local,
                bootstrap: policy.anchor.control_bootstrap,
                limits: policy.limits,
            },
            restriction,
        )?;
        Ok(Self {
            owner,
            policy,
            local,
        })
    }
    pub(in crate::replication) fn placement_group(&self) -> PlacementGroupId {
        self.policy.anchor.control_pg
    }
    pub(in crate::replication) fn local(&self) -> NodeId {
        self.local
    }
    pub(in crate::replication) fn unpublished(&self) -> RsfResult<Option<[u8; 32]>> {
        self.owner.status().map(|_| None)
    }
    pub(in crate::replication) fn unsupported<T>(&self) -> RsfResult<T> {
        self.owner.status()?;
        Err(error(
            "initial control learner has no published observation or membership authority",
        ))
    }
    fn transition(
        policy: &ControlInitialPolicy,
        local: NodeId,
        event: &VsrInitialTransition<'_>,
        persist: &mut Persist<'_>,
    ) -> RsfResult<()> {
        if event.policy().pg != policy.anchor.control_pg
            || event.policy().local != local
            || event.policy().bootstrap != policy.anchor.control_bootstrap
            || event.policy().limits != policy.limits
        {
            return Err(error(
                "initial transition differs from immutable control policy",
            ));
        }
        persist(&ControlInitialTransition {
            policy,
            local,
            expected: event.expected(),
            next: event.next(),
        })
        .map_err(error)
    }
    fn handle(
        &mut self,
        sender: NodeId,
        request: wire::Request,
        persist: Option<&mut Persist<'_>>,
    ) -> io::Result<(
        wire::Reply,
        Option<(learner::ControlLearner, PublishedControlState)>,
    )> {
        self.policy.authorize(sender, self.local)?;
        let status = self.owner.status().map_err(rsf)?;
        if matches!(&request, wire::Request::Status) {
            return Ok((
                wire::Reply::Progress {
                    state: status.state(),
                    records: status.records(),
                },
                None,
            ));
        }
        let persist = persist.ok_or_else(|| {
            invalid("initial control mutation requires native restriction persistence")
        })?;
        let policy = &self.policy;
        let local = self.local;
        let mut record =
            |event: &VsrInitialTransition<'_>| Self::transition(policy, local, event, persist);
        let result = match request {
            wire::Request::Start(descriptor) => self
                .owner
                .begin(VsrInitialBegin::Start(descriptor), &mut record)
                .map(|cursor| wire::Reply::Cursor {
                    selection: cursor.selection(),
                    records: cursor.records(),
                }),
            wire::Request::Resume {
                selection,
                descriptor,
            } => self
                .owner
                .begin(
                    VsrInitialBegin::Resume {
                        selection,
                        descriptor,
                    },
                    &mut record,
                )
                .map(|cursor| wire::Reply::Cursor {
                    selection: cursor.selection(),
                    records: cursor.records(),
                }),
            wire::Request::Replace {
                expected,
                descriptor,
            } => self
                .owner
                .begin(
                    VsrInitialBegin::Replace {
                        expected,
                        descriptor,
                    },
                    &mut record,
                )
                .map(|cursor| wire::Reply::Cursor {
                    selection: cursor.selection(),
                    records: cursor.records(),
                }),
            wire::Request::Batch { selection, batch } => {
                self.owner
                    .append(&selection, &batch)
                    .map(|cursor| wire::Reply::Cursor {
                        selection: cursor.selection(),
                        records: cursor.records(),
                    })
            }
            wire::Request::Finish(selection) => {
                let mut validate = |preview: &rsf_vsr::VsrImportPreview<'_>| {
                    ControlState::validate_initial_import_preview(
                        &policy.anchor,
                        &policy.genesis,
                        preview,
                    )
                };
                match self.owner.finish(&selection, &mut validate, &mut record) {
                    Ok(publication) => {
                        let (owner, state) =
                            learner::ControlLearner::from_initial(publication, policy.clone())
                                .map_err(rsf)?;
                        let id = owner
                            .published_history_id()
                            .map_err(rsf)?
                            .ok_or_else(|| invalid("completed initial publication absent"))?;
                        return Ok((
                            wire::Reply::Published {
                                origin: selection,
                                current: id,
                                tail: LogIndex(1),
                            },
                            Some((owner, state)),
                        ));
                    }
                    Err(e) => Err(e),
                }
            }
            wire::Request::Status => unreachable!("status handled"),
        };
        match result {
            Ok(reply) => Ok((reply, None)),
            Err(VsrInitialError::SelectionConflict { actual }) => {
                Ok((wire::Reply::Conflict(actual), None))
            }
            Err(VsrInitialError::PublicationPinned) => Ok((wire::Reply::Pinned, None)),
            Err(VsrInitialError::Failure(e)) => Err(rsf(e)),
        }
    }
}
fn error(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn rsf(error: RsfError) -> io::Error {
    invalid(error.message)
}

impl PeerReplicaServer {
    pub fn initial_control_learner(
        local: NodeId,
        cluster: &ClusterId,
        identity: PeerIdentity,
        peers: BTreeMap<NodeId, PeerEndpoint>,
        path: impl AsRef<Path>,
        binding: ControlInitialBinding,
        timeout: Duration,
    ) -> Result<Self, String> {
        let ControlInitialBinding { policy, state } = binding;
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
        let (owner, binding) = open_control(path.as_ref(), local, policy, state)?;
        let mut server = Self::with_learner_owners(
            local,
            cluster,
            identity,
            peers,
            BTreeMap::new(),
            [(pg, owner)].into(),
            timeout,
        )?;
        server.managed_control = Some(binding);
        Ok(server)
    }
    pub fn with_initial_control_learner(
        mut self,
        path: impl AsRef<Path>,
        binding: ControlInitialBinding,
    ) -> Result<Self, String> {
        let ControlInitialBinding { policy, state } = binding;
        let pg = policy.anchor.control_pg;
        if self.cells.contains_key(&pg) || self.learners.contains_key(&pg) {
            return Err("reserved control requires policy before owner open".into());
        }
        let mut groups = self
            .cells
            .keys()
            .chain(self.learners.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        groups.insert(pg);
        self.validate_control_configuration(&policy.anchor, &policy.genesis, groups)?;
        policy.validate_local(self.local)?;
        let (owner, binding) = open_control(path.as_ref(), self.local, policy, state)?;
        self.learners.insert(pg, owner);
        self.managed_control = Some(binding);
        Ok(self)
    }
    /// Same TLS dispatcher, with a mandatory native transaction dependency.
    /// Missing ordinary callbacks never turn initial mutations into no-ops.
    pub fn serve_connection_with_initial_control(
        &mut self,
        socket: TcpStream,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
        persist: &mut impl FnMut(&ControlInitialTransition<'_>) -> Result<(), String>,
    ) -> Result<(), String> {
        self.serve_io_with_initial(socket, apply, true, Some(persist))
            .map_err(|e| e.to_string())
    }
    pub(in crate::replication) fn dispatch_initial_control(
        &mut self,
        sender: NodeId,
        payload: &[u8],
        persist: Option<&mut Persist<'_>>,
    ) -> io::Result<Vec<u8>> {
        let binding = self
            .managed_control
            .as_ref()
            .ok_or_else(|| invalid("initial control is not configured"))?;
        let pg = binding.pg();
        let owner = self
            .learners
            .get_mut(&pg)
            .ok_or_else(|| invalid("initial control requires a learner owner"))?;
        let (policy, local) = match owner {
            super::super::learner::LearnerOwner::InitialControl(initial) => {
                (&initial.policy, initial.local)
            }
            super::super::learner::LearnerOwner::Control(published) => {
                return published.initial_retry(sender, self.cluster, payload, persist.is_some());
            }
            super::super::learner::LearnerOwner::Application(_) => {
                return Err(invalid("initial control cannot use application learner"));
            }
        };
        let policy_id = wire::policy_id(self.cluster, local, policy);
        policy.authorize(sender, local)?;
        let request = wire::decode_request(payload, policy_id, policy.limits)?;
        let super::super::learner::LearnerOwner::InitialControl(initial) = owner else {
            unreachable!("initial variant checked")
        };
        let (reply, completed) = initial.handle(sender, request, persist)?;
        if let Some((published, state)) = completed {
            *owner = super::super::learner::LearnerOwner::Control(Box::new(published));
            self.managed_control
                .as_mut()
                .expect("binding retained")
                .record_learner_observation(state);
        }
        wire::encode_reply(payload, policy_id, &reply)
    }
}
fn open_control(
    path: &Path,
    local: NodeId,
    policy: ControlInitialPolicy,
    state: ControlInitialState,
) -> Result<(super::super::learner::LearnerOwner, Binding), String> {
    let anchor = policy.anchor.clone();
    let genesis = policy.genesis.clone();
    match state {
        ControlInitialState::Published { selection, source } => {
            let (owner, state) =
                learner::ControlLearner::open_initial(path, local, policy, selection, source)
                    .map_err(|e| e.message)?;
            Ok((
                super::super::learner::LearnerOwner::Control(Box::new(owner)),
                Binding::for_learner(anchor, genesis, state),
            ))
        }
        _ => {
            let owner =
                InitialControlLearner::open(path, local, policy, state).map_err(|e| e.message)?;
            Ok((
                super::super::learner::LearnerOwner::InitialControl(Box::new(owner)),
                Binding {
                    anchor,
                    genesis,
                    voter: None,
                    learner: None,
                    writer: writer::Writer::default(),
                },
            ))
        }
    }
}
