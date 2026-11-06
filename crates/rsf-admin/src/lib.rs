#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_core::{
    Digest, DigestAlgorithm, Epoch, ErrorCode, FailureDomainId, LogIndex, NodeId, PlacementGroupId,
    RegionId, RsfError, RsfResult,
    cluster::{ClusterMap, NodeDescriptor, NodeRole},
    placement::{ActingSet, PlacementGroup, PlacementPolicy},
    storage::WalRecordKind,
};
use rsf_membership::{
    Member, MemberRole, MembershipCell, MembershipDecision, MembershipView, QuorumIntersection,
};
use rsf_protocol::{PlacementMap, ProtocolPolicy, VsrBackedFabricRouter};
use rsf_storage::{SegmentStore, WalAppend};
use rsf_vsr::VsrDecision;

const ADMIN_PG: PlacementGroupId = PlacementGroupId(0);
const ADMIN_EVENT_MAGIC: &[u8; 5] = b"RSFA1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InviteToken(pub String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InviteTokenVerifier {
    hash: [u8; 32],
}

impl InviteTokenVerifier {
    fn matches(&self, other: &Self) -> bool {
        let mut diff = 0_u8;
        for (left, right) in self.hash.iter().zip(other.hash.iter()) {
            diff |= left ^ right;
        }
        diff == 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinRequest {
    pub token: InviteToken,
    pub identity: NodeIdentity,
    pub region: RegionId,
    pub zone: FailureDomainId,
    pub capacity_bytes: u64,
    pub supports_storage: bool,
    pub supports_gateway: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeIdentity {
    pub public_key: String,
    pub nonce: String,
    pub transport: NodeTransportIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeTransportIdentity {
    pub trust_domain: String,
    pub spiffe_id: String,
    pub certificate_fingerprint: Digest,
}

impl NodeTransportIdentity {
    pub fn new(
        trust_domain: impl Into<String>,
        public_key: &str,
        certificate_fingerprint: Digest,
    ) -> RsfResult<Self> {
        let trust_domain = trust_domain.into();
        validate_trust_domain(&trust_domain)?;
        validate_tls_fingerprint(&certificate_fingerprint)?;
        Ok(Self {
            spiffe_id: spiffe_id(&trust_domain, public_key),
            trust_domain,
            certificate_fingerprint,
        })
    }
}

impl NodeIdentity {
    pub fn new(
        public_key: impl Into<String>,
        nonce: impl Into<String>,
        trust_domain: impl Into<String>,
        certificate_fingerprint: Digest,
    ) -> RsfResult<Self> {
        let public_key = public_key.into();
        let transport =
            NodeTransportIdentity::new(trust_domain, &public_key, certificate_fingerprint)?;
        let identity = Self {
            public_key,
            nonce: nonce.into(),
            transport,
        };
        validate_identity(&identity)?;
        Ok(identity)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionState {
    Requested,
    Validated,
    Learner,
    Promoted,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionRecord {
    pub node: NodeDescriptor,
    pub state: AdmissionState,
    pub identity: NodeIdentity,
    pub supports_gateway: bool,
}

struct MembershipProposal {
    view: Epoch,
    primary: NodeId,
    voters: Vec<NodeId>,
}

#[derive(Debug)]
pub struct AdminPlane {
    cluster: ClusterMap,
    token_verifier: InviteTokenVerifier,
    next_node_id: u64,
    records: BTreeMap<NodeId, AdmissionRecord>,
    membership: Option<MembershipCell>,
    audit: Vec<AdmissionAuditEvent>,
    store: Option<SegmentStore>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionAuditEvent {
    pub epoch: Epoch,
    pub node: NodeId,
    pub action: AdmissionAuditAction,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionAuditAction {
    LearnerAdmitted,
    LearnerSynced,
    LearnerPromoted,
    PlacementPlanned,
    PlacementApplied,
    VoterRemoved,
    VoterReplaced,
    PolicyApplied,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementPreflight {
    pub requested_groups: u64,
    pub requested_replicas: u8,
    pub eligible_nodes: usize,
    pub distinct_regions: usize,
    pub distinct_failure_domains: usize,
    pub checks: Vec<PlacementPreflightCheck>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementPreflightCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementPlan {
    pub preflight: PlacementPreflight,
    pub groups: Vec<PlacementGroup>,
}

impl AdminPlane {
    pub fn new(token: InviteToken) -> Self {
        Self {
            cluster: ClusterMap::empty(),
            token_verifier: token_verifier(&token),
            next_node_id: 0,
            records: BTreeMap::new(),
            membership: None,
            audit: Vec::new(),
            store: None,
        }
    }

    fn empty_for_recovery() -> Self {
        Self {
            cluster: ClusterMap::empty(),
            token_verifier: InviteTokenVerifier { hash: [0_u8; 32] },
            next_node_id: 0,
            records: BTreeMap::new(),
            membership: None,
            audit: Vec::new(),
            store: None,
        }
    }

    pub fn open_durable(path: impl AsRef<Path>, token: InviteToken) -> RsfResult<Self> {
        let path = path.as_ref();
        let recovery = SegmentStore::recover(path)?;
        let has_admin_events = recovery.recovery.wal_records.iter().any(|record| {
            record.pg == ADMIN_PG
                && record.kind == WalRecordKind::Membership
                && AdminEvent::is_payload(&record.payload)
        });
        if !has_admin_events {
            let mut admin = Self::new(token);
            admin.store = Some(SegmentStore::open(path)?);
            admin.append_event(AdminEvent::TokenVerifier {
                verifier: admin.token_verifier,
            })?;
            return Ok(admin);
        }

        let admin = Self::recover(path)?;
        let verifier = token_verifier(&token);
        if !admin.token_verifier.matches(&verifier) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "join token is not valid for this cluster",
            ));
        }
        Ok(admin)
    }

    pub fn open_or_bootstrap(path: impl AsRef<Path>, token: InviteToken) -> RsfResult<Self> {
        Self::open_durable(path, token)
    }

    pub fn recover(path: impl AsRef<Path>) -> RsfResult<Self> {
        let path = path.as_ref();
        let (store, recovery) = SegmentStore::open_recovered(path, Default::default())?;
        #[cfg(test)]
        tests::recovery_observation::after_open();
        let mut admin = Self::empty_for_recovery();
        for record in recovery.recovery.wal_records.iter().filter(|record| {
            record.pg == ADMIN_PG
                && record.kind == WalRecordKind::Membership
                && AdminEvent::is_payload(&record.payload)
        }) {
            let event = AdminEvent::decode(&record.payload)?;
            admin.apply_event(event)?;
        }
        admin.store = Some(store);
        if !admin.token_verifier_initialized() {
            return Err(corrupt("admin log is missing invite token verifier"));
        }
        Ok(admin)
    }

    pub fn admit(&mut self, request: JoinRequest) -> RsfResult<AdmissionRecord> {
        self.validate_request(&request)?;
        let id = NodeId(self.next_node_id);
        let mut node = NodeDescriptor {
            id,
            region: request.region,
            failure_domain: request.zone,
            capacity_bytes: request.capacity_bytes,
            roles: Vec::new(),
        };
        node.roles.push(NodeRole::Learner);
        let record = AdmissionRecord {
            node,
            state: AdmissionState::Learner,
            identity: request.identity,
            supports_gateway: request.supports_gateway,
        };
        self.append_event(AdminEvent::Admitted(record.clone()))?;
        self.apply_admitted(record.clone())?;
        Ok(record)
    }

    fn apply_admitted(&mut self, record: AdmissionRecord) -> RsfResult<()> {
        if self.records.contains_key(&record.node.id) {
            return Err(corrupt(format!(
                "admin admission replay found duplicate node {}",
                record.node.id.0
            )));
        }
        if self
            .records
            .values()
            .any(|existing| existing.identity.public_key == record.identity.public_key)
        {
            return Err(corrupt(
                "admin admission replay found duplicate node identity",
            ));
        }
        if self.records.values().any(|existing| {
            existing.identity.transport.certificate_fingerprint
                == record.identity.transport.certificate_fingerprint
        }) {
            return Err(corrupt(
                "admin admission replay found duplicate node TLS certificate fingerprint",
            ));
        }
        self.next_node_id = self.next_node_id.max(record.node.id.0 + 1);
        self.cluster.epoch = Epoch(self.cluster.epoch.0 + 1);
        self.cluster.nodes.push(record.node.clone());
        self.audit.push(AdmissionAuditEvent {
            epoch: self.cluster.epoch,
            node: record.node.id,
            action: AdmissionAuditAction::LearnerAdmitted,
            detail: format!(
                "learner admitted in region {} failure_domain {}",
                record.node.region, record.node.failure_domain
            ),
        });
        self.records.insert(record.node.id, record.clone());
        Ok(())
    }

    pub fn mark_learner_synced(&mut self, node: NodeId) -> RsfResult<AdmissionRecord> {
        self.require_state(node, AdmissionState::Learner, "sync can be marked")?;
        self.append_event(AdminEvent::LearnerSynced { node })?;
        self.apply_learner_synced(node)
    }

    fn apply_learner_synced(&mut self, node: NodeId) -> RsfResult<AdmissionRecord> {
        let record = self.record_mut(node)?;
        if record.state != AdmissionState::Learner {
            return Err(invalid(format!(
                "node {} must be a learner before sync can be marked",
                node.0
            )));
        }
        record.state = AdmissionState::Validated;
        let record = record.clone();
        self.audit.push(AdmissionAuditEvent {
            epoch: self.cluster.epoch,
            node,
            action: AdmissionAuditAction::LearnerSynced,
            detail: "learner caught up to committed prefix".to_string(),
        });
        Ok(record)
    }

    pub fn promote_synced_learner(&mut self, node: NodeId) -> RsfResult<AdmissionRecord> {
        self.require_state(node, AdmissionState::Validated, "promotion")?;
        self.append_event(AdminEvent::LearnerPromoted { node })?;
        self.apply_learner_promoted(node)
    }

    fn apply_learner_promoted(&mut self, node: NodeId) -> RsfResult<AdmissionRecord> {
        let supports_gateway = self
            .records
            .get(&node)
            .ok_or_else(|| invalid(format!("node {} is not admitted", node.0)))?
            .supports_gateway;
        {
            let record = self.record_mut(node)?;
            if record.state != AdmissionState::Validated {
                return Err(invalid(format!(
                    "node {} must be synced before promotion",
                    node.0
                )));
            }
        }

        let decision = self.promote_membership(node)?;
        let quorum = match decision {
            MembershipDecision::Promoted { quorum, .. } => quorum,
            MembershipDecision::AddedLearner(_)
            | MembershipDecision::MarkedSynced(_)
            | MembershipDecision::Removed { .. }
            | MembershipDecision::Replaced { .. } => {
                return Err(invalid("unexpected membership decision during promotion"));
            }
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
        };

        let new_epoch = Epoch(self.cluster.epoch.0 + 1);
        let record = self.record_mut(node)?;
        record.state = AdmissionState::Promoted;
        assign_promoted_roles(&mut record.node, supports_gateway);
        let promoted = record.clone();
        if let Some(node_in_map) = self.cluster.nodes.iter_mut().find(|entry| entry.id == node) {
            *node_in_map = promoted.node.clone();
        }
        self.cluster.epoch = new_epoch;
        self.ensure_membership_initialized()?;
        self.audit.push(AdmissionAuditEvent {
            epoch: self.cluster.epoch,
            node,
            action: AdmissionAuditAction::LearnerPromoted,
            detail: format!(
                "promoted with quorum overlap previous={} next={} shared={}",
                quorum.previous_voters, quorum.next_voters, quorum.shared_voters
            ),
        });
        Ok(promoted)
    }

    pub fn membership_view(&self) -> Option<rsf_membership::MembershipView> {
        self.membership.as_ref().map(MembershipCell::view)
    }

    pub fn cluster_map(&self) -> &ClusterMap {
        &self.cluster
    }

    pub fn records(&self) -> impl Iterator<Item = &AdmissionRecord> {
        self.records.values()
    }

    pub fn audit(&self) -> &[AdmissionAuditEvent] {
        &self.audit
    }

    pub fn map_replicated_groups(
        &mut self,
        count: u64,
        replicas: u8,
    ) -> RsfResult<Vec<PlacementGroup>> {
        Ok(self.plan_replicated_groups(count, replicas)?.groups)
    }

    pub fn preview_replicated_groups(&self, count: u64, replicas: u8) -> RsfResult<PlacementPlan> {
        let preflight = self.preflight_replicated_groups(count, replicas);
        if let Some(failed) = preflight.checks.iter().find(|check| !check.passed) {
            return Err(invalid(failed.detail.clone()));
        }

        let storage_nodes = self
            .cluster
            .nodes
            .iter()
            .filter(|node| node.roles.contains(&NodeRole::Storage))
            .collect::<Vec<_>>();

        let mut groups = Vec::new();
        for group_id in 1..=count {
            let start = (group_id as usize - 1) % storage_nodes.len();
            let members = select_replicas(&storage_nodes, start, usize::from(replicas))?;
            let acting_set = ActingSet {
                epoch: self.cluster.epoch,
                primary: members[0],
                replicas: members,
            };
            acting_set.validate()?;
            groups.push(PlacementGroup {
                id: PlacementGroupId(group_id),
                acting_set,
                policy: PlacementPolicy::Replicated { replicas },
            });
        }
        Ok(PlacementPlan { preflight, groups })
    }

    pub fn plan_replicated_groups(&mut self, count: u64, replicas: u8) -> RsfResult<PlacementPlan> {
        let result = self.preview_replicated_groups(count, replicas);
        if let Err(err) = &result {
            let event = AdmissionAuditEvent {
                epoch: self.cluster.epoch,
                node: NodeId(0),
                action: AdmissionAuditAction::Rejected,
                detail: format!("placement preflight failed: {}", err.message),
            };
            self.append_event(AdminEvent::Audit(event.clone()))?;
            self.audit.push(event);
            return result;
        }
        let plan = result?;
        let event = AdmissionAuditEvent {
            epoch: self.cluster.epoch,
            node: NodeId(0),
            action: AdmissionAuditAction::PlacementPlanned,
            detail: format!(
                "planned {count} placement groups with {replicas} replicas across {} regions and {} failure domains",
                plan.preflight.distinct_regions, plan.preflight.distinct_failure_domains
            ),
        };
        self.append_event(AdminEvent::Audit(event.clone()))?;
        self.audit.push(event);
        Ok(plan)
    }

    pub fn apply_placement_plan(
        &mut self,
        router: &mut VsrBackedFabricRouter,
        plan: &PlacementPlan,
    ) -> RsfResult<PlacementMap> {
        let group_ids = plan.groups.iter().map(|group| group.id).collect::<Vec<_>>();
        match router.update_placement_map(group_ids) {
            Ok(map) => {
                let event = AdmissionAuditEvent {
                    epoch: self.cluster.epoch,
                    node: NodeId(0),
                    action: AdmissionAuditAction::PlacementApplied,
                    detail: format!(
                        "applied protocol placement map version {} with {} placement groups",
                        map.version,
                        map.groups.len()
                    ),
                };
                self.append_event(AdminEvent::Audit(event.clone()))?;
                self.audit.push(event);
                Ok(map)
            }
            Err(err) => {
                let event = AdmissionAuditEvent {
                    epoch: self.cluster.epoch,
                    node: NodeId(0),
                    action: AdmissionAuditAction::Rejected,
                    detail: format!("placement map apply failed: {}", err.message),
                };
                self.append_event(AdminEvent::Audit(event.clone()))?;
                self.audit.push(event);
                Err(err)
            }
        }
    }

    pub fn remove_voter(
        &mut self,
        router: &mut VsrBackedFabricRouter,
        node: NodeId,
    ) -> RsfResult<MembershipView> {
        let result = self.propose_voter_removal(node).and_then(|proposal| {
            self.apply_router_membership(router, proposal.view, proposal.primary, &proposal.voters)
        });
        if let Err(err) = result {
            self.record_rejection(node, format!("voter removal rejected: {}", err.message))?;
            return Err(err);
        }

        self.append_event(AdminEvent::VoterRemoved { node })?;
        self.apply_voter_removed(node)
    }

    pub fn replace_voter(
        &mut self,
        router: &mut VsrBackedFabricRouter,
        removed: NodeId,
        replacement: NodeId,
    ) -> RsfResult<MembershipView> {
        let result = self
            .propose_voter_replacement(removed, replacement)
            .and_then(|(add, remove)| {
                self.apply_router_membership(router, add.view, add.primary, &add.voters)?;
                self.apply_router_membership(router, remove.view, remove.primary, &remove.voters)
            });
        if let Err(err) = result {
            self.record_rejection(
                removed,
                format!("voter replacement rejected: {}", err.message),
            )?;
            return Err(err);
        }

        self.append_event(AdminEvent::VoterReplaced {
            removed,
            replacement,
        })?;
        self.apply_voter_replaced(removed, replacement)
    }

    pub fn apply_protocol_policy(
        &mut self,
        router: &mut VsrBackedFabricRouter,
        allow_availability_first_immutable: bool,
        min_placement_groups: u64,
    ) -> RsfResult<ProtocolPolicy> {
        let current_groups = router.placement_map().groups.len() as u64;
        if current_groups < min_placement_groups {
            let err = invalid(format!(
                "policy requires at least {min_placement_groups} placement groups, current map has {current_groups}"
            ));
            self.record_rejection(NodeId(0), format!("policy apply rejected: {}", err.message))?;
            return Err(err);
        }

        match router.update_policy(allow_availability_first_immutable, min_placement_groups) {
            Ok(policy) => {
                let event = AdmissionAuditEvent {
                    epoch: self.cluster.epoch,
                    node: NodeId(0),
                    action: AdmissionAuditAction::PolicyApplied,
                    detail: format!(
                        "applied protocol policy version {} allow_ap_immutable={} min_placement_groups={}",
                        policy.version,
                        policy.allow_availability_first_immutable,
                        policy.min_placement_groups
                    ),
                };
                self.append_event(AdminEvent::Audit(event.clone()))?;
                self.audit.push(event);
                Ok(policy)
            }
            Err(err) => {
                self.record_rejection(
                    NodeId(0),
                    format!("policy apply rejected: {}", err.message),
                )?;
                Err(err)
            }
        }
    }

    pub fn preflight_replicated_groups(&self, count: u64, replicas: u8) -> PlacementPreflight {
        let storage_nodes = self
            .cluster
            .nodes
            .iter()
            .filter(|node| node.roles.contains(&NodeRole::Storage))
            .collect::<Vec<_>>();
        let regions = storage_nodes
            .iter()
            .map(|node| node.region.clone())
            .collect::<BTreeSet<_>>();
        let failure_domains = storage_nodes
            .iter()
            .map(|node| node.failure_domain.clone())
            .collect::<BTreeSet<_>>();
        let mut checks = Vec::new();
        checks.push(check(
            "group-count",
            count > 0,
            format!("requested placement groups: {count}"),
        ));
        checks.push(check(
            "replica-count",
            replicas == 1 || replicas >= 3,
            format!("requested replicas: {replicas}; choose 1 for standalone or at least 3 for redundancy"),
        ));
        checks.push(check(
            "storage-capacity",
            storage_nodes.len() >= usize::from(replicas),
            format!(
                "eligible storage nodes: {}; required replicas: {replicas}",
                storage_nodes.len()
            ),
        ));
        checks.push(check(
            "failure-domain-spread",
            failure_domains.len() >= usize::from(replicas),
            format!(
                "distinct failure domains: {}; required: {replicas}",
                failure_domains.len()
            ),
        ));
        checks.push(check(
            "region-presence",
            !regions.is_empty(),
            format!("distinct regions: {}", regions.len()),
        ));
        PlacementPreflight {
            requested_groups: count,
            requested_replicas: replicas,
            eligible_nodes: storage_nodes.len(),
            distinct_regions: regions.len(),
            distinct_failure_domains: failure_domains.len(),
            checks,
        }
    }

    fn validate_request(&self, request: &JoinRequest) -> RsfResult<()> {
        if !self.token_verifier.matches(&token_verifier(&request.token)) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "join token is not valid for this cluster",
            ));
        }
        if request.capacity_bytes == 0 || !request.supports_storage {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "node lacks required storage capability",
            ));
        }
        validate_identity(&request.identity)?;
        if self
            .records
            .values()
            .any(|record| record.identity.public_key == request.identity.public_key)
        {
            return Err(invalid("node identity public key already exists"));
        }
        if self.records.values().any(|record| {
            record.identity.transport.certificate_fingerprint
                == request.identity.transport.certificate_fingerprint
        }) {
            return Err(invalid(
                "node TLS certificate fingerprint already belongs to an admitted node",
            ));
        }
        if request.region.as_str().is_empty() || request.zone.as_str().is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "node must declare region and zone",
            ));
        }
        Ok(())
    }

    fn record_mut(&mut self, node: NodeId) -> RsfResult<&mut AdmissionRecord> {
        self.records
            .get_mut(&node)
            .ok_or_else(|| invalid(format!("node {} is not admitted", node.0)))
    }

    fn require_state(&self, node: NodeId, state: AdmissionState, action: &str) -> RsfResult<()> {
        let record = self
            .records
            .get(&node)
            .ok_or_else(|| invalid(format!("node {} is not admitted", node.0)))?;
        if record.state != state {
            return Err(invalid(format!(
                "node {} must be {:?} before {action}",
                node.0, state
            )));
        }
        Ok(())
    }

    fn propose_voter_removal(&self, node: NodeId) -> RsfResult<MembershipProposal> {
        let current = self.current_membership()?;
        if !current.voter_nodes().contains(&node) {
            return Err(invalid(format!("node {} is not a voter", node.0)));
        }
        let voters = current
            .voter_nodes()
            .into_iter()
            .filter(|candidate| *candidate != node)
            .collect::<Vec<_>>();
        self.validate_membership_proposal(&current, voters)
    }

    fn propose_voter_replacement(
        &self,
        removed: NodeId,
        replacement: NodeId,
    ) -> RsfResult<(MembershipProposal, MembershipProposal)> {
        if removed == replacement {
            return Err(invalid(
                "replacement voter must be different from removed voter",
            ));
        }
        let replacement_record = self.records.get(&replacement).ok_or_else(|| {
            invalid(format!(
                "replacement node {} is not admitted",
                replacement.0
            ))
        })?;
        if replacement_record.state != AdmissionState::Validated {
            return Err(invalid(format!(
                "replacement node {} must be a synced learner",
                replacement.0
            )));
        }
        let current = self.current_membership()?;
        let mut expanded_voters = current.voter_nodes();
        if !expanded_voters.contains(&removed) {
            return Err(invalid(format!("node {} is not a voter", removed.0)));
        }
        if expanded_voters.contains(&replacement) {
            return Err(invalid(format!(
                "replacement node {} is already a voter",
                replacement.0
            )));
        }
        expanded_voters.push(replacement);
        expanded_voters.sort();
        let add = self.validate_membership_proposal(&current, expanded_voters.clone())?;
        let expanded_view = membership_view(add.view, &expanded_voters);
        let final_voters = expanded_voters
            .into_iter()
            .filter(|candidate| *candidate != removed)
            .collect::<Vec<_>>();
        let remove = self.validate_membership_proposal(&expanded_view, final_voters)?;
        Ok((add, remove))
    }

    fn validate_membership_proposal(
        &self,
        current: &MembershipView,
        voters: Vec<NodeId>,
    ) -> RsfResult<MembershipProposal> {
        if voters.len() < 3 {
            return Err(invalid(
                "membership change would violate minimum voter count",
            ));
        }
        let next_view = membership_view(Epoch(current.epoch.0 + 1), &voters);
        let quorum = QuorumIntersection::between(current, &next_view);
        if !quorum.preserves_quorum_intersection() {
            return Err(invalid(format!(
                "membership change would violate quorum intersection: previous={} next={} shared={}",
                quorum.previous_voters, quorum.next_voters, quorum.shared_voters
            )));
        }
        let primary = voters[0];
        Ok(MembershipProposal {
            view: next_view.epoch,
            primary,
            voters,
        })
    }

    fn current_membership(&self) -> RsfResult<MembershipView> {
        self.membership
            .as_ref()
            .map(MembershipCell::view)
            .ok_or_else(|| invalid("membership is not initialized"))
    }

    fn apply_router_membership(
        &mut self,
        router: &mut VsrBackedFabricRouter,
        view: Epoch,
        primary: NodeId,
        voters: &[NodeId],
    ) -> RsfResult<()> {
        let groups = router.placement_map().groups.clone();
        if groups.is_empty() {
            return Err(invalid(
                "protocol router has no placement groups to reconfigure",
            ));
        }
        for pg in groups {
            match router.reconfigure(pg, view, primary, voters.iter().copied()) {
                VsrDecision::MembershipChanged { .. } => {}
                VsrDecision::Rejected { reason, .. } => return Err(invalid(reason)),
                _ => return Err(invalid("unexpected VSR decision during reconfiguration")),
            }
        }
        Ok(())
    }

    fn record_rejection(&mut self, node: NodeId, detail: String) -> RsfResult<()> {
        let event = AdmissionAuditEvent {
            epoch: self.cluster.epoch,
            node,
            action: AdmissionAuditAction::Rejected,
            detail,
        };
        self.append_event(AdminEvent::Audit(event.clone()))?;
        self.audit.push(event);
        Ok(())
    }

    fn promote_membership(&mut self, node: NodeId) -> RsfResult<MembershipDecision> {
        let voters = self
            .records
            .values()
            .filter(|record| record.state == AdmissionState::Promoted)
            .map(|record| record.node.id)
            .collect::<Vec<_>>();
        if self.membership.is_none() && !voters.is_empty() {
            self.membership = Some(
                MembershipCell::new(self.cluster.epoch, voters)
                    .map_err(|reason| invalid(format!("membership init failed: {reason}")))?,
            );
        }
        let Some(membership) = self.membership.as_mut() else {
            let record = self.record_mut(node)?;
            record.state = AdmissionState::Promoted;
            let supports_gateway = record.supports_gateway;
            assign_promoted_roles(&mut record.node, supports_gateway);
            return Ok(MembershipDecision::Promoted {
                member: Member {
                    node,
                    role: MemberRole::Voter,
                    synced: true,
                },
                epoch: self.cluster.epoch,
                quorum: QuorumIntersection {
                    previous_voters: 0,
                    next_voters: 1,
                    previous_quorum: 0,
                    next_quorum: 1,
                    shared_voters: 0,
                    minimum_intersection: 0,
                },
            });
        };
        match membership.add_learner(node) {
            MembershipDecision::AddedLearner(_) => {}
            MembershipDecision::Rejected { reason, .. }
                if reason == "node already exists in membership" => {}
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
            _ => {
                return Err(invalid(
                    "unexpected membership decision while adding learner",
                ));
            }
        }
        match membership.mark_synced(node) {
            MembershipDecision::MarkedSynced(_) => {}
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
            _ => {
                return Err(invalid(
                    "unexpected membership decision while syncing learner",
                ));
            }
        }
        Ok(membership.promote(node))
    }

    fn apply_voter_removed(&mut self, node: NodeId) -> RsfResult<MembershipView> {
        let decision = self
            .membership
            .as_mut()
            .ok_or_else(|| invalid("membership is not initialized"))?
            .remove_voter(node);
        let (epoch, quorum) = match decision {
            MembershipDecision::Removed { epoch, quorum, .. } => (epoch, quorum),
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
            _ => {
                return Err(invalid(
                    "unexpected membership decision during voter removal",
                ));
            }
        };
        self.retire_record(node)?;
        self.cluster.epoch = epoch;
        let view = self.current_membership()?;
        self.audit.push(AdmissionAuditEvent {
            epoch: self.cluster.epoch,
            node,
            action: AdmissionAuditAction::VoterRemoved,
            detail: format!(
                "removed voter with quorum overlap previous={} next={} shared={}",
                quorum.previous_voters, quorum.next_voters, quorum.shared_voters
            ),
        });
        Ok(view)
    }

    fn apply_voter_replaced(
        &mut self,
        removed: NodeId,
        replacement: NodeId,
    ) -> RsfResult<MembershipView> {
        let decision = self
            .membership
            .as_mut()
            .ok_or_else(|| invalid("membership is not initialized"))?
            .add_learner(replacement);
        match decision {
            MembershipDecision::AddedLearner(_) => {}
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
            _ => {
                return Err(invalid(
                    "unexpected membership decision while adding replacement",
                ));
            }
        }
        let decision = self
            .membership
            .as_mut()
            .ok_or_else(|| invalid("membership is not initialized"))?
            .mark_synced(replacement);
        match decision {
            MembershipDecision::MarkedSynced(_) => {}
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
            _ => {
                return Err(invalid(
                    "unexpected membership decision while syncing replacement",
                ));
            }
        }
        let decision = self
            .membership
            .as_mut()
            .ok_or_else(|| invalid("membership is not initialized"))?
            .promote(replacement);
        let add_quorum = match decision {
            MembershipDecision::Promoted { quorum, .. } => quorum,
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
            _ => {
                return Err(invalid(
                    "unexpected membership decision while promoting replacement",
                ));
            }
        };
        let decision = self
            .membership
            .as_mut()
            .ok_or_else(|| invalid("membership is not initialized"))?
            .remove_voter(removed);
        let (epoch, remove_quorum) = match decision {
            MembershipDecision::Removed { epoch, quorum, .. } => (epoch, quorum),
            MembershipDecision::Rejected { reason, .. } => return Err(invalid(reason)),
            _ => {
                return Err(invalid(
                    "unexpected membership decision while removing old voter",
                ));
            }
        };
        self.retire_record(removed)?;
        self.promote_replacement_record(replacement)?;
        self.cluster.epoch = epoch;
        let view = self.current_membership()?;
        self.audit.push(AdmissionAuditEvent {
            epoch: self.cluster.epoch,
            node: replacement,
            action: AdmissionAuditAction::VoterReplaced,
            detail: format!(
                "replaced voter {} with {} using add overlap previous={} next={} shared={} and remove overlap previous={} next={} shared={}",
                removed.0,
                replacement.0,
                add_quorum.previous_voters,
                add_quorum.next_voters,
                add_quorum.shared_voters,
                remove_quorum.previous_voters,
                remove_quorum.next_voters,
                remove_quorum.shared_voters
            ),
        });
        Ok(view)
    }

    fn retire_record(&mut self, node: NodeId) -> RsfResult<()> {
        let record = self.record_mut(node)?;
        record.state = AdmissionState::Retired;
        record.node.roles.clear();
        if let Some(node_in_map) = self.cluster.nodes.iter_mut().find(|entry| entry.id == node) {
            node_in_map.roles.clear();
        }
        Ok(())
    }

    fn promote_replacement_record(&mut self, node: NodeId) -> RsfResult<()> {
        let supports_gateway = self
            .records
            .get(&node)
            .ok_or_else(|| invalid(format!("replacement node {} is not admitted", node.0)))?
            .supports_gateway;
        let record = self.record_mut(node)?;
        if record.state != AdmissionState::Validated {
            return Err(invalid(format!(
                "replacement node {} must be a synced learner",
                node.0
            )));
        }
        record.state = AdmissionState::Promoted;
        assign_promoted_roles(&mut record.node, supports_gateway);
        let promoted = record.node.clone();
        if let Some(node_in_map) = self.cluster.nodes.iter_mut().find(|entry| entry.id == node) {
            *node_in_map = promoted;
        }
        Ok(())
    }

    fn ensure_membership_initialized(&mut self) -> RsfResult<()> {
        if self.membership.is_some() {
            return Ok(());
        }
        let voters = self
            .records
            .values()
            .filter(|record| record.state == AdmissionState::Promoted)
            .map(|record| record.node.id)
            .collect::<Vec<_>>();
        if !voters.is_empty() {
            self.membership = Some(
                MembershipCell::new(self.cluster.epoch, voters)
                    .map_err(|reason| invalid(format!("membership init failed: {reason}")))?,
            );
        }
        Ok(())
    }

    fn append_event(&mut self, event: AdminEvent) -> RsfResult<()> {
        let Some(store) = self.store.as_mut() else {
            return Ok(());
        };
        let payload = event.encode()?;
        let recovery = store.recover_current()?.recovery;
        let tail = recovery.pg_tail(ADMIN_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(tail.map_or(1, |tail| tail.index.0 + 1));
        store.append_wal(WalAppend {
            pg: ADMIN_PG,
            epoch: self.cluster.epoch,
            index,
            kind: WalRecordKind::Membership,
            previous_hash,
            payload: &payload,
        })?;
        Ok(())
    }

    fn apply_event(&mut self, event: AdminEvent) -> RsfResult<()> {
        match event {
            AdminEvent::TokenVerifier { verifier } => self.apply_token_verifier(verifier),
            AdminEvent::Admitted(record) => self.apply_admitted(record),
            AdminEvent::LearnerSynced { node } => self.apply_learner_synced(node).map(|_| ()),
            AdminEvent::LearnerPromoted { node } => self.apply_learner_promoted(node).map(|_| ()),
            AdminEvent::VoterRemoved { node } => self.apply_voter_removed(node).map(|_| ()),
            AdminEvent::VoterReplaced {
                removed,
                replacement,
            } => self.apply_voter_replaced(removed, replacement).map(|_| ()),
            AdminEvent::Audit(event) => {
                self.audit.push(event);
                Ok(())
            }
        }
    }

    fn apply_token_verifier(&mut self, verifier: InviteTokenVerifier) -> RsfResult<()> {
        if self.token_verifier_initialized() && !self.token_verifier.matches(&verifier) {
            return Err(corrupt(
                "admin log contains conflicting invite token verifiers",
            ));
        }
        self.token_verifier = verifier;
        Ok(())
    }

    fn token_verifier_initialized(&self) -> bool {
        self.token_verifier.hash != [0_u8; 32]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AdminEvent {
    TokenVerifier {
        verifier: InviteTokenVerifier,
    },
    Admitted(AdmissionRecord),
    LearnerSynced {
        node: NodeId,
    },
    LearnerPromoted {
        node: NodeId,
    },
    VoterRemoved {
        node: NodeId,
    },
    VoterReplaced {
        removed: NodeId,
        replacement: NodeId,
    },
    Audit(AdmissionAuditEvent),
}

impl AdminEvent {
    fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(ADMIN_EVENT_MAGIC);
        match self {
            Self::TokenVerifier { verifier } => {
                out.push(7);
                out.extend_from_slice(&verifier.hash);
            }
            Self::Admitted(record) => {
                out.push(1);
                write_u64(record.node.id.0, &mut out);
                write_string(record.node.region.as_str(), &mut out)?;
                write_string(record.node.failure_domain.as_str(), &mut out)?;
                write_u64(record.node.capacity_bytes, &mut out);
                out.push(u8::from(record.supports_gateway));
                write_string(&record.identity.public_key, &mut out)?;
                write_string(&record.identity.nonce, &mut out)?;
                write_string(&record.identity.transport.trust_domain, &mut out)?;
                write_string(&record.identity.transport.spiffe_id, &mut out)?;
                write_digest(record.identity.transport.certificate_fingerprint, &mut out);
            }
            Self::LearnerSynced { node } => {
                out.push(2);
                write_u64(node.0, &mut out);
            }
            Self::LearnerPromoted { node } => {
                out.push(3);
                write_u64(node.0, &mut out);
            }
            Self::Audit(event) => {
                out.push(4);
                write_u64(event.epoch.0, &mut out);
                write_u64(event.node.0, &mut out);
                out.push(event.action.to_u8());
                write_string(&event.detail, &mut out)?;
            }
            Self::VoterRemoved { node } => {
                out.push(5);
                write_u64(node.0, &mut out);
            }
            Self::VoterReplaced {
                removed,
                replacement,
            } => {
                out.push(6);
                write_u64(removed.0, &mut out);
                write_u64(replacement.0, &mut out);
            }
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if !Self::is_payload(bytes) {
            return Err(corrupt("admin event payload has invalid magic"));
        }
        let tag = *bytes
            .get(5)
            .ok_or_else(|| corrupt("admin event payload is missing tag"))?;
        let mut offset = 6;
        let event = match tag {
            1 => {
                let node = NodeId(read_u64(bytes, offset)?);
                offset += 8;
                let (region, next) = read_string(bytes, offset)?;
                offset = next;
                let (failure_domain, next) = read_string(bytes, offset)?;
                offset = next;
                let capacity_bytes = read_u64(bytes, offset)?;
                offset += 8;
                let supports_gateway = match *bytes
                    .get(offset)
                    .ok_or_else(|| corrupt("admin admission event missing gateway flag"))?
                {
                    0 => false,
                    1 => true,
                    _ => return Err(corrupt("admin admission event has invalid gateway flag")),
                };
                offset += 1;
                let (public_key, next) = read_string(bytes, offset)?;
                offset = next;
                let (nonce, next) = read_string(bytes, offset)?;
                offset = next;
                let (trust_domain, next) = read_string(bytes, offset)?;
                offset = next;
                let (spiffe_id, next) = read_string(bytes, offset)?;
                offset = next;
                let (certificate_fingerprint, next) = read_digest(bytes, offset)?;
                offset = next;
                let descriptor = NodeDescriptor {
                    id: node,
                    region: RegionId::new(region)?,
                    failure_domain: FailureDomainId::new(failure_domain)?,
                    capacity_bytes,
                    roles: vec![NodeRole::Learner],
                };
                let record = AdmissionRecord {
                    node: descriptor.clone(),
                    state: AdmissionState::Learner,
                    identity: NodeIdentity {
                        public_key,
                        nonce,
                        transport: NodeTransportIdentity {
                            trust_domain,
                            spiffe_id,
                            certificate_fingerprint,
                        },
                    },
                    supports_gateway,
                };
                validate_identity(&record.identity)?;
                Self::Admitted(record)
            }
            2 => {
                let node = NodeId(read_u64(bytes, offset)?);
                offset += 8;
                Self::LearnerSynced { node }
            }
            3 => {
                let node = NodeId(read_u64(bytes, offset)?);
                offset += 8;
                Self::LearnerPromoted { node }
            }
            4 => {
                let epoch = Epoch(read_u64(bytes, offset)?);
                offset += 8;
                let node = NodeId(read_u64(bytes, offset)?);
                offset += 8;
                let action = AdmissionAuditAction::from_u8(
                    *bytes
                        .get(offset)
                        .ok_or_else(|| corrupt("admin audit event missing action"))?,
                )?;
                offset += 1;
                let (detail, next) = read_string(bytes, offset)?;
                offset = next;
                Self::Audit(AdmissionAuditEvent {
                    epoch,
                    node,
                    action,
                    detail,
                })
            }
            5 => {
                let node = NodeId(read_u64(bytes, offset)?);
                offset += 8;
                Self::VoterRemoved { node }
            }
            6 => {
                let removed = NodeId(read_u64(bytes, offset)?);
                offset += 8;
                let replacement = NodeId(read_u64(bytes, offset)?);
                offset += 8;
                Self::VoterReplaced {
                    removed,
                    replacement,
                }
            }
            7 => {
                let hash = bytes
                    .get(offset..offset + 32)
                    .ok_or_else(|| corrupt("admin token verifier event missing hash"))?;
                offset += 32;
                Self::TokenVerifier {
                    verifier: InviteTokenVerifier {
                        hash: hash.try_into().expect("slice length"),
                    },
                }
            }
            _ => return Err(corrupt(format!("unknown admin event tag {tag}"))),
        };
        if offset != bytes.len() {
            return Err(corrupt("admin event payload has trailing bytes"));
        }
        Ok(event)
    }

    fn is_payload(bytes: &[u8]) -> bool {
        bytes.starts_with(ADMIN_EVENT_MAGIC)
    }
}

impl AdmissionAuditAction {
    fn to_u8(self) -> u8 {
        match self {
            Self::LearnerAdmitted => 1,
            Self::LearnerSynced => 2,
            Self::LearnerPromoted => 3,
            Self::PlacementPlanned => 4,
            Self::PlacementApplied => 5,
            Self::Rejected => 6,
            Self::VoterRemoved => 7,
            Self::VoterReplaced => 8,
            Self::PolicyApplied => 9,
        }
    }

    fn from_u8(value: u8) -> RsfResult<Self> {
        match value {
            1 => Ok(Self::LearnerAdmitted),
            2 => Ok(Self::LearnerSynced),
            3 => Ok(Self::LearnerPromoted),
            4 => Ok(Self::PlacementPlanned),
            5 => Ok(Self::PlacementApplied),
            6 => Ok(Self::Rejected),
            7 => Ok(Self::VoterRemoved),
            8 => Ok(Self::VoterReplaced),
            9 => Ok(Self::PolicyApplied),
            _ => Err(corrupt(format!("unknown admin audit action {value}"))),
        }
    }
}

fn assign_promoted_roles(node: &mut NodeDescriptor, supports_gateway: bool) {
    node.roles = vec![
        NodeRole::Storage,
        NodeRole::PlacementSecondary,
        NodeRole::MetadataReplica,
        NodeRole::RepairWorker,
    ];
    if supports_gateway {
        node.roles.push(NodeRole::QueryGateway);
        node.roles.push(NodeRole::ObjectGateway);
    }
}

fn select_replicas(
    storage_nodes: &[&NodeDescriptor],
    start: usize,
    replicas: usize,
) -> RsfResult<Vec<NodeId>> {
    let mut selected = Vec::new();
    let mut domains = BTreeSet::new();
    for offset in 0..storage_nodes.len() {
        let node = storage_nodes[(start + offset) % storage_nodes.len()];
        if domains.insert(node.failure_domain.clone()) {
            selected.push(node.id);
            if selected.len() == replicas {
                return Ok(selected);
            }
        }
    }
    Err(invalid("not enough distinct failure domains for placement"))
}

fn validate_identity(identity: &NodeIdentity) -> RsfResult<()> {
    if identity.public_key.len() < 16 {
        return Err(invalid("node identity public key is too short"));
    }
    if identity.nonce.len() < 8 {
        return Err(invalid("node identity nonce is too short"));
    }
    if !identity
        .public_key
        .bytes()
        .chain(identity.nonce.bytes())
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid("node identity contains unsupported characters"));
    }
    validate_transport_identity(identity)?;
    Ok(())
}

fn validate_transport_identity(identity: &NodeIdentity) -> RsfResult<()> {
    validate_trust_domain(&identity.transport.trust_domain)?;
    validate_tls_fingerprint(&identity.transport.certificate_fingerprint)?;
    let expected = spiffe_id(&identity.transport.trust_domain, &identity.public_key);
    if identity.transport.spiffe_id != expected {
        return Err(invalid(format!(
            "node mTLS SPIFFE identity must be {expected}"
        )));
    }
    Ok(())
}

fn validate_trust_domain(trust_domain: &str) -> RsfResult<()> {
    if trust_domain.is_empty() {
        return Err(invalid("node mTLS trust domain must not be empty"));
    }
    if trust_domain.len() > 128 {
        return Err(invalid("node mTLS trust domain must be at most 128 bytes"));
    }
    if trust_domain.starts_with('.') || trust_domain.ends_with('.') {
        return Err(invalid(
            "node mTLS trust domain must not start or end with '.'",
        ));
    }
    if !trust_domain
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
    {
        return Err(invalid(
            "node mTLS trust domain may only contain ASCII letters, digits, '.', or '-'",
        ));
    }
    Ok(())
}

fn validate_tls_fingerprint(fingerprint: &Digest) -> RsfResult<()> {
    if fingerprint.algorithm() != DigestAlgorithm::Sha256 {
        return Err(invalid("node TLS certificate fingerprint must use sha256"));
    }
    fingerprint.validate()
}

fn spiffe_id(trust_domain: &str, public_key: &str) -> String {
    format!("spiffe://{trust_domain}/rsf/node/{public_key}")
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: usize) -> RsfResult<u64> {
    let raw = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| corrupt("admin event ended while reading u64"))?;
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u64::try_from(value.len())
        .map_err(|_| invalid("admin event string length does not fit in record"))?;
    write_u64(len, out);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: usize) -> RsfResult<(String, usize)> {
    let len = usize::try_from(read_u64(bytes, offset)?)
        .map_err(|_| corrupt("admin event string length does not fit in memory"))?;
    let start = offset + 8;
    let end = start + len;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| corrupt("admin event ended while reading string"))?;
    let value = String::from_utf8(raw.to_vec())
        .map_err(|_| corrupt("admin event string is not valid utf-8"))?;
    Ok((value, end))
}

fn write_digest(value: Digest, out: &mut Vec<u8>) {
    out.push(match value.algorithm() {
        DigestAlgorithm::Blake3 => 1,
        DigestAlgorithm::Sha256 => 2,
    });
    out.extend_from_slice(value.as_bytes());
}

fn read_digest(bytes: &[u8], offset: usize) -> RsfResult<(Digest, usize)> {
    let algorithm = match *bytes
        .get(offset)
        .ok_or_else(|| corrupt("admin event ended while reading digest algorithm"))?
    {
        1 => DigestAlgorithm::Blake3,
        2 => DigestAlgorithm::Sha256,
        _ => return Err(corrupt("admin event has invalid digest algorithm")),
    };
    let start = offset + 1;
    let end = start + 32;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| corrupt("admin event ended while reading digest bytes"))?;
    let digest = Digest::new(algorithm, raw.try_into().expect("slice length"))?;
    Ok((digest, end))
}

fn check(name: &str, passed: bool, detail: String) -> PlacementPreflightCheck {
    PlacementPreflightCheck {
        name: name.to_string(),
        passed,
        detail,
    }
}

fn membership_view(epoch: Epoch, voters: &[NodeId]) -> MembershipView {
    MembershipView {
        epoch,
        members: voters
            .iter()
            .map(|node| Member {
                node: *node,
                role: MemberRole::Voter,
                synced: true,
            })
            .collect(),
    }
}

fn token_verifier(token: &InviteToken) -> InviteTokenVerifier {
    let mut payload = b"rsf-admin-invite-token-v1\0".to_vec();
    payload.extend_from_slice(token.0.as_bytes());
    InviteTokenVerifier {
        hash: *Digest::blake3_bytes(&payload).as_bytes(),
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

#[cfg(test)]
mod tests {
    pub(crate) mod recovery_observation;
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn request(zone: &str) -> JoinRequest {
        request_with_identity(zone, zone)
    }

    fn request_with_identity(zone: &str, identity_suffix: &str) -> JoinRequest {
        JoinRequest {
            token: InviteToken("invite".to_string()),
            identity: test_identity(identity_suffix, identity_suffix),
            region: RegionId::new("iad").expect("region"),
            zone: FailureDomainId::new(zone).expect("zone"),
            capacity_bytes: 1 << 30,
            supports_storage: true,
            supports_gateway: true,
        }
    }

    fn test_identity(public_key_suffix: &str, fingerprint_seed: &str) -> NodeIdentity {
        let public_key = format!("node-public-key-{public_key_suffix}");
        NodeIdentity::new(
            public_key,
            format!("nonce-{public_key_suffix}-000"),
            "cluster.example",
            fingerprint(fingerprint_seed),
        )
        .expect("identity")
    }

    fn fingerprint(seed: &str) -> Digest {
        let mut bytes = [0_u8; 32];
        for (index, byte) in seed.bytes().enumerate() {
            bytes[index % 32] = bytes[index % 32].wrapping_add(byte).wrapping_add(1);
        }
        Digest::sha256(bytes).expect("sha256 fingerprint")
    }

    #[test]
    fn valid_node_joins_as_learner_then_syncs_and_promotes() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));

        let record = admin.admit(request("a")).expect("admit");

        assert_eq!(record.state, AdmissionState::Learner);
        assert!(record.node.roles.contains(&NodeRole::Learner));
        let synced = admin
            .mark_learner_synced(record.node.id)
            .expect("mark learner synced");
        assert_eq!(synced.state, AdmissionState::Validated);
        let promoted = admin
            .promote_synced_learner(record.node.id)
            .expect("promote synced learner");

        assert_eq!(promoted.state, AdmissionState::Promoted);
        assert!(promoted.node.roles.contains(&NodeRole::Storage));
        assert!(promoted.node.roles.contains(&NodeRole::QueryGateway));
        assert_eq!(admin.cluster_map().epoch, Epoch(2));
    }

    #[test]
    fn learner_cannot_promote_before_sync() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        let record = admin.admit(request("a")).expect("admit");

        let err = admin
            .promote_synced_learner(record.node.id)
            .expect_err("unsynced learner rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn first_three_promotions_create_membership_voters() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        for zone in ["a", "b", "c"] {
            let record = admin.admit(request(zone)).expect("admit");
            admin
                .mark_learner_synced(record.node.id)
                .expect("sync learner");
            admin
                .promote_synced_learner(record.node.id)
                .expect("promote learner");
        }

        let view = admin.membership_view().expect("membership initialized");

        assert_eq!(view.voters(), 3);
        assert_eq!(view.learners(), 0);
    }

    #[test]
    fn fourth_node_promotes_through_membership_quorum_evidence() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        for zone in ["a", "b", "c", "d"] {
            let record = admin.admit(request(zone)).expect("admit");
            admin
                .mark_learner_synced(record.node.id)
                .expect("sync learner");
            admin
                .promote_synced_learner(record.node.id)
                .expect("promote learner");
        }

        let view = admin.membership_view().expect("membership initialized");
        let audit = admin.audit().last().expect("promotion audit");

        assert_eq!(view.voters(), 4);
        assert_eq!(audit.action, AdmissionAuditAction::LearnerPromoted);
        assert!(audit.detail.contains("shared=3"));
    }

    #[test]
    fn valid_nodes_promote_to_usable_capacity() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));

        let record = promote(&mut admin, request("a"));

        assert_eq!(record.state, AdmissionState::Promoted);
        assert!(record.node.roles.contains(&NodeRole::Storage));
        assert!(record.node.roles.contains(&NodeRole::QueryGateway));
        assert_eq!(admin.cluster_map().epoch, Epoch(2));
    }

    #[test]
    fn bad_invite_token_is_rejected() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        let mut bad = request("a");
        bad.token = InviteToken("wrong".to_string());

        let err = admin.admit(bad).expect_err("bad token should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(admin.cluster_map().nodes.is_empty());
    }

    #[test]
    fn durable_admin_rejects_wrong_recovered_invite_token() {
        let path = test_path("durable_admin_rejects_wrong_recovered_invite_token");
        {
            let mut admin =
                AdminPlane::open_durable(&path, InviteToken("invite".to_string())).unwrap();
            promote(&mut admin, request("a"));
        }

        let err = AdminPlane::open_durable(&path, InviteToken("wrong".to_string()))
            .expect_err("wrong recovered token rejected");
        let recovered = AdminPlane::open_durable(&path, InviteToken("invite".to_string())).unwrap();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(recovered.cluster_map().nodes.len(), 1);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn duplicate_node_identity_is_rejected() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        admin.admit(request("a")).expect("first identity");

        let err = admin
            .admit(request_with_identity("b", "a"))
            .expect_err("duplicate public key rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("public key already exists"));
    }

    #[test]
    fn duplicate_tls_certificate_fingerprint_is_rejected() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        admin.admit(request("a")).expect("first identity");
        let mut duplicate = request_with_identity("b", "b");
        duplicate.identity.transport.certificate_fingerprint = fingerprint("a");

        let err = admin
            .admit(duplicate)
            .expect_err("duplicate TLS certificate fingerprint rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("TLS certificate fingerprint"));
    }

    #[test]
    fn mtls_spiffe_identity_is_bound_to_trust_domain_and_node_key() {
        let mut request = request("a");
        request.identity.transport.spiffe_id =
            "spiffe://cluster.example/rsf/node/someone-else".to_string();
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));

        let err = admin
            .admit(request)
            .expect_err("forged SPIFFE identity rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("SPIFFE"));
    }

    #[test]
    fn promoted_nodes_map_to_replicated_placement_groups() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        for zone in ["a", "b", "c"] {
            promote(&mut admin, request(zone));
        }

        let groups = admin
            .map_replicated_groups(3, 3)
            .expect("map placement groups");

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].acting_set.replicas.len(), 3);
        assert_eq!(groups[1].acting_set.primary, NodeId(1));
        assert_eq!(groups[2].acting_set.primary, NodeId(2));
    }

    #[test]
    fn placement_preflight_rejects_same_failure_domain() {
        let mut admin = AdminPlane::new(InviteToken("invite".to_string()));
        for id in 0..3 {
            promote(
                &mut admin,
                request_with_identity("a", &format!("same-zone-{id}")),
            );
        }

        let err = admin
            .map_replicated_groups(1, 3)
            .expect_err("same failure domain rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("failure domains"));
    }

    #[test]
    fn durable_admin_recovers_admission_membership_and_audit() {
        let path = test_path("durable_admin_recovers_admission_membership_and_audit");
        {
            let mut admin =
                AdminPlane::open_durable(&path, InviteToken("invite".to_string())).unwrap();
            for zone in ["a", "b", "c"] {
                promote(&mut admin, request(zone));
            }
            admin.map_replicated_groups(2, 3).expect("placement plan");
        }

        let recovered = AdminPlane::open_durable(&path, InviteToken("invite".to_string())).unwrap();

        assert_eq!(recovered.cluster_map().nodes.len(), 3);
        assert_eq!(recovered.cluster_map().epoch, Epoch(6));
        assert_eq!(recovered.membership_view().expect("membership").voters(), 3);
        assert!(
            recovered
                .records()
                .all(|record| record.state == AdmissionState::Promoted)
        );
        assert!(
            recovered
                .audit()
                .iter()
                .any(|event| event.action == AdmissionAuditAction::PlacementPlanned)
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn placement_plan_applies_to_durable_protocol_map_and_recovers() {
        let admin_path = test_path("placement_plan_applies_admin");
        let protocol_path = test_path("placement_plan_applies_protocol");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut admin =
                AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
            for zone in ["a", "b", "c"] {
                promote(&mut admin, request(zone));
            }
            let plan = admin.plan_replicated_groups(3, 3).expect("plan placement");
            let mut router = VsrBackedFabricRouter::open(
                &protocol_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                replicas,
            )
            .expect("open protocol router");

            let map = admin
                .apply_placement_plan(&mut router, &plan)
                .expect("apply placement map");

            assert_eq!(map.version, 2);
            assert_eq!(
                map.groups,
                vec![
                    PlacementGroupId(1),
                    PlacementGroupId(2),
                    PlacementGroupId(3)
                ]
            );
        }

        let recovered_router = VsrBackedFabricRouter::open(
            &protocol_path,
            [PlacementGroupId(99)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover protocol router");
        let recovered_admin =
            AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();

        assert_eq!(recovered_router.placement_map().version, 2);
        assert_eq!(
            recovered_router.placement_map().groups,
            vec![
                PlacementGroupId(1),
                PlacementGroupId(2),
                PlacementGroupId(3)
            ]
        );
        assert!(
            recovered_admin
                .audit()
                .iter()
                .any(|event| event.action == AdmissionAuditAction::PlacementApplied)
        );

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn placement_apply_rejection_is_audited_durably() {
        let admin_path = test_path("placement_apply_rejection_admin");
        let protocol_path = test_path("placement_apply_rejection_protocol");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut admin =
                AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
            for zone in ["a", "b", "c"] {
                promote(&mut admin, request(zone));
            }
            let plan = admin.plan_replicated_groups(1, 3).expect("plan placement");
            let mut router = VsrBackedFabricRouter::open(
                &protocol_path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                replicas,
            )
            .expect("open protocol router");
            router
                .update_policy(true, 2)
                .expect("require at least two pgs");

            let err = admin
                .apply_placement_plan(&mut router, &plan)
                .expect_err("policy rejects narrow map");

            assert_eq!(err.code, ErrorCode::InvalidInput);
        }

        let recovered_admin =
            AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
        assert!(recovered_admin.audit().iter().any(|event| {
            event.action == AdmissionAuditAction::Rejected
                && event.detail.contains("placement map apply failed")
        }));

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn protocol_policy_apply_records_accepted_and_rejected_changes_durably() {
        let admin_path = test_path("protocol_policy_apply_admin");
        let protocol_path = test_path("protocol_policy_apply_protocol");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut admin =
                AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
            let mut router = VsrBackedFabricRouter::open(
                &protocol_path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                replicas,
            )
            .expect("open protocol router");

            let applied = admin
                .apply_protocol_policy(&mut router, false, 2)
                .expect("apply safe protocol policy");
            let err = admin
                .apply_protocol_policy(&mut router, true, 3)
                .expect_err("reject policy wider than current map");

            assert_eq!(applied.version, 2);
            assert!(!applied.allow_availability_first_immutable);
            assert_eq!(applied.min_placement_groups, 2);
            assert_eq!(err.code, ErrorCode::InvalidInput);
        }

        let recovered_admin =
            AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
        let recovered_router = VsrBackedFabricRouter::open(
            &protocol_path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover protocol router");

        assert_eq!(recovered_router.policy().version, 2);
        assert_eq!(recovered_router.policy().min_placement_groups, 2);
        assert!(recovered_admin.audit().iter().any(|event| {
            event.action == AdmissionAuditAction::PolicyApplied
                && event.detail.contains("min_placement_groups=2")
        }));
        assert!(recovered_admin.audit().iter().any(|event| {
            event.action == AdmissionAuditAction::Rejected
                && event.detail.contains("policy apply rejected")
        }));

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn unsafe_voter_removal_is_rejected_and_audited_durably() {
        let admin_path = test_path("unsafe_voter_removal_admin");
        let protocol_path = test_path("unsafe_voter_removal_protocol");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut admin =
                AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
            for zone in ["a", "b", "c"] {
                promote(&mut admin, request(zone));
            }
            let mut router = VsrBackedFabricRouter::open(
                &protocol_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                replicas,
            )
            .expect("open protocol router");

            let err = admin
                .remove_voter(&mut router, NodeId(0))
                .expect_err("three-voter removal is unsafe");

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert_eq!(admin.membership_view().expect("membership").voters(), 3);
        }

        let recovered_admin =
            AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
        assert!(recovered_admin.audit().iter().any(|event| {
            event.action == AdmissionAuditAction::Rejected
                && event.detail.contains("voter removal rejected")
        }));
        assert_eq!(
            recovered_admin
                .membership_view()
                .expect("membership")
                .voters(),
            3
        );

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn safe_voter_removal_reconfigures_protocol_membership() {
        let admin_path = test_path("safe_voter_removal_admin");
        let protocol_path = test_path("safe_voter_removal_protocol");
        let replicas = [NodeId(0), NodeId(1), NodeId(2), NodeId(3)];
        {
            let mut admin =
                AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
            for zone in ["a", "b", "c", "d"] {
                promote(&mut admin, request(zone));
            }
            let mut router = VsrBackedFabricRouter::open_with_transport(
                &protocol_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                replicas,
                rsf_vsr::LocalVsrTransport::all_reachable(replicas),
            )
            .expect("open protocol router");

            let view = admin
                .remove_voter(&mut router, NodeId(3))
                .expect("remove fourth voter");
            let commit = router
                .commit(
                    PlacementGroupId(1),
                    rsf_protocol::FabricOperation::KvMutation {
                        key: b"tenant/after-removal".to_vec(),
                    },
                )
                .expect("reconfigured membership certifies commit");

            assert_eq!(view.voter_nodes(), vec![NodeId(0), NodeId(1), NodeId(2)]);
            assert_eq!(commit.index, LogIndex(1));
        }

        let recovered_admin =
            AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
        assert_eq!(
            recovered_admin
                .membership_view()
                .expect("membership")
                .voter_nodes(),
            vec![NodeId(0), NodeId(1), NodeId(2)]
        );
        assert_eq!(
            recovered_admin
                .records()
                .find(|record| record.node.id == NodeId(3))
                .unwrap()
                .state,
            AdmissionState::Retired
        );

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn voter_replacement_reconfigures_protocol_and_recovers() {
        let admin_path = test_path("voter_replacement_admin");
        let protocol_path = test_path("voter_replacement_protocol");
        let initial_replicas = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut admin =
                AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
            for zone in ["a", "b", "c"] {
                promote(&mut admin, request(zone));
            }
            let replacement = admin.admit(request("d")).expect("admit replacement");
            admin
                .mark_learner_synced(replacement.node.id)
                .expect("sync replacement");
            let mut router = VsrBackedFabricRouter::open_with_transport(
                &protocol_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                initial_replicas,
                rsf_vsr::LocalVsrTransport::all_reachable([
                    NodeId(0),
                    NodeId(1),
                    NodeId(2),
                    NodeId(3),
                ]),
            )
            .expect("open protocol router");

            let view = admin
                .replace_voter(&mut router, NodeId(0), replacement.node.id)
                .expect("replace voter");
            let commit = router
                .commit(
                    PlacementGroupId(1),
                    rsf_protocol::FabricOperation::KvMutation {
                        key: b"tenant/after-replacement".to_vec(),
                    },
                )
                .expect("replacement membership certifies commit");

            assert_eq!(view.voter_nodes(), vec![NodeId(1), NodeId(2), NodeId(3)]);
            assert_eq!(commit.index, LogIndex(1));
        }

        let recovered_admin =
            AdminPlane::open_durable(&admin_path, InviteToken("invite".to_string())).unwrap();
        let mut recovered_router = VsrBackedFabricRouter::open_with_transport(
            &protocol_path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(1),
            initial_replicas,
            rsf_vsr::LocalVsrTransport::all_reachable([NodeId(1), NodeId(2), NodeId(3)]),
        )
        .expect("recover protocol router");
        let recovered_commit = recovered_router
            .commit(
                PlacementGroupId(1),
                rsf_protocol::FabricOperation::KvMutation {
                    key: b"tenant/recovered-replacement".to_vec(),
                },
            )
            .expect("recovered replacement certifies commit");

        assert_eq!(
            recovered_admin
                .membership_view()
                .expect("membership")
                .voter_nodes(),
            vec![NodeId(1), NodeId(2), NodeId(3)]
        );
        assert_eq!(
            recovered_admin
                .records()
                .find(|record| record.node.id == NodeId(0))
                .unwrap()
                .state,
            AdmissionState::Retired
        );
        assert_eq!(
            recovered_admin
                .records()
                .find(|record| record.node.id == NodeId(3))
                .unwrap()
                .state,
            AdmissionState::Promoted
        );
        assert!(recovered_admin.audit().iter().any(|event| {
            event.action == AdmissionAuditAction::VoterReplaced
                && event.detail.contains("replaced voter 0 with 3")
        }));
        assert_eq!(recovered_commit.index, LogIndex(2));

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn durable_admin_rejects_duplicate_identity_during_replay() {
        let path = test_path("durable_admin_rejects_duplicate_identity_during_replay");
        let mut store = SegmentStore::open(&path).expect("open admin store");
        let first = AdmissionRecord {
            node: NodeDescriptor {
                id: NodeId(0),
                region: RegionId::new("iad").unwrap(),
                failure_domain: FailureDomainId::new("a").unwrap(),
                capacity_bytes: 1 << 30,
                roles: vec![NodeRole::Learner],
            },
            state: AdmissionState::Learner,
            identity: test_identity("duplicate", "duplicate-a"),
            supports_gateway: true,
        };
        let second = AdmissionRecord {
            node: NodeDescriptor {
                id: NodeId(1),
                region: RegionId::new("iad").unwrap(),
                failure_domain: FailureDomainId::new("b").unwrap(),
                capacity_bytes: 1 << 30,
                roles: vec![NodeRole::Learner],
            },
            state: AdmissionState::Learner,
            identity: test_identity("duplicate", "duplicate-b"),
            supports_gateway: true,
        };
        append_raw_event(&mut store, Epoch(0), AdminEvent::Admitted(first));
        append_raw_event(&mut store, Epoch(1), AdminEvent::Admitted(second));
        drop(store);

        let err = AdminPlane::open_durable(&path, InviteToken("invite".to_string()))
            .expect_err("duplicate identity replay rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        let _ = std::fs::remove_dir_all(path);
    }

    fn promote(admin: &mut AdminPlane, request: JoinRequest) -> AdmissionRecord {
        let record = admin.admit(request).expect("admit");
        admin
            .mark_learner_synced(record.node.id)
            .expect("sync learner");
        admin
            .promote_synced_learner(record.node.id)
            .expect("promote learner")
    }

    fn append_raw_event(store: &mut SegmentStore, epoch: Epoch, event: AdminEvent) {
        let payload = event.encode().expect("encode admin event");
        let recovery = store
            .recover_current()
            .expect("recover admin store")
            .recovery;
        let tail = recovery.pg_tail(ADMIN_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(tail.map_or(1, |tail| tail.index.0 + 1));
        store
            .append_wal(WalAppend {
                pg: ADMIN_PG,
                epoch,
                index,
                kind: WalRecordKind::Membership,
                previous_hash,
                payload: &payload,
            })
            .expect("append raw admin event");
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-admin-{name}-{}-{nonce}", std::process::id()))
    }
}
