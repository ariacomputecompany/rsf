use crate::*;
use rsf_core::{ClusterId, NodeId, PlacementGroupId};
use rsf_vsr::{VsrMembershipLineage, VsrMembershipProposal, VsrMembershipPublishedQuorum};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ControlRevision(pub u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CatalogRoot(pub [u8; 32]);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct IntentId(pub [u8; 32]);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ControlRequestId(pub [u8; 32]);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoutingGeneration(pub u64);

/// An externally provisioned trust input. Recovering a directory does not create
/// this anchor. It must be required independently at every managed entry point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlBootstrap {
    pub cluster: ClusterId,
    pub control_pg: PlacementGroupId,
    pub control_bootstrap: [u8; 32],
    pub genesis_catalog: CatalogRoot,
}
impl ControlBootstrap {
    pub(crate) fn validate(&self) -> RsfResult<()> {
        ClusterId::new(self.cluster.as_str())?;
        nonzero(&self.control_bootstrap)?;
        nonzero(&self.genesis_catalog.0)
    }
}

/// Ordinary views are deliberately absent. A -> B -> A has distinct plan IDs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MembershipLineage {
    pub bootstrap: [u8; 32],
    pub plan: Option<[u8; 32]>,
}
impl MembershipLineage {
    pub(crate) fn validate(&self) -> RsfResult<()> {
        nonzero(&self.bootstrap)?;
        if let Some(plan) = self.plan {
            nonzero(&plan)?;
        }
        Ok(())
    }
    pub(crate) fn matches(&self, actual: &VsrMembershipLineage) -> bool {
        self.bootstrap == actual.bootstrap && self.plan == actual.plan
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionRole {
    Learner,
    Voter,
    Removed,
}

/// Node IDs and incarnations cannot be reused by membership commands. The first
/// regime gives every voter both storage and control-replica responsibilities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogNode {
    pub id: NodeId,
    pub incarnation: u64,
    pub public_key: String,
    pub nonce: String,
    pub trust_domain: String,
    pub spiffe_id: String,
    pub certificate: [u8; 32],
    pub role: AdmissionRole,
}
impl CatalogNode {
    fn identity_eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.incarnation == other.incarnation
            && self.public_key == other.public_key
            && self.nonce == other.nonce
            && self.trust_domain == other.trust_domain
            && self.spiffe_id == other.spiffe_id
            && self.certificate == other.certificate
    }
    pub(crate) fn validate(&self) -> RsfResult<()> {
        if self.incarnation == 0 {
            return Err(invalid("node incarnation must be nonzero"));
        }
        for text in [
            &self.public_key,
            &self.nonce,
            &self.trust_domain,
            &self.spiffe_id,
        ] {
            if text.is_empty()
                || text.len() > MAX_IDENTITY_BYTES
                || text.chars().any(char::is_control)
            {
                return Err(invalid("invalid bounded node identity text"));
            }
        }
        if self.public_key.len() < 16
            || self.nonce.len() < 8
            || !self
                .public_key
                .bytes()
                .chain(self.nonce.bytes())
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(invalid(
                "node key/nonce differs from the admission identity contract",
            ));
        }
        if self.trust_domain.len() > 128
            || self.trust_domain.starts_with('.')
            || self.trust_domain.ends_with('.')
            || !self
                .trust_domain
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
            || self.spiffe_id
                != format!(
                    "spiffe://{}/rsf/node/{}",
                    self.trust_domain, self.public_key
                )
        {
            return Err(invalid("node SPIFFE identity differs from trust domain"));
        }
        nonzero(&self.certificate)
    }
    pub(crate) fn preserves(&self, old: &Self) -> bool {
        self.identity_eq(old)
            && (old.role != AdmissionRole::Removed || self.role == AdmissionRole::Removed)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogGroup {
    pub pg: PlacementGroupId,
    pub lineage: MembershipLineage,
    pub voters: Vec<NodeId>,
}
impl CatalogGroup {
    pub(crate) fn matches(&self, actual: &VsrMembershipLineage) -> bool {
        self.pg == actual.pg && self.lineage.matches(actual) && self.voters == actual.voters
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogRouting {
    pub generation: RoutingGeneration,
    pub placement: [u8; 32],
    pub policy: [u8; 32],
}
impl CatalogRouting {
    pub(crate) fn validate(&self) -> RsfResult<()> {
        if self.generation.0 == 0 {
            return Err(invalid("routing generation must be nonzero"));
        }
        nonzero(&self.placement)?;
        nonzero(&self.policy)
    }
}

/// DTOs can be constructed for planning. They are not an admission capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlCatalog {
    pub cluster: ClusterId,
    pub control_pg: PlacementGroupId,
    pub routing: CatalogRouting,
    pub nodes: Vec<CatalogNode>,
    /// Sorted complete inventory, including the dedicated control PG.
    pub groups: Vec<CatalogGroup>,
}
impl ControlCatalog {
    pub fn root(&self) -> RsfResult<CatalogRoot> {
        self.validate()?;
        Ok(CatalogRoot(codec::hash(
            b"RSF control catalog v1",
            &codec::catalog_bytes(self)?,
        )))
    }
    pub fn group(&self, pg: PlacementGroupId) -> Option<&CatalogGroup> {
        self.groups
            .binary_search_by_key(&pg, |group| group.pg)
            .ok()
            .map(|i| &self.groups[i])
    }
    pub(crate) fn validate(&self) -> RsfResult<()> {
        ClusterId::new(self.cluster.as_str())?;
        self.routing.validate()?;
        validate_nodes(&self.nodes)?;
        if self.groups.len() < 2
            || self.groups.len() > MAX_GROUPS
            || self.groups.windows(2).any(|pair| pair[0].pg >= pair[1].pg)
            || self.group(self.control_pg).is_none()
        {
            return Err(invalid(
                "catalog requires sorted complete control and data groups",
            ));
        }
        let voters = voter_nodes(&self.nodes);
        if voters.is_empty() {
            return Err(invalid("catalog requires voting nodes"));
        }
        for group in &self.groups {
            group.lineage.validate()?;
            if group.voters != voters {
                return Err(invalid(
                    "first admission regime requires one global voter set",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupIntent {
    pub pg: PlacementGroupId,
    pub previous: MembershipLineage,
    pub proposal: VsrMembershipProposal,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipIntent {
    pub request: ControlRequestId,
    pub parent: CatalogRoot,
    pub revision: ControlRevision,
    pub routing: CatalogRouting,
    pub nodes: Vec<CatalogNode>,
    pub groups: Vec<GroupIntent>,
}
impl MembershipIntent {
    pub fn id(&self) -> RsfResult<IntentId> {
        Ok(IntentId(codec::hash(
            b"RSF control intent v1",
            &ControlCommand::Begin(self.clone()).encode()?,
        )))
    }
    /// Worst-case final command with every proposed voter reporting. Exact encoded
    /// size is known before the first fence; input and plan IDs have fixed width.
    pub fn preflight(&self) -> RsfResult<FinalizationBudget> {
        ControlCommand::Begin(self.clone()).encode()?;
        let mut proofs = Vec::with_capacity(self.groups.len());
        for group in &self.groups {
            let reports = group
                .proposal
                .new_voters
                .iter()
                .map(|replica| rsf_vsr::VsrMembershipPublishedReport {
                    proposal: group.proposal.clone(),
                    replica: *replica,
                    input: [1; 32],
                    plan: [1; 32],
                })
                .collect();
            proofs.push(GroupPublication {
                pg: group.pg,
                quorum: VsrMembershipPublishedQuorum::from_reports(reports)?,
            });
        }
        let command = ControlCommand::Finalize(MembershipFinalization {
            intent: self.id()?,
            parent: self.parent,
            proofs,
        });
        Ok(FinalizationBudget {
            maximum_encoded_bytes: command.encode()?.len(),
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalizationBudget {
    pub maximum_encoded_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupPublication {
    pub pg: PlacementGroupId,
    pub quorum: VsrMembershipPublishedQuorum,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipFinalization {
    pub intent: IntentId,
    pub parent: CatalogRoot,
    pub proofs: Vec<GroupPublication>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlCommand {
    Genesis(ControlCatalog),
    Begin(MembershipIntent),
    Finalize(MembershipFinalization),
}

pub(crate) fn validate_nodes(nodes: &[CatalogNode]) -> RsfResult<()> {
    if nodes.is_empty()
        || nodes.len() > MAX_NODES
        || nodes.windows(2).any(|pair| pair[0].id >= pair[1].id)
    {
        return Err(invalid("node list must be sorted, unique and bounded"));
    }
    let mut certificates = std::collections::BTreeSet::new();
    let mut identities = std::collections::BTreeSet::new();
    for node in nodes {
        node.validate()?;
        if !certificates.insert(node.certificate) || !identities.insert(&node.spiffe_id) {
            return Err(invalid("node transport identities must be unique"));
        }
    }
    Ok(())
}
pub(crate) fn voter_nodes(nodes: &[CatalogNode]) -> Vec<NodeId> {
    nodes
        .iter()
        .filter(|node| node.role == AdmissionRole::Voter)
        .map(|node| node.id)
        .collect()
}
