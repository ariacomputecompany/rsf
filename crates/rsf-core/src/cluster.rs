use crate::{Epoch, FailureDomainId, NodeId, RegionId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub region: RegionId,
    pub failure_domain: FailureDomainId,
    pub capacity_bytes: u64,
    pub roles: Vec<NodeRole>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeRole {
    Storage,
    PlacementPrimary,
    PlacementSecondary,
    MetadataReplica,
    Learner,
    Witness,
    QueryGateway,
    ObjectGateway,
    RepairWorker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterMap {
    pub epoch: Epoch,
    pub nodes: Vec<NodeDescriptor>,
}

impl ClusterMap {
    pub fn empty() -> Self {
        Self {
            epoch: Epoch(0),
            nodes: Vec::new(),
        }
    }
}
