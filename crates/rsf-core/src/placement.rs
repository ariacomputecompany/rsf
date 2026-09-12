use crate::{Epoch, NodeId, PlacementGroupId, RsfError, RsfResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActingSet {
    pub epoch: Epoch,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
}

impl ActingSet {
    pub fn validate(&self) -> RsfResult<()> {
        if !self.replicas.contains(&self.primary) {
            return Err(RsfError::new(
                crate::ErrorCode::InvalidInput,
                "primary must be a member of the replica set",
            ));
        }

        if self.replicas.is_empty() {
            return Err(RsfError::new(
                crate::ErrorCode::InvalidInput,
                "replica set must not be empty",
            ));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementGroup {
    pub id: PlacementGroupId,
    pub acting_set: ActingSet,
    pub policy: PlacementPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlacementPolicy {
    Replicated { replicas: u8 },
    ErasureCoded { data: u8, parity: u8 },
}
