//! One hosted owner per learner PG. Control has no raw or mutable escape.
use super::*;
use managed_control::learner::{ControlLearner, unsupported};
use rsf_core::RsfResult;
use rsf_vsr::{
    LearnerHistoryStore, MembershipStagingStore, VsrLearnerStateToken, VsrMembershipApproval,
    VsrMembershipStagedPlan, VsrMembershipStagedQuorum,
};
use std::path::Path;

#[derive(Debug)]
pub(super) enum LearnerOwner {
    Application(Box<LearnerHistoryStore>),
    Control(Box<ControlLearner>),
    InitialControl(Box<managed_control::initial::InitialControlLearner>),
}
impl LearnerOwner {
    pub(super) fn placement_group(&self) -> PlacementGroupId {
        match self {
            Self::Application(owner) => owner.placement_group(),
            Self::Control(owner) => owner.placement_group(),
            Self::InitialControl(owner) => owner.placement_group(),
        }
    }
    pub(super) fn local(&self) -> NodeId {
        match self {
            Self::Application(owner) => owner.local(),
            Self::Control(owner) => owner.local(),
            Self::InitialControl(owner) => owner.local(),
        }
    }
    pub(super) fn configured_source(&self) -> Option<NodeId> {
        match self {
            Self::Application(owner) => Some(owner.source()),
            Self::Control(owner) => Some(owner.source()),
            Self::InitialControl(_) => None,
        }
    }
    pub(super) fn published_history_id(&self) -> RsfResult<Option<[u8; 32]>> {
        match self {
            Self::Application(owner) => owner.published_history_id(),
            Self::Control(owner) => owner.published_history_id(),
            Self::InitialControl(owner) => owner.unpublished(),
        }
    }
    pub(super) fn published_state_token(&self) -> RsfResult<VsrLearnerStateToken> {
        match self {
            Self::Application(owner) => owner.published_state_token(),
            Self::Control(owner) => owner.published_state_token(),
            Self::InitialControl(owner) => owner.unsupported(),
        }
    }
    pub(super) fn committed_entries(&self) -> RsfResult<Vec<VsrCommittedEntry>> {
        match self {
            Self::Application(owner) => owner.committed_entries(),
            Self::Control(owner) => owner.committed_entries(),
            Self::InitialControl(owner) => owner.unsupported(),
        }
    }
    pub(super) fn control_observation(
        &self,
    ) -> RsfResult<rsf_control_state::PublishedControlState> {
        match self {
            Self::Control(owner) => owner.observation(),
            Self::Application(_) => Err(unsupported()),
            Self::InitialControl(owner) => owner.unsupported(),
        }
    }
    pub(super) fn validate_membership_authority(
        &self,
        approval: &VsrMembershipApproval,
    ) -> RsfResult<()> {
        match self {
            Self::Application(owner) => owner.validate_membership_authority(approval),
            Self::Control(owner) => owner.validate_membership_authority(approval),
            Self::InitialControl(owner) => owner.unsupported(),
        }
    }
    pub(super) fn validate_membership_stage(
        &self,
        stage: &MembershipStagingStore,
    ) -> RsfResult<()> {
        match self {
            Self::Application(owner) => owner.validate_membership_stage(stage),
            Self::Control(owner) => owner.validate_membership_stage(stage),
            Self::InitialControl(owner) => owner.unsupported(),
        }
    }
    pub(super) fn stage_membership(
        &self,
        path: impl AsRef<Path>,
        plan: VsrMembershipStagedPlan,
    ) -> RsfResult<MembershipStagingStore> {
        match self {
            Self::Application(owner) => owner.stage_membership(path, plan),
            Self::Control(_) => Err(unsupported()),
            Self::InitialControl(owner) => owner.unsupported(),
        }
    }
    pub(super) fn promote_membership(
        &self,
        path: impl AsRef<Path>,
        stage: &MembershipStagingStore,
        quorum: &VsrMembershipStagedQuorum,
    ) -> RsfResult<DurableVsrCell> {
        match self {
            Self::Application(owner) => owner.promote_membership(path, stage, quorum),
            Self::Control(_) => Err(unsupported()),
            Self::InitialControl(owner) => owner.unsupported(),
        }
    }
}
