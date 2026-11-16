//! Managed receiver custody is independent of scheduler ownership. The cache
//! binds a live learner to this exact journal authority, never to an RPC DTO.
use super::*;
use rsf_vsr::{VsrLearnerStateToken, VsrMembershipDecisionState};

#[derive(Debug, Eq, PartialEq)]
pub(in crate::replication) struct LearnerAnchor {
    pg: PlacementGroupId,
    proposal: [u8; 32],
    bootstrap: [u8; 32],
    authority: [u8; 32],
    selected: [u8; 32],
    token: VsrLearnerStateToken,
}

impl LearnerAnchor {
    fn new(state: &VsrMembershipDecisionState, token: VsrLearnerStateToken) -> Self {
        Self {
            pg: state.pg,
            proposal: state.proposal,
            bootstrap: state.bootstrap,
            authority: state.authority,
            selected: state.chosen.map_or(state.authority, |(_, input)| input),
            token,
        }
    }
}

impl PeerReplicaServer {
    /// A configured managed group must still own its exact hosted anchor before
    /// answering transition requests. Initial A and chosen B remain distinct.
    /// This grants neither control initiation nor client/application authority.
    pub(in crate::replication) fn validate_managed_membership_anchor(
        &mut self,
        pg: PlacementGroupId,
    ) -> Result<(), String> {
        self.validate_managed_membership_anchor_with_authority(pg, None)
    }

    // The sole production Some(...) caller is the recovery constructor, while
    // its just-replayed opaque initial proof remains in this same operation.
    // Normal RPC/poll calls pass None and cannot borrow prior-call approvals.
    pub(in crate::replication) fn validate_managed_membership_anchor_with_authority(
        &mut self,
        pg: PlacementGroupId,
        initial_authority: Option<&VsrMembershipApproval>,
    ) -> Result<(), String> {
        let Some(bound) = self.membership_recovery_bindings.get(&pg) else {
            return Ok(());
        };
        let store = self
            .membership_decisions
            .get(&pg)
            .ok_or_else(|| "managed membership lost its decision journal".to_string())?;
        if store.proposal() != bound {
            return Err("managed membership journal changed its bound proposal".into());
        }
        let state = store.decision_state().map_err(|e| e.message)?;
        self.validate_decision_nodes(bound)?;
        if let Some(cell) = self.cells.get(&pg) {
            cell.recovery_state_token().map_err(|e| e.message)?;
            if cell.membership_fence() == Some(bound)
                || self.published_decision_anchor(pg, store, self.membership_staging.get(&pg))?
            {
                return Ok(());
            }
            return Err("managed voter lost its exact fence or published decision ancestry".into());
        }
        let learner = self
            .learners
            .get(&pg)
            .ok_or_else(|| "managed membership lost its hosted anchor".to_string())?;
        if let Some(approval) = initial_authority
            && (approval.proposal() != bound
                || VsrMembershipInput::from_approval(approval)
                    .and_then(|input| input.id())
                    .map_err(|error| error.message)?
                    != state.authority)
        {
            return Err("startup proof differs from the journal's initial authority".into());
        }
        let candidate = LearnerAnchor::new(
            &state,
            learner.published_state_token().map_err(|e| e.message)?,
        );
        if self.membership_recovery_learner_anchors.get(&pg) != Some(&candidate) {
            // The journal's actual chosen proof may advance A to B. A caller's
            // input descriptor or a minority acceptance cannot select authority.
            // Re-read the selected proof before caching a changed hosted prefix
            // or a newly retained choice, even when the learner token is equal.
            let retained;
            let authority = if let Some(approval) = initial_authority
                && candidate.selected == candidate.authority
            {
                approval
            } else {
                retained = store.approval(candidate.selected).map_err(|e| e.message)?;
                &retained
            };
            learner
                .validate_membership_authority(authority)
                .map_err(|e| e.message)?;
            self.membership_recovery_learner_anchors
                .insert(pg, candidate);
        }
        Ok(())
    }
}
