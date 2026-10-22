use super::*;
use rsf_vsr::{
    MembershipStagingStore, VsrMembershipStagedPlan, VsrMembershipStagedQuorum,
    VsrMembershipStagedReport,
};
impl PeerReplicaServer {
    fn staged_receipt_retry(
        &self,
        chosen: &VsrMembershipChosen,
    ) -> Result<Option<VsrMembershipStagedReport>, String> {
        let pg = chosen.proposal().election.pg;
        let Some(stage) = self.membership_staging.get(&pg) else {
            return Ok(None);
        };
        if stage.plan().input().proposal() != chosen.proposal()
            || stage.plan().chosen().input_id() != chosen.input_id()
        {
            return Err("membership staging retry conflicts with retained chosen input".into());
        }
        let unchanged = if let Some(cell) = self.cells.get(&pg) {
            self.membership_stage_voter_tokens.get(&pg)
                == Some(&cell.recovery_state_token().map_err(|e| e.message)?)
        } else {
            self.learners
                .get(&pg)
                .ok_or_else(|| "membership staging has no hosted anchor".to_string())?
                .published_history_id()
                .map_err(|e| e.message)?
                .is_some_and(|id| self.membership_stage_learner_tokens.get(&pg) == Some(&id))
        };
        if unchanged && let Ok(report) = stage.report() {
            return Ok(Some(report));
        }
        Ok(None)
    }

    /// Trusted application effects must be durable before the local staged
    /// receipt is returned. This neither publishes nor activates membership.
    pub fn stage_membership_locally(
        &mut self,
        chosen: VsrMembershipChosen,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<VsrMembershipStagedReport, String> {
        let pg = chosen.proposal().election.pg;
        if let Some(report) = self.staged_receipt_retry(&chosen)? {
            return Ok(report);
        }
        let approval = self
            .membership_decisions
            .get(&pg)
            .ok_or_else(|| "membership decision is not initialized".to_string())?
            .approval(chosen.input_id())
            .map_err(|e| e.message)?;
        self.stage_chosen_membership(chosen, approval, apply)
    }
    fn stage_chosen_membership(
        &mut self,
        chosen: VsrMembershipChosen,
        approval: VsrMembershipApproval,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<VsrMembershipStagedReport, String> {
        let pg = chosen.proposal().election.pg;
        self.initialize_membership_decision(&approval)?;
        self.membership_decisions
            .get_mut(&pg)
            .unwrap()
            .retain_chosen_input(chosen.clone(), &approval)
            .map_err(|e| e.message)?;
        let plan = VsrMembershipStagedPlan::select(chosen, approval).map_err(|e| e.message)?;
        if !self.membership_staging.contains_key(&pg) {
            let path = self
                .membership_paths
                .get(&pg)
                .ok_or_else(|| "membership staging path is not configured".to_string())?
                .join("staging");
            let stage = if path.join("META").exists() {
                MembershipStagingStore::open_existing(&path, pg, self.local)
                    .map_err(|e| e.message)?
            } else if let Some(cell) = self.cells.get(&pg) {
                MembershipStagingStore::stage(&path, self.local, plan, cell)
                    .map_err(|e| e.message)?
            } else {
                self.learners
                    .get(&pg)
                    .ok_or_else(|| "membership staging has no hosted anchor".to_string())?
                    .stage_membership(&path, plan)
                    .map_err(|e| e.message)?
            };
            self.membership_staging.insert(pg, stage);
        }
        let stage = &self.membership_staging[&pg];
        if stage.plan().chosen().input_id()
            != self.membership_decisions[&pg].chosen().unwrap().input_id()
        {
            return Err("membership staged prefix differs from retained choice".into());
        }
        if let Some(cell) = self.cells.get(&pg) {
            stage.validate_anchor(cell).map_err(|e| e.message)?;
        } else {
            self.learners
                .get(&pg)
                .ok_or_else(|| "membership staging has no hosted anchor".to_string())?
                .validate_membership_stage(stage)
                .map_err(|e| e.message)?;
        }
        if let Some(cell) = self.cells.get(&pg) {
            self.membership_stage_voter_tokens
                .insert(pg, cell.recovery_state_token().map_err(|e| e.message)?);
        } else if let Some(id) = self.learners[&pg]
            .published_history_id()
            .map_err(|e| e.message)?
        {
            self.membership_stage_learner_tokens.insert(pg, id);
        }
        self.membership_staging
            .get_mut(&pg)
            .unwrap()
            .materialize(&mut |entry| {
                apply(entry).map_err(|reason| {
                    rsf_core::RsfError::new(rsf_core::ErrorCode::InvalidInput, reason)
                })
            })
            .map_err(|e| e.message)
    }
    pub(in crate::replication) fn dispatch_membership_stage(
        &mut self,
        sender: NodeId,
        payload: &[u8],
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<Vec<u8>> {
        let chosen = VsrMembershipChosen::decode(payload).map_err(|e| invalid(e.message))?;
        let pg = chosen.proposal().election.pg;
        if sender == self.local || !chosen.proposal().new_voters.contains(&sender) {
            return Err(invalid(
                "membership staging requires an authenticated proposed-voter coordinator",
            ));
        }
        if let Some(report) = self.staged_receipt_retry(&chosen).map_err(invalid)? {
            return report.encode().map_err(|e| invalid(e.message));
        }
        let approval = self
            .membership_inputs
            .get(&pg)
            .ok_or_else(|| invalid("membership authority inbox is not configured"))?
            .finish(sender, chosen.input_id())
            .map_err(|e| invalid(e.message))?;
        self.stage_chosen_membership(chosen, approval, apply)
            .map_err(invalid)?
            .encode()
            .map_err(|e| invalid(e.message))
    }
}
impl NetworkVsrTransport {
    /// Gather new-majority staged/materialized receipts for a chosen prefix.
    /// The local report must come from this boot's trusted materializer. No
    /// active generation, learner promotion or client admission is published.
    pub fn collect_membership_stages(
        &self,
        local: VsrMembershipStagedReport,
        plan: &VsrMembershipStagedPlan,
    ) -> Result<VsrMembershipStagedQuorum, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        check_report(&local, self.local, plan).map_err(fail)?;
        let mut reports = vec![local];
        let mut failures = Vec::new();
        let payload = plan.chosen().encode().map_err(|e| fail(e.message))?;
        for replica in plan
            .input()
            .proposal()
            .new_voters
            .iter()
            .copied()
            .filter(|n| *n != self.local)
        {
            let result = self
                .provision_membership_authority(replica, plan.approval())
                .and_then(|()| {
                    let response = self
                        .exchange_io(replica, MEMBERSHIP_STAGE, &payload)
                        .map_err(|e| VsrTransportError {
                            replica,
                            reason: e.to_string(),
                        })?;
                    let report = VsrMembershipStagedReport::decode(&response).map_err(|e| {
                        VsrTransportError {
                            replica,
                            reason: e.message,
                        }
                    })?;
                    check_report(&report, replica, plan)
                        .map_err(|reason| VsrTransportError { replica, reason })?;
                    Ok(report)
                });
            match result {
                Ok(report) => reports.push(report),
                Err(e) => failures.push(format!("{}: {}", replica.0, e.reason)),
            }
        }
        VsrMembershipStagedQuorum::from_reports(reports)
            .map_err(|e| fail(format!("{}: {}", e.message, failures.join("; "))))
    }
}
fn check_report(
    report: &VsrMembershipStagedReport,
    replica: NodeId,
    plan: &VsrMembershipStagedPlan,
) -> Result<(), String> {
    report.encode().map_err(|e| e.message)?;
    if report.replica != replica
        || report.proposal != *plan.input().proposal()
        || report.input != plan.input().id().map_err(|e| e.message)?
        || report.plan != plan.id()
        || report.committed_tail != plan.committed_tail()
        || report.accepted_tail != plan.accepted_tail()
    {
        return Err(
            "membership staged receipt differs from authenticated reporter or chosen prefix".into(),
        );
    }
    Ok(())
}
