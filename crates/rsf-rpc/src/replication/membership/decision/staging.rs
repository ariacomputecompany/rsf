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
            !self.installation_pending.contains(&pg)
                && self.membership_stage_voter_tokens.get(&pg)
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

    /// Load the exact retained choice and materialize its local stage before a
    /// publisher retains the quorum input or changes live voter custody.
    pub(in crate::replication) fn prepare_membership_publication(
        &mut self,
        quorum: &VsrMembershipStagedQuorum,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<VsrMembershipStagedReport, String> {
        quorum.encode().map_err(|e| e.message)?;
        let pg = quorum.proposal().election.pg;
        let decision = self
            .membership_decisions
            .get(&pg)
            .ok_or_else(|| "membership publication requires a retained decision".to_string())?;
        let chosen = decision
            .chosen()
            .ok_or_else(|| "membership publication requires a retained chosen input".to_string())?
            .clone();
        if chosen.proposal() != quorum.proposal() || chosen.input_id() != quorum.input_id() {
            return Err("membership publication quorum differs from retained choice".into());
        }
        let approval = decision
            .approval(chosen.input_id())
            .map_err(|e| e.message)?;
        self.load_chosen_membership(chosen.clone(), approval)?;
        let stage = &self.membership_staging[&pg];
        check_report(
            &quorum.reports()[0],
            quorum.reports()[0].replica,
            stage.plan(),
        )?;
        if quorum
            .reports()
            .iter()
            .any(|report| report.replica == self.local && report.stage != stage.stage_id())
        {
            return Err("membership publication quorum differs from the sealed local stage".into());
        }
        if let Some(report) = self.staged_receipt_retry(&chosen)? {
            return Ok(report);
        }
        self.materialize_membership_stage(pg, apply)
    }

    fn stage_chosen_membership(
        &mut self,
        chosen: VsrMembershipChosen,
        approval: VsrMembershipApproval,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<VsrMembershipStagedReport, String> {
        let pg = chosen.proposal().election.pg;
        self.load_chosen_membership(chosen, approval)?;
        self.materialize_membership_stage(pg, apply)
    }

    fn load_chosen_membership(
        &mut self,
        chosen: VsrMembershipChosen,
        approval: VsrMembershipApproval,
    ) -> Result<(), String> {
        let pg = chosen.proposal().election.pg;
        self.initialize_membership_decision(&approval)?;
        self.membership_decisions
            .get_mut(&pg)
            .unwrap()
            .retain_chosen_input(chosen.clone(), &approval)
            .map_err(|e| e.message)?;
        if !self.membership_staging.contains_key(&pg) {
            let path = self
                .membership_paths
                .get(&pg)
                .ok_or_else(|| "membership staging path is not configured".to_string())?
                .join("staging");
            let stage = if super::server::transition_path_present(&path.join("META"))? {
                MembershipStagingStore::open_existing(&path, pg, self.local)
                    .map_err(|e| e.message)?
            } else {
                let plan =
                    VsrMembershipStagedPlan::select(chosen, approval).map_err(|e| e.message)?;
                if let Some(cell) = self.cells.get(&pg) {
                    if cell
                        .membership_publication(plan.id())
                        .map_err(|e| e.message)?
                        .is_some()
                    {
                        return Err("published membership lost its sealed local stage".into());
                    }
                    MembershipStagingStore::stage(&path, self.local, plan, cell)
                        .map_err(|e| e.message)?
                } else {
                    self.learners
                        .get(&pg)
                        .ok_or_else(|| "membership staging has no hosted anchor".to_string())?
                        .stage_membership(&path, plan)
                        .map_err(|e| e.message)?
                }
            };
            self.membership_staging.insert(pg, stage);
        }
        let stage = &self.membership_staging[&pg];
        let selected = self.membership_decisions[&pg]
            .chosen()
            .ok_or_else(|| "membership staged prefix has no retained choice".to_string())?;
        if stage.plan().input().proposal() != selected.proposal()
            || stage.plan().chosen().input_id() != selected.input_id()
        {
            return Err("membership staged prefix differs from retained choice".into());
        }
        Ok(())
    }

    fn materialize_membership_stage(
        &mut self,
        pg: PlacementGroupId,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<VsrMembershipStagedReport, String> {
        // Only a fully successful materialization may establish a retry token.
        // Failed application work cannot reuse an earlier successful report.
        self.membership_stage_voter_tokens.remove(&pg);
        self.membership_stage_learner_tokens.remove(&pg);
        let mut apply_entry = |entry: &VsrCommittedEntry| {
            apply(entry).map_err(|reason| {
                rsf_core::RsfError::new(rsf_core::ErrorCode::InvalidInput, reason)
            })
        };
        if let Some(cell) = self.cells.get(&pg) {
            let stage = self
                .membership_staging
                .get_mut(&pg)
                .ok_or_else(|| "membership staged prefix is not loaded".to_string())?;
            let published = cell
                .membership_publication(stage.plan().id())
                .map_err(|e| e.message)?
                .is_some();
            self.installation_pending.insert(pg);
            let report = if published {
                // Replaying the older stage here could hide later application
                // work. Validate ancestry, then apply the current committed log.
                stage.materialize_published(cell, &mut apply_entry)
            } else {
                stage.validate_anchor(cell).map_err(|e| e.message)?;
                stage.materialize(&mut apply_entry)
            }
            .map_err(|e| e.message)?;
            self.membership_stage_voter_tokens
                .insert(pg, cell.recovery_state_token().map_err(|e| e.message)?);
            self.installation_pending.remove(&pg);
            Ok(report)
        } else {
            let learner = self
                .learners
                .get(&pg)
                .ok_or_else(|| "membership staging has no hosted anchor".to_string())?;
            let stage = self
                .membership_staging
                .get_mut(&pg)
                .ok_or_else(|| "membership staged prefix is not loaded".to_string())?;
            learner
                .validate_membership_stage(stage)
                .map_err(|e| e.message)?;
            let report = stage.materialize(&mut apply_entry).map_err(|e| e.message)?;
            if let Some(id) = learner.published_history_id().map_err(|e| e.message)? {
                self.membership_stage_learner_tokens.insert(pg, id);
            }
            Ok(report)
        }
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
        let retained = self.membership_decisions.get(&pg).filter(|store| {
            store.chosen().is_some_and(|previous| {
                previous.proposal() == chosen.proposal() && previous.input_id() == chosen.input_id()
            })
        });
        let approval = if let Some(store) = retained {
            // A new coordinator can retry a cold retained choice without the
            // old sender's inbox selection or another authority upload.
            store
                .approval(chosen.input_id())
                .map_err(|e| invalid(e.message))?
        } else {
            self.membership_inputs
                .get(&pg)
                .ok_or_else(|| invalid("membership authority inbox is not configured"))?
                .finish(sender, chosen.input_id())
                .map_err(|e| invalid(e.message))?
        };
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
