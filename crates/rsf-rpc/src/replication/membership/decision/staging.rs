use super::*;
use rsf_vsr::{
    MembershipStagingStore, VsrMembershipStagedPlan, VsrMembershipStagedQuorum,
    VsrMembershipStagedReport,
};
impl PeerReplicaServer {
    fn staged_receipt_retry(
        &mut self,
        chosen: &VsrMembershipChosen,
    ) -> Result<Option<VsrMembershipStagedReport>, String> {
        let pg = chosen.proposal().election.pg;
        self.control_stage_supported(pg)?;
        // A warm receipt is independently bound to this stage and live hosted
        // token. Fresh staging below still requires the managed decision owner.
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
                .published_state_token()
                .map(|token| self.membership_stage_learner_tokens.get(&pg) == Some(&token))
                .map_err(|e| e.message)?
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
        self.control_stage_supported(pg)?;
        if let Some(report) = self.staged_receipt_retry(&chosen)? {
            return Ok(report);
        }
        self.control_membership_mutation(pg, Some(chosen.proposal()))?;
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
        self.control_stage_supported(pg)?;
        self.control_membership_mutation(pg, Some(quorum.proposal()))?;
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
        let approval = match self.membership_staging.get(&pg) {
            Some(stage)
                if stage.plan().input().proposal() == chosen.proposal()
                    && stage.plan().chosen().input_id() == chosen.input_id()
                    && stage.report().is_ok() =>
            {
                // This boot-materialized stage already owns the replay proof.
                // load_chosen_membership still checks the journal and compares
                // every retained chosen-input byte before publication proceeds.
                stage.plan().approval().clone()
            }
            _ => decision
                .approval(chosen.input_id())
                .map_err(|e| e.message)?,
        };
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
        self.control_stage_supported(pg)?;
        self.load_chosen_membership(chosen, approval)?;
        self.materialize_membership_stage(pg, apply)
    }

    fn load_chosen_membership(
        &mut self,
        chosen: VsrMembershipChosen,
        approval: VsrMembershipApproval,
    ) -> Result<(), String> {
        let pg = chosen.proposal().election.pg;
        self.control_stage_supported(pg)?;
        self.control_membership_mutation(pg, Some(chosen.proposal()))?;
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
            self.membership_stage_learner_tokens
                .insert(pg, learner.published_state_token().map_err(|e| e.message)?);
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
        self.control_stage_supported(pg).map_err(invalid)?;
        if sender == self.local || !chosen.proposal().new_voters.contains(&sender) {
            return Err(invalid(
                "membership staging requires an authenticated proposed-voter coordinator",
            ));
        }
        if let Some(report) = self.staged_receipt_retry(&chosen).map_err(invalid)? {
            return report.encode().map_err(|e| invalid(e.message));
        }
        self.control_membership_mutation(pg, Some(chosen.proposal()))
            .map_err(invalid)?;
        let retained = if let Some(store) = self.membership_decisions.get(&pg) {
            if store.proposal() != chosen.proposal() {
                return Err(invalid("membership staging differs from retained proposal"));
            }
            store
                .retained_approval(chosen.input_id())
                .map_err(|e| invalid(e.message))?
        } else {
            None
        };
        let approval = if let Some(approval) = retained {
            // An acceptor may have complete B without learning its certificate.
            // A new coordinator can supply that certificate without the old
            // uploader's inbox; stage_chosen_membership retains/validates it.
            approval
        } else {
            self.membership_inputs
                .get(&pg)
                .ok_or_else(|| invalid("membership authority inbox is not configured"))?
                .retained_approval(sender, chosen.input_id())
                .map_err(|e| invalid(e.message))?
        };
        self.stage_chosen_membership(chosen, approval, apply)
            .map_err(invalid)?
            .encode()
            .map_err(|e| invalid(e.message))
    }
}
impl NetworkVsrTransport {
    /// Request one peer's staged receipt. The peer must already retain this
    /// input or have a complete authority inbox. Provisioning is separate so a
    /// recovery poll performs only this bounded exchange (and its lost-reply retry).
    pub fn stage_membership(
        &self,
        replica: NodeId,
        plan: &VsrMembershipStagedPlan,
    ) -> Result<VsrMembershipStagedReport, VsrTransportError> {
        let report = self.exchange_membership_stage(replica, plan.chosen())?;
        check_report(&report, replica, plan)
            .map_err(|reason| VsrTransportError { replica, reason })?;
        Ok(report)
    }

    /// A recovery worker needs only the chosen proof and an actual local staged
    /// receipt, avoiding a copy of the complete plan/history for every peer.
    pub(in crate::replication) fn stage_membership_report(
        &self,
        replica: NodeId,
        chosen: &VsrMembershipChosen,
        expected: &VsrMembershipStagedReport,
    ) -> Result<VsrMembershipStagedReport, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        expected.encode().map_err(|e| fail(e.message))?;
        if expected.replica != self.local
            || expected.proposal != *chosen.proposal()
            || expected.input != chosen.input_id()
        {
            return Err(fail(
                "membership expected stage differs from chosen input".into(),
            ));
        }
        let report = self.exchange_membership_stage(replica, chosen)?;
        if report.replica != replica
            || report.proposal != expected.proposal
            || report.input != expected.input
            || report.plan != expected.plan
            || report.committed_tail != expected.committed_tail
            || report.accepted_tail != expected.accepted_tail
        {
            return Err(fail(
                "membership staged receipt differs from authenticated reporter or chosen prefix"
                    .into(),
            ));
        }
        Ok(report)
    }

    fn exchange_membership_stage(
        &self,
        replica: NodeId,
        chosen: &VsrMembershipChosen,
    ) -> Result<VsrMembershipStagedReport, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if replica == self.local
            || !chosen.proposal().new_voters.contains(&self.local)
            || !chosen.proposal().new_voters.contains(&replica)
        {
            return Err(fail(
                "membership staging requires distinct proposed-voter endpoints".into(),
            ));
        }
        let payload = chosen.encode().map_err(|e| fail(e.message))?;
        let response = self
            .exchange_io(replica, MEMBERSHIP_STAGE, &payload)
            .map_err(|e| fail(e.to_string()))?;
        VsrMembershipStagedReport::decode(&response).map_err(|e| fail(e.message))
    }

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
                .and_then(|()| self.stage_membership(replica, plan));
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
