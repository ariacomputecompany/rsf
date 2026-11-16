use super::*;

impl PeerReplicaServer {
    /// Configure owned voter targets for hosted learners, or their recovered
    /// promoted cells. RPC data can never choose a filesystem path.
    pub fn with_promotion_paths(
        mut self,
        paths: BTreeMap<PlacementGroupId, std::path::PathBuf>,
    ) -> Result<Self, String> {
        if !self.membership_promotion_paths.is_empty() {
            return Err("membership promotion paths already configured".into());
        }
        for pg in paths.keys() {
            if !self.learners.contains_key(pg)
                && self
                    .cells
                    .get(pg)
                    .map(|cell| cell.promoted_replica())
                    .transpose()
                    .map_err(|e| e.message)?
                    .flatten()
                    != Some(self.local)
            {
                return Err(
                    "promotion target requires a hosted learner or owned promoted cell".into(),
                );
            }
        }
        self.membership_promotion_paths = paths;
        Ok(self)
    }

    /// Publish the chosen, locally staged prefix using authenticated staging
    /// evidence. A historical retry materializes current history without moving
    /// authority backwards. Client admission is a separate control decision.
    pub fn publish_membership_locally(
        &mut self,
        quorum: &VsrMembershipStagedQuorum,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<VsrMembershipPublishedReport, String> {
        quorum.encode().map_err(|e| e.message)?;
        let pg = quorum.proposal().election.pg;
        self.control_stage_supported(pg)?;
        if !quorum.proposal().new_voters.contains(&self.local) {
            return Err("membership publication requires a proposed local voter".into());
        }
        if let Some(report) = self.historical_publication(pg, quorum.plan_id())? {
            check_published(&report, self.local, quorum)?;
            return self.query_membership_publication(pg, quorum.plan_id(), apply);
        }
        self.control_membership_mutation(pg, Some(quorum.proposal()))?;
        if quorum
            .proposal()
            .new_voters
            .iter()
            .any(|node| !self.nodes.values().any(|mapped| mapped == node))
        {
            return Err(
                "membership publication requires endpoints for every proposed voter".into(),
            );
        }
        // Resolve custody before materialization or retention. The owned factory
        // performs the complete overlap, symlink and target-identity validation.
        let target = if self.cells.contains_key(&pg) {
            None
        } else {
            Some(
                self.membership_promotion_paths
                    .get(&pg)
                    .ok_or_else(|| "learner has no configured promotion target".to_string())?
                    .clone(),
            )
        };
        self.prepare_membership_publication(quorum, apply)?;
        self.membership_staging
            .get_mut(&pg)
            .ok_or_else(|| "membership publication has no owned stage".to_string())?
            .retain_publication(quorum)
            .map_err(|e| e.message)?;
        let stage = &self.membership_staging[&pg];
        if let Some(cell) = self.cells.get_mut(&pg) {
            cell.install_membership(stage, quorum)
                .map_err(|e| e.message)?;
        } else {
            let learner = self
                .learners
                .get(&pg)
                .ok_or_else(|| "membership publication has no learner custody".to_string())?;
            let cell = learner
                .promote_membership(target.unwrap(), stage, quorum)
                .map_err(|e| e.message)?;
            // No callback or scheduling point intervenes between insertion and
            // release of the read-only learner. The factory retains voter lock
            // custody through authoritative replay and into this owned map.
            self.cells.insert(pg, cell);
            self.learners.remove(&pg);
        }
        self.election_clock
            .insert(pg, control::Clock::new(Instant::now()));
        self.election_snapshots.remove(&pg);
        self.membership_snapshots.remove(&pg);
        self.membership_stage_voter_tokens.remove(&pg);
        self.membership_stage_learner_tokens.remove(&pg);
        self.membership_recovery_learner_anchors.remove(&pg);
        self.learner_materialized.remove(&pg);
        self.installation_pending.insert(pg);
        self.query_membership_publication(pg, quorum.plan_id(), apply)
    }

    /// Acknowledge an actual historical installation only after durably applying
    /// the CURRENT committed prefix. This cannot stage or first-publish a plan,
    /// clear an election/membership fence, or refresh primary-contact clocks.
    pub fn query_membership_publication(
        &mut self,
        pg: PlacementGroupId,
        plan: [u8; 32],
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<VsrMembershipPublishedReport, String> {
        // Active-cell ancestry and current materialization prove this historical
        // ACK independently of an auxiliary transition decision journal.
        let expected = self
            .historical_publication(pg, plan)?
            .ok_or_else(|| "membership plan is not published locally".to_string())?;
        self.installation_pending.insert(pg);
        self.materialize_owned_prefix(pg, apply)?;
        let report = self.historical_publication(pg, plan)?.ok_or_else(|| {
            "membership publication disappeared during materialization".to_string()
        })?;
        if report != expected {
            return Err("membership publication changed during materialization".into());
        }
        self.installation_pending.remove(&pg);
        Ok(report)
    }

    fn historical_publication(
        &self,
        pg: PlacementGroupId,
        plan: [u8; 32],
    ) -> Result<Option<VsrMembershipPublishedReport>, String> {
        let Some(cell) = self.cells.get(&pg) else {
            return Ok(None);
        };
        let Some(proof) = cell.membership_publication(plan).map_err(|e| e.message)? else {
            return Ok(None);
        };
        let report = VsrMembershipPublishedReport {
            proposal: proof.proposal().clone(),
            replica: self.local,
            input: proof.input_id(),
            plan: proof.plan_id(),
        };
        report.encode().map_err(|e| e.message)?;
        Ok(Some(report))
    }

    pub(in crate::replication) fn dispatch_membership_publication(
        &mut self,
        sender: NodeId,
        kind: u8,
        payload: &[u8],
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> io::Result<Vec<u8>> {
        if sender == self.local {
            return Err(invalid(
                "membership publication RPC requires a remote authenticated node",
            ));
        }
        let report = if kind == MEMBERSHIP_PUBLISH {
            let quorum =
                VsrMembershipStagedQuorum::decode(payload).map_err(|e| invalid(e.message))?;
            if !quorum.proposal().new_voters.contains(&sender) {
                return Err(invalid("membership publisher is not a proposed voter"));
            }
            self.publish_membership_locally(&quorum, apply)
                .map_err(invalid)?
        } else if kind == MEMBERSHIP_PUBLICATION && payload.len() == 40 {
            let pg = PlacementGroupId(u64_at(payload, 0)?);
            let plan = id_at(payload, 8)?;
            let proof = self
                .historical_publication(pg, plan)
                .map_err(invalid)?
                .ok_or_else(|| invalid("membership plan is not published locally"))?;
            if !proof.proposal.new_voters.contains(&sender) {
                return Err(invalid(
                    "membership publication query requires a proposed voter",
                ));
            }
            self.query_membership_publication(pg, plan, apply)
                .map_err(invalid)?
        } else {
            return Err(invalid("invalid membership publication request"));
        };
        report.encode().map_err(|e| invalid(e.message))
    }
}
