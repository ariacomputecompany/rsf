use super::*;
impl NetworkVsrTransport {
    /// Fetch a retained complete input over bounded authenticated batches. This
    /// can recover old-quorum proof after every original old voter is lost.
    pub fn fetch_membership_input(
        &self,
        replica: NodeId,
        pg: PlacementGroupId,
        id: [u8; 32],
    ) -> Result<VsrMembershipApproval, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let query = input_query(pg, id);
        let bytes = self
            .exchange_io(replica, MEMBERSHIP_INPUT_DESCRIPTOR, &query)
            .map_err(|e| fail(e.to_string()))?;
        let input = VsrMembershipInput::decode(&bytes).map_err(|e| fail(e.message))?;
        if input.id().map_err(|e| fail(e.message))? != id
            || input.proposal().election.pg != pg
            || !input.proposal().new_voters.contains(&self.local)
        {
            return Err(fail(
                "membership input differs from requested identity, group or reader".into(),
            ));
        }
        let count = input.records().map_err(|e| fail(e.message))?;
        let mut records = Vec::new();
        while (records.len() as u64) < count {
            let ordinal = records.len() as u64;
            let mut payload = query.clone();
            payload.extend_from_slice(&ordinal.to_le_bytes());
            let bytes = self
                .exchange_io(replica, MEMBERSHIP_INPUT_READ, &payload)
                .map_err(|e| fail(e.to_string()))?;
            let batch = rsf_vsr::VsrHistoryBatch::decode(&bytes).map_err(|e| fail(e.message))?;
            if batch.ordinal != ordinal || ordinal + batch.records.len() as u64 > count {
                return Err(fail("membership input history range mismatch".into()));
            }
            records.extend(batch.records);
        }
        input
            .validate_records(&records)
            .map_err(|e| fail(e.message))
    }
    /// Provision verified old authority, collect a new quorum's promises, retain
    /// its highest accepted input, nominate durably, and obtain a chosen proof.
    /// The local store must already be anchored to its configured voter/learner.
    /// Success chooses input only; it does not install or activate a new cell.
    pub fn choose_membership_plan(
        &self,
        local: &mut MembershipDecisionStore,
        round: VsrMembershipRound,
        candidate: &VsrMembershipApproval,
    ) -> Result<VsrMembershipChosen, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        let authority = local.authority().map_err(|e| fail(e.message))?;
        if round.coordinator != self.local
            || candidate.proposal() != local.proposal()
            || authority.snapshots()[0].records.first() != candidate.snapshots()[0].records.first()
        {
            return Err(fail(
                "membership coordinator or candidate differs from anchored authority".into(),
            ));
        }
        let proposal = candidate.proposal();
        let proposal_id = proposal.id().map_err(|e| fail(e.message))?;
        let prefix = context(proposal.election.pg, round, proposal_id);
        let mut promises = vec![
            local
                .promise(self.local, round)
                .map_err(|e| fail(e.message))?,
        ];
        let mut failures = Vec::new();
        for replica in proposal
            .new_voters
            .iter()
            .copied()
            .filter(|n| *n != self.local)
        {
            let result=self.provision_membership_authority(replica,&authority).and_then(|()|{
                let bytes=self.exchange_io(replica,MEMBERSHIP_PROMISE,&prefix).map_err(|e|VsrTransportError{replica,reason:e.to_string()})?;
                let report=VsrMembershipPromise::decode(&bytes).map_err(|e|VsrTransportError{replica,reason:e.message})?;
                if report.replica!=replica || report.proposal!=*proposal || report.round!=round{return Err(VsrTransportError{replica,reason:"membership promise differs from authenticated reporter, proposal or round".into()});}Ok(report)
            });
            match result {
                Ok(report) => promises.push(report),
                Err(e) => failures.push(format!("{}: {}", replica.0, e.reason)),
            }
        }
        if promises.len() <= proposal.new_voters.len() / 2 {
            return Err(fail(format!(
                "membership promise quorum unavailable: {}",
                failures.join("; ")
            )));
        }
        let highest = promises
            .iter()
            .filter_map(|p| p.accepted.map(|(round, id)| (round, id, p.replica)))
            .max_by_key(|(round, _, replica)| (*round, std::cmp::Reverse(*replica)));
        let initial = VsrMembershipInput::from_approval(candidate).map_err(|e| fail(e.message))?;
        let recovered;
        let selected = if let Some((_, id, replica)) = highest {
            if id == initial.id().map_err(|e| fail(e.message))? {
                candidate
            } else {
                recovered = if replica == self.local {
                    local.approval(id).map_err(|e| fail(e.message))?
                } else {
                    self.fetch_membership_input(replica, proposal.election.pg, id)?
                };
                &recovered
            }
        } else {
            candidate
        };
        let input = VsrMembershipInput::from_approval(selected).map_err(|e| fail(e.message))?;
        let decision = VsrMembershipDecision::from_promises(promises, input.clone())
            .map_err(|e| fail(e.message))?;
        self.stage_membership_plan(local, round, selected)?;
        let nomination = local.nominate(&decision).map_err(|e| fail(e.message))?;
        let payload = acceptance_request(&decision, &nomination).map_err(|e| fail(e.message))?;
        let mut accepted = vec![nomination];
        let participants = decision
            .promises()
            .iter()
            .map(|p| p.replica)
            .filter(|n| *n != self.local)
            .collect::<Vec<_>>();
        for replica in participants {
            let result=self.send_membership_plan(replica,round,selected).and_then(|()|{
                let bytes=self.exchange_io(replica,MEMBERSHIP_ACCEPT,&payload).map_err(|e|VsrTransportError{replica,reason:e.to_string()})?;
                let report=VsrMembershipAccepted::decode(&bytes).map_err(|e|VsrTransportError{replica,reason:e.message})?;
                if report.replica!=replica || report.proposal!=*proposal || report.round!=round || report.input!=input.id().map_err(|e|VsrTransportError{replica,reason:e.message})? {return Err(VsrTransportError{replica,reason:"membership acceptance differs from authenticated reporter or decision".into()});}Ok(report)
            });
            match result {
                Ok(report) => accepted.push(report),
                Err(e) => failures.push(format!("{}: {}", replica.0, e.reason)),
            }
        }
        let chosen = VsrMembershipChosen::from_accepted(accepted)
            .map_err(|e| fail(format!("{}: {}", e.message, failures.join("; "))))?;
        local
            .learn_chosen(chosen.clone())
            .map_err(|e| fail(e.message))?;
        let bytes = chosen.encode().map_err(|e| fail(e.message))?;
        // The accepted new quorum is already durable. Losing dissemination replies
        // cannot erase its choice; survivors reconstruct it in a higher round.
        for replica in proposal
            .new_voters
            .iter()
            .copied()
            .filter(|n| *n != self.local)
        {
            let _ = self.exchange_io(replica, MEMBERSHIP_CHOSEN, &bytes);
        }
        Ok(chosen)
    }
    pub fn provision_membership_authority(
        &self,
        replica: NodeId,
        approval: &VsrMembershipApproval,
    ) -> Result<(), VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let input = VsrMembershipInput::from_approval(approval).map_err(|e| fail(e.message))?;
        let descriptor = input.encode().map_err(|e| fail(e.message))?;
        let id = input.id().map_err(|e| fail(e.message))?;
        let mut count = self
            .request_io(replica, MEMBERSHIP_AUTH_BEGIN, &descriptor)
            .map_err(|e| fail(e.to_string()))?;
        let records = input
            .flattened_records(approval)
            .map_err(|e| fail(e.message))?;
        let prefix = input_query(input.proposal().election.pg, id);
        self.upload_membership_records(
            replica,
            MEMBERSHIP_AUTH_BATCH,
            &prefix,
            &records,
            &mut count,
        )?;
        let response = self
            .exchange_io(replica, MEMBERSHIP_AUTH_FINISH, &prefix)
            .map_err(|e| fail(e.to_string()))?;
        if response != id {
            return Err(fail(
                "membership authority acknowledgement identity mismatch".into(),
            ));
        }
        Ok(())
    }
    fn stage_membership_plan(
        &self,
        local: &mut MembershipDecisionStore,
        round: VsrMembershipRound,
        approval: &VsrMembershipApproval,
    ) -> Result<(), VsrTransportError> {
        let fail = |e: rsf_core::RsfError| VsrTransportError {
            replica: self.local,
            reason: e.message,
        };
        let input = VsrMembershipInput::from_approval(approval).map_err(fail)?;
        let id = input.id().map_err(fail)?;
        let records = input.flattened_records(approval).map_err(fail)?;
        let mut count = local.begin_input(self.local, round, input).map_err(fail)?;
        if count > records.len() as u64 {
            return Err(VsrTransportError {
                replica: self.local,
                reason: "local membership input exceeds descriptor".into(),
            });
        }
        while count < records.len() as u64 {
            let batch =
                rsf_vsr::VsrHistoryBatch::pack(count, &records[count as usize..]).map_err(fail)?;
            count = local
                .append_input(self.local, round, id, batch)
                .map_err(fail)?;
        }
        local.approval(id).map_err(fail)?;
        Ok(())
    }
    fn send_membership_plan(
        &self,
        replica: NodeId,
        round: VsrMembershipRound,
        approval: &VsrMembershipApproval,
    ) -> Result<(), VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let input = VsrMembershipInput::from_approval(approval).map_err(|e| fail(e.message))?;
        let prefix = context(
            input.proposal().election.pg,
            round,
            input.proposal().id().map_err(|e| fail(e.message))?,
        );
        let mut payload = prefix.clone();
        payload.extend_from_slice(&input.encode().map_err(|e| fail(e.message))?);
        let mut count = self
            .request_io(replica, MEMBERSHIP_INPUT_BEGIN, &payload)
            .map_err(|e| fail(e.to_string()))?;
        let mut prefix = prefix;
        prefix.extend_from_slice(&input.id().map_err(|e| fail(e.message))?);
        let records = input
            .flattened_records(approval)
            .map_err(|e| fail(e.message))?;
        self.upload_membership_records(
            replica,
            MEMBERSHIP_INPUT_BATCH,
            &prefix,
            &records,
            &mut count,
        )
    }
    fn upload_membership_records(
        &self,
        replica: NodeId,
        kind: u8,
        prefix: &[u8],
        records: &[rsf_vsr::VsrHistoryRecord],
        count: &mut u64,
    ) -> Result<(), VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if *count > records.len() as u64 {
            return Err(fail("remote membership input exceeds descriptor".into()));
        }
        while *count < records.len() as u64 {
            let batch = rsf_vsr::VsrHistoryBatch::pack(*count, &records[*count as usize..])
                .map_err(|e| fail(e.message))?;
            let expected = *count + batch.records.len() as u64;
            let mut payload = prefix.to_vec();
            payload.extend_from_slice(&batch.encode().map_err(|e| fail(e.message))?);
            let next = self
                .request_io(replica, kind, &payload)
                .map_err(|e| fail(e.to_string()))?;
            if next != expected {
                return Err(fail(
                    "membership batch acknowledgement range mismatch".into(),
                ));
            }
            *count = next;
        }
        Ok(())
    }
}
