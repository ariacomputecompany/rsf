use super::*;

impl NetworkVsrTransport {
    fn decision_exchange(
        &self,
        replica: NodeId,
        proposal: &VsrMembershipProposal,
        bootstrap: [u8; 32],
        tag: u8,
        body: &[u8],
    ) -> Result<Vec<u8>, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if !proposal.new_voters.contains(&self.local) || !proposal.new_voters.contains(&replica) {
            return Err(fail(
                "membership recovery requires proposed-voter endpoints".into(),
            ));
        }
        let request = request(tag, proposal, bootstrap, body).map_err(fail)?;
        let bytes = self
            .exchange_io(replica, MEMBERSHIP_RECOVERY, &request)
            .map_err(|e| fail(e.to_string()))?;
        reply(&bytes, tag).map(|bytes| bytes.to_vec()).map_err(fail)
    }

    pub fn membership_decision_state(
        &self,
        replica: NodeId,
        proposal: &VsrMembershipProposal,
        bootstrap: [u8; 32],
    ) -> Result<VsrMembershipDecisionState, VsrTransportError> {
        let bytes = self.decision_exchange(replica, proposal, bootstrap, STATE, &[])?;
        let state = VsrMembershipDecisionState::decode(&bytes).map_err(|e| VsrTransportError {
            replica,
            reason: e.message,
        })?;
        check_state(&state, replica, proposal, bootstrap)?;
        Ok(state)
    }
    pub fn promise_membership_recovery(
        &self,
        replica: NodeId,
        proposal: &VsrMembershipProposal,
        bootstrap: [u8; 32],
        round: VsrMembershipRound,
    ) -> Result<VsrMembershipPromiseReply, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if round.coordinator != self.local {
            return Err(fail(
                "membership promise coordinator differs from local sender".into(),
            ));
        }
        let mut body = round.counter.to_le_bytes().to_vec();
        body.extend_from_slice(&round.coordinator.0.to_le_bytes());
        let bytes = self.decision_exchange(replica, proposal, bootstrap, PROMISE, &body)?;
        let reply = VsrMembershipPromiseReply::decode(&bytes).map_err(|e| fail(e.message))?;
        match &reply {
            VsrMembershipPromiseReply::Granted(p)
                if p.replica == replica && p.proposal == *proposal && p.round == round => {}
            VsrMembershipPromiseReply::Preempted(state) => {
                check_state(state, replica, proposal, bootstrap)?;
                if state.promised.is_none_or(|current| current <= round) {
                    return Err(fail(
                        "membership preemption does not exceed requested round".into(),
                    ));
                }
            }
            _ => {
                return Err(fail(
                    "membership promise reply differs from request/reporter".into(),
                ));
            }
        }
        Ok(reply)
    }
    pub fn query_membership_chosen(
        &self,
        replica: NodeId,
        proposal: &VsrMembershipProposal,
        bootstrap: [u8; 32],
        id: [u8; 32],
    ) -> Result<Option<VsrMembershipChosen>, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let bytes = self.decision_exchange(replica, proposal, bootstrap, CHOSEN, &id)?;
        match bytes.first() {
            Some(0) if bytes.len() == 1 => Ok(None),
            Some(1) => {
                let chosen =
                    VsrMembershipChosen::decode(&bytes[1..]).map_err(|e| fail(e.message))?;
                if chosen.proposal() != proposal || chosen.input_id() != id {
                    return Err(fail("chosen response differs from bound query".into()));
                }
                Ok(Some(chosen))
            }
            _ => Err(fail("invalid membership chosen response".into())),
        }
    }
    pub fn membership_input_progress(
        &self,
        replica: NodeId,
        proposal: &VsrMembershipProposal,
        bootstrap: [u8; 32],
        id: [u8; 32],
    ) -> Result<VsrMembershipInputProgress, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let bytes = self.decision_exchange(replica, proposal, bootstrap, INPUT, &id)?;
        let value = VsrMembershipInputProgress::decode(&bytes).map_err(|e| fail(e.message))?;
        match &value {
            VsrMembershipInputProgress::Missing | VsrMembershipInputProgress::Initializing => {}
            VsrMembershipInputProgress::Partial { input, .. }
            | VsrMembershipInputProgress::Complete { input } => {
                if input.proposal() != proposal || input.id().map_err(|e| fail(e.message))? != id {
                    return Err(fail("membership input progress differs from query".into()));
                }
            }
        }
        Ok(value)
    }
    /// Read one retained prefix batch. The caller must replay the complete input
    /// before treating it as authority; bytes are not an application receipt.
    pub fn membership_input_batch(
        &self,
        replica: NodeId,
        input: &VsrMembershipInput,
        bootstrap: [u8; 32],
        ordinal: u64,
    ) -> Result<rsf_vsr::VsrHistoryBatch, VsrTransportError> {
        self.read_membership_input_batch(replica, input, bootstrap, None, ordinal)
    }
    /// Scheduler-only copy activity carries its exact durable round. Ordinary
    /// read-only observers never affect coordinator failure detection.
    pub(in crate::replication) fn membership_round_input_batch(
        &self,
        replica: NodeId,
        input: &VsrMembershipInput,
        bootstrap: [u8; 32],
        round: VsrMembershipRound,
        ordinal: u64,
    ) -> Result<rsf_vsr::VsrHistoryBatch, VsrTransportError> {
        if round.coordinator != self.local || round.counter == 0 {
            return Err(VsrTransportError {
                replica,
                reason: "membership active read has another coordinator".into(),
            });
        }
        self.read_membership_input_batch(replica, input, bootstrap, Some(round), ordinal)
    }
    fn read_membership_input_batch(
        &self,
        replica: NodeId,
        input: &VsrMembershipInput,
        bootstrap: [u8; 32],
        round: Option<VsrMembershipRound>,
        ordinal: u64,
    ) -> Result<rsf_vsr::VsrHistoryBatch, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let count = input.records().map_err(|e| fail(e.message))?;
        if ordinal >= count {
            return Err(fail("membership read ordinal exceeds descriptor".into()));
        }
        let mut body = input.id().map_err(|e| fail(e.message))?.to_vec();
        body.extend_from_slice(&ordinal.to_le_bytes());
        if let Some(round) = round {
            body.extend_from_slice(&round.counter.to_le_bytes());
            body.extend_from_slice(&round.coordinator.0.to_le_bytes());
        }
        let bytes = self.decision_exchange(replica, input.proposal(), bootstrap, READ, &body)?;
        let batch = rsf_vsr::VsrHistoryBatch::decode(&bytes).map_err(|e| fail(e.message))?;
        if batch.ordinal != ordinal
            || ordinal
                .checked_add(batch.records.len() as u64)
                .is_none_or(|end| end > count)
        {
            return Err(fail(
                "membership read batch differs from requested range".into(),
            ));
        }
        Ok(batch)
    }
    pub fn begin_membership_chosen_input(
        &self,
        replica: NodeId,
        chosen: &VsrMembershipChosen,
        input: &VsrMembershipInput,
        bootstrap: [u8; 32],
    ) -> Result<VsrMembershipImportReply, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let package = VsrMembershipChosenInput::new(chosen.clone(), input.clone())
            .and_then(|v| v.encode())
            .map_err(|e| fail(e.message))?;
        let bytes =
            self.decision_exchange(replica, input.proposal(), bootstrap, IMPORT_BEGIN, &package)?;
        check_import(&bytes, replica, input, IMPORT_BEGIN, None)
    }
    pub fn append_membership_chosen_input(
        &self,
        replica: NodeId,
        input: &VsrMembershipInput,
        bootstrap: [u8; 32],
        batch: rsf_vsr::VsrHistoryBatch,
    ) -> Result<VsrMembershipImportReply, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let end = batch
            .ordinal
            .checked_add(batch.records.len() as u64)
            .ok_or_else(|| fail("membership batch range overflow".into()))?;
        if end > input.records().map_err(|e| fail(e.message))? {
            return Err(fail("membership batch exceeds descriptor".into()));
        }
        let mut body = input.id().map_err(|e| fail(e.message))?.to_vec();
        body.extend_from_slice(&batch.encode().map_err(|e| fail(e.message))?);
        let bytes =
            self.decision_exchange(replica, input.proposal(), bootstrap, IMPORT_BATCH, &body)?;
        check_import(&bytes, replica, input, IMPORT_BATCH, Some(end))
    }
    pub fn finish_membership_chosen_input(
        &self,
        replica: NodeId,
        input: &VsrMembershipInput,
        bootstrap: [u8; 32],
    ) -> Result<VsrMembershipImportReply, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let bytes = self.decision_exchange(
            replica,
            input.proposal(),
            bootstrap,
            IMPORT_FINISH,
            &input.id().map_err(|e| fail(e.message))?,
        )?;
        check_import(&bytes, replica, input, IMPORT_FINISH, None)
    }
}

fn check_state(
    state: &VsrMembershipDecisionState,
    replica: NodeId,
    proposal: &VsrMembershipProposal,
    bootstrap: [u8; 32],
) -> Result<(), VsrTransportError> {
    let fail = |reason: String| VsrTransportError { replica, reason };
    state.validate_for(proposal).map_err(|e| fail(e.message))?;
    if state.replica != replica || state.bootstrap != bootstrap {
        return Err(fail(
            "decision metadata differs from authenticated reporter or bound bootstrap".into(),
        ));
    }
    Ok(())
}
fn check_import(
    bytes: &[u8],
    replica: NodeId,
    input: &VsrMembershipInput,
    tag: u8,
    end: Option<u64>,
) -> Result<VsrMembershipImportReply, VsrTransportError> {
    let fail = |reason: String| VsrTransportError { replica, reason };
    let value = VsrMembershipImportReply::decode(bytes).map_err(|e| fail(e.message))?;
    let id = input.id().map_err(|e| fail(e.message))?;
    let total = input.records().map_err(|e| fail(e.message))?;
    match &value {
        VsrMembershipImportReply::BeginRequired if tag != IMPORT_BEGIN => {}
        VsrMembershipImportReply::Retained { input, records }
            if *input == id
                && *records <= total
                && end.is_none_or(|end| *records >= end)
                && tag != IMPORT_FINISH => {}
        VsrMembershipImportReply::Chosen { input } if *input == id => {}
        _ => {
            return Err(fail(
                "chosen import response differs from input or operation bounds".into(),
            ));
        }
    }
    Ok(value)
}
