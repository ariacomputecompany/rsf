//! Single exchanges for the existing round protocol. Bulk APIs share these
//! checks; an incremental scheduler keeps its own bounded cursor and ballot.
use super::*;

impl NetworkVsrTransport {
    fn membership_round_prefix(
        &self,
        replica: NodeId,
        input: &VsrMembershipInput,
        round: VsrMembershipRound,
    ) -> Result<Vec<u8>, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if round.counter == 0
            || round.coordinator != self.local
            || !input.proposal().new_voters.contains(&self.local)
            || !input.proposal().new_voters.contains(&replica)
        {
            return Err(fail(
                "membership round endpoints differ from authenticated coordinator/proposed voters"
                    .into(),
            ));
        }
        Ok(context(
            input.proposal().election.pg,
            round,
            input.proposal().id().map_err(|e| fail(e.message))?,
        ))
    }
    pub fn begin_membership_input(
        &self,
        replica: NodeId,
        round: VsrMembershipRound,
        input: &VsrMembershipInput,
    ) -> Result<u64, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let mut payload = self.membership_round_prefix(replica, input, round)?;
        payload.extend_from_slice(&input.encode().map_err(|e| fail(e.message))?);
        let count = self
            .request_io(replica, MEMBERSHIP_INPUT_BEGIN, &payload)
            .map_err(|e| fail(e.to_string()))?;
        if count > input.records().map_err(|e| fail(e.message))? {
            return Err(fail("membership begin exceeds descriptor".into()));
        }
        Ok(count)
    }
    pub fn append_membership_input(
        &self,
        replica: NodeId,
        round: VsrMembershipRound,
        input: &VsrMembershipInput,
        batch: rsf_vsr::VsrHistoryBatch,
    ) -> Result<u64, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let mut payload = self.membership_round_prefix(replica, input, round)?;
        payload.extend_from_slice(&input.id().map_err(|e| fail(e.message))?);
        let end = batch
            .ordinal
            .checked_add(batch.records.len() as u64)
            .ok_or_else(|| fail("membership batch range overflow".into()))?;
        let total = input.records().map_err(|e| fail(e.message))?;
        if end > total {
            return Err(fail("membership batch exceeds descriptor".into()));
        }
        payload.extend_from_slice(&batch.encode().map_err(|e| fail(e.message))?);
        let count = self
            .request_io(replica, MEMBERSHIP_INPUT_BATCH, &payload)
            .map_err(|e| fail(e.to_string()))?;
        if count < end || count > total {
            return Err(fail(
                "membership append acknowledgement range mismatch".into(),
            ));
        }
        Ok(count)
    }
    pub fn accept_membership(
        &self,
        replica: NodeId,
        decision: &VsrMembershipDecision,
        nomination: &VsrMembershipAccepted,
    ) -> Result<VsrMembershipAccepted, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        self.membership_round_prefix(replica, decision.input(), decision.round())?;
        let input = decision.input().id().map_err(|e| fail(e.message))?;
        if nomination.replica != self.local
            || nomination.round != decision.round()
            || nomination.input != input
            || nomination.proposal != *decision.input().proposal()
        {
            return Err(fail(
                "membership nomination differs from local coordinator decision".into(),
            ));
        }
        let payload = acceptance_request(decision, nomination).map_err(|e| fail(e.message))?;
        let bytes = self
            .exchange_io(replica, MEMBERSHIP_ACCEPT, &payload)
            .map_err(|e| fail(e.to_string()))?;
        let report = VsrMembershipAccepted::decode(&bytes).map_err(|e| fail(e.message))?;
        if report.replica != replica
            || report.proposal != *decision.input().proposal()
            || report.round != decision.round()
            || report.input != input
        {
            return Err(fail(
                "membership acceptance differs from authenticated reporter or decision".into(),
            ));
        }
        Ok(report)
    }
    pub fn learn_membership_chosen(
        &self,
        replica: NodeId,
        chosen: &VsrMembershipChosen,
    ) -> Result<(), VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if !chosen.proposal().new_voters.contains(&self.local)
            || !chosen.proposal().new_voters.contains(&replica)
        {
            return Err(fail(
                "membership choice requires proposed-voter endpoints".into(),
            ));
        }
        let bytes = self
            .exchange_io(
                replica,
                MEMBERSHIP_CHOSEN,
                &chosen.encode().map_err(|e| fail(e.message))?,
            )
            .map_err(|e| fail(e.to_string()))?;
        if bytes != chosen.input_id() {
            return Err(fail(
                "membership chosen acknowledgement differs from input".into(),
            ));
        }
        Ok(())
    }
}
