//! Authenticated old-quorum membership fences. Approval does not install new
//! voters, promote a learner or permit application traffic in the proposed view.
use super::*;
mod decision;
pub use decision::recovery::{PeerMembershipRecoveryTiming, PeerMembershipRecoveryUpdate};
pub(super) use decision::{anchor::LearnerAnchor, recovery::Recovery};
use rsf_vsr::{
    VsrMembershipApproval, VsrMembershipProposal, VsrMembershipReport, VsrMembershipSnapshot,
};

impl NetworkVsrTransport {
    /// Persist the exact old-primary proposal and collect replay-checked old
    /// votes. Success is approval only, not installation or client readiness.
    pub fn approve_membership(
        &self,
        local: &mut DurableVsrCell,
        proposal: &VsrMembershipProposal,
    ) -> Result<VsrMembershipApproval, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        if self.local != proposal.old_primary {
            return Err(fail(
                "only the authenticated old primary may initiate membership approval".into(),
            ));
        }
        let snapshot = local
            .begin_membership(self.local, self.local, proposal.clone())
            .map_err(|error| fail(error.message))?;
        let mut snapshots = vec![snapshot];
        let mut failures = Vec::new();
        let payload = proposal.encode().map_err(|error| fail(error.message))?;
        for replica in proposal
            .election
            .voters
            .iter()
            .copied()
            .filter(|node| *node != self.local)
        {
            let result = self
                .exchange_io(replica, MEMBERSHIP_BEGIN, &payload)
                .map_err(|error| VsrTransportError {
                    replica,
                    reason: error.to_string(),
                })
                .and_then(|bytes| self.membership_records(replica, proposal.election.pg, &bytes));
            match result {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => failures.push(format!("{}: {}", replica.0, error.reason)),
            }
        }
        VsrMembershipApproval::from_snapshots(snapshots)
            .map_err(|error| fail(format!("{}: {}", error.message, failures.join("; "))))
    }
    pub fn fetch_membership_snapshot(
        &self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<VsrMembershipSnapshot, VsrTransportError> {
        let response = self
            .exchange_io(replica, MEMBERSHIP_SNAPSHOT, &pg.0.to_le_bytes())
            .map_err(|error| VsrTransportError {
                replica,
                reason: error.to_string(),
            })?;
        self.membership_records(replica, pg, &response)
    }
    /// Recover an already approved exact proposal from authenticated old voters,
    /// without the original primary. It cannot create or replace a fence.
    pub fn recover_membership_approval(
        &self,
        local: &mut DurableVsrCell,
        proposal: &VsrMembershipProposal,
    ) -> Result<VsrMembershipApproval, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        if !proposal.election.voters.contains(&self.local) {
            return Err(fail("membership recovery requires an old voter".into()));
        }
        let mut snapshots = Vec::new();
        let mut failures = Vec::new();
        for replica in &proposal.election.voters {
            let result = if *replica == self.local {
                local
                    .membership_snapshot(self.local, self.local)
                    .map_err(|error| VsrTransportError {
                        replica: *replica,
                        reason: error.message,
                    })
            } else {
                self.fetch_membership_snapshot(*replica, proposal.election.pg)
            };
            match result {
                Ok(snapshot) if snapshot.report.proposal == *proposal => snapshots.push(snapshot),
                Ok(_) => {
                    return Err(fail(
                        "membership recovery reports a conflicting proposal".into(),
                    ));
                }
                Err(error) => failures.push(format!("{}: {}", replica.0, error.reason)),
            }
        }
        VsrMembershipApproval::from_snapshots(snapshots)
            .map_err(|error| fail(format!("{}: {}", error.message, failures.join("; "))))
    }
    /// Complete an interrupted proposal using an existing authenticated old
    /// voter's report. A survivor may adopt the report locally and relay its own
    /// durable fence; no replacement or unfenced proposal is initiated here.
    /// Success remains old-quorum approval, not new-voter activation.
    pub fn resume_membership_approval(
        &self,
        local: &mut DurableVsrCell,
        proposal: &VsrMembershipProposal,
    ) -> Result<VsrMembershipApproval, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        if !proposal.election.voters.contains(&self.local) {
            return Err(fail("membership continuation requires an old voter".into()));
        }
        if let Some(existing) = local.membership_fence() {
            if existing != proposal {
                return Err(fail(
                    "membership continuation conflicts with local fence".into(),
                ));
            }
        } else {
            let mut source = None;
            let mut failures = Vec::new();
            for replica in proposal
                .election
                .voters
                .iter()
                .copied()
                .filter(|node| *node != self.local)
            {
                match self.fetch_membership_snapshot(replica, proposal.election.pg) {
                    Ok(snapshot) => {
                        if snapshot.report.proposal != *proposal {
                            return Err(fail(
                                "membership continuation reports a conflicting proposal".into(),
                            ));
                        }
                        source = Some((replica, snapshot));
                        break;
                    }
                    Err(error) => failures.push(format!("{}: {}", replica.0, error.reason)),
                }
            }
            let (replica, snapshot) = source.ok_or_else(|| {
                fail(format!(
                    "membership continuation has no surviving durable report: {}",
                    failures.join("; ")
                ))
            })?;
            local
                .resume_membership(self.local, replica, &snapshot.report)
                .map_err(|error| fail(error.message))?;
        }
        let snapshot = local
            .membership_snapshot(self.local, self.local)
            .map_err(|error| fail(error.message))?;
        let payload = snapshot
            .report
            .encode()
            .map_err(|error| fail(error.message))?;
        let mut snapshots = vec![snapshot];
        let mut failures = Vec::new();
        for replica in proposal
            .election
            .voters
            .iter()
            .copied()
            .filter(|node| *node != self.local)
        {
            let result = self
                .exchange_io(replica, MEMBERSHIP_RESUME, &payload)
                .map_err(|error| VsrTransportError {
                    replica,
                    reason: error.to_string(),
                })
                .and_then(|bytes| self.membership_records(replica, proposal.election.pg, &bytes));
            match result {
                Ok(snapshot) if snapshot.report.proposal == *proposal => snapshots.push(snapshot),
                Ok(_) => {
                    return Err(fail(
                        "membership continuation reports a conflicting proposal".into(),
                    ));
                }
                Err(error) => failures.push(format!("{}: {}", replica.0, error.reason)),
            }
        }
        VsrMembershipApproval::from_snapshots(snapshots)
            .map_err(|error| fail(format!("{}: {}", error.message, failures.join("; "))))
    }
    fn membership_records(
        &self,
        replica: NodeId,
        pg: PlacementGroupId,
        bytes: &[u8],
    ) -> Result<VsrMembershipSnapshot, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let report = VsrMembershipReport::decode(bytes).map_err(|error| fail(error.message))?;
        if report.proposal.election.pg != pg
            || report.voter.replica != replica
            || !report.proposal.election.voters.contains(&self.local)
        {
            return Err(fail(
                "membership report differs from authenticated old voter and group".into(),
            ));
        }
        let mut query = report.encode().map_err(|error| fail(error.message))?;
        let prefix = query.len();
        let mut records = Vec::new();
        while (records.len() as u64) < report.voter.history.records {
            let ordinal = records.len() as u64;
            query.truncate(prefix);
            query.extend_from_slice(&ordinal.to_le_bytes());
            let response = self
                .exchange_io(replica, MEMBERSHIP_BATCH, &query)
                .map_err(|error| fail(error.to_string()))?;
            let batch =
                rsf_vsr::VsrHistoryBatch::decode(&response).map_err(|error| fail(error.message))?;
            if batch.ordinal != ordinal
                || ordinal + batch.records.len() as u64 > report.voter.history.records
            {
                return Err(fail("membership history batch range mismatch".into()));
            }
            records.extend(batch.records);
        }
        let snapshot = VsrMembershipSnapshot { report, records };
        snapshot.validate().map_err(|error| fail(error.message))?;
        Ok(snapshot)
    }
}
impl PeerReplicaServer {
    pub(super) fn dispatch_membership(
        &mut self,
        sender: NodeId,
        kind: u8,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        let rsf = |error: rsf_core::RsfError| invalid(error.message);
        if sender == self.local {
            return Err(invalid(
                "membership RPC requires a remote authenticated voter",
            ));
        }
        let (pg, proposal, resumed, expected) = match kind {
            MEMBERSHIP_BEGIN => {
                let proposal = VsrMembershipProposal::decode(payload).map_err(rsf)?;
                (proposal.election.pg, Some(proposal), None, None)
            }
            MEMBERSHIP_RESUME => {
                let report = VsrMembershipReport::decode(payload).map_err(rsf)?;
                (report.proposal.election.pg, None, Some(report), None)
            }
            MEMBERSHIP_SNAPSHOT => {
                if payload.len() != 8 {
                    return Err(invalid("invalid membership snapshot query"));
                }
                (PlacementGroupId(u64_at(payload, 0)?), None, None, None)
            }
            MEMBERSHIP_BATCH => {
                let end = payload
                    .len()
                    .checked_sub(8)
                    .ok_or_else(|| invalid("truncated membership batch query"))?;
                let report = VsrMembershipReport::decode(&payload[..end]).map_err(rsf)?;
                let ordinal = u64_at(payload, end)?;
                (
                    report.proposal.election.pg,
                    None,
                    None,
                    Some((report, ordinal)),
                )
            }
            _ => return Err(invalid("unknown membership RPC")),
        };
        if let Some(proposal) = proposal
            .as_ref()
            .or_else(|| resumed.as_ref().map(|report| &report.proposal))
        {
            self.control_membership_mutation(pg, Some(proposal))
                .map_err(invalid)?;
        }
        let cell = self
            .cells
            .get_mut(&pg)
            .ok_or_else(|| invalid("membership group is not hosted as an old voter"))?;
        if !cell.contains_replica(sender) {
            return Err(invalid("membership requester is not an old voter"));
        }
        if let Some(proposal) = proposal {
            let snapshot = cell
                .begin_membership(self.local, sender, proposal)
                .map_err(rsf)?;
            let bytes = snapshot.report.encode().map_err(rsf)?;
            self.election_snapshots.remove(&pg);
            self.membership_snapshots.insert(pg, snapshot);
            return Ok(bytes);
        }
        if let Some(report) = resumed {
            let snapshot = cell
                .resume_membership(self.local, sender, &report)
                .map_err(rsf)?;
            let bytes = snapshot.report.encode().map_err(rsf)?;
            self.election_snapshots.remove(&pg);
            self.membership_snapshots.insert(pg, snapshot);
            return Ok(bytes);
        }
        if let Some((report, ordinal)) = expected {
            if report.voter.replica != self.local
                || cell.membership_fence() != Some(&report.proposal)
            {
                return Err(invalid("membership batch differs from local durable fence"));
            }
            if !self.membership_snapshots.contains_key(&pg) {
                self.membership_snapshots.insert(
                    pg,
                    cell.membership_snapshot(self.local, sender).map_err(rsf)?,
                );
            }
            let snapshot = &self.membership_snapshots[&pg];
            if snapshot.report != report {
                return Err(invalid("membership batch report is not the frozen history"));
            }
            let start = usize::try_from(ordinal)
                .map_err(|_| invalid("membership ordinal exceeds platform range"))?;
            let records = snapshot
                .records
                .get(start..)
                .ok_or_else(|| invalid("membership ordinal exceeds history"))?;
            return rsf_vsr::VsrHistoryBatch::pack(ordinal, records)
                .and_then(|batch| batch.encode())
                .map_err(rsf);
        }
        let snapshot = cell.membership_snapshot(self.local, sender).map_err(rsf)?;
        let bytes = snapshot.report.encode().map_err(rsf)?;
        self.membership_snapshots.insert(pg, snapshot);
        Ok(bytes)
    }
}
