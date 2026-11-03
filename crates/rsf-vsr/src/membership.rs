//! Durable old-configuration approval. A fence does not activate new voters.
//! Report identities must be authenticated by the caller; this is crash-fault
//! quorum evidence, not independent node signatures.
use super::*;
mod authority;
mod capacity;
pub(crate) mod staging;
pub use staging::{
    MembershipStagingStore, VsrMembershipPublication, VsrMembershipPublishedQuorum,
    VsrMembershipPublishedReport, VsrMembershipStagedPlan, VsrMembershipStagedQuorum,
    VsrMembershipStagedReport,
};
mod decision;
use crate::transfer::MAX_HISTORY_RECORD_BYTES;
pub use decision::{
    MembershipAuthorityInbox, MembershipDecisionStore, VsrMembershipAccepted, VsrMembershipChosen,
    VsrMembershipChosenInput, VsrMembershipDecision, VsrMembershipDecisionState,
    VsrMembershipImportReply, VsrMembershipInput, VsrMembershipInputProgress, VsrMembershipPromise,
    VsrMembershipPromiseReply, VsrMembershipRound,
};
pub(super) const MAGIC: &[u8] = b"VSRMP1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipProposal {
    pub election: VsrViewChangeRequest,
    pub old_primary: NodeId,
    pub new_voters: Vec<NodeId>,
}
impl VsrMembershipProposal {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let election = self.election.encode()?;
        if !self.election.voters.contains(&self.old_primary)
            || self.new_voters.is_empty()
            || self.new_voters.len() > 4096
            || self.new_voters.windows(2).any(|pair| pair[0] >= pair[1])
            || !self.new_voters.contains(&self.election.candidate)
            || self.new_voters == self.election.voters
        {
            return Err(invalid_input("invalid membership transition proposal"));
        }
        let mut out = MAGIC.to_vec();
        out.extend_from_slice(&(election.len() as u32).to_le_bytes());
        out.extend_from_slice(&election);
        out.extend_from_slice(&self.old_primary.0.to_le_bytes());
        out.extend_from_slice(&(self.new_voters.len() as u32).to_le_bytes());
        for node in &self.new_voters {
            out.extend_from_slice(&node.0.to_le_bytes());
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 10 || bytes.len() > MAX_HISTORY_RECORD_BYTES || !bytes.starts_with(MAGIC) {
            return Err(corrupt("invalid membership proposal encoding"));
        }
        let length = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
        let end = 10usize
            .checked_add(length)
            .ok_or_else(|| corrupt("membership proposal length overflow"))?;
        let election = VsrViewChangeRequest::decode(
            bytes
                .get(10..end)
                .ok_or_else(|| corrupt("truncated membership election"))?,
        )?;
        let old_primary = NodeId(read_u64(bytes, end)?);
        let count = u32::from_le_bytes(
            bytes
                .get(end + 8..end + 12)
                .ok_or_else(|| corrupt("missing membership voter count"))?
                .try_into()
                .unwrap(),
        ) as usize;
        if count > 4096 || bytes.len() != end + 12 + count * 8 {
            return Err(corrupt("invalid membership voter bytes"));
        }
        let new_voters = (0..count)
            .map(|i| read_u64(bytes, end + 12 + i * 8).map(NodeId))
            .collect::<RsfResult<Vec<_>>>()?;
        let proposal = Self {
            election,
            old_primary,
            new_voters,
        };
        if proposal.encode()? != bytes {
            return Err(corrupt("noncanonical membership proposal"));
        }
        Ok(proposal)
    }
    pub fn id(&self) -> RsfResult<[u8; 32]> {
        Ok(rsf_core::Digest::blake3_bytes(&self.encode()?).bytes)
    }
    pub(super) fn validate_cell(&self, cell: &VsrCell) -> RsfResult<()> {
        self.encode()?;
        self.election.validate_cell(cell)?;
        if self.election.previous_view != cell.view || self.old_primary != cell.primary {
            return Err(invalid_input(
                "membership proposal differs from exact historical configuration",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipReport {
    pub proposal: VsrMembershipProposal,
    pub voter: VsrViewChangeReport,
}
impl VsrMembershipReport {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let proposal = self.proposal.encode()?;
        let report = self.voter.encode()?;
        if self.voter.request != self.proposal.election
            || self.voter.history.probe.view != self.proposal.election.previous_view
            || self.voter.history.probe.primary != self.proposal.old_primary
        {
            return Err(invalid_input(
                "membership report differs from proposed old authority",
            ));
        }
        let mut out = b"VSRMR1".to_vec();
        out.extend_from_slice(&(proposal.len() as u32).to_le_bytes());
        out.extend_from_slice(&proposal);
        out.extend_from_slice(&report);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 10
            || bytes.len() > MAX_HISTORY_RECORD_BYTES
            || !bytes.starts_with(b"VSRMR1")
        {
            return Err(corrupt("invalid membership report"));
        }
        let size = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
        let end = 10usize
            .checked_add(size)
            .ok_or_else(|| corrupt("membership report length overflow"))?;
        let proposal = VsrMembershipProposal::decode(
            bytes
                .get(10..end)
                .ok_or_else(|| corrupt("truncated membership proposal"))?,
        )?;
        let voter = VsrViewChangeReport::decode(
            bytes
                .get(end..)
                .ok_or_else(|| corrupt("truncated membership report"))?,
        )?;
        let report = Self { proposal, voter };
        if report.encode()? != bytes {
            return Err(corrupt("noncanonical membership report"));
        }
        Ok(report)
    }
}
#[derive(Clone, Debug)]
pub struct VsrMembershipSnapshot {
    pub report: VsrMembershipReport,
    pub records: Vec<VsrHistoryRecord>,
}
impl VsrMembershipSnapshot {
    pub fn validate(&self) -> RsfResult<()> {
        self.recover_state().map(|_| ())
    }
    fn recover_state(&self) -> RsfResult<replay::Recovered> {
        self.report.encode()?;
        let voter = &self.report.voter;
        let proposal_bytes = self.report.proposal.encode()?;
        if self.records.len() as u64 != voter.history.records
            || transfer::history_digest(&self.records)? != voter.history.digest
            || !self.records.last().is_some_and(|record| {
                record.kind == WalRecordKind::MembershipPromise
                    && record.epoch == voter.history.probe.view
                    && record.payload == proposal_bytes
            })
        {
            return Err(corrupt(
                "membership snapshot is not its exact durable fence",
            ));
        }
        let state = installation::recover_records(&self.records)?;
        if state.3.as_ref() != Some(&self.report.proposal)
            || state.2.as_ref() != Some(&self.report.proposal.election)
            || installation::probe_for_cell(&state.0)? != voter.history.probe
            || LogIndex(state.0.next_index - 1) != voter.accepted_tail
        {
            return Err(corrupt(
                "membership report differs from replayed authority and prefix",
            ));
        }
        Ok(state)
    }
}
#[derive(Clone, Debug)]
pub struct VsrMembershipApproval {
    proposal: VsrMembershipProposal,
    snapshots: Vec<VsrMembershipSnapshot>,
    // Snapshots have no mutable public access after construction. Retain their
    // replay-validated states instead of replaying full histories at every
    // authority check and merge. Disk reopen still constructs a fresh approval.
    states: Vec<replay::Recovered>,
}
impl VsrMembershipApproval {
    /// Authenticate every report's node and cluster before constructing approval.
    pub fn from_snapshots(snapshots: Vec<VsrMembershipSnapshot>) -> RsfResult<Self> {
        let proposal = snapshots
            .first()
            .ok_or_else(|| invalid_input("membership approval requires reports"))?
            .report
            .proposal
            .clone();
        let mut voters = BTreeSet::new();
        let mut states = Vec::new();
        for snapshot in &snapshots {
            states.push(snapshot.recover_state()?);
            if snapshot.report.proposal != proposal || !voters.insert(snapshot.report.voter.replica)
            {
                return Err(invalid_input(
                    "membership reports conflict or duplicate a voter",
                ));
            }
        }
        if voters.len() <= proposal.election.voters.len() / 2 {
            return Err(invalid_input(
                "membership approval lacks an old voting quorum",
            ));
        }
        let source = snapshots
            .iter()
            .enumerate()
            .max_by_key(|(_, snapshot)| {
                (
                    snapshot.report.voter.accepted_tail,
                    std::cmp::Reverse(snapshot.report.voter.replica),
                )
            })
            .map(|(position, _)| position)
            .expect("old quorum");
        let mut clients = BTreeMap::new();
        for (position, state) in states.iter().enumerate() {
            if snapshots[position].records.first() != snapshots[source].records.first()
                || !states[source].1.compatible(&state.1)
            {
                return Err(corrupt(
                    "membership reports disagree on historical authority",
                ));
            }
            for (index, pending) in &state.0.pending {
                if !states[source].0.pending.get(index).is_some_and(|selected| {
                    installation::same_operation(&selected.prepare, &pending.prepare)
                }) {
                    return Err(corrupt("membership quorum accepted histories conflict"));
                }
            }
            for (client, index) in &state.0.client_table {
                if clients
                    .insert(client.clone(), *index)
                    .is_some_and(|previous| previous != *index)
                {
                    return Err(corrupt("membership quorum client histories conflict"));
                }
            }
        }
        Ok(Self {
            proposal,
            snapshots,
            states,
        })
    }
    pub fn proposal(&self) -> &VsrMembershipProposal {
        &self.proposal
    }
    pub fn snapshots(&self) -> &[VsrMembershipSnapshot] {
        &self.snapshots
    }
}
impl DurableVsrCell {
    pub fn membership_fence(&self) -> Option<&VsrMembershipProposal> {
        self.membership_fence.as_ref()
    }
    /// Only the current old primary may initiate a new immutable proposal.
    /// This acknowledges a durable fence, never new voting authority.
    pub fn begin_membership(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        proposal: VsrMembershipProposal,
    ) -> RsfResult<VsrMembershipSnapshot> {
        self.begin_membership_with_hook(replica, sender, proposal, || {})
    }
    fn begin_membership_with_hook(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        proposal: VsrMembershipProposal,
        after_synced: impl FnOnce(),
    ) -> RsfResult<VsrMembershipSnapshot> {
        self.ensure_healthy()?;
        proposal.validate_cell(&self.cell)?;
        if sender != self.cell.primary
            || !self.cell.replicas.contains(&replica)
            || !self.bootstrap_persisted
        {
            return Err(invalid_input(
                "membership proposal requires authenticated old primary and voter",
            ));
        }
        if self.membership_fence.is_none() {
            // Reject unpublishable nested proofs before persisting any fence.
            // Exact retries and recovery of historical fences remain available.
            proposal.validate_publication_capacity()?;
        }
        self.persist_membership_fence(&proposal, after_synced)?;
        self.membership_snapshot(replica, sender)
    }
    /// Continue an existing old-primary proposal reported directly by an
    /// authenticated old voter. The caller must bind sender to the report's
    /// authenticated node and cluster; report bytes are not signatures.
    /// This persists only the exact fence and does not copy application state,
    /// grant new votes, abort another promise or admit clients.
    pub fn resume_membership(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        report: &VsrMembershipReport,
    ) -> RsfResult<VsrMembershipSnapshot> {
        self.resume_membership_with_hook(replica, sender, report, || {})
    }
    fn resume_membership_with_hook(
        &mut self,
        replica: NodeId,
        sender: NodeId,
        report: &VsrMembershipReport,
        after_synced: impl FnOnce(),
    ) -> RsfResult<VsrMembershipSnapshot> {
        self.ensure_healthy()?;
        report.encode()?;
        report.proposal.validate_cell(&self.cell)?;
        if report.voter.replica != sender
            || !self.cell.replicas.contains(&sender)
            || !self.cell.replicas.contains(&replica)
            || !self.bootstrap_persisted
        {
            return Err(invalid_input(
                "membership continuation requires the authenticated reporting old voter",
            ));
        }
        self.persist_membership_fence(&report.proposal, after_synced)?;
        self.membership_snapshot(replica, sender)
    }
    fn persist_membership_fence(
        &mut self,
        proposal: &VsrMembershipProposal,
        after_synced: impl FnOnce(),
    ) -> RsfResult<()> {
        if let Some(previous) = &self.membership_fence {
            if previous != proposal {
                return Err(invalid_input(
                    "membership proposal conflicts with durable fence",
                ));
            }
        } else {
            if self.view_change.is_some() {
                return Err(invalid_input(
                    "membership proposal cannot replace an election fence",
                ));
            }
            self.append_control(WalRecordKind::MembershipPromise, &proposal.encode()?)?;
            self.view_change = Some(proposal.election.clone());
            self.membership_fence = Some(proposal.clone());
            after_synced();
        }
        Ok(())
    }
    /// Any authenticated old voter may recover an existing fence and its complete
    /// history after loss of the initiating primary. This cannot create a vote.
    pub fn membership_snapshot(
        &mut self,
        replica: NodeId,
        sender: NodeId,
    ) -> RsfResult<VsrMembershipSnapshot> {
        self.ensure_healthy()?;
        let proposal = self
            .membership_fence
            .clone()
            .ok_or_else(|| invalid_input("no durable membership fence"))?;
        if !proposal.election.voters.contains(&sender)
            || !proposal.election.voters.contains(&replica)
        {
            return Err(invalid_input(
                "membership snapshot requires authenticated old voters",
            ));
        }
        let history = self.history_snapshot_for_recovery()?;
        let snapshot = VsrMembershipSnapshot {
            report: VsrMembershipReport {
                proposal: proposal.clone(),
                voter: VsrViewChangeReport {
                    request: proposal.election,
                    replica,
                    accepted_tail: LogIndex(self.cell.next_index - 1),
                    history: history.descriptor,
                },
            },
            records: history.records,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests;
