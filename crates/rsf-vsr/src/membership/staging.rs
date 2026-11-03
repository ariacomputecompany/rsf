//! Chosen history staging. No method exposes a writable application voter.
use super::decision::{Reader, put_bytes, put_u64};
use super::*;
use rsf_core::Digest;
pub(crate) mod activation;
pub use activation::VsrMembershipPublication;
mod store;
pub use store::MembershipStagingStore;
mod published;
pub use published::{VsrMembershipPublishedQuorum, VsrMembershipPublishedReport};
mod proof;
pub use proof::{VsrMembershipStagedQuorum, VsrMembershipStagedReport};

#[derive(Debug)]
pub struct VsrMembershipStagedPlan {
    chosen: VsrMembershipChosen,
    input: VsrMembershipInput,
    approval: VsrMembershipApproval,
    records: Vec<VsrHistoryRecord>,
    recovered: replay::Recovered,
    id: [u8; 32],
}
impl VsrMembershipStagedPlan {
    /// Authenticate old-quorum input and the new quorum's chosen proof before
    /// using them. This crash-fault evidence does not itself activate membership.
    pub fn select(chosen: VsrMembershipChosen, approval: VsrMembershipApproval) -> RsfResult<Self> {
        chosen.encode()?;
        let input = VsrMembershipInput::from_approval(&approval)?;
        if chosen.proposal() != approval.proposal() || chosen.input_id() != input.id()? {
            return Err(invalid_input(
                "staged membership proof differs from chosen input",
            ));
        }
        let source = approval
            .snapshots()
            .iter()
            .enumerate()
            .max_by_key(|(_, s)| {
                (
                    s.report.voter.accepted_tail,
                    std::cmp::Reverse(s.report.voter.replica),
                )
            })
            .map(|(n, _)| n)
            .ok_or_else(|| invalid_input("staged membership input has no source"))?;
        let epoch = approval.proposal().election.previous_view;
        let (recovered, deltas) = installation::merge_quorum(
            approval.states.clone(),
            source,
            approval.proposal().election.pg,
            epoch,
            WalRecordKind::ViewInstallation,
        )?;
        let mut records = approval.snapshots()[source].records.clone();
        records.extend(deltas);
        let mut identity = input.id()?.to_vec();
        identity.extend_from_slice(transfer::history_digest(&records)?.as_bytes());
        let id = Digest::blake3_bytes(&identity).bytes;
        Ok(Self {
            chosen,
            input,
            approval,
            records,
            recovered,
            id,
        })
    }
    pub fn id(&self) -> [u8; 32] {
        self.id
    }
    pub fn approval(&self) -> &VsrMembershipApproval {
        &self.approval
    }
    pub fn input(&self) -> &VsrMembershipInput {
        &self.input
    }
    pub fn chosen(&self) -> &VsrMembershipChosen {
        &self.chosen
    }
    pub fn committed_tail(&self) -> LogIndex {
        self.recovered.0.committed_tail()
    }
    pub fn accepted_tail(&self) -> LogIndex {
        LogIndex(self.recovered.0.next_index - 1)
    }
    pub fn committed_entries(&self) -> RsfResult<Vec<VsrCommittedEntry>> {
        self.recovered
            .0
            .committed
            .iter()
            .map(|commit| {
                let prepare = self
                    .recovered
                    .0
                    .pending
                    .get(&commit.index)
                    .ok_or_else(|| corrupt("staged committed entry lacks selected prepare"))?
                    .prepare
                    .clone();
                Ok(VsrCommittedEntry {
                    prepare,
                    commit: commit.clone(),
                })
            })
            .collect()
    }
    pub fn accepted_entries(&self) -> Vec<VsrPrepare> {
        self.recovered
            .0
            .pending
            .values()
            .map(|p| p.prepare.clone())
            .collect()
    }
    pub fn request_commit(&self, request: &VsrClientRequestId) -> Option<LogIndex> {
        self.recovered.0.client_table.get(request).copied()
    }
    /// Extend only receiver-local request mappings already backed by the selected
    /// committed prefix. The canonical plan ID remains the chosen input's merge;
    /// the physical stage digest additionally binds these local cache entries.
    fn retain_anchor(&mut self, anchor: &DurableVsrCell) -> RsfResult<()> {
        anchor.validate_membership_authority(&self.approval)?;
        if !installation::preserves_committed(&self.recovered.0, &anchor.cell) {
            return Err(corrupt(
                "membership stage would drop a locally committed operation",
            ));
        }
        for (request, index) in &anchor.cell.client_table {
            if self.recovered.0.client_table.get(request) == Some(index) {
                continue;
            }
            let record = VsrHistoryRecord {
                epoch: self.approval.proposal().election.previous_view,
                kind: WalRecordKind::ViewInstallation,
                payload: installation::membership_client_delta(&VsrClientRequestCommit {
                    pg: anchor.cell.pg,
                    request: request.clone(),
                    index: *index,
                })?,
            };
            installation::apply_membership_delta(
                &mut self.recovered.0,
                &mut self.recovered.1,
                &record.payload,
            )?;
            self.records.push(record);
        }
        Ok(())
    }
    fn apply_local_suffix(&mut self, records: &[VsrHistoryRecord]) -> RsfResult<()> {
        if !records.starts_with(&self.records) {
            return Err(corrupt(
                "membership stage differs from deterministic chosen history",
            ));
        }
        for record in &records[self.records.len()..] {
            if record.kind != WalRecordKind::ViewInstallation
                || record.epoch != self.approval.proposal().election.previous_view
                || !record.payload.starts_with(b"VSIT1")
            {
                return Err(corrupt(
                    "membership stage local suffix is not a receipt delta",
                ));
            }
            let client = decode_client_request_commit(&record.payload[5..])?;
            if self.recovered.0.client_table.contains_key(&client.request) {
                return Err(corrupt(
                    "membership stage repeats a canonical or local receipt",
                ));
            }
            installation::apply_membership_delta(
                &mut self.recovered.0,
                &mut self.recovered.1,
                &record.payload,
            )?;
        }
        self.records = records.to_vec();
        Ok(())
    }
}

fn physical_stage_id(plan: [u8; 32], records: &[VsrHistoryRecord]) -> RsfResult<[u8; 32]> {
    let mut bytes = b"RSF staged membership prefix v1".to_vec();
    bytes.extend_from_slice(&plan);
    bytes.extend_from_slice(transfer::history_digest(records)?.as_bytes());
    Ok(Digest::blake3_bytes(&bytes).bytes)
}
