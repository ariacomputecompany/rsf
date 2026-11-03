//! Quorum-certified membership activation on the authoritative VSR history.
//! As with ordinary view installation, reports are authenticated crash-fault
//! evidence, not independent signatures. Peer callers must authenticate them.
use super::*;
use crate::installation::publication::Boundary;

const MAGIC: &[u8] = b"VSRMAI1";

/// Historical membership activation proven by replay of authoritative history.
/// This is not a publication acknowledgement, a current role, or evidence of
/// application materialization on this boot. Ordinary views and later removal
/// do not erase it. Only authoritative replay constructs this descriptor.
#[derive(Clone, Debug)]
pub struct VsrMembershipPublication {
    input: VsrMembershipInput,
    chosen: VsrMembershipChosen,
    quorum: VsrMembershipStagedQuorum,
    plan: [u8; 32],
}
impl VsrMembershipPublication {
    pub fn proposal(&self) -> &VsrMembershipProposal {
        self.input.proposal()
    }
    pub fn input(&self) -> &VsrMembershipInput {
        &self.input
    }
    pub fn input_id(&self) -> [u8; 32] {
        self.chosen.input_id()
    }
    pub fn chosen(&self) -> &VsrMembershipChosen {
        &self.chosen
    }
    pub fn staged_quorum(&self) -> &VsrMembershipStagedQuorum {
        &self.quorum
    }
    pub fn plan_id(&self) -> [u8; 32] {
        self.plan
    }
}

struct Activation {
    input: VsrMembershipInput,
    chosen: VsrMembershipChosen,
    quorum: VsrMembershipStagedQuorum,
    local: VsrMembershipStagedReport,
    canonical_records: u64,
}
impl Activation {
    fn validate(&self) -> RsfResult<()> {
        self.input.encode()?;
        self.chosen.encode()?;
        self.quorum.encode()?;
        self.local.encode()?;
        let proposal = self.input.proposal();
        let id = self.input.id()?;
        let first = &self.quorum.reports()[0];
        if self.chosen.proposal() != proposal
            || self.quorum.proposal() != proposal
            || self.local.proposal != *proposal
            || self.chosen.input_id() != id
            || self.quorum.input_id() != id
            || self.local.input != id
            || self.local.plan != self.quorum.plan_id()
            || self.local.committed_tail != first.committed_tail
            || self.local.accepted_tail != first.accepted_tail
            || self.canonical_records < self.source().voter.history.records
        {
            return Err(corrupt("membership activation certificates disagree"));
        }
        Ok(())
    }
    fn source(&self) -> &VsrMembershipReport {
        self.input
            .reports()
            .iter()
            .max_by_key(|r| (r.voter.accepted_tail, std::cmp::Reverse(r.voter.replica)))
            .expect("validated membership input")
    }
    fn encode(&self) -> RsfResult<Vec<u8>> {
        self.validate()?;
        let mut out = MAGIC.to_vec();
        put_bytes(&mut out, &self.input.encode()?)?;
        put_bytes(&mut out, &self.chosen.encode()?)?;
        put_bytes(&mut out, &self.quorum.encode()?)?;
        put_bytes(&mut out, &self.local.encode()?)?;
        put_u64(&mut out, self.canonical_records);
        if out.len() > MAX_HISTORY_RECORD_BYTES {
            return Err(invalid_input("membership activation exceeds record limit"));
        }
        Ok(out)
    }
    fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, MAGIC)?;
        let value = Self {
            input: VsrMembershipInput::decode(r.bytes()?)?,
            chosen: VsrMembershipChosen::decode(r.bytes()?)?,
            quorum: VsrMembershipStagedQuorum::decode(r.bytes()?)?,
            local: VsrMembershipStagedReport::decode(r.bytes()?)?,
            canonical_records: r.u64()?,
        };
        r.finish()?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership activation"));
        }
        Ok(value)
    }
    fn from_stage(
        stage: &MembershipStagingStore,
        quorum: &VsrMembershipStagedQuorum,
    ) -> RsfResult<Self> {
        // report() requires this boot's successful durable materializer.
        let local = stage.report()?;
        let plan = stage.plan();
        let canonical =
            VsrMembershipStagedPlan::select(plan.chosen.clone(), plan.approval.clone())?;
        if canonical.id != plan.id || !plan.records.starts_with(&canonical.records) {
            return Err(corrupt(
                "membership stage differs from canonical chosen prefix",
            ));
        }
        let value = Self {
            input: plan.input.clone(),
            chosen: plan.chosen.clone(),
            quorum: quorum.clone(),
            local,
            canonical_records: canonical.records.len() as u64,
        };
        value.validate()?;
        Ok(value)
    }
}

pub(crate) fn replay_activation(
    cell: &mut VsrCell,
    history: &mut history::ConfigurationHistory,
    fences: (
        &mut Option<VsrViewChangeRequest>,
        &mut Option<VsrMembershipProposal>,
    ),
    records: &[replay::Record<'_>],
    position: usize,
    delta_start: &mut Option<usize>,
) -> RsfResult<()> {
    let (promise, fence) = fences;
    let proposal = fence
        .as_ref()
        .ok_or_else(|| corrupt("membership activation has no fence"))?
        .clone();
    let record = records[position];
    if record.epoch != cell.view {
        return Err(corrupt("membership activation epoch mismatch"));
    }
    if installation::is_membership_delta(record.payload) {
        delta_start.get_or_insert(position);
        return Ok(());
    }
    if !record.payload.starts_with(MAGIC) {
        return Err(corrupt(
            "ordinary view installation cannot discard a membership fence",
        ));
    }
    let marker = Activation::decode(record.payload)?;
    let source = &marker.source().voter;
    let start = delta_start.unwrap_or(position);
    let canonical = usize::try_from(marker.canonical_records)
        .map_err(|_| corrupt("membership canonical count exceeds platform range"))?;
    if marker.input.proposal() != &proposal
        || promise.as_ref() != Some(&proposal.election)
        || source.history.records != start as u64
        || source.history.probe != installation::probe_for_cell(cell)?
        || source.accepted_tail != LogIndex(cell.next_index - 1)
        || canonical < start
        || canonical > position
    {
        return Err(corrupt(
            "membership activation source differs from fenced history",
        ));
    }
    let prefix = records[..position]
        .iter()
        .map(|r| VsrHistoryRecord {
            kind: r.kind,
            epoch: r.epoch,
            payload: r.payload.to_vec(),
        })
        .collect::<Vec<_>>();
    if transfer::history_digest(&prefix[..start])? != source.history.digest {
        return Err(corrupt("membership activation source digest mismatch"));
    }
    let mut identity = marker.input.id()?.to_vec();
    identity.extend_from_slice(transfer::history_digest(&prefix[..canonical])?.as_bytes());
    if Digest::blake3_bytes(&identity).bytes != marker.local.plan
        || physical_stage_id(marker.local.plan, &prefix)? != marker.local.stage
    {
        return Err(corrupt(
            "membership activation canonical or physical identity mismatch",
        ));
    }
    for (offset, delta) in prefix[start..].iter().enumerate() {
        if delta.kind != WalRecordKind::ViewInstallation || delta.epoch != cell.view {
            return Err(corrupt("membership activation delta envelope mismatch"));
        }
        if start + offset >= canonical {
            if !delta.payload.starts_with(b"VSIT1") {
                return Err(corrupt("membership local suffix is not a receipt"));
            }
            let receipt = decode_client_request_commit(&delta.payload[5..])?;
            if cell.client_table.contains_key(&receipt.request) {
                return Err(corrupt("membership local suffix repeats a receipt"));
            }
        }
        installation::apply_membership_delta(cell, history, &delta.payload)?;
    }
    let committed = marker
        .input
        .reports()
        .iter()
        .map(|r| r.voter.history.probe.committed_tail)
        .max()
        .ok_or_else(|| corrupt("membership activation has no old reports"))?;
    if cell.committed_tail() != committed
        || marker.local.committed_tail != committed
        || marker.local.accepted_tail != LogIndex(cell.next_index - 1)
    {
        return Err(corrupt(
            "membership activation lost the quorum prefix boundary",
        ));
    }
    cell.view = proposal.election.target_view;
    cell.primary = proposal.election.candidate;
    cell.replicas = proposal.new_voters.into_iter().collect();
    for pending in cell.pending.values_mut().filter(|p| !p.committed) {
        pending.acknowledgements.clear();
    }
    // Record ancestry only after validating the complete activation context,
    // canonical prefix, local suffix and resulting committed/accepted bounds.
    history.record_membership_publication(
        cell,
        VsrMembershipPublication {
            input: marker.input,
            chosen: marker.chosen,
            quorum: marker.quorum,
            plan: marker.local.plan,
        },
    )?;
    *promise = None;
    *fence = None;
    *delta_start = None;
    Ok(())
}

pub(crate) fn publication_records(
    stage: &MembershipStagingStore,
    quorum: &VsrMembershipStagedQuorum,
) -> RsfResult<Vec<VsrHistoryRecord>> {
    let marker = Activation::from_stage(stage, quorum)?;
    let mut records = stage.plan().records.clone();
    records.push(VsrHistoryRecord {
        kind: WalRecordKind::ViewInstallation,
        epoch: stage.plan().input.proposal().election.previous_view,
        payload: marker.encode()?,
    });
    Ok(records)
}

pub(crate) fn retains_stage(cell: &DurableVsrCell, stage: &MembershipStagingStore) -> bool {
    let plan = stage.plan();
    installation::preserves_committed(&cell.cell, &plan.recovered.0)
        && cell.history.contains_authority(&plan.recovered.1)
        && plan
            .recovered
            .0
            .client_table
            .iter()
            .all(|(key, index)| cell.cell.client_table.get(key) == Some(index))
}

pub(crate) fn matches_promotion_plan(
    payload: &[u8],
    local: NodeId,
    plan: [u8; 32],
) -> RsfResult<bool> {
    if !payload.starts_with(MAGIC) {
        return Ok(false);
    }
    let marker = Activation::decode(payload)?;
    // A later ordinary view can select another voter's generation. Its local
    // materialization reporter may differ; the canonical plan and original
    // addition of this node must survive. The root pointer binds node custody.
    let proposal = marker.input.proposal();
    Ok(marker.local.plan == plan
        && !proposal.election.voters.contains(&local)
        && proposal.new_voters.contains(&local))
}

impl DurableVsrCell {
    /// Current authoritative membership lineage. Ordinary view changes and
    /// pending promises preserve it. Publication ancestry alone is not control
    /// admission; callers must compare this fact with independent control state.
    pub fn membership_lineage(&self) -> RsfResult<VsrMembershipLineage> {
        self.membership_lineage_at(self.cell.view)?
            .ok_or_else(|| corrupt("current VSR view has no historical configuration"))
    }

    /// Exact replayed configuration for an installed view. An unknown view is
    /// None; it is never inferred from a neighboring epoch or current voters.
    /// Existing epochs after an uncertified legacy membership change return an
    /// error, even if a later certified activation establishes a new lineage.
    pub fn membership_lineage_at(&self, view: Epoch) -> RsfResult<Option<VsrMembershipLineage>> {
        self.ensure_healthy()?;
        if !self.bootstrap_persisted {
            return Err(invalid_input(
                "membership lineage requires persisted original bootstrap authority",
            ));
        }
        self.history.membership_lineage_at(view)
    }

    /// Whether healthy authoritative history contains a membership activation.
    /// This includes ancestry inherited through ordinary view installation and
    /// grants no client admission or this-boot materialization acknowledgement.
    pub fn has_membership_publication(&self) -> RsfResult<bool> {
        self.ensure_healthy()?;
        Ok(self.history.has_membership_publication())
    }

    /// Query historical activation in the currently authoritative history.
    /// A higher ordinary view or pending promise does not invalidate ancestry.
    /// The result grants no current voting role, client admission, publication
    /// acknowledgement, or this-boot application-materialization guarantee.
    pub fn membership_publication(
        &self,
        plan: [u8; 32],
    ) -> RsfResult<Option<&VsrMembershipPublication>> {
        self.ensure_healthy()?;
        Ok(self.history.membership_publication(plan))
    }

    /// Publish a staged/materialized membership with authenticated new-quorum
    /// receipts. This kernel operation does not update native client admission.
    /// The existing root must retain the matching durable membership fence so
    /// an absent active pointer can never restore unfenced old authority.
    pub fn install_membership(
        &mut self,
        stage: &MembershipStagingStore,
        quorum: &VsrMembershipStagedQuorum,
    ) -> RsfResult<()> {
        self.install_membership_with_hook(stage, quorum, &mut |_| Ok(()))
    }
    pub(crate) fn install_membership_with_hook(
        &mut self,
        stage: &MembershipStagingStore,
        quorum: &VsrMembershipStagedQuorum,
        hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.ensure_healthy()?;
        let records = publication_records(stage, quorum)?;
        let plan = stage.plan();
        let proposal = plan.input.proposal();
        if plan.records.first().map(|r| r.payload.as_slice())
            != Some(self.initial_bootstrap.as_slice())
        {
            return Err(corrupt(
                "membership publication would replace bootstrap authority",
            ));
        }
        if self.installed_plan == Some(plan.id)
            && self.cell.view == proposal.election.target_view
            && self.cell.primary == proposal.election.candidate
            && self.cell.replicas.iter().copied().collect::<Vec<_>>() == proposal.new_voters
            && self.view_change.is_none()
            && self.membership_fence.is_none()
        {
            if !installation::preserves_committed(&self.cell, &plan.recovered.0)
                || !self.history.contains_authority(&plan.recovered.1)
                || plan
                    .recovered
                    .0
                    .client_table
                    .iter()
                    .any(|(key, index)| self.cell.client_table.get(key) != Some(index))
            {
                return Err(corrupt("membership retry conflicts with installed state"));
            }
            self.reclaim_inactive_views()?;
            return stage.validate_custody();
        }
        if self.membership_fence.as_ref() != Some(proposal)
            || self.view_change.as_ref() != Some(&proposal.election)
        {
            return Err(invalid_input(
                "membership publication requires its exact durable root fence",
            ));
        }
        stage.validate_anchor(self)?;
        let recovered = installation::recover_records(&records)?;
        stage.validate_custody()?;
        self.publish_generation(plan.id, &records, recovered, hook)?;
        stage.validate_custody()
    }
}
