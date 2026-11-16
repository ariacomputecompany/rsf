use super::super::decision::disk;
use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
mod custody;
mod publication;
use std::sync::atomic::{AtomicBool, Ordering};

/// Separately sealed chosen prefix. Reopen replays its complete proof, validates
/// every selected byte and resets this boot's materialization acknowledgement.
#[derive(Debug)]
pub struct MembershipStagingStore {
    root: PathBuf,
    local: NodeId,
    plan: VsrMembershipStagedPlan,
    stage: [u8; 32],
    materialized: bool,
    // V2 metadata binds the first durable PUBLICATION bytes.
    publication: Option<[u8; 32]>,
    failed: AtomicBool,
    custody: custody::StageCustody,
}
impl MembershipStagingStore {
    pub fn stage(
        path: impl AsRef<Path>,
        local: NodeId,
        mut plan: VsrMembershipStagedPlan,
        anchor: &DurableVsrCell,
    ) -> RsfResult<Self> {
        plan.retain_anchor(anchor)?;
        if !plan.input.proposal().new_voters.contains(&local) {
            return Err(invalid_input(
                "membership stage receiver is not a proposed voter",
            ));
        }
        let root = path.as_ref().to_path_buf();
        rsf_storage::create_dir_all_synced(&root).map_err(io_error)?;
        let owner = custody::Owner::acquire(&root)?;
        if let Some(meta) = custody::Exact::optional(&root.join("META"))? {
            let existing =
                Self::recover(root, plan.input.proposal().election.pg, local, owner, meta)?;
            if existing.plan.id != plan.id || existing.plan.records != plan.records {
                return Err(corrupt(
                    "membership stage is already sealed with another input or receiver prefix",
                ));
            }
            return Ok(existing);
        }
        if publication::present(&root.join("INSTALLED"))?
            || publication::present(&root.join("PUBLICATION"))?
            || publication::present(&root.join(".PUBLICATION.tmp"))?
        {
            return Err(corrupt(
                "membership stage receipt or publication exists without sealed authority",
            ));
        }
        let input_records = plan.input.flattened_records(&plan.approval)?;
        let mut failed = false;
        disk::create_and_retain_records(&root, &mut failed, &plan.input, &input_records)?;
        // Validate bytes recovered from the staged source, including a prior
        // upload acknowledged by another process before this factory resumed.
        let (source, records) = custody::Source::read(&root, &plan.input)?;
        if records != input_records {
            return Err(corrupt(
                "staged membership source differs from validated input",
            ));
        }
        write_history(&root, &plan)?;
        // One physical readback binds selected bytes to the immutable stamp;
        // the already validated plan needs no second semantic replay.
        let (selected_records, selected) = read_history(
            &root,
            plan.input.proposal().election.pg,
            plan.input.proposal().election.previous_view,
            plan.records.len() as u64,
        )?;
        if selected_records != plan.records {
            return Err(corrupt("selected stage differs from its written plan"));
        }
        owner.validate()?;
        source.validate()?;
        selected.validate()?;
        custody::absent(&root.join("META"))?;
        custody::absent(&root.join("INSTALLED"))?;
        custody::absent(&root.join("PUBLICATION"))?;
        let stage = stage_id(&plan)?;
        let metadata = encode_meta(local, &plan, stage, None)?;
        atomic(&root, "META", &metadata)?;
        let meta = custody::Exact::expected(&root.join("META"), &metadata)?;
        let custody = custody::StageCustody::new(owner, source, selected, meta, None, None)?;
        let store = Self {
            root,
            local,
            plan,
            stage,
            materialized: false,
            publication: None,
            failed: AtomicBool::new(false),
            custody,
        };
        store.validate_receipt()?;
        store.publication_quorum()?;
        Ok(store)
    }
    pub fn open_existing(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
    ) -> RsfResult<Self> {
        let root = path.as_ref().to_path_buf();
        let owner = custody::Owner::acquire(&root)?;
        let meta = custody::Exact::optional(&root.join("META"))?
            .ok_or_else(|| corrupt("sealed membership stage metadata is missing"))?;
        Self::recover(root, pg, local, owner, meta)
    }
    fn recover(
        root: PathBuf,
        pg: PlacementGroupId,
        local: NodeId,
        owner: custody::Owner,
        meta: custody::Exact,
    ) -> RsfResult<Self> {
        let bytes = meta.bytes();
        let installed = custody::Exact::optional(&root.join("INSTALLED"))?;
        let retained_publication = custody::Exact::optional(&root.join("PUBLICATION"))?;
        let version_two = bytes.starts_with(b"VSRMST2");
        let mut r = Reader::new(bytes, if version_two { b"VSRMST2" } else { b"VSRMST1" })?;
        if r.u64()? != pg.0 || r.u64()? != local.0 {
            return Err(corrupt("membership stage identity mismatch"));
        }
        let input = VsrMembershipInput::decode(r.bytes()?)?;
        let chosen = VsrMembershipChosen::decode(r.bytes()?)?;
        let plan_id = r.id()?;
        let stage = r.id()?;
        let count = r.u64()?;
        let publication = if version_two { Some(r.id()?) } else { None };
        let checksum = r.id()?;
        r.finish()?;
        if bytes.len() < 32
            || Digest::blake3_bytes(&bytes[..bytes.len() - 32]).bytes != checksum
            || input.proposal().election.pg != pg
            || !input.proposal().new_voters.contains(&local)
        {
            return Err(corrupt(
                "membership stage metadata checksum or authority mismatch",
            ));
        }
        let (source, records) = custody::Source::read(&root, &input)?;
        let approval = input.validate_records(&records)?;
        let mut plan = VsrMembershipStagedPlan::select(chosen, approval)?;
        if plan.id != plan_id {
            return Err(corrupt(
                "membership stage deterministic plan identity changed",
            ));
        }
        let (history, selected) =
            read_history(&root, pg, input.proposal().election.previous_view, count)?;
        plan.apply_local_suffix(&history)?;
        if stage_id(&plan)? != stage || encode_meta(local, &plan, stage, publication)? != bytes {
            return Err(corrupt(
                "membership stage history differs from sealed identity",
            ));
        }
        let custody = custody::StageCustody::new(
            owner,
            source,
            selected,
            meta,
            installed,
            retained_publication,
        )?;
        let store = Self {
            root,
            local,
            plan,
            stage,
            materialized: false,
            publication,
            failed: AtomicBool::new(false),
            custody,
        };
        store.validate_receipt()?;
        store.publication_quorum()?;
        // Strict reads establish content, not completion of an earlier rename.
        // The routes were validated before replay and remain under stage custody.
        disk::sync_input_publication(&store.root, store.plan.input.id()?)?;
        rsf_storage::sync_existing_directory(store.root.join("selected/segments"))
            .map_err(io_error)?;
        store.healthy()?;
        Ok(store)
    }
    pub fn validate_anchor(&self, anchor: &DurableVsrCell) -> RsfResult<()> {
        self.healthy()?;
        anchor.validate_membership_authority(&self.plan.approval)?;
        if !installation::preserves_committed(&self.plan.recovered.0, &anchor.cell)
            || anchor
                .cell
                .client_table
                .iter()
                .any(|(request, index)| self.plan.request_commit(request) != Some(*index))
        {
            return Err(corrupt(
                "membership staged prefix drops a hosted commit or receipt",
            ));
        }
        Ok(())
    }
    /// Validate a retained stage against a replay-proven historical activation.
    /// Unlike validate_anchor, this accepts later ordinary views and membership
    /// changes while preserving the stage's committed data and local receipts.
    /// It neither materializes applications nor reactivates this stage's role.
    pub fn validate_published_anchor(&self, anchor: &DurableVsrCell) -> RsfResult<()> {
        self.healthy()?;
        let publication = anchor
            .membership_publication(self.plan.id())?
            .ok_or_else(|| invalid_input("membership stage has no authoritative publication"))?;
        if publication.proposal() != self.plan.input.proposal()
            || publication.input_id() != self.plan.chosen.input_id()
            || self.plan.records.first().map(|r| r.payload.as_slice())
                != Some(anchor.initial_bootstrap.as_slice())
            || !activation::retains_stage(anchor, self)
        {
            return Err(corrupt(
                "membership publication does not retain the staged authority, prefix or receipts",
            ));
        }
        Ok(())
    }

    pub fn plan(&self) -> &VsrMembershipStagedPlan {
        &self.plan
    }
    pub fn stage_id(&self) -> [u8; 32] {
        self.stage
    }
    pub fn report(&self) -> RsfResult<VsrMembershipStagedReport> {
        self.healthy()?;
        if !self.materialized {
            return Err(invalid_input(
                "membership staged prefix is not materialized on this boot",
            ));
        }
        self.expected_report()
    }
    /// The trusted idempotent callback must sync application effects before
    /// returning. Every reopen requires it again, even with a retained receipt.
    pub fn materialize(
        &mut self,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> RsfResult<()>,
    ) -> RsfResult<VsrMembershipStagedReport> {
        self.materialize_with_hook(apply, || {})
    }
    /// Renew this stage's boot receipt from a published descendant's complete
    /// current committed log. Do not replay an older stage over later effects.
    /// The trusted callback must durably apply every entry before returning.
    /// Historical identity and retained PUBLICATION bytes remain unchanged.
    pub fn materialize_published(
        &mut self,
        anchor: &DurableVsrCell,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> RsfResult<()>,
    ) -> RsfResult<VsrMembershipStagedReport> {
        self.healthy()?;
        self.materialized = false;
        self.validate_published_anchor(anchor)?;
        for entry in anchor.committed_prefix()? {
            apply(&entry)?;
        }
        self.finish_materialization(|| {})
    }

    fn materialize_with_hook(
        &mut self,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> RsfResult<()>,
        after_receipt: impl FnOnce(),
    ) -> RsfResult<VsrMembershipStagedReport> {
        self.healthy()?;
        self.materialized = false;
        for entry in self.plan.committed_entries()? {
            apply(&entry)?;
        }
        self.finish_materialization(after_receipt)
    }
    fn finish_materialization(
        &mut self,
        after_receipt: impl FnOnce(),
    ) -> RsfResult<VsrMembershipStagedReport> {
        // Materializers may perform arbitrary application I/O. Recheck before
        // replacing a previously observed receipt, then check the own write.
        self.healthy()?;
        let report = self.expected_report()?;
        let bytes = report.encode()?;
        let result = (|| {
            atomic(&self.root, "INSTALLED", &bytes)?;
            self.custody.renew_installed(&bytes)?;
            after_receipt();
            self.healthy()
        })();
        if let Err(error) = result {
            self.failed.store(true, Ordering::Relaxed);
            return Err(error);
        }
        self.materialized = true;
        Ok(report)
    }
    fn expected_report(&self) -> RsfResult<VsrMembershipStagedReport> {
        let report = VsrMembershipStagedReport {
            proposal: self.plan.input.proposal().clone(),
            replica: self.local,
            input: self.plan.input.id()?,
            plan: self.plan.id,
            stage: self.stage,
            committed_tail: self.plan.committed_tail(),
            accepted_tail: self.plan.accepted_tail(),
        };
        report.encode()?;
        Ok(report)
    }
    fn validate_receipt(&self) -> RsfResult<()> {
        self.healthy()?;
        if let Some(bytes) = self.custody.installed_bytes()
            && VsrMembershipStagedReport::decode(bytes)? != self.expected_report()?
        {
            return Err(corrupt(
                "membership staged receipt differs from sealed prefix",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_custody(&self) -> RsfResult<()> {
        self.healthy()
    }
    fn healthy(&self) -> RsfResult<()> {
        if self.failed.load(Ordering::Relaxed) {
            return Err(corrupt(
                "membership staging durability failed; reopen before further use",
            ));
        }
        if let Err(error) = self.custody.validate() {
            self.failed.store(true, Ordering::Relaxed);
            return Err(error);
        }
        Ok(())
    }
}
fn write_history(root: &Path, plan: &VsrMembershipStagedPlan) -> RsfResult<()> {
    disk::records_route(&root.join("selected"), true)?;
    let mut options = SegmentStoreOptions::default();
    options.log_options.sync_policy = rsf_storage::SyncPolicy::Full;
    let (mut store, recovery) = SegmentStore::open_recovered(root.join("selected"), options)?;
    let existing = decode_history(
        recovery.recovery,
        plan.input.proposal().election.pg,
        plan.input.proposal().election.previous_view,
        plan.records.len() as u64,
    )?;
    if !plan.records.starts_with(&existing) {
        return Err(corrupt(
            "partial membership selected history differs from chosen plan",
        ));
    }
    disk::append_history_suffix(
        &mut store,
        plan.input.proposal().election.pg,
        plan.input.proposal().election.previous_view,
        &plan.records,
        existing.len(),
    )
}
fn read_history(
    root: &Path,
    pg: PlacementGroupId,
    epoch: Epoch,
    count: u64,
) -> RsfResult<(Vec<VsrHistoryRecord>, rsf_storage::SegmentReadCustody)> {
    read_history_at(&root.join("selected"), pg, epoch, count)
}
fn read_history_at(
    path: &Path,
    pg: PlacementGroupId,
    epoch: Epoch,
    count: u64,
) -> RsfResult<(Vec<VsrHistoryRecord>, rsf_storage::SegmentReadCustody)> {
    // Each nonempty WAL batch covers >=1 logical record. Rotation can leave
    // only the final active segment empty; these stage factories never compact.
    let max_segments = count
        .checked_add(1)
        .ok_or_else(|| corrupt("stage record bound overflow"))?;
    let max_bytes = max_segments
        .checked_mul(64)
        .and_then(|n| n.checked_add(64))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| corrupt("stage manifest bound exceeds platform range"))?;
    let (recovered, custody) = SegmentStore::recover_immutable(path, max_bytes)?;
    if recovered.segments.len() as u64 > max_segments
        || !recovered.recovery.compacted_prefixes.is_empty()
        || recovered.recovery.records.len() != recovered.recovery.wal_records.len()
    {
        return Err(corrupt(
            "immutable stage store differs from append-only batch format",
        ));
    }
    let records = decode_history(recovered.recovery, pg, epoch, count)?;
    if records.len() as u64 != count {
        return Err(corrupt(
            "sealed stage history lost its complete record count",
        ));
    }
    custody.validate()?;
    Ok((records, custody))
}
fn decode_history(
    recovery: rsf_storage::Recovery,
    pg: PlacementGroupId,
    epoch: Epoch,
    max_records: u64,
) -> RsfResult<Vec<VsrHistoryRecord>> {
    let mut records = Vec::new();
    for (n, record) in recovery.wal_records.iter().enumerate() {
        if record.index != LogIndex(n as u64 + 1)
            || record.pg != pg
            || record.epoch != epoch
            || record.kind != WalRecordKind::ViewInstallation
        {
            return Err(corrupt("membership selected history envelope mismatch"));
        }
        let batch = VsrHistoryBatch::decode(&record.payload)?;
        if batch.ordinal != records.len() as u64 {
            return Err(corrupt(
                "membership selected history batch changes ordering",
            ));
        }
        let end = batch
            .ordinal
            .checked_add(batch.records.len() as u64)
            .ok_or_else(|| corrupt("membership history record count overflow"))?;
        if end > max_records {
            return Err(corrupt(
                "membership history exceeds its sealed record bound",
            ));
        }
        records.extend(batch.records);
    }
    Ok(records)
}
fn encode_meta(
    local: NodeId,
    plan: &VsrMembershipStagedPlan,
    stage: [u8; 32],
    publication: Option<[u8; 32]>,
) -> RsfResult<Vec<u8>> {
    let mut bytes = if publication.is_some() {
        b"VSRMST2"
    } else {
        b"VSRMST1"
    }
    .to_vec();
    put_u64(&mut bytes, plan.input.proposal().election.pg.0);
    put_u64(&mut bytes, local.0);
    put_bytes(&mut bytes, &plan.input.encode()?)?;
    put_bytes(&mut bytes, &plan.chosen.encode()?)?;
    bytes.extend_from_slice(&plan.id);
    bytes.extend_from_slice(&stage);
    put_u64(&mut bytes, plan.records.len() as u64);
    if let Some(digest) = publication {
        bytes.extend_from_slice(&digest);
    }
    let sum = Digest::blake3_bytes(&bytes).bytes;
    bytes.extend_from_slice(&sum);
    if bytes.len() > MAX_HISTORY_RECORD_BYTES {
        return Err(invalid_input("membership stage metadata exceeds limit"));
    }
    Ok(bytes)
}
fn stage_id(plan: &VsrMembershipStagedPlan) -> RsfResult<[u8; 32]> {
    physical_stage_id(plan.id, &plan.records)
}

fn atomic(root: &Path, name: &str, bytes: &[u8]) -> RsfResult<()> {
    let temporary = root.join(format!(".{name}.tmp"));
    // A regular temporary file can remain after an unacknowledged write. Under
    // the exclusive stage lock, discard it without following a substituted
    // symlink, then use create_new so a racing path cannot redirect writes.
    match fs::symlink_metadata(&temporary) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            fs::remove_file(&temporary).map_err(io_error)?;
        }
        Ok(_) => {
            return Err(corrupt(
                "membership staging temporary is not a regular owned file",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(io_error)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(io_error)?;
    fs::rename(temporary, root.join(name)).map_err(io_error)?;
    File::open(root)
        .and_then(|f| f.sync_all())
        .map_err(io_error)
}
fn io_error(e: std::io::Error) -> RsfError {
    corrupt(format!("membership staged generation: {e}"))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
