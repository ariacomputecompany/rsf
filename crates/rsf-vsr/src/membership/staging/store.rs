use super::super::decision::disk;
use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
mod publication;

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
    failed: bool,
    _lock: rsf_storage::ExclusiveFileLock,
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
        let lock = lock(&root)?;
        match fs::read(root.join("META")) {
            Ok(bytes) => {
                let existing =
                    Self::recover(root, plan.input.proposal().election.pg, local, lock, &bytes)?;
                if existing.plan.id != plan.id || existing.plan.records != plan.records {
                    return Err(corrupt(
                        "membership stage is already sealed with another input or receiver prefix",
                    ));
                }
                return Ok(existing);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_error(e)),
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
        let (input, records) = disk::read_records(&root, plan.input.id()?)?;
        if input != plan.input || records != input_records {
            return Err(corrupt(
                "staged membership source differs from validated input",
            ));
        }
        write_history(&root, &plan)?;
        let stage = stage_id(&plan)?;
        let metadata = encode_meta(local, &plan, stage, None)?;
        atomic(&root, "META", &metadata)?;
        let store = Self {
            root,
            local,
            plan,
            stage,
            materialized: false,
            publication: None,
            failed: false,
            _lock: lock,
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
        let lock = lock(&root)?;
        let bytes = fs::read(root.join("META")).map_err(io_error)?;
        Self::recover(root, pg, local, lock, &bytes)
    }
    fn recover(
        root: PathBuf,
        pg: PlacementGroupId,
        local: NodeId,
        lock: rsf_storage::ExclusiveFileLock,
        bytes: &[u8],
    ) -> RsfResult<Self> {
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
        let (stored, records) = disk::read_records(&root, input.id()?)?;
        if stored != input {
            return Err(corrupt("membership stage input descriptor changed"));
        }
        let approval = input.validate_records(&records)?;
        let mut plan = VsrMembershipStagedPlan::select(chosen, approval)?;
        if plan.id != plan_id {
            return Err(corrupt(
                "membership stage deterministic plan identity changed",
            ));
        }
        let history = read_history(&root, pg, input.proposal().election.previous_view)?;
        if history.len() as u64 != count {
            return Err(corrupt("membership sealed prefix lost staged records"));
        }
        plan.apply_local_suffix(&history)?;
        if stage_id(&plan)? != stage || encode_meta(local, &plan, stage, publication)? != bytes {
            return Err(corrupt(
                "membership stage history differs from sealed identity",
            ));
        }
        let store = Self {
            root,
            local,
            plan,
            stage,
            materialized: false,
            publication,
            failed: false,
            _lock: lock,
        };
        store.validate_receipt()?;
        store.publication_quorum()?;
        // Strict reads establish content, not completion of an earlier rename.
        // The routes were validated before replay and remain under stage custody.
        disk::sync_input_publication(&store.root, store.plan.input.id()?)?;
        rsf_storage::create_dir_all_synced(store.root.join("selected/segments"))
            .map_err(io_error)?;
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
        let report = self.expected_report()?;
        if let Err(error) = atomic(&self.root, "INSTALLED", &report.encode()?) {
            self.failed = true;
            return Err(error);
        }
        self.materialized = true;
        after_receipt();
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
        match fs::read(self.root.join("INSTALLED")) {
            Ok(bytes) => {
                if VsrMembershipStagedReport::decode(&bytes)? != self.expected_report()? {
                    return Err(corrupt(
                        "membership staged receipt differs from sealed prefix",
                    ));
                }
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error(e)),
        }
    }
    fn healthy(&self) -> RsfResult<()> {
        if self.failed {
            return Err(corrupt(
                "membership staging durability failed; reopen before further use",
            ));
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
    )?;
    if !plan.records.starts_with(&existing) {
        return Err(corrupt(
            "partial membership selected history differs from chosen plan",
        ));
    }
    let mut ordinal = existing.len();
    while ordinal < plan.records.len() {
        let batch = VsrHistoryBatch::pack(ordinal as u64, &plan.records[ordinal..])?;
        ordinal += batch.records.len();
        store.append_next_wal(
            plan.input.proposal().election.pg,
            plan.input.proposal().election.previous_view,
            WalRecordKind::ViewInstallation,
            &batch.encode()?,
        )?;
    }
    Ok(())
}
fn read_history(
    root: &Path,
    pg: PlacementGroupId,
    epoch: Epoch,
) -> RsfResult<Vec<VsrHistoryRecord>> {
    disk::records_route(&root.join("selected"), false)?;
    let recovery = SegmentStore::recover_existing(root.join("selected"))?.recovery;
    decode_history(recovery, pg, epoch)
}
fn decode_history(
    recovery: rsf_storage::Recovery,
    pg: PlacementGroupId,
    epoch: Epoch,
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

fn lock(root: &Path) -> RsfResult<rsf_storage::ExclusiveFileLock> {
    rsf_storage::ExclusiveFileLock::acquire(
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join(".staging.lock"))
            .map_err(io_error)?,
    )
    .map_err(|e| invalid_input(format!("membership staging store already owned: {e}")))
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
