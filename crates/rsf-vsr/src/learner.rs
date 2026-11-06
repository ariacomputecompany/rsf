//! Staged history is never a voting replica. Publication requires complete
//! hash-checked history, normal VSR recovery, and extension of published truth.
use super::*;
mod custody;
mod ordinary;
mod replay;
pub use replay::PublishedVsrReplay;
mod prefix;
pub use prefix::VsrImportPreview;
pub(crate) mod promotion;
pub use custody::VsrLearnerStateToken;
use rsf_core::Digest;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
struct Stage {
    custody: custody::CandidateCustody,
    descriptor: VsrHistoryDescriptor,
    store: SegmentStore,
    count: u64,
    digest: Digest,
    previous_hash: [u8; 32],
}

#[derive(Debug)]
struct Published {
    custody: custody::CandidateCustody,
    bounded_flat: Option<prefix::guard::Flat>,
    id: [u8; 32],
    descriptor: VsrHistoryDescriptor,
    cell: DurableVsrCell,
}

/// A separately configured non-voter. No method exposes a writable VSR cell or
/// a prepare/commit acknowledgement. The configured source is authenticated by
/// the transport; a transferred voter list cannot change this receiver's role.
#[derive(Debug)]
pub struct LearnerHistoryStore {
    owner_custody: custody::OwnerCustody,
    root: PathBuf,
    pg: PlacementGroupId,
    local: NodeId,
    source: NodeId,
    stage: Option<Stage>,
    published: Option<Published>,
    failed: AtomicBool,
    prefix_context: Option<prefix::PrefixContext>,
    _lock: rsf_storage::ExclusiveFileLock,
}

impl LearnerHistoryStore {
    /// Transition votes require the already published bootstrap/history anchor.
    /// Catch-up and this check never promote the learner's application role.
    pub fn validate_membership_authority(&self, approval: &VsrMembershipApproval) -> RsfResult<()> {
        self.healthy()?;
        self.published
            .as_ref()
            .ok_or_else(|| invalid_input("learner has no published authority anchor"))?
            .cell
            .validate_membership_authority(approval)
    }

    /// Stage chosen history against this learner's published anchor without
    /// exposing its underlying cell or changing the learner's voting role.
    pub fn stage_membership(
        &self,
        path: impl AsRef<Path>,
        plan: VsrMembershipStagedPlan,
    ) -> RsfResult<MembershipStagingStore> {
        self.healthy()?;
        let published = self
            .published
            .as_ref()
            .ok_or_else(|| invalid_input("learner has no published authority anchor"))?;
        MembershipStagingStore::stage(path, self.local, plan, &published.cell)
    }

    pub fn validate_membership_stage(&self, stage: &MembershipStagingStore) -> RsfResult<()> {
        self.healthy()?;
        stage.validate_anchor(
            &self
                .published
                .as_ref()
                .ok_or_else(|| invalid_input("learner has no published authority anchor"))?
                .cell,
        )
    }

    pub fn published_history_id(&self) -> RsfResult<Option<[u8; 32]>> {
        self.healthy()?;
        Ok(self.published.as_ref().map(|p| p.id))
    }

    pub fn placement_group(&self) -> PlacementGroupId {
        self.pg
    }
    pub fn local(&self) -> NodeId {
        self.local
    }
    pub fn source(&self) -> NodeId {
        self.source
    }

    /// Return the number of durable records already staged, for resume after a
    /// lost reply or process restart. A different descriptor has a separate WAL.
    pub fn begin(&mut self, descriptor: VsrHistoryDescriptor) -> RsfResult<u64> {
        self.healthy()?;
        self.require_ordinary_transfer()?;
        self.validate_descriptor(&descriptor)?;
        let id = descriptor.id()?;
        if let Some(published) = &self.published {
            if published.id == id {
                let count = published.descriptor.records;
                self.reclaim_unreferenced()?;
                return Ok(count);
            }
            if descriptor.probe.view < published.descriptor.probe.view
                || descriptor.probe.committed_tail < published.descriptor.probe.committed_tail
            {
                return Err(invalid_input(
                    "learner history would regress published view or committed tail",
                ));
            }
        }
        if let Some(stage) = &self.stage
            && stage.descriptor == descriptor
        {
            let count = stage.count;
            self.reclaim_unreferenced()?;
            return Ok(count);
        }
        self.failed.store(true, Ordering::Relaxed);
        self.stage = None;
        let directory = self.directory(&id);
        fs::create_dir_all(&directory).map_err(io_error)?;
        let directory_identity = prefix::guard::directory(&directory)?;
        sync_directory(&self.root)?;
        let encoded = descriptor.encode()?;
        let exact = match fs::symlink_metadata(directory.join("DESCRIPTOR")) {
            Ok(_) => prefix::guard::Exact::capture(&directory.join("DESCRIPTOR"), &encoded)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                prefix::io::prepare_publication(&directory, "DESCRIPTOR", &encoded)?
                    .publish_guarded()?
            }
            Err(err) => return Err(io_error(err)),
        };
        fs::create_dir_all(directory.join("cell")).map_err(io_error)?;
        let custody = custody::CandidateCustody::capture(&directory, &descriptor)?;
        let stage = self.recover_stage(descriptor, custody)?;
        exact.validate()?;
        if prefix::guard::directory(&directory)? != directory_identity {
            return Err(corrupt(
                "learner candidate directory changed during recovery",
            ));
        }
        self.owner_custody.validate(&self.root, &self._lock)?;
        let count = stage.count;
        self.stage = Some(stage);
        self.reclaim_unreferenced()?;
        self.owner_custody.validate(&self.root, &self._lock)?;
        self.failed.store(false, Ordering::Relaxed);
        Ok(count)
    }

    /// Ordinals are zero based. Exact duplicate records are idempotent; gaps,
    /// replacements and records beyond the descriptor are rejected.
    pub fn append(
        &mut self,
        id: [u8; 32],
        ordinal: u64,
        record: VsrHistoryRecord,
    ) -> RsfResult<u64> {
        self.healthy()?;
        self.require_ordinary_transfer()?;
        record.encode()?;
        let stage = self
            .stage
            .as_mut()
            .ok_or_else(|| invalid_input("begin learner transfer before appending"))?;
        if stage.descriptor.id()? != id
            || ordinal > stage.count
            || ordinal >= stage.descriptor.records
        {
            return Err(invalid_input(
                "learner record has wrong snapshot or ordinal",
            ));
        }
        if ordinal == 0 {
            let bootstrap = decode_bootstrap(&record.payload)?;
            if record.kind != WalRecordKind::Membership
                || bootstrap.pg != self.pg
                || bootstrap.view != record.epoch
            {
                return Err(corrupt(
                    "learner history must start with its original bootstrap",
                ));
            }
        } else if record.payload.starts_with(VSR_BOOTSTRAP_MAGIC)
            || migration::is_record(&record.payload)
        {
            return Err(corrupt(
                "learner history contains duplicate bootstrap authority",
            ));
        }
        if ordinal < stage.count {
            self.failed.store(true, Ordering::Relaxed);
            let recovered = stage.store.recover_current();
            stage.custody.validate()?;
            self.owner_custody.validate(&self.root, &self._lock)?;
            let recovery = recovered?.recovery;
            let previous = recovery
                .wal_records
                .get(ordinal as usize)
                .ok_or_else(|| corrupt("learner staged record disappeared"))?;
            let different = previous.epoch != record.epoch
                || previous.kind != record.kind
                || previous.payload != record.payload;
            let count = stage.count;
            // Physical prior-publication loss dominates either duplicate result.
            // Only a checked healthy semantic refusal may clear this sentinel.
            self.validate_ordinary_anchor()?;
            if different {
                self.failed.store(false, Ordering::Relaxed);
                return Err(corrupt(
                    "learner duplicate record differs from durable history",
                ));
            }
            self.failed.store(false, Ordering::Relaxed);
            return Ok(count);
        }
        let digest = transfer::extend_digest(stage.digest, &record)?;
        self.failed.store(true, Ordering::Relaxed);
        let result = stage.store.append_wal(WalAppend {
            pg: self.pg,
            epoch: record.epoch,
            index: LogIndex(ordinal + 1),
            kind: record.kind,
            previous_hash: stage.previous_hash,
            payload: &record.payload,
        });
        match result {
            Ok((_, _, wal)) => {
                stage.previous_hash = wal.record_hash;
                stage.digest = digest;
                stage.count += 1;
                stage.custody.validate()?;
                let count = stage.count;
                self.validate_ordinary_anchor()?;
                self.failed.store(false, Ordering::Relaxed);
                Ok(count)
            }
            Err(err) => {
                self.failed.store(true, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub fn committed_entries(&self) -> RsfResult<Vec<VsrCommittedEntry>> {
        self.healthy()?;
        self.published.as_ref().map_or_else(
            || Ok(Vec::new()),
            |published| published.cell.committed_prefix(),
        )
    }

    fn directory(&self, id: &[u8; 32]) -> PathBuf {
        self.root.join(
            id.iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
    }
    // A synced ACTIVE always points to the retained full history. Only names
    // generated by this store are reclaimed; partial deletions are retryable.
    fn reclaim_unreferenced(&self) -> RsfResult<()> {
        let active = self
            .published
            .as_ref()
            .map(|published| self.directory(&published.id));
        let staged = self
            .stage
            .as_ref()
            .map(|stage| stage.descriptor.id().map(|id| self.directory(&id)))
            .transpose()?;
        let mut removed = false;
        for entry in fs::read_dir(&self.root).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.len() != 64
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || !entry.file_type().map_err(io_error)?.is_dir()
            {
                continue;
            }
            let path = entry.path();
            if active.as_ref() != Some(&path) && staged.as_ref() != Some(&path) {
                fs::remove_dir_all(path).map_err(io_error)?;
                removed = true;
            }
        }
        if removed {
            sync_directory(&self.root)?;
        }
        Ok(())
    }
    fn require_ordinary_transfer(&self) -> RsfResult<()> {
        if self.prefix_context.is_some() {
            return Err(invalid_input(
                "bounded learner transfer requires its bounded API and validator",
            ));
        }
        Ok(())
    }
    fn healthy(&self) -> RsfResult<()> {
        self.check_failure()?;
        self.owner_custody
            .validate(&self.root, &self._lock)
            .and_then(|()| {
                self.stage
                    .as_ref()
                    .map_or(Ok(()), |stage| stage.custody.validate())
            })
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))?;
        if self.published.is_some() {
            self.published_state_token()?;
        }
        Ok(())
    }
    fn check_failure(&self) -> RsfResult<()> {
        if self.failed.load(Ordering::Relaxed) {
            return Err(corrupt(
                "learner durability failed; reopen before further operations",
            ));
        }
        Ok(())
    }
    fn validate_descriptor(&self, descriptor: &VsrHistoryDescriptor) -> RsfResult<()> {
        descriptor.encode()?;
        if descriptor.probe.pg != self.pg
            || descriptor.probe.primary != self.source
            || descriptor.probe.voters.contains(&self.local)
        {
            return Err(invalid_input(
                "history must come from the configured primary and exclude this learner from voters",
            ));
        }
        Ok(())
    }
}

fn io_error(err: std::io::Error) -> RsfError {
    corrupt(format!("learner history storage: {err}"))
}
fn sync_directory(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)
}
fn atomic_write(directory: &Path, name: &str, bytes: &[u8]) -> RsfResult<()> {
    let temporary = directory.join(format!(".{name}.tmp"));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(temporary, directory.join(name)).map_err(io_error)?;
    sync_directory(directory)
}

#[cfg(test)]
mod tests;
