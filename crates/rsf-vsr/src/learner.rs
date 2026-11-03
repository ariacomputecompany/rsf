//! Staged history is never a voting replica. Publication requires complete
//! hash-checked history, normal VSR recovery, and extension of published truth.
use super::*;
mod custody;
pub(crate) mod promotion;
pub use custody::VsrLearnerStateToken;
use rsf_core::Digest;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug)]
struct Stage {
    descriptor: VsrHistoryDescriptor,
    store: SegmentStore,
    count: u64,
    digest: Digest,
    previous_hash: [u8; 32],
}

#[derive(Debug)]
struct Published {
    id: [u8; 32],
    descriptor: VsrHistoryDescriptor,
    cell: DurableVsrCell,
}

/// A separately configured non-voter. No method exposes a writable VSR cell or
/// a prepare/commit acknowledgement. The configured source is authenticated by
/// the transport; a transferred voter list cannot change this receiver's role.
#[derive(Debug)]
pub struct LearnerHistoryStore {
    root: PathBuf,
    pg: PlacementGroupId,
    local: NodeId,
    source: NodeId,
    stage: Option<Stage>,
    published: Option<Published>,
    failed: bool,
    _lock: rsf_storage::ExclusiveFileLock,
}

impl LearnerHistoryStore {
    pub fn open(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        local: NodeId,
        source: NodeId,
    ) -> RsfResult<Self> {
        if local == source {
            return Err(invalid_input("learner and source must differ"));
        }
        let root = path.as_ref().to_path_buf();
        rsf_storage::create_dir_all_synced(&root).map_err(io_error)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join(".learner.lock"))
            .map_err(io_error)?;
        let lock = rsf_storage::ExclusiveFileLock::acquire(file)
            .map_err(|err| invalid_input(format!("learner store already has an owner: {err}")))?;
        let mut identity = b"RSFL1".to_vec();
        for value in [pg.0, local.0, source.0] {
            identity.extend_from_slice(&value.to_le_bytes());
        }
        match fs::read(root.join("IDENTITY")) {
            Ok(existing) if existing == identity => {}
            Ok(_) => {
                return Err(invalid_input(
                    "learner store identity differs from configuration",
                ));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                for entry in fs::read_dir(&root).map_err(io_error)? {
                    let entry = entry.map_err(io_error)?;
                    if entry.file_name() != ".learner.lock" && entry.file_name() != ".IDENTITY.tmp"
                    {
                        return Err(corrupt("nonempty learner store has no identity"));
                    }
                }
                atomic_write(&root, "IDENTITY", &identity)?
            }
            Err(err) => return Err(io_error(err)),
        }
        let mut learner = Self {
            root,
            pg,
            local,
            source,
            stage: None,
            published: None,
            failed: false,
            _lock: lock,
        };
        match fs::read(learner.root.join("ACTIVE")) {
            Ok(bytes) => {
                let id: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| corrupt("invalid active learner history id"))?;
                let directory = learner.directory(&id);
                let descriptor = VsrHistoryDescriptor::decode(
                    &fs::read(directory.join("DESCRIPTOR")).map_err(io_error)?,
                )?;
                learner.validate_descriptor(&descriptor)?;
                if descriptor.id()? != id {
                    return Err(corrupt("active learner descriptor identity mismatch"));
                }
                let stage = learner.recover_stage(descriptor)?;
                learner.published = Some(learner.validate_complete(stage)?);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_error(err)),
        }
        Ok(learner)
    }

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
        self.stage = None;
        let directory = self.directory(&id);
        fs::create_dir_all(&directory).map_err(io_error)?;
        sync_directory(&self.root)?;
        let encoded = descriptor.encode()?;
        match fs::read(directory.join("DESCRIPTOR")) {
            Ok(existing) if existing == encoded => {}
            Ok(_) => return Err(corrupt("staged descriptor identity mismatch")),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                atomic_write(&directory, "DESCRIPTOR", &encoded)?
            }
            Err(err) => return Err(io_error(err)),
        }
        let stage = self.recover_stage(descriptor)?;
        let count = stage.count;
        self.stage = Some(stage);
        self.reclaim_unreferenced()?;
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
            let recovery = stage.store.recover_current()?.recovery;
            let previous = recovery
                .wal_records
                .get(ordinal as usize)
                .ok_or_else(|| corrupt("learner staged record disappeared"))?;
            if previous.epoch != record.epoch
                || previous.kind != record.kind
                || previous.payload != record.payload
            {
                return Err(corrupt(
                    "learner duplicate record differs from durable history",
                ));
            }
            return Ok(stage.count);
        }
        let digest = transfer::extend_digest(stage.digest, &record)?;
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
                Ok(stage.count)
            }
            Err(err) => {
                self.failed = true;
                Err(err)
            }
        }
    }

    /// Publish only after replay validates every historical certificate. The
    /// prior snapshot remains authoritative until the synced ACTIVE rename.
    pub fn finish(&mut self, id: [u8; 32]) -> RsfResult<LogIndex> {
        self.healthy()?;
        if let Some(published) = &self.published
            && published.id == id
        {
            let tail = published.cell.committed_tail();
            self.reclaim_unreferenced()?;
            return Ok(tail);
        }
        let stage = self
            .stage
            .as_ref()
            .ok_or_else(|| invalid_input("no staged learner transfer"))?;
        if stage.descriptor.id()? != id {
            return Err(invalid_input("learner finish snapshot mismatch"));
        }
        if stage.count != stage.descriptor.records {
            return Err(invalid_input("learner transfer is incomplete"));
        }
        let stage = self.stage.take().unwrap();
        let descriptor = stage.descriptor.clone();
        drop(stage);
        // Bind publication to bytes recovered from disk, including records that
        // were acknowledged before this process or transfer attempt began.
        let candidate = self.validate_complete(self.recover_stage(descriptor)?)?;
        if let Some(previous) = &self.published {
            let old = &previous.cell;
            let new = &candidate.cell;
            if old.initial_bootstrap != new.initial_bootstrap
                || new.view() < old.view()
                || !new.history.compatible(&old.history)
                || !installation::preserves_committed(&new.cell, &old.cell)
                || old
                    .cell
                    .client_table
                    .iter()
                    .any(|(key, value)| new.cell.client_table.get(key) != Some(value))
            {
                return Err(corrupt(
                    "learner history conflicts with published commits, configurations, receipts or bootstrap",
                ));
            }
        }
        if let Err(err) = atomic_write(&self.root, "ACTIVE", &id) {
            self.failed = true;
            return Err(err);
        }
        let tail = candidate.cell.committed_tail();
        self.published = Some(candidate);
        self.reclaim_unreferenced()?;
        Ok(tail)
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
    fn healthy(&self) -> RsfResult<()> {
        if self.failed {
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
    fn recover_stage(&self, descriptor: VsrHistoryDescriptor) -> RsfResult<Stage> {
        let directory = self.directory(&descriptor.id()?).join("cell");
        let store = SegmentStore::open(&directory)?;
        let recovery = store.recover_current()?.recovery;
        let mut digest = Digest::blake3_bytes(b"RSF history chain v1");
        let mut previous_hash = [0; 32];
        let count = recovery.wal_records.len() as u64;
        if count > descriptor.records {
            return Err(corrupt("learner history exceeds declared record count"));
        }
        for (position, record) in recovery.wal_records.iter().enumerate() {
            if record.pg != self.pg || record.index.0 != position as u64 + 1 {
                return Err(corrupt(
                    "learner history has foreign or noncontiguous records",
                ));
            }
            digest = transfer::extend_digest(
                digest,
                &VsrHistoryRecord {
                    epoch: record.epoch,
                    kind: record.kind,
                    payload: record.payload.to_vec(),
                },
            )?;
            previous_hash = record.record_hash;
        }
        Ok(Stage {
            descriptor,
            store,
            count,
            digest,
            previous_hash,
        })
    }
    fn validate_complete(&self, stage: Stage) -> RsfResult<Published> {
        let Stage {
            descriptor,
            store,
            count,
            digest,
            ..
        } = stage;
        if count != descriptor.records || digest != descriptor.digest {
            return Err(corrupt("learner history count or digest mismatch"));
        }
        let path = store.root().to_path_buf();
        drop(store);
        let cell = DurableVsrCell::open(
            &path,
            self.pg,
            descriptor.probe.view,
            descriptor.probe.primary,
            descriptor.probe.voters.clone(),
        )?;
        if cell.primary_probe()? != descriptor.probe || !cell.bootstrap_persisted {
            return Err(corrupt(
                "recovered learner history differs from source probe",
            ));
        }
        Ok(Published {
            id: descriptor.id()?,
            descriptor,
            cell,
        })
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
