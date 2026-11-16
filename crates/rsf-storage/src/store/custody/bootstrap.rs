//! Exact reserved bootstrap continuation; never general WAL recovery.
use super::*;
use std::io::{Seek, SeekFrom};
mod inventory;
use inventory::Inventory;

/// One-shot physical factory under an independently durable reservation.
///
/// The caller must already hold the root namespace and the supplied original
/// `.vsr-writer.lock`, and must prove this factory has never exposed a voter or
/// acknowledged custody. Neither canonical bytes nor this type proves that
/// reservation. Never use after InitializedVoter, a failed strict existing open,
/// or observed warm custody loss. This is storage evidence, not vote authority.
/// The domain factory must keep the auxiliary owner and withhold the returned
/// store until its own native durable publication and custody checks succeed.
#[derive(Debug)]
pub struct ProvisionalBootstrap<'a> {
    root: PathBuf,
    auxiliary: &'a crate::ExclusiveFileLock,
    inventory: Inventory,
    frame: Vec<u8>,
    record: WalRecord,
    options: SegmentStoreOptions,
    gate: StoreGate,
    attempted: bool,
}

impl SegmentStore {
    /// Read-only finite preflight, retaining actual original file descriptors.
    /// The root and held auxiliary must already exist. Only canonical initial
    /// manifest prefixes and the exact one-frame bootstrap prefix are accepted.
    /// `payload` must be the domain's canonical bootstrap encoding; storage does
    /// not interpret membership or establish the caller's reservation authority.
    pub fn provisional_bootstrap<'a>(
        root: impl AsRef<Path>,
        auxiliary: &'a crate::ExclusiveFileLock,
        pg: PlacementGroupId,
        epoch: rsf_core::Epoch,
        payload: &[u8],
        options: SegmentStoreOptions,
        limits: SegmentReadLimits,
    ) -> RsfResult<ProvisionalBootstrap<'a>> {
        let disk_bytes = wal_record_disk_bytes(payload.len() as u64)?;
        let manifest = SegmentManifest::initial().encode();
        if limits.max_manifest_bytes < manifest.len()
            || limits.max_records == 0
            || limits.max_record_bytes < disk_bytes
            || limits.max_encoded_bytes < disk_bytes
            || options.log_options.sync_policy == crate::SyncPolicy::None
        {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "bootstrap exceeds finite durable policy",
            ));
        }
        // Cap before encoding/copying caller bytes or inspecting the filesystem.
        let frame_capacity = usize::try_from(disk_bytes).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "bootstrap frame does not fit memory",
            )
        })?;
        let encoded = super::super::wal::encode_at_tail(
            WalAppend {
                pg,
                epoch,
                index: LogIndex(1),
                kind: WalRecordKind::Membership,
                previous_hash: [0; 32],
                payload,
            },
            None,
        )?;
        let record = WalRecord {
            pg,
            epoch,
            index: LogIndex(1),
            kind: WalRecordKind::Membership,
            previous_hash: [0; 32],
            payload: payload.to_vec(),
            payload_hash: encoded.payload_hash,
            record_hash: encoded.record_hash,
        };
        let mut frame = Vec::with_capacity(frame_capacity);
        frame.extend_from_slice(&crate::encode_segment_header(&encoded.encoded)?);
        frame.extend_from_slice(&encoded.encoded);
        let root = immutable::normalize_root(root.as_ref());
        let gate = store_gate(&root)?;
        let inventory = {
            let _guard = lock_gate(&gate)?;
            Inventory::capture(&root, auxiliary, &frame)?
        };
        Ok(ProvisionalBootstrap {
            root,
            auxiliary,
            inventory,
            frame,
            record,
            options,
            gate,
            attempted: false,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
    ManifestLock,
    Directory,
    Segment,
    SegmentLock,
    TemporaryAppended,
    Temporary,
    Manifest,
    PrefixAppended,
    PrefixWritten,
    PrefixSynced,
    Published,
}

impl ProvisionalBootstrap<'_> {
    /// Attempt once. Any result consumes the attempt; failure is sticky even if
    /// original names are restored. Cold retry needs fresh caller authorization.
    /// Success returns the same actual descriptor/locks, never a reopened writer.
    pub fn complete(&mut self) -> RsfResult<(SegmentStore, WalRecord)> {
        self.complete_with(|_| Ok(()))
    }

    fn complete_with(
        &mut self,
        mut after: impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<(SegmentStore, WalRecord)> {
        if self.attempted {
            return Err(custody_error(
                "provisional bootstrap attempt already consumed",
            ));
        }
        self.attempted = true;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        let result = self.run(&mut after);
        // Custody failures dominate injected/I/O failures, including callbacks.
        self.inventory.validate(&self.root, self.auxiliary)?;
        result
    }

    fn run(
        &mut self,
        after: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<(SegmentStore, WalRecord)> {
        self.inventory.validate(&self.root, self.auxiliary)?;
        let initial = SegmentManifest::initial();
        let segment = PathBuf::from(SEGMENT_DIR).join(&initial.segments[0].file_name);
        let segment_lock = segment.with_extension("lock");
        let manifest_lock = self.lock(Path::new("MANIFEST.lock"))?;
        self.boundary(Boundary::ManifestLock, after)?;
        self.inventory
            .create_directory(&self.root, self.auxiliary)?;
        self.boundary(Boundary::Directory, after)?;
        self.inventory
            .ensure_file(&self.root, &segment, self.auxiliary)?;
        self.boundary(Boundary::Segment, after)?;
        let active_lock = self.lock(&segment_lock)?;
        self.boundary(Boundary::SegmentLock, after)?;
        if !self.inventory.files.contains_key(Path::new(MANIFEST_FILE)) {
            let temporary = Path::new("MANIFEST.tmp");
            self.inventory
                .ensure_file(&self.root, temporary, self.auxiliary)?;
            self.append_prefix(
                temporary,
                initial.encode().as_bytes(),
                Boundary::TemporaryAppended,
                after,
            )?;
            self.boundary(Boundary::Temporary, after)?;
            self.inventory.rename_manifest(&self.root, self.auxiliary)?;
        }
        // Publish/renew the final manifest name BEFORE any WAL suffix. Otherwise
        // a crash may retain MANIFEST.tmp plus durable nonempty WAL, a state the
        // exact provisional classifier correctly refuses. This also renews a
        // cold final manifest whose previous rename barrier was uncertain.
        self.inventory.validate(&self.root, self.auxiliary)?;
        let published =
            crate::durable_directory::sync_file_publication(&self.root.join(MANIFEST_FILE))
                .map_err(custody_io);
        self.inventory.validate(&self.root, self.auxiliary)?;
        published?;
        self.boundary(Boundary::Manifest, after)?;
        // This writes only missing suffix bytes and verifies actual complete
        // contents through the original read-capable descriptor before syncing.
        let frame = std::mem::take(&mut self.frame);
        self.append_prefix(&segment, &frame, Boundary::PrefixAppended, after)?;
        self.frame = frame;
        self.boundary(Boundary::PrefixWritten, after)?;
        crate::sync_segment(
            &self.inventory.files[&segment].0,
            &self.root.join(&segment),
            self.options.log_options.sync_policy,
        )?;
        self.boundary(Boundary::PrefixSynced, after)?;
        for (name, (file, _)) in &self.inventory.files {
            if *name != segment {
                file.sync_all().map_err(custody_io)?;
            }
            crate::durable_directory::sync_file_publication(&self.root.join(name))
                .map_err(custody_io)?;
        }
        crate::sync_existing_directory(&self.root).map_err(custody_io)?;
        self.boundary(Boundary::Published, after)?;
        validate_lock_name(&manifest_lock, &self.root.join("MANIFEST.lock"))?;
        validate_lock_name(&active_lock, &self.root.join(&segment_lock))?;
        let (file, stamp) = self
            .inventory
            .files
            .get(&segment)
            .ok_or_else(|| custody_error("bootstrap segment absent"))?;
        // Duplication preserves the exact descriptor/inode and append mode;
        // storage locks themselves move directly into the returned owner.
        let file = file.try_clone().map_err(custody_io)?;
        let mut tails = BTreeMap::new();
        tails.insert(self.record.pg, WalTail::from(&self.record));
        let store = SegmentStore {
            root: self.root.clone(),
            manifest: initial,
            active: SegmentLog {
                path: self.root.join(&segment),
                file,
                options: self.options.log_options,
                _lock: active_lock,
            },
            _lock: manifest_lock,
            options: self.options,
            write_failed: false,
            gate: self.gate.clone(),
            observer: Arc::new(()),
            external_changes: 0,
            cursor: Some(WalCursor {
                tails,
                stamp: vec![SegmentStamp {
                    id: 1,
                    bytes: stamp.bytes,
                    modified: stamp.modified,
                }],
            }),
        };
        store.capture_owned_custody_locked()?;
        Ok((store, self.record.clone()))
    }

    fn lock(&mut self, name: &Path) -> RsfResult<SegmentLock> {
        self.inventory
            .ensure_file(&self.root, name, self.auxiliary)?;
        let (file, stamp) = &self.inventory.files[name];
        if stamp.bytes != 0 {
            return Err(custody_error("nonempty provisional lock"));
        }
        let path = self.root.join(name);
        let lock = SegmentLock::register_exclusive_opened(
            fs::canonicalize(&path).map_err(custody_io)?,
            file.try_clone().map_err(custody_io)?,
        )?;
        validate_lock_name(&lock, &path)?;
        self.inventory.validate(&self.root, self.auxiliary)?;
        Ok(lock)
    }

    fn boundary(
        &self,
        phase: Boundary,
        after: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.inventory.validate(&self.root, self.auxiliary)?;
        let result = after(phase);
        self.inventory.validate(&self.root, self.auxiliary)?;
        result
    }

    fn append_prefix(
        &mut self,
        name: &Path,
        expected: &[u8],
        phase: Boundary,
        after: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.inventory.validate(&self.root, self.auxiliary)?;
        let (file, stamp) = self
            .inventory
            .files
            .get_mut(name)
            .ok_or_else(|| custody_error("bootstrap file absent"))?;
        let prefix = inventory::read_prefix(file, stamp, expected)?;
        file.write_all(&expected[prefix..]).map_err(custody_io)?;
        let callback = after(phase);
        let written = file_stamp(&file.metadata().map_err(custody_io)?)?;
        if written.identity != stamp.identity
            || written.bytes != expected.len() as u64
            || regular_file(&self.root.join(name))? != written
        {
            return Err(custody_error("bootstrap append lost original descriptor"));
        }
        if inventory::read_prefix(file, &written, expected)? != expected.len() {
            return Err(custody_error("bootstrap append readback incomplete"));
        }
        *stamp = written;
        self.inventory.validate(&self.root, self.auxiliary)?;
        callback?;
        if name == Path::new("MANIFEST.tmp") {
            file_sync(&self.inventory.files[name].0)?;
        }
        Ok(())
    }
}
fn file_sync(file: &File) -> RsfResult<()> {
    file.sync_all().map_err(custody_io)
}

#[cfg(all(test, unix))]
mod tests;
