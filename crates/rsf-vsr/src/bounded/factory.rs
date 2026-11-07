//! Reserved-only single-bootstrap factory. No voting cell exists before finish.
use super::*;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, Ordering};
mod spec;
pub use spec::VsrBootstrapSpec;

const AUXILIARY: &str = ".vsr-writer.lock";
const INITIAL_SEGMENT: &str = "000000000000000001.segment";
const INITIAL_SIDECAR: &str = "000000000000000001.lock";

/// An actual initialized bootstrap whose owner cannot yet vote or serve RPCs.
///
/// The caller MUST hold an independently durable matching reservation and
/// trusted exclusive namespace custody, with no prior protocol exposure. Never
/// use this factory under Initialized/Existing voter state, after a failed
/// strict existing open, or as repair of acknowledged state. This API cannot
/// infer historical exposure after native identity rollback or erasure.
#[derive(Debug)]
pub struct ProvisionalVsrVoter {
    root: PathBuf,
    spec: VsrBootstrapSpec,
    limits: VsrPrefixLimits,
    guard: custody::Guard,
    store: SegmentStore,
    custody: rsf_storage::SegmentStoreCustody,
    record: rsf_core::storage::WalRecord,
    writer_lock: rsf_storage::ExclusiveFileLock,
    failed: AtomicBool,
}

/// Borrowed evidence tied to one actual nonvoting owner. It is neither decoded
/// from caller metadata nor retained across the consuming completion call.
/// Metadata getters let the caller compare its own durable policy; only the
/// caller's successful durable publication authorizes this factory to finish.
#[derive(Debug)]
pub struct VsrBootstrapCompletion<'a> {
    owner: &'a ProvisionalVsrVoter,
}
impl VsrBootstrapCompletion<'_> {
    pub fn bootstrap(&self) -> &VsrBootstrapSpec {
        &self.owner.spec
    }
    pub fn limits(&self) -> VsrPrefixLimits {
        self.owner.limits
    }
    pub fn path(&self) -> &Path {
        &self.owner.root
    }
    pub fn validate(&self) -> RsfResult<()> {
        self.owner.recovery().map(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
    Root,
    Auxiliary,
    Storage,
}

impl ProvisionalVsrVoter {
    /// Complete only this reservation's canonical factory. The returned wrapper
    /// exposes no vote/prepare/commit/probe/transport method or raw cell/store.
    pub fn open(
        root: impl AsRef<Path>,
        spec: VsrBootstrapSpec,
        limits: VsrPrefixLimits,
    ) -> RsfResult<Self> {
        Self::open_with(root.as_ref(), spec, limits, &mut |_| Ok(()))
    }

    fn open_with(
        path: &Path,
        spec: VsrBootstrapSpec,
        limits: VsrPrefixLimits,
        hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<Self> {
        spec.check_limits(limits)?;
        let root = normalized(path)?;
        match fs::symlink_metadata(&root) {
            Ok(_) => {
                custody::directory(&root)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                rsf_storage::create_dir_all_synced(&root).map_err(io_error)?;
            }
            Err(error) => return Err(io_error(error)),
        }
        let mut guard = custody::Guard::new(&root)?;
        inventory(&root, false)?;
        let result = hook(Boundary::Root);
        guard.validate()?;
        inventory(&root, false)?;
        result?;
        let writer_lock = auxiliary(&root, &mut guard)?;
        let result = hook(Boundary::Auxiliary);
        guard.validate()?;
        guard.held_lock(&root.join(AUXILIARY), &writer_lock)?;
        inventory(&root, false)?;
        result?;
        let (store, record) = {
            let mut factory = SegmentStore::provisional_bootstrap(
                &root,
                &writer_lock,
                spec.pg(),
                spec.view(),
                &spec.bytes,
                SegmentStoreOptions::default(),
                limits.storage()?,
            )?;
            factory.complete()?
        };
        guard.directory(&root.join("segments"))?;
        guard.validate()?;
        guard.held_lock(&root.join(AUXILIARY), &writer_lock)?;
        let custody = store.capture_owned_custody()?;
        let provisional = Self {
            root,
            spec,
            limits,
            guard,
            store,
            custody,
            record,
            writer_lock,
            failed: AtomicBool::new(false),
        };
        let result = hook(Boundary::Storage);
        provisional.validate_owner()?;
        result?;
        provisional.recovery()?;
        Ok(provisional)
    }

    /// Consume this owner once. The callback must durably publish the caller's
    /// matching Initialized state; returning Ok alone is not native permission.
    /// Original physical custody and actual bootstrap bytes bracket every normal
    /// callback result. Errors and unwind drop this owner without a voting handle;
    /// the caller retains any required native sticky failure. No warm reopen occurs.
    pub fn finish_with(
        self,
        publish: impl FnOnce(&VsrBootstrapCompletion<'_>) -> RsfResult<()>,
    ) -> RsfResult<DurableVsrCell> {
        self.recovery()?;
        let result = publish(&VsrBootstrapCompletion { owner: &self });
        // Physical/readback failures dominate a callback error. This recovery is
        // parsed from disk, not constructed from the expected bootstrap payload.
        let recovery = self.recovery()?;
        result?;
        let active = publication::active(&self.root, self.spec.pg())?;
        if active.plan.is_some() || active.path != self.root {
            return Err(corrupt(
                "provisional voter acquired unexpected active authority",
            ));
        }
        let replayed = DurableVsrCell::replay_store(&recovery, self.spec.pg(), || {
            Err(corrupt("provisional voter lost its persisted bootstrap"))
        })?;
        let Self {
            root,
            spec,
            limits: _,
            guard,
            store,
            custody,
            record: _,
            writer_lock,
            failed: _,
        } = self;
        let cell = DurableVsrCell::from_replayed_store_with(
            OpenedVsrStore {
                root: root.clone(),
                active,
                store,
                recovery,
                options: SegmentStoreOptions::default(),
                writer_lock,
            },
            spec.pg(),
            replayed,
            |root, _, store| {
                guard.validate()?;
                inventory(root, true)?;
                store.validate_owned_custody(&custody)
            },
        )?;
        guard.validate()?;
        guard.held_lock(&root.join(AUXILIARY), &cell._writer_lock)?;
        inventory(&root, true)?;
        cell.store.validate_owned_custody(&custody)?;
        cell.recovery_state_token()?;
        Ok(cell)
    }

    fn validate_owner(&self) -> RsfResult<()> {
        self.guard.validate()?;
        self.guard
            .held_lock(&self.root.join(AUXILIARY), &self.writer_lock)?;
        self.store.validate_owned_custody(&self.custody)?;
        inventory(&self.root, true)?;
        self.guard.validate()?;
        self.guard
            .held_lock(&self.root.join(AUXILIARY), &self.writer_lock)?;
        self.store.validate_owned_custody(&self.custody)
    }

    fn recovery(&self) -> RsfResult<rsf_storage::Recovery> {
        if self.failed.load(Ordering::Acquire) {
            return Err(corrupt(
                "provisional voter custody failed; cold reserved recovery required",
            ));
        }
        let result = self.read_bootstrap();
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }

    fn read_bootstrap(&self) -> RsfResult<rsf_storage::Recovery> {
        self.validate_owner()?;
        let read = SegmentStore::recover_immutable_bounded(
            &self.root,
            self.limits.storage()?,
            self.limits.admission()?,
        );
        // A freshly read snapshot never replaces this original owner's evidence.
        self.validate_owner()?;
        let (recovered, read_custody) = read.map_err(storage_error)?;
        read_custody.validate()?;
        self.validate_owner()?;
        let recovery = recovered.recovery;
        if recovered.segments.len() != 1
            || recovery.records.len() != 1
            || recovery.wal_records.as_slice() != std::slice::from_ref(&self.record)
            || !recovery.maintenance_records.is_empty()
            || !recovery.compacted_prefixes.is_empty()
            || self.record.pg != self.spec.pg()
            || self.record.epoch != self.spec.view()
            || self.record.index != LogIndex(1)
            || self.record.kind != WalRecordKind::Membership
            || self.record.previous_hash != [0; 32]
            || self.record.payload.as_slice() != self.spec.bytes.as_slice()
        {
            return Err(corrupt(
                "provisional voter does not contain its exact one-record bootstrap",
            ));
        }
        Ok(recovery)
    }
}

fn auxiliary(root: &Path, guard: &mut custody::Guard) -> RsfResult<rsf_storage::ExclusiveFileLock> {
    let path = root.join(AUXILIARY);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    custody::nofollow(&mut options);
    let file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            guard.optional(&path, 0)?;
            let mut options = OpenOptions::new();
            options.write(true);
            custody::nofollow(&mut options);
            options.open(&path).map_err(io_error)?
        }
        Err(error) => return Err(io_error(error)),
    };
    let stamp = custody::regular(&file.metadata().map_err(io_error)?)?;
    custody::validate_descriptor(&path, &file, &stamp)?;
    guard.optional(&path, 0)?;
    // Keep a duplicate of the actual acquired descriptor only for its barrier;
    // it does not acquire another owner or outlive the factory construction.
    let renewal = file.try_clone().map_err(io_error)?;
    let lock = rsf_storage::ExclusiveFileLock::acquire(file).map_err(|error| {
        RsfError::new(
            ErrorCode::QuorumUnavailable,
            format!("VSR factory already has a writer: {error}"),
        )
    })?;
    guard.held_lock(&path, &lock)?;
    custody::validate_descriptor(&path, &renewal, &stamp)?;
    renewal.sync_all().map_err(io_error)?;
    guard.validate()?;
    guard.held_lock(&path, &lock)?;
    custody::validate_descriptor(&path, &renewal, &stamp)?;
    rsf_storage::sync_existing_directory(root).map_err(io_error)?;
    guard.validate()?;
    guard.held_lock(&path, &lock)?;
    Ok(lock)
}

fn inventory(root: &Path, complete: bool) -> RsfResult<()> {
    let mut names = BTreeSet::new();
    for (index, entry) in fs::read_dir(root).map_err(io_error)?.enumerate() {
        let entry = entry.map_err(io_error)?;
        if index >= 5 {
            return Err(corrupt("provisional voter root inventory exceeds bound"));
        }
        let name = entry.file_name();
        if name == "segments" {
            custody::directory(&entry.path())?;
            let mut segments = BTreeSet::new();
            for (index, entry) in fs::read_dir(entry.path()).map_err(io_error)?.enumerate() {
                let entry = entry.map_err(io_error)?;
                let name = entry.file_name();
                if index >= 2 || (name != INITIAL_SEGMENT && name != INITIAL_SIDECAR) {
                    return Err(corrupt("provisional voter has foreign segment inventory"));
                }
                custody::regular(&fs::symlink_metadata(entry.path()).map_err(io_error)?)?;
                segments.insert(name);
            }
            if complete
                && (segments.len() != 2
                    || !segments.contains(std::ffi::OsStr::new(INITIAL_SEGMENT))
                    || !segments.contains(std::ffi::OsStr::new(INITIAL_SIDECAR)))
            {
                return Err(corrupt(
                    "complete provisional voter lost its initial segment or lock",
                ));
            }
        } else if name == AUXILIARY
            || name == "MANIFEST"
            || name == "MANIFEST.lock"
            || (!complete && name == "MANIFEST.tmp")
        {
            custody::regular(&fs::symlink_metadata(entry.path()).map_err(io_error)?)?;
        } else {
            return Err(corrupt("provisional voter has foreign root inventory"));
        }
        names.insert(name);
    }
    if complete
        && (names.len() != 4
            || [AUXILIARY, "MANIFEST", "MANIFEST.lock", "segments"]
                .iter()
                .any(|name| !names.contains(std::ffi::OsStr::new(name))))
    {
        return Err(corrupt("complete provisional voter inventory is missing"));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests;
