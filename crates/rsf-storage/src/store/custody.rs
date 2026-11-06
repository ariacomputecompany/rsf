//! Opt-in physical custody for a caller that exclusively owns this store.
//! Generic SegmentStore handles may still coordinate and refresh other writers.
//! This stamp neither repairs nor refreshes an externally changed owner.
use super::*;
use std::io::Read;
use std::sync::Arc;
mod append;
mod discard;
pub use discard::{SegmentDiscardInventory, SegmentDiscardRemainder};
mod immutable;
pub(in crate::store) use append::OwnedAppend;
pub use append::OwnedWalGroup;
mod renewal;
mod scan;
pub use immutable::SegmentReadCustody;
pub use scan::{
    SegmentReadCursor, SegmentReadError, SegmentReadLimit, SegmentReadLimits, SegmentScanControl,
    SegmentScanOutcome, append_only_manifest_byte_bound, wal_record_disk_bytes,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Identity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileStamp {
    identity: Identity,
    bytes: u64,
    modified: SystemTime,
}

/// Process-local evidence of the physical files belonging to one validated
/// store handle. This is not a content checksum scrub or a durable certificate.
/// Namespace routing must remain under caller custody. Metadata-preserving
/// hostile writes and device corruption invisible to metadata are not detected.
#[derive(Clone, Debug)]
pub struct SegmentStoreCustody {
    observer: Arc<()>,
    root: Identity,
    directory: Identity,
    manifest: FileStamp,
    segments: Vec<(u64, FileStamp)>,
}

impl SegmentStore {
    /// Capture only after validated open or this owner's successful WAL write.
    /// The validated cursor and manifest must still describe the named files.
    /// The opt-in owner uses canonical manifests and regular, singly linked
    /// files. Trusted ancestor aliases and relative/parent routes remain valid;
    /// a final owned directory/file alias does not establish exclusive custody.
    pub fn capture_owned_custody(&self) -> RsfResult<SegmentStoreCustody> {
        self.ensure_writable()?;
        let _guard = lock_gate(&self.gate)?;
        self.capture_owned_custody_locked()
    }

    /// Compare without adopting another writer, replaying/truncating a log,
    /// creating a path, syncing ancestors, or changing a caller's cache.
    pub fn validate_owned_custody(&self, expected: &SegmentStoreCustody) -> RsfResult<()> {
        self.ensure_writable()?;
        let _guard = lock_gate(&self.gate)?;
        self.validate_owned_custody_locked(expected)
    }

    /// Renew retained closed-file data barriers for an exclusively owned
    /// recovered prefix before publishing it as authority. A valid manifest or
    /// successful replay alone cannot prove a previous writer's sync succeeded.
    /// The active file is renewed by `append_next_wal_group`, including an empty
    /// suffix; this avoids an early and late flush of that same descriptor.
    /// No generic open/read path gains a barrier or changes its sync policy.
    pub fn sync_owned_closed_segments(&mut self, expected: &SegmentStoreCustody) -> RsfResult<()> {
        self.ensure_writable()?;
        if self.options.log_options.sync_policy == crate::SyncPolicy::None {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "durable prefix renewal requires Data or Full sync",
            ));
        }
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.write_failed = true;
        let result = (|| {
            self.validate_owned_custody_locked(expected)?;
            for ((id, stamp), descriptor) in expected.segments.iter().zip(&self.manifest.segments) {
                if *id == self.manifest.active_id || stamp.bytes == 0 {
                    continue;
                }
                if descriptor.id != *id {
                    return Err(custody_error("retained segment lost its descriptor"));
                }
                let path = self.root.join(SEGMENT_DIR).join(&descriptor.file_name);
                let mut options = OpenOptions::new();
                options.read(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NOFOLLOW);
                }
                let file = options.open(&path).map_err(custody_io)?;
                if file_stamp(&file.metadata().map_err(custody_io)?)? != *stamp {
                    return Err(custody_error(
                        "retained segment descriptor differs from validated custody",
                    ));
                }
                crate::sync_segment(&file, &path, self.options.log_options.sync_policy)?;
                if file_stamp(&file.metadata().map_err(custody_io)?)? != *stamp {
                    return Err(custody_error(
                        "retained segment changed during durability renewal",
                    ));
                }
            }
            self.validate_owned_custody_locked(expected)
        })();
        if result.is_err() {
            self.cursor = None;
        } else {
            self.write_failed = false;
        }
        result
    }

    fn validate_owned_custody_locked(&self, expected: &SegmentStoreCustody) -> RsfResult<()> {
        if !Arc::ptr_eq(&self.observer, &expected.observer) {
            return Err(custody_error(
                "custody stamp belongs to another store handle",
            ));
        }
        let current = self.capture_owned_custody_locked()?;
        if current.root != expected.root
            || current.directory != expected.directory
            || current.manifest != expected.manifest
            || current.segments != expected.segments
        {
            return Err(custody_error(
                "exclusive store physical custody changed; reopen required",
            ));
        }
        Ok(())
    }

    fn capture_owned_custody_locked(&self) -> RsfResult<SegmentStoreCustody> {
        self.capture_owned_custody_for_cursor_locked(None)
    }

    // An owned group temporarily holds the same cursor outside self while it
    // mutates. This shares the exact bounded physical checks, not recovery.
    fn capture_owned_custody_for_cursor_locked(
        &self,
        cursor: Option<&WalCursor>,
    ) -> RsfResult<SegmentStoreCustody> {
        // Removing harmless '.' components exposes a final alias such as x/.
        // Preserve ParentDir: collapsing it across a trusted ancestor alias
        // would change the actual filesystem route.
        let mut path = PathBuf::new();
        for component in self.root.components() {
            if component != std::path::Component::CurDir {
                path.push(component.as_os_str());
            }
        }
        if path.as_os_str().is_empty() {
            path.push(".");
        }
        let root = directory_identity(&path)?;
        validate_lock_name(
            &self._lock,
            &path.join(MANIFEST_FILE).with_extension("lock"),
        )?;
        validate_lock_name(&self.active._lock, &self.active.path.with_extension("lock"))?;
        let directory = directory_identity(&path.join(SEGMENT_DIR))?;
        let expected = self.manifest.encode();
        let manifest_path = path.join(MANIFEST_FILE);
        let manifest = regular_file(&manifest_path)?;
        if manifest.bytes != expected.len() as u64 {
            return Err(custody_error(
                "owned manifest length differs from validated layout",
            ));
        }
        // Bound the read by the already known manifest encoding, never a
        // potentially changed on-disk length. No WAL payload is read here.
        let bytes = immutable::read_checked(&manifest_path, &manifest, expected.len() as u64)?;
        if bytes != expected.as_bytes() {
            return Err(custody_error(
                "owned manifest differs from validated layout",
            ));
        }
        let cursor = cursor.or(self.cursor.as_ref()).ok_or_else(|| {
            custody_error("owned custody requires an already validated WAL cursor")
        })?;
        if cursor.stamp.len() != self.manifest.segments.len() {
            return Err(custody_error("owned segment cursor differs from manifest"));
        }
        let mut segments = Vec::with_capacity(cursor.stamp.len());
        for (descriptor, previous) in self.manifest.segments.iter().zip(&cursor.stamp) {
            let stamp = regular_file(&path.join(SEGMENT_DIR).join(&descriptor.file_name))?;
            if previous.id != descriptor.id
                || previous.bytes != stamp.bytes
                || previous.modified != stamp.modified
            {
                return Err(custody_error(
                    "owned segment changed after validated WAL state",
                ));
            }
            if descriptor.id == self.manifest.active_id {
                let opened = file_stamp(&self.active.file.metadata().map_err(custody_io)?)?;
                if opened != stamp {
                    return Err(custody_error(
                        "owned active segment path differs from open writer",
                    ));
                }
            }
            segments.push((descriptor.id, stamp));
        }
        Ok(SegmentStoreCustody {
            observer: self.observer.clone(),
            root,
            directory,
            manifest,
            segments,
        })
    }
}

// Compare the actual retained exclusion descriptor, never adopt a replaced
// sidecar merely because its current named bytes are still empty.
fn held_lock_stamp(lock: &SegmentLock) -> RsfResult<FileStamp> {
    let registry = crate::lock_registry()
        .lock()
        .map_err(|_| custody_error("segment lock registry is poisoned"))?;
    let owner = registry
        .get(&lock.path)
        .ok_or_else(|| custody_error("owned sidecar lock is absent"))?;
    file_stamp(&owner._file.metadata().map_err(custody_io)?)
}
fn validate_lock_name(lock: &SegmentLock, path: &Path) -> RsfResult<()> {
    if regular_file(path)? != held_lock_stamp(lock)? {
        return Err(custody_error(
            "owned sidecar path differs from actual held lock",
        ));
    }
    Ok(())
}

fn directory_identity(path: &Path) -> RsfResult<Identity> {
    let metadata = fs::symlink_metadata(path).map_err(custody_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(custody_error("owned store directory is absent or aliased"));
    }
    identity(&metadata)
}

fn regular_file(path: &Path) -> RsfResult<FileStamp> {
    file_stamp(&fs::symlink_metadata(path).map_err(custody_io)?)
}

fn file_stamp(metadata: &fs::Metadata) -> RsfResult<FileStamp> {
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(custody_error("owned store file is absent or aliased"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(custody_error("owned store file is not singly linked"));
        }
    }
    Ok(FileStamp {
        identity: identity(metadata)?,
        bytes: metadata.len(),
        modified: metadata.modified().map_err(custody_io)?,
    })
}

#[cfg(unix)]
fn identity(metadata: &fs::Metadata) -> RsfResult<Identity> {
    use std::os::unix::fs::MetadataExt;
    Ok(Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn identity(_: &fs::Metadata) -> RsfResult<Identity> {
    Err(custody_error(
        "exclusive physical custody requires qualified file identity support",
    ))
}

fn custody_error(message: &str) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

fn custody_io(error: std::io::Error) -> RsfError {
    storage_error(StorageIoOperation::Recover, error)
}

#[cfg(all(test, unix))]
mod tests;
