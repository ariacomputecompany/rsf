//! Opt-in physical custody for a caller that exclusively owns this store.
//! Generic SegmentStore handles may still coordinate and refresh other writers.
//! This stamp neither repairs nor refreshes an externally changed owner.
use super::*;
use std::io::Read;
use std::sync::Arc;

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
        let mut bytes = Vec::with_capacity(expected.len());
        File::open(&manifest_path)
            .map_err(custody_io)?
            .take(expected.len() as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(custody_io)?;
        if bytes != expected.as_bytes() {
            return Err(custody_error(
                "owned manifest differs from validated layout",
            ));
        }
        let cursor = self.cursor.as_ref().ok_or_else(|| {
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
