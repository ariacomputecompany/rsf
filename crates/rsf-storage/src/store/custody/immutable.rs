//! Physical continuity of a fully read, immutable store. Callers exclusively
//! own the surrounding namespace; this type neither owns a writer nor repairs.
use super::*;

#[derive(Debug)]
pub struct SegmentReadCustody {
    root: PathBuf,
    gate: StoreGate,
    root_identity: Identity,
    directory: Identity,
    manifest: FileStamp,
    manifest_bytes: Vec<u8>,
    segments: Vec<(String, FileStamp)>,
}

impl SegmentStore {
    /// Read existing immutable files and bind the actual validated bytes to
    /// their physical identities. No directories, lock sidecars or files are
    /// created, no tail is repaired, and no generic writer state is refreshed.
    /// The caller holds exclusive parent namespace custody. The manifest byte
    /// budget must also have been enforced before acknowledging its factory.
    pub fn recover_immutable(
        root: impl AsRef<Path>,
        max_manifest_bytes: usize,
    ) -> RsfResult<(SegmentStoreRecovery, SegmentReadCustody)> {
        recover_immutable(root.as_ref(), max_manifest_bytes, || {})
    }
}

// The callback is an internal deterministic fault boundary after the complete
// physical snapshot and before byte recovery. Public callers always pass no-op.
fn recover_immutable(
    root: &Path,
    max_manifest_bytes: usize,
    captured: impl FnOnce(),
) -> RsfResult<(SegmentStoreRecovery, SegmentReadCustody)> {
    let mut path = PathBuf::new();
    for component in root.components() {
        if component != std::path::Component::CurDir {
            path.push(component.as_os_str());
        }
    }
    if path.as_os_str().is_empty() {
        path.push(".");
    }
    let root_identity = directory_identity(&path)?;
    let directory = directory_identity(&path.join(SEGMENT_DIR))?;
    let gate = store_gate(&path)?;
    let _guard = lock_gate(&gate)?;
    let manifest_path = path.join(MANIFEST_FILE);
    let manifest_stamp = regular_file(&manifest_path)?;
    let limit = u64::try_from(max_manifest_bytes)
        .map_err(|_| custody_error("immutable manifest budget exceeds platform range"))?;
    if manifest_stamp.bytes > limit {
        return Err(custody_error(
            "immutable manifest exceeds caller byte budget",
        ));
    }
    let bytes = read_checked(&manifest_path, &manifest_stamp, limit)?;
    let manifest = SegmentManifest::decode(&bytes)?;
    if manifest.encode().as_bytes() != bytes {
        return Err(custody_error("immutable manifest is not canonical"));
    }
    let segments = manifest
        .segments
        .iter()
        .map(|segment| {
            let stamp = regular_file(&path.join(SEGMENT_DIR).join(&segment.file_name))?;
            Ok((segment.file_name.clone(), stamp))
        })
        .collect::<RsfResult<Vec<_>>>()?;
    let custody = SegmentReadCustody {
        root: path,
        gate: gate.clone(),
        root_identity,
        directory,
        manifest: manifest_stamp,
        manifest_bytes: bytes,
        segments,
    };
    captured();
    let mut recovery = Recovery {
        records: Vec::new(),
        wal_records: Vec::new(),
        maintenance_records: Vec::new(),
        pg_tails: manifest
            .compacted_prefixes
            .iter()
            .map(|(pg, prefix)| {
                (
                    *pg,
                    WalTail {
                        epoch: prefix.epoch,
                        index: prefix.index,
                        record_hash: prefix.record_hash,
                    },
                )
            })
            .collect(),
        compacted_prefixes: manifest.compacted_prefixes.clone(),
        truncated_to: 0,
    };
    for (name, expected) in &custody.segments {
        let path = custody.root.join(SEGMENT_DIR).join(name);
        let file = open_checked(&path, expected)?;
        // Keep a descriptor to the exact parsed inode for the post-read
        // metadata check; only recover_opened advances the shared file offset.
        let observed = file.try_clone().map_err(custody_io)?;
        let current = SegmentLog::recover_opened(file, recovery.pg_tails, false)?;
        if file_stamp(&observed.metadata().map_err(custody_io)?)? != *expected
            || regular_file(&path)? != *expected
        {
            return Err(custody_error("immutable segment changed during recovery"));
        }
        recovery.records.extend(current.records);
        recovery.wal_records.extend(current.wal_records);
        recovery
            .maintenance_records
            .extend(current.maintenance_records);
        recovery.pg_tails = current.pg_tails;
        recovery.truncated_to = recovery
            .truncated_to
            .checked_add(current.truncated_to)
            .ok_or_else(|| custody_error("immutable recovery length overflow"))?;
    }
    custody.validate_locked()?;
    Ok((
        SegmentStoreRecovery {
            segments: manifest.segments,
            recovery,
        },
        custody,
    ))
}

impl SegmentReadCustody {
    /// Metadata/manifest-only comparison. Never adopts a changed file, reads WAL
    /// payloads, syncs, creates, truncates or repairs anything. Unix file identity
    /// is required, as for SegmentStoreCustody. This is not a checksum scrub or
    /// protection against hostile metadata-preserving content modification.
    pub fn validate(&self) -> RsfResult<()> {
        let _guard = lock_gate(&self.gate)?;
        self.validate_locked()
    }

    fn validate_locked(&self) -> RsfResult<()> {
        if directory_identity(&self.root)? != self.root_identity
            || directory_identity(&self.root.join(SEGMENT_DIR))? != self.directory
        {
            return Err(custody_error("immutable store directory custody changed"));
        }
        let path = self.root.join(MANIFEST_FILE);
        let bytes = read_checked(&path, &self.manifest, self.manifest_bytes.len() as u64)?;
        if bytes != self.manifest_bytes {
            return Err(custody_error("immutable store manifest changed"));
        }
        for (name, expected) in &self.segments {
            if regular_file(&self.root.join(SEGMENT_DIR).join(name))? != *expected {
                return Err(custody_error("immutable store segment custody changed"));
            }
        }
        Ok(())
    }
}

fn open_checked(path: &Path, expected: &FileStamp) -> RsfResult<File> {
    if regular_file(path)? != *expected {
        return Err(custody_error("immutable named file changed before open"));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(custody_io)?;
    if file_stamp(&file.metadata().map_err(custody_io)?)? != *expected {
        return Err(custody_error("immutable opened file differs from its path"));
    }
    Ok(file)
}

fn read_checked(path: &Path, expected: &FileStamp, limit: u64) -> RsfResult<Vec<u8>> {
    let file = open_checked(path, expected)?;
    if expected.bytes > limit {
        return Err(custody_error("immutable file exceeds bounded read"));
    }
    let observed = file.try_clone().map_err(custody_io)?;
    let mut bytes = Vec::new();
    file.take(
        expected
            .bytes
            .checked_add(1)
            .ok_or_else(|| custody_error("immutable read limit overflow"))?,
    )
    .read_to_end(&mut bytes)
    .map_err(custody_io)?;
    if bytes.len() as u64 != expected.bytes
        || file_stamp(&observed.metadata().map_err(custody_io)?)? != *expected
        || regular_file(path)? != *expected
    {
        return Err(custody_error("immutable file changed during bounded read"));
    }
    Ok(bytes)
}

#[cfg(all(test, unix))]
mod tests;
