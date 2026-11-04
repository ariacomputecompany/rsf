//! Strict existing actual-writer construction from the bounded recovery itself.
use super::*;

impl SegmentStore {
    /// Open an existing append-only store without creating or repairing data or
    /// sidecars. The returned writer consumes this bounded recovery directly.
    /// Caller must own the namespace and separately renew recovered data before
    /// acknowledging unsealed progress whose earlier write barrier is unknown.
    pub fn open_existing_recovered_bounded(
        root: impl AsRef<Path>,
        options: SegmentStoreOptions,
        limits: SegmentReadLimits,
        admit: impl FnMut(u64) -> Result<(), SegmentReadLimit>,
    ) -> Result<(Self, SegmentStoreRecovery), SegmentReadError> {
        limits.validate()?;
        let root = immutable::normalize_root(root.as_ref());
        let gate = store_gate(&root)?;
        let opening_gate = gate.clone();
        let _guard = lock_gate(&opening_gate)?;
        let (manifest, custody) = snapshot_locked(root.clone(), gate.clone(), limits)?;
        let lock = ExistingLock::acquire(&root.join(MANIFEST_FILE))?;
        let descriptor = manifest.active_segment()?;
        let active_path = root.join(SEGMENT_DIR).join(&descriptor.file_name);
        let active_lock = ExistingLock::acquire(&active_path)?;
        let expected = &custody
            .segments
            .last()
            .ok_or_else(|| custody_error("bounded store has no active segment"))?
            .1;
        // active_segment and canonical manifest decoding require the last entry
        // to be active; compare it explicitly before binding the actual writer.
        if custody.segments.last().map(|entry| &entry.0) != Some(&descriptor.file_name) {
            return Err(
                custody_error("bounded active segment differs from captured layout").into(),
            );
        }
        let file = open_existing_append(&active_path, expected)?;
        let recovery = collect_locked(&manifest, &custody, limits, admit)?;
        // Existing names can survive a failed factory barrier. Renew names and
        // all ancestor links without mkdir/create or an implicit WAL data flush.
        // The caller controls trusted ancestor routing throughout this call.
        crate::durable_directory::sync_file_publication(&active_path).map_err(custody_io)?;
        custody.validate_locked()?;
        lock.validate()?;
        active_lock.validate()?;
        if file_stamp(&file.metadata().map_err(custody_io)?)? != *expected {
            return Err(custody_error("bounded writer descriptor changed during recovery").into());
        }
        let cursor = WalCursor {
            tails: recovery.pg_tails.clone(),
            stamp: manifest
                .segments
                .iter()
                .zip(&custody.segments)
                .map(|(descriptor, (_, stamp))| SegmentStamp {
                    id: descriptor.id,
                    bytes: stamp.bytes,
                    modified: stamp.modified,
                })
                .collect(),
        };
        let active = SegmentLog {
            path: active_path,
            file,
            options: options.log_options,
            _lock: active_lock.lock,
        };
        let result = SegmentStoreRecovery {
            segments: manifest.segments.clone(),
            recovery,
        };
        Ok((
            Self {
                root,
                manifest,
                active,
                _lock: lock.lock,
                options,
                write_failed: false,
                gate,
                cursor: Some(cursor),
                observer: Arc::new(()),
                external_changes: 0,
            },
            result,
        ))
    }
}

struct ExistingLock {
    lock: SegmentLock,
    path: PathBuf,
    expected: FileStamp,
}
impl ExistingLock {
    fn acquire(segment: &Path) -> RsfResult<Self> {
        let path = segment.with_extension("lock");
        let expected = regular_file(&path)?;
        let mut options = OpenOptions::new();
        options.write(true);
        no_follow(&mut options);
        let file = options.open(&path).map_err(custody_io)?;
        if file_stamp(&file.metadata().map_err(custody_io)?)? != expected {
            return Err(custody_error(
                "bounded sidecar descriptor differs from named lock",
            ));
        }
        let canonical = fs::canonicalize(&path).map_err(custody_io)?;
        let lock = SegmentLock::register_opened(canonical, file)?;
        let result = Self {
            lock,
            path,
            expected,
        };
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> RsfResult<()> {
        if regular_file(&self.path)? != self.expected {
            return Err(custody_error("bounded sidecar changed during open"));
        }
        // Reentrant registration may retain an earlier descriptor. Check the
        // actual lock owner, never merely the newly opened descriptor's path.
        if held_lock_stamp(&self.lock)? != self.expected {
            return Err(custody_error("bounded sidecar path differs from held lock"));
        }
        Ok(())
    }
}
pub(super) fn open_existing_append(path: &Path, expected: &FileStamp) -> RsfResult<File> {
    if regular_file(path)? != *expected {
        return Err(custody_error("bounded active path changed before open"));
    }
    let mut options = OpenOptions::new();
    options.read(true).append(true);
    no_follow(&mut options);
    let file = options.open(path).map_err(custody_io)?;
    if file_stamp(&file.metadata().map_err(custody_io)?)? != *expected {
        return Err(custody_error(
            "bounded active descriptor differs from named file",
        ));
    }
    Ok(file)
}
pub(super) fn no_follow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(not(unix))]
    let _ = options;
}
