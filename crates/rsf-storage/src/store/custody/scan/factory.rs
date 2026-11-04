//! Explicit provisional empty factory completion, separate from strict open.
use super::*;

impl SegmentStore {
    /// Complete writer sidecars only for an exact physically empty initial
    /// factory. The caller must independently authorize a currently provisional
    /// empty root with no authoritative/bound reference. Physical emptiness and
    /// non-ACTIVE status prove neither that authority nor historical absence of
    /// ACKs/publication. Cold prior BEGIN ACK0 may be reconstructible under its
    /// caller protocol; observed warm custody loss must remain failed closed.
    /// `false` grants no success/authority: the shape was not this empty factory
    /// and must instead pass strict existing bounded recovery. No data is reset.
    pub fn complete_unbound_empty_initialization(root: impl AsRef<Path>) -> RsfResult<bool> {
        complete(root.as_ref(), || Ok(()))
    }
}

fn complete(root: &Path, after_create: impl FnOnce() -> RsfResult<()>) -> RsfResult<bool> {
    // Existing initializer remains unchanged and refuses final MANIFEST. Its
    // exact unpublished prefixes may be completed only under caller authority.
    if SegmentStore::initialization_pending(root)? {
        SegmentStore::resume_empty_initialization(root)?;
    }
    let root = immutable::normalize_root(root);
    let gate = store_gate(&root)?;
    let _guard = lock_gate(&gate)?;
    let initial = SegmentManifest::initial();
    let encoded = initial.encode();
    let manifest_path = root.join(MANIFEST_FILE);
    let stamp = regular_file(&manifest_path)?;
    if stamp.bytes != encoded.len() as u64 {
        return Ok(false);
    }
    if immutable::read_checked(&manifest_path, &stamp, stamp.bytes)? != encoded.as_bytes() {
        return Ok(false);
    }
    let (_, custody) = immutable::capture_locked(root.clone(), gate.clone(), encoded.len())?;
    if custody.manifest != stamp {
        return Err(custody_error(
            "empty factory manifest changed during capture",
        ));
    }
    if custody.segments.len() != 1 || custody.segments[0].1.bytes != 0 {
        return Ok(false);
    }
    let segment = root.join(SEGMENT_DIR).join(&initial.segments[0].file_name);
    let active_lock = segment.with_extension("lock");
    let existing = match fs::symlink_metadata(&active_lock) {
        Ok(metadata) => Some(file_stamp(&metadata)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(custody_io(error)),
    };
    if existing.as_ref().is_some_and(|stamp| stamp.bytes != 0) {
        return Ok(false);
    }
    // Bound enumeration and reject foreign entries before any sidecar creation.
    if !exact_names(&root, &[MANIFEST_FILE, "MANIFEST.lock", SEGMENT_DIR])?
        || !exact_names(
            &root.join(SEGMENT_DIR),
            &[
                initial.segments[0].file_name.as_str(),
                active_lock
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| custody_error("invalid empty sidecar name"))?,
            ],
        )?
    {
        return Ok(false);
    }
    let lock_path = root.join("MANIFEST.lock");
    let lock_stamp = regular_file(&lock_path)?;
    if lock_stamp.bytes != 0 {
        return Ok(false);
    }
    let file = open::open_existing_append(&lock_path, &lock_stamp)?;
    let lock = crate::ExclusiveFileLock::acquire(file).map_err(|error| {
        RsfError::new(
            ErrorCode::InvalidInput,
            format!("empty factory lock unavailable: {error}"),
        )
    })?;
    custody.validate_locked()?;
    if regular_file(&lock_path)? != lock_stamp
        || file_stamp(&lock.metadata().map_err(custody_io)?)? != lock_stamp
    {
        return Err(custody_error("empty factory held lock changed"));
    }
    let file = if let Some(expected) = &existing {
        open::open_existing_append(&active_lock, expected)?
    } else {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        open::no_follow(&mut options);
        options.open(&active_lock).map_err(custody_io)?
    };
    let expected = file_stamp(&file.metadata().map_err(custody_io)?)?;
    if expected.bytes != 0
        || existing
            .as_ref()
            .is_some_and(|previous| previous != &expected)
    {
        return Err(custody_error("empty active sidecar changed"));
    }
    after_create()?;
    file.sync_all().map_err(custody_io)?;
    // No WAL write or replay. Renew the new/existing sidecar's name route and
    // compare the original directory/data/manifest/held-lock evidence afterward.
    crate::durable_directory::sync_file_publication(&active_lock).map_err(custody_io)?;
    custody.validate_locked()?;
    if regular_file(&lock_path)? != lock_stamp
        || file_stamp(&lock.metadata().map_err(custody_io)?)? != lock_stamp
        || regular_file(&active_lock)? != expected
        || file_stamp(&file.metadata().map_err(custody_io)?)? != expected
    {
        return Err(custody_error(
            "empty factory custody changed during sidecar publication",
        ));
    }
    Ok(true)
}
fn exact_names(path: &Path, allowed: &[&str]) -> RsfResult<bool> {
    for (position, entry) in fs::read_dir(path).map_err(custody_io)?.enumerate() {
        let entry = entry.map_err(custody_io)?;
        if position >= allowed.len() || !allowed.iter().any(|name| entry.file_name() == *name) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(all(test, unix))]
mod tests {
    use super::super::tests::{allow, inventory, limits, root};
    use super::*;
    #[test]
    fn bounded_empty_factory_completes_post_manifest_and_post_sidecar_crash_prefixes() {
        for after_manifest in [false, true] {
            let path = root("factory-crash");
            if after_manifest {
                SegmentStore::resume_empty_initialization(&path).unwrap();
            }
            let error = complete(&path, || {
                Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    "injected interruption after sidecar creation",
                ))
            })
            .unwrap_err();
            assert_eq!(
                error.message,
                "injected interruption after sidecar creation"
            );
            let before = inventory(&path);
            assert!(SegmentStore::complete_unbound_empty_initialization(&path).unwrap());
            assert_eq!(
                inventory(&path),
                before,
                "retry must retain exact empty factory bytes"
            );
            let (store, recovered) = SegmentStore::open_existing_recovered_bounded(
                &path,
                Default::default(),
                limits(),
                allow,
            )
            .unwrap();
            assert!(recovered.recovery.records.is_empty());
            drop(store);
            fs::remove_dir_all(path).unwrap();
        }
    }
    #[test]
    fn bounded_empty_factory_never_repairs_nonempty_or_foreign_shapes() {
        for foreign in [false, true] {
            let path = root("factory-refuse");
            SegmentStore::resume_empty_initialization(&path).unwrap();
            if foreign {
                fs::write(path.join("foreign"), b"unrecognized").unwrap();
            } else {
                fs::write(
                    path.join(SEGMENT_DIR).join(segment_file_name(1)),
                    b"not empty",
                )
                .unwrap();
            }
            let before = inventory(&path);
            assert!(!SegmentStore::complete_unbound_empty_initialization(&path).unwrap());
            assert_eq!(inventory(&path), before);
            fs::remove_dir_all(path).unwrap();
        }
    }
    #[test]
    fn bounded_published_open_refuses_the_same_missing_sidecar_the_unbound_factory_can_complete() {
        let path = root("factory-boundary");
        SegmentStore::resume_empty_initialization(&path).unwrap();
        let before = inventory(&path);
        assert!(
            SegmentStore::open_existing_recovered_bounded(
                &path,
                Default::default(),
                limits(),
                allow
            )
            .is_err()
        );
        assert_eq!(
            inventory(&path),
            before,
            "strict published opener must never infer unbound from emptiness"
        );
        // Only this explicit caller assertion enables the separate factory path.
        assert!(SegmentStore::complete_unbound_empty_initialization(&path).unwrap());
        let (store, _) = SegmentStore::open_existing_recovered_bounded(
            &path,
            Default::default(),
            limits(),
            allow,
        )
        .unwrap();
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn bounded_provisional_completion_does_not_recreate_a_nonempty_wal_sidecar() {
        let path = root("nonempty-sidecar");
        let mut store = SegmentStore::open(&path).unwrap();
        store
            .append_next_wal(
                PlacementGroupId(1),
                rsf_core::Epoch(1),
                WalRecordKind::Prepare,
                b"retained bytes",
            )
            .unwrap();
        let lock = store.active.path.with_extension("lock");
        drop(store);
        fs::remove_file(lock).unwrap();
        let before = inventory(&path);
        assert!(!SegmentStore::complete_unbound_empty_initialization(&path).unwrap());
        assert!(
            SegmentStore::open_existing_recovered_bounded(
                &path,
                Default::default(),
                limits(),
                allow
            )
            .is_err()
        );
        assert_eq!(inventory(&path), before);
        fs::remove_dir_all(path).unwrap();
    }
}
