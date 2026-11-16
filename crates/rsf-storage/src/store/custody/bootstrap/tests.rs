use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    auxiliary: crate::ExclusiveFileLock,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "rsf-provisional-bootstrap-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join(".vsr-writer.lock"))
            .unwrap();
        Self {
            root,
            auxiliary: crate::ExclusiveFileLock::acquire(file).unwrap(),
        }
    }
    fn begin(&self) -> RsfResult<ProvisionalBootstrap<'_>> {
        SegmentStore::provisional_bootstrap(
            &self.root,
            &self.auxiliary,
            PlacementGroupId(7),
            rsf_core::Epoch(3),
            b"canonical-bootstrap",
            SegmentStoreOptions::default(),
            limits(),
        )
    }
    fn segment(&self) -> PathBuf {
        self.root.join(SEGMENT_DIR).join(segment_file_name(1))
    }
    fn seed(&self, bytes: &[u8]) {
        fs::create_dir(self.root.join(SEGMENT_DIR)).unwrap();
        fs::write(
            self.root.join(MANIFEST_FILE),
            SegmentManifest::initial().encode(),
        )
        .unwrap();
        fs::write(self.root.join("MANIFEST.lock"), []).unwrap();
        fs::write(self.segment(), bytes).unwrap();
        fs::write(self.segment().with_extension("lock"), []).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}
fn limits() -> SegmentReadLimits {
    SegmentReadLimits {
        max_manifest_bytes: 1024,
        max_records: 1,
        max_encoded_bytes: 1024,
        max_record_bytes: 1024,
    }
}
fn failure() -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, "injected factory boundary")
}
fn ordinary_frame() -> Vec<u8> {
    let f = Fixture::new("codec-oracle");
    let path = f.root.join("oracle");
    let mut log = SegmentLog::open(&path).unwrap();
    log.append_wal(WalAppend {
        pg: PlacementGroupId(7),
        epoch: rsf_core::Epoch(3),
        index: LogIndex(1),
        kind: WalRecordKind::Membership,
        previous_hash: [0; 32],
        payload: b"canonical-bootstrap",
    })
    .unwrap();
    fs::read(&path).unwrap()
}
#[test]
fn provisional_bootstrap_every_physical_prefix_matches_ordinary_append_and_strict_replay() {
    let expected = ordinary_frame();
    for end in 0..=expected.len() {
        let f = Fixture::new("all-prefixes");
        f.seed(&expected[..end]);
        let original = regular_file(&f.segment()).unwrap().identity;
        let mut factory = f.begin().unwrap();
        let (store, record) = factory.complete().unwrap();
        assert_eq!(fs::read(f.segment()).unwrap(), expected);
        assert_eq!(regular_file(&f.segment()).unwrap().identity, original);
        assert_eq!(record.index, LogIndex(1));
        assert_eq!(record.payload.as_slice(), b"canonical-bootstrap");
        let custody = store.capture_owned_custody().unwrap();
        let mut seen = Vec::new();
        store
            .scan_owned_prefix(
                &custody,
                limits(),
                |_| Ok(()),
                |record| {
                    seen.push(record.clone());
                    Ok(SegmentScanControl::Continue)
                },
            )
            .unwrap();
        assert_eq!(seen, vec![record]);
        assert!(factory.complete().is_err());
        drop(store);
        drop(factory);
        let (strict, replay) = SegmentStore::open_existing_recovered_bounded(
            &f.root,
            SegmentStoreOptions::default(),
            limits(),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(replay.recovery.wal_records.len(), 1);
        drop(strict);
    }
}
#[test]
fn provisional_bootstrap_actual_nonprefix_and_oversize_are_refused_without_repair() {
    let expected = ordinary_frame();
    for at in 0..expected.len() {
        let f = Fixture::new("wrong-byte");
        let mut bytes = expected[..=at].to_vec();
        bytes[at] ^= 1;
        f.seed(&bytes);
        assert!(f.begin().is_err());
        assert_eq!(fs::read(f.segment()).unwrap(), bytes);
    }
    let f = Fixture::new("oversize");
    let mut bytes = expected;
    bytes.push(0);
    f.seed(&bytes);
    assert!(f.begin().is_err());
    assert_eq!(fs::read(f.segment()).unwrap(), bytes);
}
#[test]
fn provisional_bootstrap_each_factory_boundary_cold_continues_but_warm_failure_is_sticky() {
    let expected = ordinary_frame();
    for phase in [
        Boundary::ManifestLock,
        Boundary::Directory,
        Boundary::Segment,
        Boundary::SegmentLock,
        Boundary::TemporaryAppended,
        Boundary::Temporary,
        Boundary::Manifest,
        Boundary::PrefixAppended,
        Boundary::PrefixWritten,
        Boundary::PrefixSynced,
        Boundary::Published,
    ] {
        let f = Fixture::new("boundaries");
        let mut factory = f.begin().unwrap();
        let mut reached = false;
        assert!(
            factory
                .complete_with(|actual| {
                    if actual == phase {
                        reached = true;
                        Err(failure())
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert!(reached);
        assert!(factory.complete().is_err());
        drop(factory);
        let (store, _) = f.begin().unwrap().complete().unwrap();
        assert_eq!(fs::read(f.segment()).unwrap(), expected);
        drop(store);
    }
}
#[test]
fn provisional_bootstrap_every_manifest_temporary_prefix_completes_exact_bytes() {
    let expected = SegmentManifest::initial().encode();
    for end in 0..=expected.len() {
        let f = Fixture::new("manifest-prefix");
        f.seed(&[]);
        fs::remove_file(f.root.join(MANIFEST_FILE)).unwrap();
        fs::write(f.root.join("MANIFEST.tmp"), &expected.as_bytes()[..end]).unwrap();
        let (store, _) = f.begin().unwrap().complete().unwrap();
        assert_eq!(
            fs::read(f.root.join(MANIFEST_FILE)).unwrap(),
            expected.as_bytes()
        );
        assert!(!f.root.join("MANIFEST.tmp").exists());
        drop(store);
    }
}
#[test]
fn provisional_bootstrap_auxiliary_swap_and_hardlink_at_every_phase_fence_original_handle() {
    for hardlink in [false, true] {
        for phase in [
            Boundary::ManifestLock,
            Boundary::Directory,
            Boundary::Segment,
            Boundary::SegmentLock,
            Boundary::TemporaryAppended,
            Boundary::Temporary,
            Boundary::Manifest,
            Boundary::PrefixAppended,
            Boundary::PrefixWritten,
            Boundary::PrefixSynced,
            Boundary::Published,
        ] {
            let f = Fixture::new("auxiliary-loss");
            let mut factory = f.begin().unwrap();
            let named = f.root.join(".vsr-writer.lock");
            let saved = f.root.with_extension("held-original");
            let mut reached = false;
            assert!(
                factory
                    .complete_with(|actual| {
                        if actual == phase {
                            reached = true;
                            if hardlink {
                                fs::hard_link(&named, &saved).unwrap();
                            } else {
                                fs::rename(&named, &saved).unwrap();
                                fs::write(&named, []).unwrap();
                            }
                        }
                        Ok(())
                    })
                    .is_err()
            );
            assert!(reached);
            if hardlink {
                fs::remove_file(&saved).unwrap();
            } else {
                fs::remove_file(&named).unwrap();
                fs::rename(&saved, &named).unwrap();
            }
            assert!(factory.complete().is_err());
        }
    }
}
#[test]
fn provisional_bootstrap_inventory_and_caps_refuse_before_creating_storage() {
    for name in ["ACTIVE", "foreign", "MANIFEST.tmp"] {
        let f = Fixture::new("foreign");
        fs::write(f.root.join(name), b"foreign").unwrap();
        assert!(f.begin().is_err());
        assert!(!f.root.join("MANIFEST.lock").exists());
        assert!(!f.root.join(SEGMENT_DIR).exists());
    }
    let f = Fixture::new("limit");
    let mut cap = limits();
    cap.max_record_bytes = 1;
    assert!(
        SegmentStore::provisional_bootstrap(
            &f.root,
            &f.auxiliary,
            PlacementGroupId(7),
            rsf_core::Epoch(3),
            b"canonical-bootstrap",
            SegmentStoreOptions::default(),
            cap
        )
        .is_err()
    );
    assert_eq!(fs::read_dir(&f.root).unwrap().count(), 1);
    // Existing generic empty classification still refuses the explicit auxiliary.
    assert!(SegmentStore::initialization_pending(&f.root).is_err());
    // Strict existing open does not create missing factory names.
    assert!(
        SegmentStore::open_existing_recovered_bounded(
            &f.root,
            SegmentStoreOptions::default(),
            limits(),
            |_| Ok(())
        )
        .is_err()
    );
    assert_eq!(fs::read_dir(&f.root).unwrap().count(), 1);
}
#[test]
fn provisional_bootstrap_transfers_actual_exclusive_locks_and_rejects_live_writer_reentry() {
    let f = Fixture::new("lock-transfer");
    let mut factory = f.begin().unwrap();
    let (store, _) = factory.complete().unwrap();
    for path in [
        f.root.join("MANIFEST.lock"),
        f.segment().with_extension("lock"),
    ] {
        let file = OpenOptions::new().write(true).open(path).unwrap();
        assert!(crate::ExclusiveFileLock::acquire(file).is_err());
    }
    let mut second = f.begin().unwrap();
    assert!(second.complete().is_err());
    drop(second);
    drop(store);
    // The exhausted factory's retained duplicates cannot keep exclusion alive.
    let file = OpenOptions::new()
        .write(true)
        .open(f.root.join("MANIFEST.lock"))
        .unwrap();
    assert!(crate::ExclusiveFileLock::acquire(file).is_ok());
    drop(factory);
}
#[test]
fn provisional_bootstrap_warm_segment_substitution_fails_without_writing_replacement() {
    let f = Fixture::new("segment-swap");
    f.seed(&ordinary_frame()[..20]);
    let mut factory = f.begin().unwrap();
    let saved = f.root.with_extension("saved-segment");
    fs::rename(f.segment(), &saved).unwrap();
    fs::write(f.segment(), b"replacement").unwrap();
    assert!(factory.complete().is_err());
    assert_eq!(fs::read(f.segment()).unwrap(), b"replacement");
    fs::remove_file(f.segment()).unwrap();
    fs::rename(saved, f.segment()).unwrap();
    assert!(factory.complete().is_err());
}

#[test]
fn provisional_bootstrap_readback_rejects_actual_same_length_mutation_before_sync() {
    for phase in [Boundary::TemporaryAppended, Boundary::PrefixAppended] {
        let f = Fixture::new("readback");
        let mut factory = f.begin().unwrap();
        let target = if phase == Boundary::TemporaryAppended {
            f.root.join("MANIFEST.tmp")
        } else {
            f.segment()
        };
        let mut changed = Vec::new();
        assert!(
            factory
                .complete_with(|actual| {
                    if actual == phase {
                        changed = fs::read(&target).unwrap();
                        changed[0] ^= 1;
                        fs::write(&target, &changed).unwrap();
                    }
                    Ok(())
                })
                .is_err()
        );
        assert!(!changed.is_empty());
        assert_eq!(fs::read(target).unwrap(), changed);
        assert!(factory.complete().is_err());
    }
}

#[test]
fn provisional_bootstrap_failed_real_sync_boundary_withholds_owner_and_cold_renews() {
    let f = Fixture::new("sync-failure");
    let mut factory = f.begin().unwrap();
    let watch = crate::sync_observation::watch(&f.root, Some(0));
    assert!(factory.complete().is_err());
    let events = watch.events();
    assert_eq!(events.len(), 1);
    assert!(!events[0].completed);
    assert_eq!(events[0].path, f.segment());
    assert_eq!(fs::read(f.segment()).unwrap(), ordinary_frame());
    assert!(factory.complete().is_err());
    drop(watch);
    drop(factory);
    let watch = crate::sync_observation::watch(&f.root, None);
    let (store, _) = f.begin().unwrap().complete().unwrap();
    let events = watch.events();
    assert_eq!(events.len(), 1);
    assert!(events[0].completed);
    drop(store);
}

#[test]
fn provisional_bootstrap_cold_aliases_and_hardlinks_never_become_custody() {
    use std::os::unix::fs::symlink;
    for hardlink in [false, true] {
        for relative in [
            PathBuf::from("MANIFEST"),
            PathBuf::from("MANIFEST.lock"),
            PathBuf::from(SEGMENT_DIR).join(segment_file_name(1)),
        ] {
            let f = Fixture::new("alias");
            f.seed(&[]);
            let name = f.root.join(relative);
            let saved = f.root.with_extension("alias-target");
            if hardlink {
                fs::hard_link(&name, &saved).unwrap();
            } else {
                fs::rename(&name, &saved).unwrap();
                symlink(&saved, &name).unwrap();
            }
            assert!(f.begin().is_err());
            assert_eq!(
                fs::metadata(&saved).unwrap().len(),
                if name == f.root.join(MANIFEST_FILE) {
                    SegmentManifest::initial().encode().len() as u64
                } else {
                    0
                }
            );
            fs::remove_file(&saved).unwrap();
        }
    }
}

#[test]
fn provisional_bootstrap_warm_directory_swap_never_populates_replacement() {
    let f = Fixture::new("directory-swap");
    f.seed(&[]);
    let mut factory = f.begin().unwrap();
    let named = f.root.join(SEGMENT_DIR);
    let saved = f.root.with_extension("saved-directory");
    fs::rename(&named, &saved).unwrap();
    fs::create_dir(&named).unwrap();
    assert!(factory.complete().is_err());
    assert_eq!(fs::read_dir(&named).unwrap().count(), 0);
    fs::remove_dir(&named).unwrap();
    fs::rename(&saved, &named).unwrap();
    assert!(factory.complete().is_err());
}

#[test]
fn provisional_bootstrap_real_manifest_publication_precedes_any_wal_suffix() {
    use crate::durable_directory::store_open_observation;
    let seed = |f: &Fixture| {
        f.seed(&[]);
        fs::rename(f.root.join(MANIFEST_FILE), f.root.join("MANIFEST.tmp")).unwrap();
    };
    let f = Fixture::new("manifest-barrier-order");
    seed(&f);
    let watch = store_open_observation::watch(&f.root, None);
    let mut factory = f.begin().unwrap();
    let mut checked = false;
    let (store, _) = factory
        .complete_with(|phase| {
            if phase == Boundary::PrefixAppended {
                // The observer records only after the real directory sync succeeds.
                // Looking here rules out satisfying the test with the final flush.
                let events = watch.events();
                let event = events
                    .iter()
                    .find(|event| event.directory == f.root && event.manifest.is_some())
                    .expect("final manifest root sync before WAL append");
                assert_eq!(
                    event.manifest.as_deref(),
                    Some(SegmentManifest::initial().encode().as_bytes())
                );
                assert_eq!(event.segments.len(), 1);
                assert_eq!(event.segments[0].1, 0);
                checked = true;
            }
            Ok(())
        })
        .unwrap();
    assert!(checked);
    let events = watch.events();
    let position = events
        .iter()
        .position(|event| event.directory == f.root && event.manifest.is_some())
        .unwrap();
    drop(store);
    drop(factory);
    drop(watch);

    let f = Fixture::new("manifest-barrier-uncertainty");
    seed(&f);
    let watch = store_open_observation::watch(&f.root, Some(position));
    let mut factory = f.begin().unwrap();
    assert!(factory.complete().is_err());
    let events = watch.events();
    assert_eq!(events.len(), position + 1);
    assert_eq!(events[position].directory, f.root);
    assert_eq!(events[position].segments.len(), 1);
    assert_eq!(events[position].segments[0].1, 0);
    assert_eq!(fs::read(f.segment()).unwrap(), Vec::<u8>::new());
    assert!(factory.complete().is_err());
    drop(factory);
    drop(watch);
    let (store, _) = f.begin().unwrap().complete().unwrap();
    assert_eq!(fs::read(f.segment()).unwrap(), ordinary_frame());
    drop(store);
}
