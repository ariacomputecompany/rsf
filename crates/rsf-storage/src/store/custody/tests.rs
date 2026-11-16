use super::*;

struct Fixture {
    parent: PathBuf,
    store: Option<SegmentStore>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.store.take());
        let _ = fs::remove_dir_all(&self.parent);
    }
}
fn fixture() -> Fixture {
    fixture_at(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
}
fn fixture_at(timestamp: u128) -> Fixture {
    static NEXT_FIXTURE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let parent = loop {
        let sequence = NEXT_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let parent = std::env::temp_dir().join(format!(
            "rsf-owned-custody-{}-{timestamp}-{sequence}",
            std::process::id(),
        ));
        match fs::create_dir(&parent) {
            Ok(()) => break parent,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("cannot reserve custody fixture: {error}"),
        }
    };
    let store = SegmentStore::open_with_options(
        parent.join("store"),
        SegmentStoreOptions {
            max_segment_bytes: 256,
            log_options: SegmentLogOptions {
                sync_policy: crate::SyncPolicy::Full,
            },
        },
    )
    .unwrap();
    Fixture {
        parent,
        store: Some(store),
    }
}

#[test]
fn same_clock_fixture_drop_preserves_another_owned_store() {
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let first = fixture_at(timestamp);
    let mut second = fixture_at(timestamp);
    let store = second.store.as_mut().unwrap();
    append(store, b"second fixture retained bytes");
    let custody = store.capture_owned_custody().unwrap();
    drop(first);
    assert!(
        second.parent.is_dir(),
        "dropping one fixture removed another fixture with the same clock value"
    );
    store.validate_owned_custody(&custody).unwrap();
    assert_eq!(
        store.wal_tail(PlacementGroupId(1)).unwrap().unwrap().index,
        LogIndex(1)
    );
}
fn append(store: &mut SegmentStore, payload: &[u8]) {
    store
        .append_next_wal(
            PlacementGroupId(1),
            rsf_core::Epoch(1),
            WalRecordKind::Prepare,
            payload,
        )
        .unwrap();
}

#[test]
fn owned_stamp_rejects_missing_truncated_aliased_and_replaced_files_without_repair() {
    for fault in [
        "manifest", "segment", "truncate", "alias", "hardlink", "replace",
    ] {
        let mut f = fixture();
        let store = f.store.as_mut().unwrap();
        append(store, b"retained bytes");
        let stamp = store.capture_owned_custody().unwrap();
        let path = if fault == "manifest" || fault == "replace" {
            store.root().join(MANIFEST_FILE)
        } else {
            store
                .root()
                .join(SEGMENT_DIR)
                .join(&store.active_segment().file_name)
        };
        let original = fs::read(&path).unwrap();
        match fault {
            "manifest" | "segment" => fs::remove_file(&path).unwrap(),
            "truncate" => OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(0)
                .unwrap(),
            "alias" | "hardlink" => {
                let saved = path.with_extension("saved");
                fs::rename(&path, &saved).unwrap();
                if fault == "alias" {
                    std::os::unix::fs::symlink(saved, &path).unwrap();
                } else {
                    fs::hard_link(saved, &path).unwrap();
                }
            }
            "replace" => {
                let replacement = path.with_extension("replacement");
                fs::write(&replacement, &original).unwrap();
                fs::rename(replacement, &path).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(store.validate_owned_custody(&stamp).is_err(), "{fault}");
        if matches!(fault, "manifest" | "segment") {
            assert!(!path.exists(), "validation must not recreate {fault}");
        }
        if fault == "truncate" {
            assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        }
        if fault == "replace" {
            assert_eq!(fs::read(&path).unwrap(), original);
        }
    }
}

#[test]
fn own_wal_append_rotation_and_cold_open_establish_new_stamps_without_generic_refresh() {
    let mut f = fixture();
    let store = f.store.as_mut().unwrap();
    let initial = store.capture_owned_custody().unwrap();
    append(store, &[9; 1024]);
    append(store, &[8; 1024]);
    assert!(store.segments().len() > 1);
    assert!(store.validate_owned_custody(&initial).is_err());
    let current = store.capture_owned_custody().unwrap();
    store.validate_owned_custody(&current).unwrap();
    let path = store.root().to_path_buf();
    drop(f.store.take());
    let reopened = SegmentStore::open(&path).unwrap();
    assert!(reopened.validate_owned_custody(&current).is_err());
    reopened
        .validate_owned_custody(&reopened.capture_owned_custody().unwrap())
        .unwrap();
    drop(reopened);
}

#[test]
fn generic_second_writer_remains_supported_but_exclusive_stamp_refuses_to_adopt_it() {
    let mut f = fixture();
    let store = f.store.as_mut().unwrap();
    append(store, b"first");
    let stamp = store.capture_owned_custody().unwrap();
    let mut other = SegmentStore::open(store.root()).unwrap();
    append(&mut other, b"second");
    assert!(store.validate_owned_custody(&stamp).is_err());
    // Explicit generic refresh retains the existing shared-store contract.
    assert_eq!(
        store.wal_tail(PlacementGroupId(1)).unwrap().unwrap().index,
        LogIndex(2)
    );
    append(store, b"third");
    assert_eq!(
        other.wal_tail(PlacementGroupId(1)).unwrap().unwrap().index,
        LogIndex(3)
    );
    drop(other);
}

#[test]
fn trusted_ancestor_alias_parent_route_is_preserved_but_final_owned_alias_is_rejected() {
    let f = fixture();
    fs::create_dir_all(f.parent.join("physical/child")).unwrap();
    std::os::unix::fs::symlink(f.parent.join("physical/child"), f.parent.join("route")).unwrap();
    let owner = SegmentStore::open(f.parent.join("route/../owned")).unwrap();
    let stamp = owner.capture_owned_custody().unwrap();
    owner.validate_owned_custody(&stamp).unwrap();
    assert!(f.parent.join("physical/owned/MANIFEST").is_file());
    let owned = f.parent.join("physical/owned");
    fs::rename(&owned, f.parent.join("physical/moved")).unwrap();
    std::os::unix::fs::symlink(f.parent.join("physical/moved"), &owned).unwrap();
    assert!(owner.validate_owned_custody(&stamp).is_err());
    drop(owner);
}
