use super::*;
use rsf_core::Epoch;
use std::os::unix::fs::{PermissionsExt, symlink};

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-immutable-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn fixture(path: &Path) -> PathBuf {
    let mut store = SegmentStore::open(path).unwrap();
    store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(1),
            WalRecordKind::Prepare,
            b"owned bytes",
        )
        .unwrap();
    let segment = path
        .join(SEGMENT_DIR)
        .join(&store.active_segment().file_name);
    drop(store);
    segment
}

#[test]
fn immutable_recovery_never_repairs_torn_active_tail_or_creates_missing_sidecars() {
    let root = root("readonly");
    let segment = fixture(&root);
    let lock = root.join("MANIFEST.lock");
    fs::remove_file(&lock).unwrap();
    let (recovered, stamp) = SegmentStore::recover_immutable(&root, 4096).unwrap();
    assert_eq!(recovered.recovery.wal_records.len(), 1);
    assert!(!lock.exists());
    let missing = root.join("uncreated/child");
    assert!(crate::sync_existing_directory(&missing).is_err());
    assert!(!root.join("uncreated").exists());
    stamp.validate().unwrap();
    OpenOptions::new()
        .append(true)
        .open(&segment)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    let damaged = fs::read(&segment).unwrap();
    assert!(stamp.validate().is_err());
    assert!(SegmentStore::recover_immutable(&root, 4096).is_err());
    assert_eq!(fs::read(&segment).unwrap(), damaged);
    assert!(!lock.exists());
    fs::remove_file(root.join(MANIFEST_FILE)).unwrap();
    assert!(SegmentStore::recover_immutable(&root, 4096).is_err());
    assert!(!root.join(MANIFEST_FILE).exists());
    assert!(!lock.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn immutable_custody_rejects_replacement_alias_and_hardlink_without_reading_wal_on_validation() {
    for file_name in ["manifest", "segment"] {
        for fault in ["replace", "symlink", "hardlink", "short"] {
            let root = root(&format!("{file_name}-{fault}"));
            let segment = fixture(&root);
            let segment = if file_name == "manifest" {
                root.join(MANIFEST_FILE)
            } else {
                segment
            };
            let (_, stamp) = SegmentStore::recover_immutable(&root, 4096).unwrap();
            let original = root.join("saved-segment");
            fs::rename(&segment, &original).unwrap();
            match fault {
                "replace" => {
                    fs::copy(&original, &segment).unwrap();
                }
                "symlink" => symlink(&original, &segment).unwrap(),
                "hardlink" => fs::hard_link(&original, &segment).unwrap(),
                "short" => {
                    fs::write(&segment, b"").unwrap();
                }
                _ => unreachable!(),
            }
            assert!(stamp.validate().is_err(), "{fault}");
            if matches!(fault, "symlink" | "hardlink") {
                assert!(SegmentStore::recover_immutable(&root, 4096).is_err());
            }
            fs::remove_dir_all(root).unwrap();
        }
    }
    let root = root("metadata-only");
    let segment = fixture(&root);
    let (_, stamp) = SegmentStore::recover_immutable(&root, 4096).unwrap();
    let permissions = fs::metadata(&segment).unwrap().permissions();
    fs::set_permissions(&segment, fs::Permissions::from_mode(0o000)).unwrap();
    File::open(&segment).expect_err("qualification requires an unprivileged Unix host");
    let result = stamp.validate();
    fs::set_permissions(&segment, permissions).unwrap();
    result.unwrap();
    let manifest_len = fs::metadata(root.join(MANIFEST_FILE)).unwrap().len() as usize;
    assert!(SegmentStore::recover_immutable(&root, manifest_len - 1).is_err());
    SegmentStore::recover_immutable(&root, manifest_len).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn immutable_capture_rejects_equal_byte_replacement_before_scanning() {
    for file_name in ["manifest", "segment"] {
        let root = root(&format!("capture-{file_name}"));
        let segment = fixture(&root);
        let target = if file_name == "manifest" {
            root.join(MANIFEST_FILE)
        } else {
            segment
        };
        let bytes = fs::read(&target).unwrap();
        let result = recover_immutable(&root, 4096, || {
            fs::rename(&target, root.join("previous-inode")).unwrap();
            fs::write(&target, &bytes).unwrap();
        });
        assert!(
            result.is_err(),
            "{file_name}: captured old file but certified replacement"
        );
        assert_eq!(fs::read(&target).unwrap(), bytes);
        fs::remove_dir_all(root).unwrap();
    }
}
