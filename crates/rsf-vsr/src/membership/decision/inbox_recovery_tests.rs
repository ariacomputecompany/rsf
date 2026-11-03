use super::*;

fn metadata_segment(path: &Path) -> std::path::PathBuf {
    std::fs::read_dir(path.join("metadata/segments"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension() != Some(std::ffi::OsStr::new("lock")))
        .unwrap()
}
fn metadata_bytes(path: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        std::fs::read(path.join("metadata/MANIFEST")).unwrap(),
        std::fs::read(metadata_segment(path)).unwrap(),
    )
}

#[test]
fn inbox_existing_rejects_missing_or_lost_identity_without_initialization() {
    let path = root("inbox-existing-missing");
    for _ in 0..2 {
        assert!(
            MembershipAuthorityInbox::open_existing(&path, PlacementGroupId(1), NodeId(3)).is_err()
        );
        assert!(!path.exists());
    }
    drop(MembershipAuthorityInbox::open(&path, PlacementGroupId(1), NodeId(3)).unwrap());
    let segment = metadata_segment(&path);
    let original = std::fs::read(&segment).unwrap();
    std::fs::remove_file(path.join("metadata/MANIFEST")).unwrap();
    for _ in 0..2 {
        assert!(
            MembershipAuthorityInbox::open_existing(&path, PlacementGroupId(1), NodeId(3)).is_err()
        );
        assert!(!path.join("metadata/MANIFEST").exists());
        assert_eq!(std::fs::read(&segment).unwrap(), original);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn inbox_existing_never_recreates_empty_or_torn_to_zero_identity() {
    for bytes in [0, 5] {
        let path = root("inbox-existing-empty");
        drop(MembershipAuthorityInbox::open(&path, PlacementGroupId(1), NodeId(3)).unwrap());
        let segment = metadata_segment(&path);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&segment)
            .unwrap()
            .set_len(bytes)
            .unwrap();
        assert!(
            MembershipAuthorityInbox::open_existing(&path, PlacementGroupId(1), NodeId(3)).is_err()
        );
        // Recovery may truncate the torn first record. It must not replace it
        // with a fresh identity, including on subsequent recovery attempts.
        let after = metadata_bytes(&path);
        assert!(after.1.is_empty());
        for _ in 0..2 {
            assert!(
                MembershipAuthorityInbox::open_existing(&path, PlacementGroupId(1), NodeId(3))
                    .is_err()
            );
            assert_eq!(metadata_bytes(&path), after);
            assert!(
                SegmentStore::recover_existing(path.join("metadata"))
                    .unwrap()
                    .recovery
                    .wal_records
                    .is_empty()
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn inbox_existing_reuses_valid_identity_and_recovers_interrupted_later_selection() {
    let path = root("inbox-existing-selection");
    let (a, _) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&a).unwrap();
    let inbox_path = path.join("inbox");
    let mut inbox =
        MembershipAuthorityInbox::open(&inbox_path, PlacementGroupId(1), NodeId(3)).unwrap();
    let initial = metadata_bytes(&inbox_path);
    inbox.begin(NodeId(1), input).unwrap();
    drop(inbox);
    let segment = metadata_segment(&inbox_path);
    assert!(std::fs::metadata(&segment).unwrap().len() > initial.1.len() as u64 + 5);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_len(initial.1.len() as u64 + 5)
        .unwrap();
    for _ in 0..2 {
        let inbox =
            MembershipAuthorityInbox::open_existing(&inbox_path, PlacementGroupId(1), NodeId(3))
                .unwrap();
        assert!(inbox.selected_input().is_none());
        drop(inbox);
        assert_eq!(metadata_bytes(&inbox_path), initial);
    }
    for (pg, node) in [
        (PlacementGroupId(2), NodeId(3)),
        (PlacementGroupId(1), NodeId(2)),
    ] {
        assert!(MembershipAuthorityInbox::open_existing(&inbox_path, pg, node).is_err());
        assert_eq!(metadata_bytes(&inbox_path), initial);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(unix)]
#[test]
fn inbox_existing_rejects_symlink_and_non_file_custody_without_touching_targets() {
    use std::os::unix::fs::symlink;
    let path = root("inbox-existing-obstruction");
    let real = path.join("real");
    drop(MembershipAuthorityInbox::open(&real, PlacementGroupId(1), NodeId(3)).unwrap());
    let before = metadata_bytes(&real);
    let alias = path.join("alias");
    symlink(&real, &alias).unwrap();
    for _ in 0..2 {
        assert!(
            MembershipAuthorityInbox::open_existing(&alias, PlacementGroupId(1), NodeId(3))
                .is_err()
        );
        assert_eq!(metadata_bytes(&real), before);
    }
    let manifest = real.join("metadata/MANIFEST");
    let saved = path.join("saved-manifest");
    std::fs::rename(&manifest, &saved).unwrap();
    symlink(&saved, &manifest).unwrap();
    for _ in 0..2 {
        assert!(
            MembershipAuthorityInbox::open_existing(&real, PlacementGroupId(1), NodeId(3)).is_err()
        );
        assert_eq!(std::fs::read(&saved).unwrap(), before.0);
    }
    std::fs::remove_file(&manifest).unwrap();
    std::fs::create_dir(&manifest).unwrap();
    for _ in 0..2 {
        assert!(
            MembershipAuthorityInbox::open_existing(&real, PlacementGroupId(1), NodeId(3)).is_err()
        );
        assert!(manifest.is_dir());
        assert_eq!(std::fs::read(&saved).unwrap(), before.0);
    }
    std::fs::remove_dir_all(path).unwrap();
}
