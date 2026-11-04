use super::*;

fn next(store: &SegmentStore) -> (SegmentManifest, PathBuf) {
    let mut manifest = store.manifest.clone();
    let id = manifest.next_id().unwrap();
    let descriptor = SegmentDescriptor {
        id,
        file_name: segment_file_name(id),
    };
    let path = store.root.join(SEGMENT_DIR).join(&descriptor.file_name);
    manifest.segments.push(descriptor);
    manifest.active_id = id;
    (manifest, path)
}

#[test]
fn owned_rotation_resumes_only_empty_next_files_and_exact_canonical_manifest_prefix() {
    for prefix in [None, Some(0), Some(1), Some(usize::MAX)] {
        let path = root("provisional");
        let mut store = seeded(&path, SyncPolicy::Full, 1);
        let original = store.capture_owned_custody().unwrap();
        let (manifest, segment) = next(&store);
        let encoded = manifest.encode();
        if let Some(prefix) = prefix {
            fs::write(&segment, b"").unwrap();
            fs::write(segment.with_extension("lock"), b"").unwrap();
            fs::write(
                path.join("MANIFEST.tmp"),
                &encoded.as_bytes()[..prefix.min(encoded.len())],
            )
            .unwrap();
        }
        let result = store
            .try_append_owned_wal_group(&original, PlacementGroupId(1), [record(b"after rotation")])
            .unwrap();
        assert_eq!(result.tail().unwrap().index, LogIndex(2));
        store
            .validate_owned_custody(&result.into_custody())
            .unwrap();
        assert_eq!(
            fs::read(path.join(MANIFEST_FILE)).unwrap(),
            encoded.as_bytes()
        );
        assert!(!path.join("MANIFEST.tmp").exists());
        drop(store);
        let recovered = SegmentStore::recover_existing(&path).unwrap();
        assert_eq!(recovered.recovery.wal_records.len(), 2);
        assert_eq!(recovered.recovery.wal_records[1].payload, b"after rotation");
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn owned_rotation_rejects_nonempty_or_aliased_provisional_names_without_foreign_truncation() {
    for fault in [
        "segment-content",
        "segment-alias",
        "sidecar-alias",
        "manifest-content",
        "manifest-alias",
        "segment-hardlink",
        "sidecar-hardlink",
        "manifest-hardlink",
    ] {
        let path = root("foreign");
        let target = path.join("store");
        let mut store = seeded(&target, SyncPolicy::Data, 1);
        let original = store.capture_owned_custody().unwrap();
        let old_manifest = fs::read(target.join(MANIFEST_FILE)).unwrap();
        let old_active = fs::read(&store.active.path).unwrap();
        let (_, segment) = next(&store);
        let victim = path.join("foreign-victim");
        fs::write(&victim, b"foreign bytes must survive").unwrap();
        match fault {
            "segment-content" => fs::write(&segment, b"unreferenced but nonempty").unwrap(),
            "segment-alias" => std::os::unix::fs::symlink(&victim, &segment).unwrap(),
            "sidecar-alias" => {
                std::os::unix::fs::symlink(&victim, segment.with_extension("lock")).unwrap()
            }
            "manifest-content" => {
                fs::write(target.join("MANIFEST.tmp"), b"foreign metadata").unwrap()
            }
            "manifest-alias" => {
                std::os::unix::fs::symlink(&victim, target.join("MANIFEST.tmp")).unwrap()
            }
            "segment-hardlink" => fs::hard_link(&victim, &segment).unwrap(),
            "sidecar-hardlink" => fs::hard_link(&victim, segment.with_extension("lock")).unwrap(),
            "manifest-hardlink" => fs::hard_link(&victim, target.join("MANIFEST.tmp")).unwrap(),
            _ => unreachable!(),
        }
        let error = store
            .try_append_owned_wal_group(
                &original,
                PlacementGroupId(1),
                [record(b"must not acknowledge")],
            )
            .unwrap_err();
        assert_eq!(
            error.code,
            ErrorCode::CorruptionDetected,
            "{fault}: {error:?}"
        );
        assert!(store.write_failed && store.cursor.is_none(), "{fault}");
        assert_eq!(
            fs::read(&victim).unwrap(),
            b"foreign bytes must survive",
            "{fault}"
        );
        assert_eq!(
            fs::read(target.join(MANIFEST_FILE)).unwrap(),
            old_manifest,
            "{fault}"
        );
        assert_eq!(fs::read(&store.active.path).unwrap(), old_active, "{fault}");
        if fault == "segment-content" {
            assert_eq!(fs::read(&segment).unwrap(), b"unreferenced but nonempty");
        }
        if fault == "manifest-content" {
            assert_eq!(
                fs::read(target.join("MANIFEST.tmp")).unwrap(),
                b"foreign metadata"
            );
        }
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn owned_rotation_manifest_publication_failure_fences_live_owner_and_exact_empty_orphan_cold_retry_works()
 {
    let path = root("publication-error");
    let mut store = seeded(&path, SyncPolicy::Data, 1);
    let original = store.capture_owned_custody().unwrap();
    let before = fs::read(path.join(MANIFEST_FILE)).unwrap();
    let (_, segment) = next(&store);
    fs::create_dir(path.join("MANIFEST.tmp")).unwrap();
    let observed = sync_observation::watch(&path, None);
    assert!(
        store
            .try_append_owned_wal_group(&original, PlacementGroupId(1), [record(b"not yet")])
            .is_err()
    );
    assert!(store.write_failed && store.cursor.is_none());
    let events = observed.events();
    assert_eq!(events.len(), 1);
    assert!(events[0].completed);
    assert_eq!(events[0].manifest, before);
    assert_eq!(fs::read(path.join(MANIFEST_FILE)).unwrap(), before);
    assert_eq!(fs::metadata(&segment).unwrap().len(), 0);
    fs::remove_dir(path.join("MANIFEST.tmp")).unwrap();
    assert!(
        store
            .try_append_owned_wal_group(&original, PlacementGroupId(1), [record(b"forbidden warm")])
            .is_err()
    );
    assert!(
        store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Prepare,
                b"forbidden ordinary"
            )
            .is_err()
    );
    drop(observed);
    drop(store);
    let mut reopened =
        SegmentStore::open_with_options(&path, options(SyncPolicy::Data, 1)).unwrap();
    let original = reopened.capture_owned_custody().unwrap();
    let result = reopened
        .try_append_owned_wal_group(&original, PlacementGroupId(1), [record(b"cold complete")])
        .unwrap();
    assert_eq!(result.tail().unwrap().index, LogIndex(2));
    reopened
        .validate_owned_custody(&result.into_custody())
        .unwrap();
    drop(reopened);
    let recovered = SegmentStore::recover_existing(&path).unwrap();
    assert_eq!(recovered.recovery.wal_records.len(), 2);
    assert_eq!(recovered.recovery.wal_records[1].payload, b"cold complete");
    fs::remove_dir_all(path).unwrap();
}
