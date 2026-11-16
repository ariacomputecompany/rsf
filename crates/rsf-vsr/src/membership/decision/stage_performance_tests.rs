use super::*;

#[test]
fn created_stage_input_copy_resumes_exact_prefix_and_rejects_conflicting_retry() {
    let path = root("created-stage-input-copy");
    let (_, approval) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let id = input.id().unwrap();
    let records = input.flattened_records(&approval).unwrap();
    assert!(records.len() > 2);
    for partial in [false, true] {
        let target = path.join(if partial { "partial" } else { "fresh" });
        std::fs::create_dir(&target).unwrap();
        if partial {
            disk::create_input(&target, &input).unwrap();
            disk::append_records(
                &target,
                &mut false,
                &input,
                VsrHistoryBatch {
                    ordinal: 0,
                    records: records[..2].to_vec(),
                },
            )
            .unwrap();
        }
        let mut failed = false;
        disk::create_and_retain_records(&target, &mut failed, &input, &records).unwrap();
        let (stored, copied) = disk::read_records(&target, id).unwrap();
        assert_eq!(stored, input);
        assert_eq!(copied, records);
        input.validate_records(&copied).unwrap();
        let name = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let physical = target.join("inputs").join(name).join("records");
        let before = SegmentStore::recover_existing(&physical)
            .unwrap()
            .recovery
            .wal_records;
        disk::create_and_retain_records(&target, &mut failed, &input, &records).unwrap();
        let mut conflict = records.clone();
        conflict[1].payload.push(1);
        assert!(disk::create_and_retain_records(&target, &mut failed, &input, &conflict).is_err());
        assert!(!failed);
        assert_eq!(
            SegmentStore::recover_existing(&physical)
                .unwrap()
                .recovery
                .wal_records,
            before
        );
        assert_eq!(disk::read_records(&target, id).unwrap().1, records);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(unix)]
mod owner_boundaries {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    struct NoDirectoryRead {
        path: std::path::PathBuf,
        previous: std::fs::Permissions,
    }
    impl NoDirectoryRead {
        fn enter(path: &Path) -> Self {
            let previous = std::fs::metadata(path).unwrap().permissions();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o111)).unwrap();
            let fault = Self {
                path: path.to_path_buf(),
                previous,
            };
            let error = std::fs::File::open(path).expect_err(
                "host qualification requires an unprivileged Unix user whose directory read permissions are enforced",
            );
            assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
            fault
        }
    }
    impl Drop for NoDirectoryRead {
        fn drop(&mut self) {
            if let Err(error) = std::fs::set_permissions(&self.path, self.previous.clone()) {
                eprintln!(
                    "cannot restore directory permissions {}: {error}",
                    self.path.display()
                );
            }
        }
    }

    #[test]
    fn bound_choice_uses_live_custody_but_cold_owner_still_renews_ancestors() {
        for binding in ["authority", "accepted"] {
            let path = root(&format!("bound-choice-live-{binding}"));
            let (accepted, approval, chosen) = accepted_b(&path);
            let (mut store, target) = if binding == "authority" {
                drop(accepted);
                let target = path.join("authority");
                (
                    MembershipDecisionStore::open(&target, NodeId(2), &approval).unwrap(),
                    target,
                )
            } else {
                (accepted, path.join("decision-2"))
            };
            let high = round(91, 3);
            store.promise(NodeId(3), high).unwrap();
            let records = store.input(chosen.input_id()).unwrap();
            let before = store
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records
                .len();
            {
                // This ancestor was published before the live owner escaped.
                // Known child routes remain traversable; only directory-open
                // for redundant ancestor sync is denied, not owned data reads.
                let _fault = NoDirectoryRead::enter(&path);
                assert_eq!(store.input(chosen.input_id()).unwrap(), records);
                store
                    .retain_chosen_input_with_hook(chosen.clone(), &approval, || {
                        panic!("complete bound input must not enter the copy branch");
                    })
                    .unwrap();
                assert_eq!(store.state.promised, Some(high));
                assert_eq!(store.chosen(), Some(&chosen));
                assert_eq!(
                    store
                        .journal
                        .recover_current()
                        .unwrap()
                        .recovery
                        .wal_records
                        .len(),
                    before + 1
                );
                drop(store);
                assert!(
                    MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(2))
                        .is_err()
                );
            }
            let store =
                MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(2))
                    .unwrap();
            assert_eq!(store.state.promised, Some(high));
            assert_eq!(store.chosen(), Some(&chosen));
            assert_eq!(store.input(chosen.input_id()).unwrap(), records);
            drop(store);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn selected_inbox_begin_reuses_owner_but_still_requires_input_publication() {
        let path = root("selected-inbox-live-publication");
        let (_, approval) = approvals(&path);
        let input = VsrMembershipInput::from_approval(&approval).unwrap();
        let id = input.id().unwrap();
        let records = input.flattened_records(&approval).unwrap();
        let target = path.join("inbox");
        let mut inbox =
            MembershipAuthorityInbox::open(&target, PlacementGroupId(1), NodeId(3)).unwrap();
        inbox.begin(NodeId(1), input.clone()).unwrap();
        let mut ordinal = 0;
        while ordinal < records.len() {
            let batch = VsrHistoryBatch::pack(ordinal as u64, &records[ordinal..]).unwrap();
            ordinal += batch.records.len();
            inbox.append(NodeId(1), id, batch).unwrap();
        }
        {
            let _fault = NoDirectoryRead::enter(&path);
            assert_eq!(
                inbox.begin(NodeId(2), input.clone()).unwrap(),
                records.len() as u64
            );
            assert!(inbox.begin(NodeId(99), input.clone()).is_err());
        }
        let name = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let record_root = target.join("inputs").join(name).join("records");
        {
            let _fault = NoDirectoryRead::enter(&record_root);
            // Fresh read succeeds. The following failure must therefore arise
            // from publication, rather than an inaccessible input or record.
            assert_eq!(disk::read_records(&target, id).unwrap().1, records);
            assert!(inbox.begin(NodeId(2), input.clone()).is_err());
            assert_eq!(inbox.selected_input(), Some(&input));
        }
        assert_eq!(
            inbox.begin(NodeId(2), input.clone()).unwrap(),
            records.len() as u64
        );
        assert_eq!(disk::read_records(&target, id).unwrap().1, records);
        drop(inbox);
        {
            let _fault = NoDirectoryRead::enter(&path);
            assert!(
                MembershipAuthorityInbox::open_existing(&target, PlacementGroupId(1), NodeId(3))
                    .is_err()
            );
        }
        let inbox =
            MembershipAuthorityInbox::open_existing(&target, PlacementGroupId(1), NodeId(3))
                .unwrap();
        assert_eq!(inbox.selected_input(), Some(&input));
        inbox.finish(NodeId(2), id).unwrap();
        drop(inbox);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn selected_inbox_begin_rejects_missing_manifest_without_recreating_input() {
    let path = root("selected-inbox-missing-manifest");
    let (_, approval) = approvals(&path);
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    let id = input.id().unwrap();
    let target = path.join("inbox");
    let mut inbox =
        MembershipAuthorityInbox::open(&target, PlacementGroupId(1), NodeId(3)).unwrap();
    inbox.begin(NodeId(1), input.clone()).unwrap();
    let name = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let manifest = target.join("inputs").join(name).join("records/MANIFEST");
    std::fs::remove_file(&manifest).unwrap();
    assert!(inbox.begin(NodeId(2), input.clone()).is_err());
    assert!(!manifest.exists());
    assert_eq!(inbox.selected_input(), Some(&input));
    drop(inbox);
    assert!(
        MembershipAuthorityInbox::open_existing(&target, PlacementGroupId(1), NodeId(3)).is_err()
    );
    std::fs::remove_dir_all(path).unwrap();
}
