use super::*;
fn assert_fenced(error: rsf_core::RsfError) {
    assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "membership decision durability failed; reopen before further use"
    );
}
fn staged(
    path: &Path,
) -> (
    MembershipDecisionStore,
    VsrMembershipApproval,
    VsrMembershipChosen,
) {
    let (accepted, approval, chosen) = accepted_b(path);
    drop(accepted);
    let mut owner = MembershipDecisionStore::open_existing(
        path.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    let input = VsrMembershipInput::from_approval(&approval).unwrap();
    owner.promise(NodeId(1), round(1, 1)).unwrap();
    owner
        .begin_input(NodeId(1), round(1, 1), input.clone())
        .unwrap();
    let records = input.flattened_records(&approval).unwrap();
    owner
        .append_input(
            NodeId(1),
            round(1, 1),
            input.id().unwrap(),
            VsrHistoryBatch::pack(0, &records[..1]).unwrap(),
        )
        .unwrap();
    (owner, approval, chosen)
}
#[test]
fn staged_source_supplier_error_preserves_healthy_owner_and_resolution_is_read_only() {
    let path = root("stage-source-error");
    let (mut owner, approval, chosen) = staged(&path);
    let before = owner
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    assert_eq!(
        owner
            .stage_approval(&chosen, || Err(invalid_input("supplier refused")))
            .unwrap_err()
            .message,
        "supplier refused"
    );
    assert!(owner.healthy().is_ok());
    let result = owner
        .stage_approval(&chosen, || Ok(approval.clone()))
        .unwrap();
    assert_eq!(
        owner
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        before
    );
    assert!(owner.chosen().is_none());
    assert!(owner.retained_approval(chosen.input_id()).is_err());
    owner.retain_chosen_input(chosen.clone(), &result).unwrap();
    assert_eq!(owner.chosen(), Some(&chosen));
    drop(owner);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn staged_source_supplier_swap_success_and_error_fence_original_owner_after_restoration() {
    for role in ["staged", "authority"] {
        for supplier_error in [false, true] {
            let path = root("stage-source-swap");
            let (owner, approval, chosen) = staged(&path);
            let id = if role == "staged" {
                chosen.input_id()
            } else {
                owner.authority
            };
            let directory = owner.directory(id);
            let saved = directory.with_extension("held-original");
            let before = owner
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records;
            let result = owner.stage_approval(&chosen, || {
                std::fs::rename(&directory, &saved).unwrap();
                if supplier_error {
                    Err(invalid_input("supplier refused"))
                } else {
                    Ok(approval.clone())
                }
            });
            assert!(result.is_err());
            std::fs::rename(&saved, &directory).unwrap();
            assert_fenced(owner.healthy().unwrap_err());
            assert_fenced(
                owner
                    .stage_approval(&chosen, || Ok(approval.clone()))
                    .unwrap_err(),
            );
            assert_eq!(
                owner
                    .journal
                    .recover_current()
                    .unwrap()
                    .recovery
                    .wal_records,
                before
            );
            assert!(owner.chosen().is_none());
            drop(owner);
            let cold = MembershipDecisionStore::open_existing(
                path.join("decision-3"),
                PlacementGroupId(1),
                NodeId(3),
            )
            .unwrap();
            assert!(cold.chosen().is_none());
            assert!(
                cold.stage_approval(&chosen, || Ok(approval.clone()))
                    .is_ok()
            );
            drop(cold);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
#[test]
fn staged_source_supplier_unwind_fences_original_owner_without_durable_choice() {
    let path = root("stage-source-unwind");
    let (owner, _, chosen) = staged(&path);
    let before = owner
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = owner.stage_approval(&chosen, || panic!("supplier unwind"));
    }))
    .unwrap_err();
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("supplier unwind")
    );
    assert_fenced(owner.healthy().unwrap_err());
    assert!(owner.chosen().is_none());
    assert_eq!(
        owner
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        before
    );
    drop(owner);
    std::fs::remove_dir_all(path).unwrap();
}

fn absent(
    path: &Path,
) -> (
    MembershipDecisionStore,
    VsrMembershipApproval,
    VsrMembershipChosen,
) {
    let (accepted, approval, chosen) = accepted_b(path);
    drop(accepted);
    let owner = MembershipDecisionStore::open_existing(
        path.join("decision-3"),
        PlacementGroupId(1),
        NodeId(3),
    )
    .unwrap();
    assert!(
        owner
            .retained_approval(chosen.input_id())
            .unwrap()
            .is_none()
    );
    (owner, approval, chosen)
}
#[test]
fn absent_stage_source_pins_authority_across_supplier_success_and_error() {
    for supplier_error in [false, true] {
        let path = root("absent-stage-source-authority");
        let (owner, approval, chosen) = absent(&path);
        let authority = owner.directory(owner.authority);
        let saved = authority.with_extension("held-original");
        let before = owner
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records;
        let result = owner.stage_approval(&chosen, || {
            std::fs::rename(&authority, &saved).unwrap();
            if supplier_error {
                Err(invalid_input("supplier refused"))
            } else {
                Ok(approval.clone())
            }
        });
        assert!(result.is_err());
        std::fs::rename(&saved, &authority).unwrap();
        assert_fenced(owner.healthy().unwrap_err());
        assert_fenced(
            owner
                .stage_approval(&chosen, || Ok(approval.clone()))
                .unwrap_err(),
        );
        assert_eq!(
            owner
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records,
            before
        );
        assert!(!owner.directory(chosen.input_id()).exists());
        drop(owner);
        let cold = MembershipDecisionStore::open_existing(
            path.join("decision-3"),
            PlacementGroupId(1),
            NodeId(3),
        )
        .unwrap();
        assert!(cold.stage_approval(&chosen, || Ok(approval)).is_ok());
        assert!(!cold.directory(chosen.input_id()).exists());
        drop(cold);
        std::fs::remove_dir_all(path).unwrap();
    }
}
#[cfg(unix)]
#[test]
fn stage_source_requires_original_namespace_lock_for_every_supplier_path() {
    use std::os::unix::fs::MetadataExt;
    for is_staged in [false, true] {
        for fault in ["replace", "hardlink", "nonempty"] {
            for supplier_error in [false, true] {
                let path = root("stage-source-namespace");
                let (owner, approval, chosen) = if is_staged {
                    staged(&path)
                } else {
                    absent(&path)
                };
                let named = owner.root.join(".decision.lock");
                let saved = owner.root.join(".decision-lock-original");
                let original = owner._lock.metadata().unwrap();
                let before = owner
                    .journal
                    .recover_current()
                    .unwrap()
                    .recovery
                    .wal_records;
                let result = owner.stage_approval(&chosen, || {
                    match fault {
                        "replace" => {
                            std::fs::rename(&named, &saved).unwrap();
                            std::fs::write(&named, []).unwrap();
                        }
                        "hardlink" => std::fs::hard_link(&named, &saved).unwrap(),
                        "nonempty" => std::fs::write(&named, b"foreign").unwrap(),
                        _ => unreachable!(),
                    }
                    if supplier_error {
                        Err(invalid_input("supplier refused"))
                    } else {
                        Ok(approval.clone())
                    }
                });
                assert!(result.is_err());
                match fault {
                    "replace" => {
                        std::fs::remove_file(&named).unwrap();
                        std::fs::rename(&saved, &named).unwrap();
                    }
                    "hardlink" => std::fs::remove_file(&saved).unwrap(),
                    "nonempty" => std::fs::write(&named, []).unwrap(),
                    _ => unreachable!(),
                }
                let restored = std::fs::symlink_metadata(&named).unwrap();
                let held = owner._lock.metadata().unwrap();
                assert_eq!((original.dev(), original.ino()), (held.dev(), held.ino()));
                assert_eq!((restored.dev(), restored.ino()), (held.dev(), held.ino()));
                assert_eq!(held.len(), 0);
                assert_eq!(held.nlink(), 1);
                assert_fenced(owner.healthy().unwrap_err());
                assert_fenced(
                    owner
                        .stage_approval(&chosen, || Ok(approval.clone()))
                        .unwrap_err(),
                );
                assert!(owner.chosen().is_none());
                assert_eq!(
                    owner
                        .journal
                        .recover_current()
                        .unwrap()
                        .recovery
                        .wal_records,
                    before
                );
                drop(owner);
                let cold = MembershipDecisionStore::open_existing(
                    path.join("decision-3"),
                    PlacementGroupId(1),
                    NodeId(3),
                )
                .unwrap();
                assert!(cold.stage_approval(&chosen, || Ok(approval)).is_ok());
                drop(cold);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }
}

#[test]
fn staged_source_rejects_real_maintenance_and_nonwal_frames_before_supplier() {
    for kind in ["maintenance", "raw"] {
        let path = root("stage-source-nonhistory");
        let (owner, approval, chosen) = staged(&path);
        let directory = owner.directory(chosen.input_id()).join("records");
        let mut writer = SegmentStore::open(&directory).unwrap();
        if kind == "maintenance" {
            writer
                .append_maintenance(rsf_storage::MaintenanceRecord::retention(
                    PlacementGroupId(1),
                    LogIndex(0),
                    b"not input history",
                ))
                .unwrap();
        } else {
            writer.append(b"not a WAL frame").unwrap();
        }
        drop(writer);
        let mut retained = std::fs::read_dir(directory.join("segments"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        retained.push(directory.join("MANIFEST"));
        let before = retained
            .iter()
            .map(|path| std::fs::read(path).unwrap())
            .collect::<Vec<_>>();
        let journal = owner
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records;
        let called = std::cell::Cell::new(false);
        let error = owner
            .stage_approval(&chosen, || {
                called.set(true);
                Ok(approval)
            })
            .unwrap_err();
        assert_eq!(
            error.message,
            "membership STAGE input is not append-only history WAL"
        );
        assert!(!called.get());
        assert_eq!(
            retained
                .iter()
                .map(|path| std::fs::read(path).unwrap())
                .collect::<Vec<_>>(),
            before
        );
        assert_eq!(
            owner
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records,
            journal
        );
        assert_fenced(owner.healthy().unwrap_err());
        assert!(owner.chosen().is_none());
        drop(owner);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn complete_stage_source_cannot_bypass_original_namespace_owner() {
    use std::os::unix::fs::MetadataExt;
    let path = root("complete-stage-source-namespace");
    let (owner, approval, chosen) = accepted_b(&path);
    assert!(
        owner
            .retained_approval(chosen.input_id())
            .unwrap()
            .is_some()
    );
    let named = owner.root.join(".decision.lock");
    let saved = owner.root.join(".decision-lock-original");
    let original = owner._lock.metadata().unwrap();
    let before = owner
        .journal
        .recover_current()
        .unwrap()
        .recovery
        .wal_records;
    std::fs::rename(&named, &saved).unwrap();
    std::fs::write(&named, []).unwrap();
    let called = std::cell::Cell::new(false);
    let error = owner
        .stage_approval(&chosen, || {
            called.set(true);
            Ok(approval.clone())
        })
        .unwrap_err();
    assert_eq!(
        error.message,
        "membership STAGE original namespace lock custody changed"
    );
    assert!(!called.get());
    std::fs::remove_file(&named).unwrap();
    std::fs::rename(&saved, &named).unwrap();
    let restored = std::fs::symlink_metadata(&named).unwrap();
    let held = owner._lock.metadata().unwrap();
    assert_eq!((held.dev(), held.ino()), (original.dev(), original.ino()));
    assert_eq!(
        (restored.dev(), restored.ino()),
        (original.dev(), original.ino())
    );
    assert_fenced(owner.healthy().unwrap_err());
    assert_fenced(
        owner
            .stage_approval(&chosen, || Ok(approval.clone()))
            .unwrap_err(),
    );
    assert_eq!(
        owner
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records,
        before
    );
    assert!(owner.chosen().is_none());
    drop(owner);
    let cold = MembershipDecisionStore::open_existing(
        path.join("decision-2"),
        PlacementGroupId(1),
        NodeId(2),
    )
    .unwrap();
    assert!(
        cold.stage_approval(&chosen, || panic!(
            "complete retained input must not call supplier"
        ))
        .is_ok()
    );
    drop(cold);
    std::fs::remove_dir_all(path).unwrap();
}
