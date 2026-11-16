use super::*;
use std::os::unix::fs::{PermissionsExt, symlink};

fn materialized(
    path: &Path,
) -> (
    Fixture,
    Vec<MembershipStagingStore>,
    Vec<VsrMembershipStagedReport>,
) {
    let f = fixture(path);
    let mut stages = (0..3)
        .map(|node| {
            MembershipStagingStore::stage(
                path.join(format!("stage-{node}")),
                NodeId(node as u64),
                plan(&f, false),
                &f.cells[node],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    stages.push(
        f.learner
            .stage_membership(path.join("stage-3"), plan(&f, false))
            .unwrap(),
    );
    let reports = stages
        .iter_mut()
        .map(|stage| stage.materialize(&mut |_| Ok(())).unwrap())
        .collect();
    (f, stages, reports)
}
fn source(stage: &MembershipStagingStore) -> PathBuf {
    let name = stage
        .plan
        .input
        .id()
        .unwrap()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    stage.root.join("inputs").join(name)
}
fn segment(root: &Path) -> PathBuf {
    std::fs::read_dir(root.join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension() == Some(std::ffi::OsStr::new("segment")))
        .unwrap()
}

#[test]
fn stage_report_first_observes_each_owned_loss_and_stays_failed_after_original_inode_restore() {
    for fault in [
        "meta",
        "descriptor",
        "source-manifest",
        "source-segment",
        "selected-manifest",
        "selected-segment",
        "receipt",
        "lock",
        "replace",
        "symlink",
        "hardlink",
        "short",
        "truncate",
    ] {
        let path = root(&format!("stage-custody-{fault}"));
        let (f, mut stages, _) = materialized(&path);
        let stage = stages.remove(1);
        let target = match fault {
            "meta" => stage.root.join("META"),
            "descriptor" => source(&stage).join("DESCRIPTOR"),
            "source-manifest" => source(&stage).join("records/MANIFEST"),
            "source-segment" => segment(&source(&stage).join("records")),
            "selected-manifest" => stage.root.join("selected/MANIFEST"),
            "receipt" => stage.root.join("INSTALLED"),
            "lock" => stage.root.join(".staging.lock"),
            _ => segment(&stage.root.join("selected")),
        };
        let saved = path.join("original");
        let original = std::fs::read(&target).unwrap();
        if fault == "truncate" {
            OpenOptions::new()
                .write(true)
                .open(&target)
                .unwrap()
                .set_len(1)
                .unwrap();
        } else {
            std::fs::rename(&target, &saved).unwrap();
        }
        match fault {
            "replace" => {
                std::fs::copy(&saved, &target).unwrap();
            }
            "symlink" => symlink(&saved, &target).unwrap(),
            "hardlink" => std::fs::hard_link(&saved, &target).unwrap(),
            "short" => {
                std::fs::write(&target, b"").unwrap();
            }
            _ => {}
        }
        assert!(stage.report().is_err(), "{fault}");
        if fault == "truncate" {
            std::fs::write(&target, original).unwrap();
        } else {
            if std::fs::symlink_metadata(&target).is_ok() {
                std::fs::remove_file(&target).unwrap();
            }
            std::fs::rename(&saved, &target).unwrap();
        }
        assert!(
            stage.report().is_err(),
            "{fault}: restored path revived failed owner"
        );
        drop(stage);
        drop(stages);
        let mut reopened = MembershipStagingStore::open_existing(
            path.join("stage-1"),
            PlacementGroupId(1),
            NodeId(1),
        )
        .unwrap();
        assert!(reopened.report().is_err());
        reopened.materialize(&mut |_| Ok(())).unwrap();
        reopened.report().unwrap();
        drop(reopened);
        drop(f);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn each_stage_effect_rejects_its_first_custody_fault_before_callback_or_target_publication() {
    for operation in ["materialize", "retain", "install", "promote"] {
        let path = root(&format!("stage-effect-{operation}"));
        let (mut f, mut stages, reports) = materialized(&path);
        let quorum = VsrMembershipStagedQuorum::from_reports(reports[..3].to_vec()).unwrap();
        let local = if operation == "promote" { 3 } else { 1 };
        let mut stage = stages.remove(local);
        let missing = stage.root.join("selected/MANIFEST");
        std::fs::remove_file(&missing).unwrap();
        match operation {
            "materialize" => {
                let mut calls = 0;
                assert!(
                    stage
                        .materialize(&mut |_| {
                            calls += 1;
                            Ok(())
                        })
                        .is_err()
                );
                assert_eq!(calls, 0);
            }
            "retain" => {
                assert!(stage.retain_publication(&quorum).is_err());
                assert!(!stage.root.join("PUBLICATION").exists());
            }
            "install" => {
                let before = f.cells[1].recovery_state_token().unwrap();
                assert!(f.cells[1].install_membership(&stage, &quorum).is_err());
                assert_eq!(f.cells[1].recovery_state_token().unwrap(), before);
                assert!(!path.join("old/1/VSR_ACTIVE").exists());
            }
            "promote" => {
                let target = path.join("promoted");
                assert!(
                    f.learner
                        .promote_membership(&target, &stage, &quorum)
                        .is_err()
                );
                assert!(!target.exists());
            }
            _ => unreachable!(),
        }
        assert!(!missing.exists());
        drop(stage);
        drop(stages);
        drop(f);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn stage_own_receipt_and_publication_transitions_preserve_first_proof_and_need_no_wal_read() {
    let path = root("stage-custody-own-writes");
    let (f, mut stages, reports) = materialized(&path);
    let mut stage = stages.remove(1);
    let quorum = VsrMembershipStagedQuorum::from_reports(reports[..3].to_vec()).unwrap();
    let alternate = VsrMembershipStagedQuorum::from_reports(reports[1..].to_vec()).unwrap();
    stage.materialize(&mut |_| Ok(())).unwrap();
    stage.retain_publication(&quorum).unwrap();
    let original = std::fs::read(stage.root.join("PUBLICATION")).unwrap();
    stage.retain_publication(&alternate).unwrap();
    assert_eq!(
        std::fs::read(stage.root.join("PUBLICATION")).unwrap(),
        original
    );
    assert!(
        stage
            .materialize(&mut |_| Err(corrupt("application failed")))
            .is_err()
    );
    assert!(stage.report().is_err());
    stage.materialize(&mut |_| Ok(())).unwrap();
    assert_eq!(
        std::fs::read(stage.root.join("PUBLICATION")).unwrap(),
        original
    );
    let files = [
        segment(&stage.root.join("selected")),
        segment(&source(&stage).join("records")),
    ];
    let permissions = files
        .iter()
        .map(|file| std::fs::metadata(file).unwrap().permissions())
        .collect::<Vec<_>>();
    for file in &files {
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o000)).unwrap();
        File::open(file).expect_err("qualification requires an unprivileged Unix host");
    }
    let report = stage.report();
    for (file, permissions) in files.iter().zip(permissions) {
        std::fs::set_permissions(file, permissions).unwrap();
    }
    assert_eq!(report.unwrap(), reports[1]);
    drop(stage);
    drop(stages);
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-1"), PlacementGroupId(1), NodeId(1))
            .unwrap();
    assert!(stage.report().is_err());
    stage.materialize(&mut |_| Ok(())).unwrap();
    stage.retain_publication(&alternate).unwrap();
    assert_eq!(
        std::fs::read(stage.root.join("PUBLICATION")).unwrap(),
        original
    );
    drop(stage);
    drop(f);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn stage_callback_and_publication_hook_cannot_repair_or_acknowledge_lost_custody() {
    for boundary in ["callback", "publication"] {
        let path = root(&format!("stage-custody-boundary-{boundary}"));
        let (f, mut stages, reports) = materialized(&path);
        let mut stage = stages.remove(1);
        let installed = stage.root.join("INSTALLED");
        if boundary == "callback" {
            let mut removed = false;
            assert!(
                stage
                    .materialize(&mut |_| {
                        if !removed {
                            std::fs::remove_file(&installed).unwrap();
                            removed = true;
                        }
                        Ok(())
                    })
                    .is_err()
            );
            assert!(removed);
            assert!(!installed.exists());
        } else {
            let quorum = VsrMembershipStagedQuorum::from_reports(reports[..3].to_vec()).unwrap();
            let meta = std::fs::read(stage.root.join("META")).unwrap();
            assert!(
                stage
                    .retain_publication_with_hook(&quorum, || {
                        std::fs::remove_file(&installed).unwrap();
                    })
                    .is_err()
            );
            assert_eq!(std::fs::read(stage.root.join("META")).unwrap(), meta);
            assert!(!installed.exists());
        }
        assert!(stage.report().is_err());
        drop(stage);
        drop(stages);
        drop(f);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn stage_root_replacement_and_retained_publication_loss_require_fresh_owner() {
    for fault in ["root", "publication"] {
        let path = root(&format!("stage-custody-{fault}"));
        let (f, mut stages, reports) = materialized(&path);
        let mut stage = stages.remove(1);
        let quorum = VsrMembershipStagedQuorum::from_reports(reports[..3].to_vec()).unwrap();
        stage.retain_publication(&quorum).unwrap();
        let saved = path.join("original-owned");
        let owned = stage.root.clone();
        if fault == "root" {
            std::fs::rename(&owned, &saved).unwrap();
            std::fs::create_dir(&owned).unwrap();
        } else {
            std::fs::rename(owned.join("PUBLICATION"), &saved).unwrap();
        }
        assert!(stage.publication_quorum().is_err(), "{fault}");
        assert!(stage.report().is_err(), "{fault}");
        if fault == "root" {
            assert_eq!(std::fs::read_dir(&owned).unwrap().count(), 0);
            std::fs::remove_dir(&owned).unwrap();
            std::fs::rename(&saved, &owned).unwrap();
        } else {
            assert!(!owned.join("PUBLICATION").exists());
            std::fs::rename(&saved, owned.join("PUBLICATION")).unwrap();
        }
        assert!(stage.publication_quorum().is_err());
        drop(stage);
        let mut recovered =
            MembershipStagingStore::open_existing(&owned, PlacementGroupId(1), NodeId(1)).unwrap();
        assert_eq!(recovered.publication_quorum().unwrap().unwrap(), quorum);
        assert!(recovered.report().is_err());
        recovered.materialize(&mut |_| Ok(())).unwrap();
        recovered.report().unwrap();
        drop(recovered);
        drop(stages);
        drop(f);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn sealed_stage_cold_recovery_never_truncates_source_or_selected_torn_tail() {
    for source_history in [false, true] {
        let path = root(if source_history {
            "stage-torn-source"
        } else {
            "stage-torn-selected"
        });
        let (f, mut stages, _) = materialized(&path);
        let stage = stages.remove(1);
        let history = if source_history {
            source(&stage).join("records")
        } else {
            stage.root.join("selected")
        };
        let file = segment(&history);
        drop(stage);
        OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap()
            .write_all(b"torn")
            .unwrap();
        let bytes = std::fs::read(&file).unwrap();
        for _ in 0..2 {
            assert!(
                MembershipStagingStore::open_existing(
                    path.join("stage-1"),
                    PlacementGroupId(1),
                    NodeId(1)
                )
                .is_err()
            );
            assert_eq!(std::fs::read(&file).unwrap(), bytes);
        }
        drop(stages);
        drop(f);
        std::fs::remove_dir_all(path).unwrap();
    }
}
