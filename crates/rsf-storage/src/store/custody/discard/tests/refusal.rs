use super::*;

#[test]
fn discard_rejects_foreign_aliased_hardlinked_nonempty_sidecars_and_rotation_residue_before_effects()
 {
    for fault in [
        "two-other-roots",
        "foreign-segment",
        "nonzero-manifest-lock",
        "nonzero-active-lock",
        "nonempty-next",
        "wrong-next",
        "bad-temp",
        "segment-alias",
        "segment-hardlink",
        "manifest-alias",
        "manifest-hardlink",
    ] {
        let fixture = Fixture::new("refusal");
        rotation_residue(&fixture.a, usize::MAX);
        let manifest =
            SegmentManifest::decode(&fs::read(fixture.a.join(MANIFEST_FILE)).unwrap()).unwrap();
        let (_, next) = manifest.next_rotation().unwrap();
        let first = fixture
            .a
            .join(SEGMENT_DIR)
            .join(&manifest.segments[0].file_name);
        let active = fixture
            .a
            .join(SEGMENT_DIR)
            .join(&manifest.active_segment().unwrap().file_name);
        let victim = fixture.root.join("foreign-victim");
        fs::write(&victim, b"foreign original bytes").unwrap();
        match fault {
            "two-other-roots" => {
                fs::write(fixture.a.join("extra1"), []).unwrap();
                fs::write(fixture.a.join("extra2"), []).unwrap();
            }
            "foreign-segment" => {
                fs::write(fixture.a.join(SEGMENT_DIR).join("unknown"), []).unwrap()
            }
            "nonzero-manifest-lock" => fs::write(fixture.a.join("MANIFEST.lock"), b"x").unwrap(),
            "nonzero-active-lock" => fs::write(active.with_extension("lock"), b"x").unwrap(),
            "nonempty-next" => {
                fs::write(fixture.a.join(SEGMENT_DIR).join(&next.file_name), b"x").unwrap()
            }
            "wrong-next" => fs::write(
                fixture
                    .a
                    .join(SEGMENT_DIR)
                    .join(segment_file_name(next.id + 1)),
                [],
            )
            .unwrap(),
            "bad-temp" => fs::write(
                fixture.a.join("MANIFEST.tmp"),
                b"foreign noncanonical bytes",
            )
            .unwrap(),
            "segment-alias" => {
                fs::remove_file(&first).unwrap();
                std::os::unix::fs::symlink(&victim, &first).unwrap();
            }
            "segment-hardlink" => {
                fs::remove_file(&first).unwrap();
                fs::hard_link(&victim, &first).unwrap();
            }
            "manifest-alias" => {
                fs::remove_file(fixture.a.join(MANIFEST_FILE)).unwrap();
                std::os::unix::fs::symlink(&victim, fixture.a.join(MANIFEST_FILE)).unwrap();
            }
            "manifest-hardlink" => {
                fs::remove_file(fixture.a.join(MANIFEST_FILE)).unwrap();
                fs::hard_link(&victim, fixture.a.join(MANIFEST_FILE)).unwrap();
            }
            _ => unreachable!(),
        }
        let before = image(&fixture.root);
        let reads = crate::reader::payload_reads();
        assert!(
            SegmentStore::inspect_provisional_discard(&fixture.a, limits()).is_err(),
            "{fault}"
        );
        assert_eq!(image(&fixture.root), before, "{fault}");
        assert_eq!(crate::reader::payload_reads(), reads, "{fault}");
        fixture.validate().unwrap();
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn discard_rejects_root_replacement_or_new_name_after_inventory_without_adopting_it() {
    for replace in [false, true] {
        let fixture = Fixture::new("between-phases");
        drop(populated(&fixture.a));
        let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).unwrap();
        let held = fixture.root.join("held-original");
        if replace {
            fs::rename(&fixture.a, &held).unwrap();
            fs::create_dir(&fixture.a).unwrap();
            fs::write(fixture.a.join("foreign"), b"unrelated").unwrap();
        } else {
            fs::write(fixture.a.join("late-name"), b"unrelated").unwrap();
        }
        let before = image(&fixture.root);
        assert!(inventory.remove_children(|| fixture.validate()).is_err());
        assert_eq!(image(&fixture.root), before);
        if replace {
            fs::remove_file(fixture.a.join("foreign")).unwrap();
            fs::remove_dir(&fixture.a).unwrap();
            fs::rename(held, &fixture.a).unwrap();
        } else {
            fs::remove_file(fixture.a.join("late-name")).unwrap();
        }
        discard(&fixture).unwrap();
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn discard_remainder_refuses_surviving_auxiliary_and_reappeared_removed_children() {
    for external in [false, true] {
        let fixture = Fixture::new("remainder-refuse");
        drop(populated(&fixture.a));
        if external {
            fs::write(fixture.a.join("caller-owned"), []).unwrap();
        }
        let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).unwrap();
        let remainder = inventory.remove_children(|| fixture.validate()).unwrap();
        let foreign = if external {
            fixture.a.join("caller-owned")
        } else {
            let segments = fixture.a.join(SEGMENT_DIR);
            fs::create_dir(&segments).unwrap();
            segments
        };
        let before = image(&fixture.root);
        assert!(remainder.finish(|| fixture.validate()).is_err());
        assert_eq!(image(&fixture.root), before);
        assert!(fixture.a.join(MANIFEST_FILE).is_file());
        if external {
            fs::remove_file(foreign).unwrap();
        } else {
            fs::remove_dir(foreign).unwrap();
        }
        discard(&fixture).unwrap();
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn missing_declared_files_are_discard_only_while_manifestless_sidecars_and_auxiliaries_reject() {
    let fixture = Fixture::new("missing-declared");
    drop(populated(&fixture.a));
    let manifest =
        SegmentManifest::decode(&fs::read(fixture.a.join(MANIFEST_FILE)).unwrap()).unwrap();
    fs::remove_file(
        fixture
            .a
            .join(SEGMENT_DIR)
            .join(&manifest.segments[0].file_name),
    )
    .unwrap();
    assert!(
        SegmentStore::open_existing_recovered_bounded(
            &fixture.a,
            Default::default(),
            limits(),
            |_| Ok(())
        )
        .is_err(),
        "discard subset must never become an openable writer"
    );
    discard(&fixture).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
    for sidecar in [false, true] {
        let fixture = Fixture::new("factory-unknown");
        crate::store::initialization::test_interrupt_factory(&fixture.a, 5).unwrap_err();
        let foreign = if sidecar {
            fixture
                .a
                .join(SEGMENT_DIR)
                .join(segment_file_name(1))
                .with_extension("lock")
        } else {
            fixture.a.join("caller-owned")
        };
        fs::write(foreign, []).unwrap();
        let before = image(&fixture.root);
        assert!(SegmentStore::inspect_provisional_discard(&fixture.a, limits()).is_err());
        assert_eq!(image(&fixture.root), before);
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn actual_unlink_permission_failure_returns_no_remainder_and_leaves_anchor_and_children() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new("actual-unlink-denied");
    drop(populated(&fixture.a));
    let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).unwrap();
    let before = image(&fixture.root);
    let segments = fixture.a.join(SEGMENT_DIR);
    let permissions = fs::metadata(&segments).unwrap().permissions();
    fs::set_permissions(&segments, fs::Permissions::from_mode(0o555)).unwrap();
    let result = inventory.remove_children(|| fixture.validate());
    // Restore before assertions. This oracle requires ordinary unprivileged
    // POSIX permission enforcement; a privileged bypass is a failure, not skip.
    fs::set_permissions(&segments, permissions).unwrap();
    let error =
        result.expect_err("actual unlink must be denied on this unprivileged qualification host");
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(error.message.starts_with("storage Recover failed:"));
    assert!(!error.message.contains("injected"));
    assert_eq!(image(&fixture.root), before);
    fixture.validate().unwrap();
    discard(&fixture).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
