use super::*;

fn make(fixture: &Fixture, factory: bool) {
    if factory {
        let error =
            crate::store::initialization::test_interrupt_factory(&fixture.a, 5).unwrap_err();
        assert_eq!(
            error.message,
            "injected actual initializer boundary interruption (test only)"
        );
    } else {
        rotation_residue(&fixture.a, usize::MAX);
    }
}
#[test]
fn every_discard_unlink_rmdir_and_sync_boundary_failure_preserves_recognizable_cold_remainder() {
    for factory in [false, true] {
        let baseline = Fixture::new("event-baseline");
        make(&baseline, factory);
        let observed = observation::watch(None);
        discard(&baseline).unwrap();
        let events = observed.events();
        drop(observed);
        assert!(events.len() > 12);
        for operation in [
            remove::Operation::Unlink,
            remove::Operation::RemoveDirectory,
            remove::Operation::Sync,
        ] {
            assert!(
                events
                    .iter()
                    .any(|e| e.operation == operation && !e.completed)
            );
            assert!(
                events
                    .iter()
                    .any(|e| e.operation == operation && e.completed)
            );
        }
        let root = baseline.root.clone();
        drop(baseline);
        fs::remove_dir_all(root).unwrap();
        for (failure, expected) in events.iter().enumerate() {
            let fixture = Fixture::new("each-boundary");
            make(&fixture, factory);
            let b = image(&fixture.root.join("B"));
            let observed = observation::watch(Some(failure));
            let error = discard(&fixture).unwrap_err();
            assert_eq!(
                error.message,
                "injected discard boundary failure (test only)"
            );
            let actual = observed.events();
            assert_eq!(actual.len(), failure + 1);
            assert_eq!(actual.last().unwrap().operation, expected.operation);
            assert_eq!(actual.last().unwrap().completed, expected.completed);
            drop(observed);
            fixture.validate().unwrap();
            assert_eq!(image(&fixture.root.join("B")), b);
            // These hooks model interruption/error around actual operations,
            // not machine loss. Complete removals may already be visible.
            if fixture.a.exists() {
                discard(&fixture).unwrap();
            } else {
                File::open(&fixture.root).unwrap().sync_all().unwrap();
            }
            assert!(!fixture.a.exists());
            fixture.validate().unwrap();
            assert_eq!(image(&fixture.root.join("B")), b);
            let root = fixture.root.clone();
            drop(fixture);
            fs::remove_dir_all(root).unwrap();
        }
    }
}
#[test]
fn discard_barriers_keep_manifest_last_and_factory_temporary_before_its_required_segment() {
    for factory in [false, true] {
        let fixture = Fixture::new("barrier-order");
        make(&fixture, factory);
        let observed = observation::watch(None);
        discard(&fixture).unwrap();
        let events = observed.events();
        let after = |operation, path: &Path| {
            events
                .iter()
                .position(|e| e.operation == operation && e.completed && e.path == path)
                .unwrap()
        };
        let temp = after(remove::Operation::Unlink, &fixture.a.join("MANIFEST.tmp"));
        let first_segment = events
            .iter()
            .position(|e| {
                e.operation == remove::Operation::Unlink
                    && !e.completed
                    && e.path.parent() == Some(fixture.a.join(SEGMENT_DIR).as_path())
            })
            .unwrap();
        assert!(
            events[temp + 1..first_segment]
                .iter()
                .any(|e| e.operation == remove::Operation::Sync
                    && e.completed
                    && e.path == fixture.a)
        );
        let segments_removed = after(
            remove::Operation::RemoveDirectory,
            &fixture.a.join(SEGMENT_DIR),
        );
        assert!(
            events[..segments_removed]
                .iter()
                .any(|e| e.operation == remove::Operation::Sync
                    && e.completed
                    && e.path == fixture.a.join(SEGMENT_DIR))
        );
        let root_removed = after(remove::Operation::RemoveDirectory, &fixture.a);
        if !factory {
            let manifest = after(remove::Operation::Unlink, &fixture.a.join(MANIFEST_FILE));
            assert!(manifest > segments_removed);
            assert!(
                events[segments_removed + 1..manifest]
                    .iter()
                    .any(|e| e.operation == remove::Operation::Sync
                        && e.completed
                        && e.path == fixture.a)
            );
            assert!(
                events[manifest + 1..root_removed]
                    .iter()
                    .any(|e| e.operation == remove::Operation::Sync
                        && e.completed
                        && e.path == fixture.a)
            );
        }
        assert!(
            events[root_removed + 1..]
                .iter()
                .any(|e| e.operation == remove::Operation::Sync
                    && e.completed
                    && e.path == fixture.root)
        );
        drop(observed);
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn discard_callback_error_still_observes_physical_loss_and_never_deletes_through_replacement() {
    let fixture = Fixture::new("callback-loss");
    drop(populated(&fixture.a));
    let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).unwrap();
    let original = fixture.a.join("MANIFEST");
    let saved = fixture.root.join("original-manifest");
    let bytes = fs::read(&original).unwrap();
    let error = inventory
        .remove_children(|| {
            fixture.validate()?;
            fs::rename(&original, &saved).unwrap();
            fs::write(&original, &bytes).unwrap();
            Err(RsfError::new(
                ErrorCode::InvalidInput,
                "semantic parent refusal",
            ))
        })
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_ne!(error.message, "semantic parent refusal");
    assert!(fixture.a.join(SEGMENT_DIR).is_dir());
    fs::remove_file(original.clone()).unwrap();
    fs::rename(saved, original).unwrap();
    fixture.validate().unwrap();
    discard(&fixture).unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn discard_clean_callback_refusal_precedes_effects_and_callback_unwind_returns_no_capability() {
    let fixture = Fixture::new("callback-refusal");
    drop(populated(&fixture.a));
    let before = image(&fixture.root);
    let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).unwrap();
    let error = inventory
        .remove_children(|| {
            Err(RsfError::new(
                ErrorCode::InvalidInput,
                "parent not authorized",
            ))
        })
        .unwrap_err();
    assert_eq!(error.message, "parent not authorized");
    assert_eq!(image(&fixture.root), before);
    let inventory = SegmentStore::inspect_provisional_discard(&fixture.a, limits()).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        inventory.remove_children(|| panic!("injected parent unwind"))
    }));
    assert!(result.is_err());
    assert_eq!(image(&fixture.root), before);
    // A panic can poison the ordinary per-root gate; no live recovery promise.
    // The API has returned neither a remainder nor a completion capability.
    fixture.validate().unwrap();
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
