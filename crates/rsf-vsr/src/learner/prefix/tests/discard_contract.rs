//! Private caller-boundary contracts; not an old-API red selection.
use super::discard::{begin_active, cold, image, staged};
use super::*;
use namespace::{Operation, observation as boundary};

fn exercise(label: &str, publish: bool, fail_at: Option<usize>) -> (PathBuf, Vec<boundary::Event>) {
    let mut fixture = Fixture::new(label);
    let (id, _) = staged(&mut fixture);
    let old_active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let observer = boundary::watch(fail_at);
    let result = if publish {
        fixture.four.finish_prefix(id, assert_semantics).map(|_| ())
    } else {
        begin_active(&mut fixture.four).map(|_| ())
    };
    let events = observer.events();
    drop(observer);
    if fail_at.is_some() {
        let error = result.unwrap_err();
        assert_eq!(
            error.message,
            "injected bounded namespace boundary failure (test only)"
        );
        assert!(
            begin_active(&mut fixture.four)
                .unwrap_err()
                .message
                .contains("learner durability failed")
        );
    } else {
        result.unwrap();
    }
    let root = fixture.root.clone();
    drop(fixture);
    let mut owner = cold(&root.join("four"));
    let current = owner.published.as_ref().unwrap().id;
    if current == id {
        assert!(publish, "cancellation cannot publish its abandoned stage");
        begin_active(&mut owner).unwrap();
        assert_eq!(
            owner.finish_prefix(id, assert_semantics).unwrap(),
            LogIndex(2)
        );
    } else {
        assert_eq!(fs::read(root.join("four/ACTIVE")).unwrap(), old_active);
        assert_eq!(
            owner.published.as_ref().unwrap().cell.committed_tail(),
            LogIndex(1)
        );
        begin_active(&mut owner).unwrap();
        assert!(
            !owner.directory(&id).exists(),
            "actual ACTIVE explicitly cancels every disposal remainder"
        );
    }
    assert_eq!(
        owner
            .published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    drop(owner);
    (root, events)
}

#[test]
fn every_actual_caller_disposal_boundary_reopens_the_exact_current_publication_and_completes_cancel()
 {
    for publish in [false, true] {
        let (root, events) = exercise("caller-events", publish, None);
        assert!(
            events
                .iter()
                .any(|event| event.operation == Operation::RemoveChildren)
        );
        assert!(
            events
                .iter()
                .any(|event| event.operation == Operation::FinishStorage)
        );
        assert!(
            events
                .iter()
                .any(|event| event.operation == Operation::Unlink
                    && event.path.file_name().unwrap() == "DESCRIPTOR")
        );
        assert!(
            events
                .iter()
                .any(|event| event.operation == Operation::RemoveDirectory)
        );
        assert!(
            events
                .iter()
                .any(|event| event.operation == Operation::Sync)
        );
        assert_eq!(
            events
                .iter()
                .any(|event| event.operation == Operation::PublishActive),
            publish
        );
        fs::remove_dir_all(root).unwrap();
        for index in 0..events.len() {
            let (root, actual) = exercise("caller-each-boundary", publish, Some(index));
            assert_eq!(actual.len(), index + 1);
            assert_eq!(actual[index].operation, events[index].operation);
            assert_eq!(actual[index].completed, events[index].completed);
            fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn actual_successor_pointer_barrier_precedes_old_descriptor_and_candidate_removal() {
    let mut fixture = Fixture::new("successor-before-retire");
    let old = fixture.four.published.as_ref().unwrap().id;
    let old_path = fixture.four.directory(&old);
    let (id, _) = staged(&mut fixture);
    let observer = boundary::watch(None);
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    let events = observer.events();
    drop(observer);
    let at = |operation, path: &Path, completed| {
        events
            .iter()
            .position(|event| {
                event.operation == operation && event.path == path && event.completed == completed
            })
            .unwrap()
    };
    let published = at(Operation::PublishActive, &fixture.root.join("four"), true);
    let children = at(Operation::RemoveChildren, &old_path.join("cell"), false);
    let cell = at(Operation::FinishStorage, &old_path.join("cell"), true);
    let descriptor = at(Operation::Unlink, &old_path.join("DESCRIPTOR"), false);
    assert!(published < children);
    assert!(cell < descriptor);
    assert!(
        events[cell + 1..descriptor]
            .iter()
            .any(|event| event.operation == Operation::Sync
                && event.completed
                && event.path == old_path)
    );
    let removed = at(Operation::RemoveDirectory, &old_path, true);
    assert!(
        events[removed + 1..]
            .iter()
            .any(|event| event.operation == Operation::Sync
                && event.completed
                && event.path == fixture.root.join("four"))
    );
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn interrupted_replacement_retains_rejected_b_and_requires_explicit_cancel_of_cold_factory_c() {
    // Find the actual after-cell-create boundary without inventing disk shape.
    let mut baseline = Fixture::new("replacement-boundary-discovery");
    let (b, _) = staged(&mut baseline);
    baseline
        .four
        .finish_prefix(b, |_| Err(invalid_input("retained B")))
        .unwrap_err();
    commit(&mut baseline.source, LogIndex(3));
    let export = baseline
        .source
        .export_committed_prefix(LogIndex(3), limits())
        .unwrap();
    let c = export.descriptor().id().unwrap();
    let c_cell = baseline.four.directory(&c).join("cell");
    let watch = boundary::watch(None);
    baseline
        .four
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap();
    let events = watch.events();
    drop(watch);
    let stop = events
        .iter()
        .position(|event| {
            event.operation == Operation::CreateDirectory && event.completed && event.path == c_cell
        })
        .unwrap();
    let root = baseline.root.clone();
    drop(baseline);
    fs::remove_dir_all(root).unwrap();

    let mut fixture = Fixture::new("retained-B-replacement");
    let (b, _) = staged(&mut fixture);
    fixture
        .four
        .finish_prefix(b, |preview| {
            assert_semantics(preview)?;
            Err(invalid_input("retained B"))
        })
        .unwrap_err();
    let b_path = fixture.four.directory(&b);
    let b_bytes = image(&b_path);
    commit(&mut fixture.source, LogIndex(3));
    let export = fixture
        .source
        .export_committed_prefix(LogIndex(3), limits())
        .unwrap();
    let c = export.descriptor().id().unwrap();
    let watch = boundary::watch(Some(stop));
    let error = fixture
        .four
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap_err();
    assert_eq!(
        error.message,
        "injected bounded namespace boundary failure (test only)"
    );
    assert_eq!(watch.events().len(), stop + 1);
    drop(watch);
    assert_eq!(image(&b_path), b_bytes);
    let file = OpenOptions::new()
        .write(true)
        .open(b_path.join("cell/.vsr-writer.lock"))
        .unwrap();
    assert!(
        rsf_storage::ExclusiveFileLock::acquire(file).is_err(),
        "C factory preparation cannot release actual rejected B"
    );
    let Fixture {
        root,
        source,
        three,
        four,
    } = fixture;
    drop(four);
    let before = image(&root.join("four"));
    let mut owner = cold(&root.join("four"));
    assert!(
        owner
            .begin_prefix(export.descriptor().clone(), limits())
            .is_err(),
        "cold incomplete C is not resumable custody"
    );
    assert_eq!(image(&root.join("four")), before);
    drop(owner);
    let mut owner = cold(&root.join("four"));
    begin_active(&mut owner).unwrap();
    assert!(!owner.directory(&b).exists());
    assert!(!owner.directory(&c).exists());
    copy_voter(&source, &export, &mut owner);
    owner
        .finish_prefix(c, |preview| {
            assert_eq!(preview.committed_tail(), LogIndex(3));
            preview.validate_live()
        })
        .unwrap();
    assert_eq!(
        owner
            .published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    drop(owner);
    drop(source);
    drop(three);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_ordinary_api_does_not_claim_cold_detection_of_whole_unrecorded_stage_loss() {
    let mut fixture = Fixture::new("ordinary-selection-boundary");
    let (id, export) = staged(&mut fixture);
    let directory = fixture.four.directory(&id);
    let Fixture {
        root,
        source,
        three,
        four,
    } = fixture;
    drop(four);
    fs::remove_dir_all(&directory).unwrap();
    let mut owner = cold(&root.join("four"));
    // No durable selection or explicit Resume expectation exists in this API.
    // This is a documented missing release prerequisite, not a protection claim.
    assert_eq!(
        owner
            .begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        0
    );
    copy_voter(&source, &export, &mut owner);
    owner.finish_prefix(id, assert_semantics).unwrap();
    drop(owner);
    drop(source);
    drop(three);
    fs::remove_dir_all(root).unwrap();
}
