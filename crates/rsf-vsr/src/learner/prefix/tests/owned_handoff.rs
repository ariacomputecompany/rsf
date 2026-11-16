//! Real bounded owners; fault hooks run only after the preceding healthy check.
use super::super::observation::{self, Boundary};
use super::*;
use std::io::Write;

fn active_file(cell: &Path) -> PathBuf {
    active_segment_lock(cell).with_extension("segment")
}
fn damage_tail(cell: &Path) -> (PathBuf, Vec<u8>) {
    let path = active_file(cell);
    let mut bytes = fs::read(&path).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[0xa7])
        .unwrap();
    bytes.push(0xa7);
    (path, bytes)
}
fn assert_latched(owner: &LearnerHistoryStore) {
    assert!(owner.failed.load(Ordering::Relaxed));
    assert!(
        owner
            .published_history_id()
            .unwrap_err()
            .message
            .contains("learner durability failed")
    );
}

#[test]
fn bounded_append_rejects_mid_call_torn_tail_without_repair_or_ack() {
    let mut fixture = Fixture::new("owned-append-tail");
    let export = fixture.advance();
    let id = export.descriptor().id().unwrap();
    fixture
        .four
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap();
    let first = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap();
    fixture
        .four
        .append_prefix_batch(
            id,
            &VsrHistoryBatch {
                ordinal: 0,
                records: first.records[..1].to_vec(),
            },
        )
        .unwrap();
    let path = active_file(&fixture.four.directory(&id).join("cell"));
    let good = fs::read(&path).unwrap();
    let mut damaged = good.clone();
    damaged.push(0xa7);
    let previous = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let watch = observation::fault(Boundary::BeforeAppend, |cell| {
        damage_tail(cell);
    });
    let outcome = fixture.four.append_prefix_batch(
        id,
        &VsrHistoryBatch {
            ordinal: 1,
            records: first.records[1..].to_vec(),
        },
    );
    assert_eq!(watch.calls(), 1);
    assert!(
        outcome.is_err(),
        "changed physical suffix must not be repaired into a successful ACK"
    );
    assert_eq!(
        fs::read(&path).unwrap(),
        damaged,
        "no truncation or appended records after fault"
    );
    assert_eq!(
        fs::read(fixture.root.join("four/ACTIVE")).unwrap(),
        previous
    );
    fs::write(&path, good).unwrap();
    assert_latched(&fixture.four);
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_cold_begin_rejects_tail_loss_between_closed_and_active_renewal_without_repair() {
    let mut fixture = Fixture::new("owned-renew-tail");
    let export = fixture.advance();
    let id = export.descriptor().id().unwrap();
    fixture
        .four
        .begin_prefix(export.descriptor().clone(), limits())
        .unwrap();
    let first = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap();
    fixture
        .four
        .append_prefix_batch(
            id,
            &VsrHistoryBatch {
                ordinal: 0,
                records: first.records[..1].to_vec(),
            },
        )
        .unwrap();
    let path = active_file(&fixture.four.directory(&id).join("cell"));
    let good = fs::read(&path).unwrap();
    let mut damaged = good.clone();
    damaged.push(0xa7);
    let root = fixture.root.clone();
    drop(fixture);
    let mut owner = LearnerHistoryStore::open_published_existing(
        root.join("four"),
        PlacementGroupId(1),
        NodeId(4),
        NodeId(0),
        limits(),
    )
    .unwrap();
    let previous = fs::read(root.join("four/ACTIVE")).unwrap();
    let watch = observation::fault(Boundary::AfterClosedRenewal, |cell| {
        damage_tail(cell);
    });
    let outcome = owner.begin_prefix(export.descriptor().clone(), limits());
    assert_eq!(watch.calls(), 1);
    assert!(
        outcome.is_err(),
        "cold renewal may not repair a newly damaged suffix"
    );
    assert_eq!(fs::read(&path).unwrap(), damaged);
    assert_eq!(fs::read(root.join("four/ACTIVE")).unwrap(), previous);
    fs::write(&path, good).unwrap();
    assert_latched(&owner);
    drop(owner);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_handoff_keeps_original_closed_inode_across_same_store_move() {
    let mut fixture = Fixture::new("owned-handoff-closed");
    let export = fixture.advance();
    let descriptor = export.descriptor().clone();
    let id = descriptor.id().unwrap();
    let batch = fixture
        .source
        .read_committed_prefix_batch(&export, 0)
        .unwrap();
    assert_eq!(batch.records.len() as u64, descriptor.records);
    // A valid preexisting, non-ACTIVE transfer can span small real segments.
    // Create it through the normal storage writer, not a manufactured cursor.
    let directory = fixture.four.directory(&id);
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("DESCRIPTOR"), descriptor.encode().unwrap()).unwrap();
    let options = SegmentStoreOptions {
        max_segment_bytes: 1,
        log_options: rsf_storage::SegmentLogOptions {
            sync_policy: rsf_storage::SyncPolicy::Full,
        },
    };
    let mut store = SegmentStore::open_with_options(directory.join("cell"), options).unwrap();
    for record in &batch.records {
        store
            .append_next_wal(
                PlacementGroupId(1),
                record.epoch,
                record.kind,
                &record.payload,
            )
            .unwrap();
    }
    drop(store);
    assert_eq!(
        fixture.four.begin_prefix(descriptor, limits()).unwrap(),
        batch.records.len() as u64
    );
    let segments = directory.join("cell/segments");
    let mut names = fs::read_dir(&segments)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "segment"))
        .collect::<Vec<_>>();
    names.sort();
    assert!(names.len() > 1);
    let closed = names[0].clone();
    let saved = directory.join("original-closed");
    let for_fault = closed.clone();
    let original = saved.clone();
    let old_active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let watch = observation::fault(Boundary::BeforeHandoff, move |_| {
        let modified = fs::metadata(&for_fault).unwrap().modified().unwrap();
        fs::rename(&for_fault, &original).unwrap();
        fs::write(&for_fault, fs::read(&original).unwrap()).unwrap();
        File::options()
            .write(true)
            .open(&for_fault)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
    });
    let mut validated = false;
    let outcome = fixture.four.finish_prefix(id, |_| {
        validated = true;
        Ok(())
    });
    assert_eq!(watch.calls(), 1);
    assert!(
        outcome.is_err(),
        "new capture cannot replace the original closed-file identity"
    );
    assert!(
        !validated,
        "physical handoff must fail before semantic callback"
    );
    assert_eq!(
        fs::read(fixture.root.join("four/ACTIVE")).unwrap(),
        old_active
    );
    fs::remove_file(&closed).unwrap();
    fs::rename(saved, closed).unwrap();
    assert_latched(&fixture.four);
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}

struct Installed {
    root: PathBuf,
    owner: LearnerHistoryStore,
    snapshot: VsrHistorySnapshot,
    pointer: Vec<u8>,
    generation: String,
    seal: Vec<u8>,
}
fn installed(label: &str) -> Installed {
    let root = root(label);
    let mut source = open(&root.join("source"));
    let mut follower = open(&root.join("follower"));
    let accepted = prepare(&mut source, 1, b"real installed committed history");
    follower
        .receive_prepare(NodeId(1), NodeId(0), accepted.clone())
        .unwrap();
    source.acknowledge(NodeId(1), Epoch(1), accepted.index);
    let VsrDecision::Committed(certificate) = source.commit(NodeId(0), accepted.index).unwrap()
    else {
        panic!("real commit")
    };
    follower
        .receive_commit(NodeId(1), NodeId(0), certificate)
        .unwrap();
    let prior = source.history_snapshot().unwrap();
    let owner = published(&root.join("learner"), 4, &prior);
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(0),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let reports = [
        source
            .begin_view_change(NodeId(0), NodeId(0), request.clone())
            .unwrap(),
        follower
            .begin_view_change(NodeId(1), NodeId(0), request)
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&reports).unwrap();
    source.install_view(&plan).unwrap();
    let snapshot = source.history_snapshot().unwrap();
    assert_eq!(snapshot.descriptor.probe.view, Epoch(2));
    let active = installation::publication::active(&source.root, PlacementGroupId(1)).unwrap();
    let raw = SegmentStore::recover_existing(&active.path).unwrap();
    installation::publication::validate_active(
        &active,
        PlacementGroupId(1),
        &raw.recovery.wal_records.iter().collect::<Vec<_>>(),
    )
    .unwrap();
    let generation = active
        .path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let seal = fs::read(active.path.join("VSR_SEAL")).unwrap();
    let pointer = fs::read(source.root.join("VSR_ACTIVE")).unwrap();
    drop(source);
    drop(follower);
    Installed {
        root,
        owner,
        snapshot,
        pointer,
        generation,
        seal,
    }
}
fn retain_snapshot(owner: &mut LearnerHistoryStore, snapshot: &VsrHistorySnapshot) {
    let id = snapshot.descriptor.id().unwrap();
    owner
        .begin_prefix(snapshot.descriptor.clone(), limits())
        .unwrap();
    owner
        .append_prefix_batch(
            id,
            &VsrHistoryBatch {
                ordinal: 0,
                records: snapshot.records.clone(),
            },
        )
        .unwrap();
}

#[cfg(unix)]
fn late_generation_marker_alias(cold: bool) {
    let mut fixture = installed("owned-flat-marker");
    let id = fixture.snapshot.descriptor.id().unwrap();
    retain_snapshot(&mut fixture.owner, &fixture.snapshot);
    // The exact actual installed history must pass bounded replay first.
    if cold {
        fixture
            .owner
            .finish_prefix(id, |preview| preview.validate_live())
            .unwrap();
    } else {
        let mut control = published(&fixture.root.join("positive"), 5, &fixture.snapshot);
        control
            .begin_prefix(fixture.snapshot.descriptor.clone(), limits())
            .unwrap();
        control
            .finish_prefix(id, |preview| preview.validate_live())
            .unwrap();
        drop(control);
    }
    let previous = fs::read(fixture.root.join("learner/ACTIVE")).unwrap();
    let cell = fixture.owner.directory(&id).join("cell");
    let victim = fixture.root.join("unrelated-file");
    fs::write(&victim, b"foreign bytes must survive").unwrap();
    let marker_target = victim.clone();
    let pointer = fixture.pointer;
    let generation = fixture.generation;
    let seal = fixture.seal;
    let watch = observation::fault(Boundary::BeforeHandoff, move |root| {
        let directory = root.join("vsr-views").join(&generation);
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("VSR_SEAL"), &seal).unwrap();
        fs::write(root.join("VSR_ACTIVE"), &pointer).unwrap();
        std::os::unix::fs::symlink(&marker_target, root.join(".VSR_PUBLISHED.tmp")).unwrap();
    });
    if cold {
        drop(fixture.owner);
        let opened = LearnerHistoryStore::open_published_existing(
            fixture.root.join("learner"),
            PlacementGroupId(1),
            NodeId(4),
            NodeId(0),
            limits(),
        );
        assert!(opened.is_err());
    } else {
        let result = fixture.owner.finish_prefix(id, |_| {
            panic!("late activation must fail before semantic hook")
        });
        assert!(result.is_err());
        assert_latched(&fixture.owner);
        drop(fixture.owner);
    }
    assert_eq!(watch.calls(), 1);
    assert_eq!(fs::read(&victim).unwrap(), b"foreign bytes must survive");
    assert!(
        !cell.join("VSR_PUBLISHED").exists(),
        "no generic marker completion"
    );
    assert!(
        fs::symlink_metadata(cell.join(".VSR_PUBLISHED.tmp"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read(fixture.root.join("learner/ACTIVE")).unwrap(),
        previous
    );
    fs::remove_dir_all(fixture.root).unwrap();
}

#[test]
#[cfg(unix)]
fn bounded_stage_finish_never_resolves_late_real_generation_or_truncates_marker_alias() {
    late_generation_marker_alias(false);
}
#[test]
#[cfg(unix)]
fn bounded_cold_published_open_never_resolves_late_real_generation_or_truncates_marker_alias() {
    late_generation_marker_alias(true);
}

#[test]
fn bounded_published_flat_absence_remains_live_and_observed_loss_is_sticky() {
    let mut fixture = Fixture::new("owned-flat-lifetime");
    let export = fixture.advance();
    copy_voter(&fixture.source, &export, &mut fixture.four);
    let id = export.descriptor().id().unwrap();
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    let cell = fixture.four.directory(&id).join("cell");
    let token = fixture.four.published_state_token().unwrap();
    fixture.four.finish_prefix(id, assert_semantics).unwrap();
    assert_eq!(fixture.four.published_state_token().unwrap(), token);
    fs::create_dir(cell.join("vsr-views")).unwrap();
    assert!(fixture.four.published_history_id().is_err());
    fs::remove_dir(cell.join("vsr-views")).unwrap();
    assert_latched(&fixture.four);
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}

fn cached_raw_publication_callback_fault(semantic_error: bool) {
    let mut fixture = Fixture::new("raw-current-callback");
    // Fixture publication used the original generic begin/append/finish path.
    // Enter bounded mode using that exact current descriptor, with no new stage.
    let descriptor = fixture.four.published.as_ref().unwrap().descriptor.clone();
    let id = descriptor.id().unwrap();
    let previous = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    assert_eq!(
        fixture
            .four
            .begin_prefix(descriptor.clone(), limits())
            .unwrap(),
        descriptor.records
    );
    // Pure validator rejection preserves healthy retries in bounded mode.
    let healthy = fixture
        .four
        .finish_prefix(id, |_| Err(invalid_input("healthy semantic refusal")))
        .unwrap_err();
    assert_eq!(healthy.message, "healthy semantic refusal");
    assert!(!fixture.four.failed.load(Ordering::Relaxed));
    fixture
        .four
        .finish_prefix(id, |preview| preview.validate_live())
        .unwrap();
    let cell = fixture.four.directory(&id).join("cell");
    let changed = cell.clone();
    let mut calls = 0;
    let result = fixture.four.finish_prefix(id, |_| {
        calls += 1;
        fs::create_dir(changed.join("vsr-views")).unwrap();
        if semantic_error {
            Err(invalid_input("callback semantic refusal"))
        } else {
            Ok(())
        }
    });
    assert_eq!(calls, 1);
    let error =
        result.expect_err("same-id admission must retain original flat guard after callback");
    assert!(
        error.message.contains("flat unpromoted history"),
        "physical guard must dominate semantic outcome: {error:?}"
    );
    assert_ne!(error.message, "callback semantic refusal");
    assert_eq!(
        fs::read(fixture.root.join("four/ACTIVE")).unwrap(),
        previous
    );
    fs::remove_dir(cell.join("vsr-views")).unwrap();
    assert_latched(&fixture.four);
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn same_id_bounded_begin_binds_raw_publication_flat_guard_after_successful_callback() {
    cached_raw_publication_callback_fault(false);
}
#[test]
fn same_id_bounded_begin_binds_raw_publication_flat_guard_after_semantic_error() {
    cached_raw_publication_callback_fault(true);
}
