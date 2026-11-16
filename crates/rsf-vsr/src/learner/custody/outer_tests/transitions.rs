//! Additional physical paths and actual installed-history transfer, using old APIs.
use super::*;

fn copy_tree(source: &Path, target: &Path) {
    fs::create_dir(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let next = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &next);
        } else {
            fs::copy(entry.path(), next).unwrap();
        }
    }
}
fn directory_replacement(root: bool) {
    let mut f = Fixture::new();
    let path = if root {
        f.owner().root.clone()
    } else {
        f.selected()
    };
    let saved = f.root.join("original");
    let before = inventory(&path);
    fs::rename(&path, &saved).unwrap();
    copy_tree(&saved, &path);
    let copied = inventory(&path);
    assert_eq!(
        copied.keys().collect::<Vec<_>>(),
        before.keys().collect::<Vec<_>>()
    );
    for (name, original) in &before {
        assert_eq!(
            (copied[name].directory, &copied[name].bytes),
            (original.directory, &original.bytes)
        );
    }
    let error = f.owner().published_state_token().unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        if root {
            "learner owner directory was replaced"
        } else {
            "learner candidate directory was replaced"
        }
    );
    fs::remove_dir_all(&path).unwrap();
    fs::rename(&saved, &path).unwrap();
    fenced(f.owner());
    f.reopen(false);
    f.owner().published_state_token().unwrap();
}
#[test]
fn ordinary_original_root_identity_survives_equal_byte_tree_substitution() {
    directory_replacement(true);
}
#[test]
fn ordinary_original_candidate_identity_survives_equal_byte_tree_substitution() {
    directory_replacement(false);
}
#[test]
fn existing_export_batch_observation_cannot_bypass_lost_outer_exclusion() {
    let mut f = Fixture::new();
    let export = f
        .owner()
        .export_committed_prefix(LogIndex(1), limits())
        .unwrap();
    let path = f.owner().root.join(".learner.lock");
    let saved = f.root.join("held");
    fs::rename(&path, &saved).unwrap();
    let cause = fs::symlink_metadata(&path).unwrap_err();
    assert_eq!(
        f.owner()
            .read_committed_prefix_batch(&export, 0)
            .unwrap_err()
            .message,
        format!("learner history storage: {cause}")
    );
    fs::rename(&saved, &path).unwrap();
    fenced(f.owner());
    f.reopen(false);
    f.owner()
        .export_committed_prefix(LogIndex(1), limits())
        .unwrap();
}
#[test]
fn ordinary_cold_published_missing_outer_lock_is_not_recreated() {
    let mut f = Fixture::new();
    drop(f.learner.take());
    let path = f.root.join("target/.learner.lock");
    let saved = f.root.join("held");
    fs::rename(&path, &saved).unwrap();
    let before = inventory(&f.root.join("target"));
    let error = LearnerHistoryStore::open(
        f.root.join("target"),
        PlacementGroupId(1),
        NodeId(3),
        NodeId(0),
    )
    .unwrap_err();
    assert!(
        !path.exists(),
        "published cold open cannot recreate its lost outer lock"
    );
    assert_eq!(inventory(&f.root.join("target")), before);
    assert_eq!(
        error.message,
        format!(
            "learner history storage: {}",
            fs::symlink_metadata(&path).unwrap_err()
        )
    );
    fs::rename(saved, path).unwrap();
    f.reopen(false);
    f.owner().published_state_token().unwrap();
}
fn installed(root: &Path) -> DurableVsrCell {
    let open = |name| {
        DurableVsrCell::open(
            root.join(name),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap()
    };
    let mut first = open("installed-source");
    let mut second = open("installation-peer");
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(0),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    };
    let reports = [
        first
            .begin_view_change(NodeId(0), NodeId(0), request.clone())
            .unwrap(),
        second
            .begin_view_change(NodeId(1), NodeId(0), request)
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&reports).unwrap();
    first.install_view(&plan).unwrap();
    assert_eq!(first.view(), Epoch(2));
    assert!(first.root.join("VSR_ACTIVE").is_file());
    first
}
#[test]
fn ordinary_transfer_preserves_actual_view_installation_replay_in_its_root_store() {
    let mut f = Fixture::new();
    let mut source = installed(&f.root);
    let snapshot = source.history_snapshot().unwrap();
    assert!(
        snapshot
            .records
            .iter()
            .any(|r| r.kind == WalRecordKind::ViewInstallation)
    );
    let target = f.root.join("installed-transfer");
    let mut owner =
        LearnerHistoryStore::open(&target, PlacementGroupId(1), NodeId(3), NodeId(0)).unwrap();
    transfer(&mut source, &mut owner);
    assert_eq!(
        owner
            .published_replay()
            .unwrap()
            .membership_lineage()
            .unwrap(),
        source.membership_lineage().unwrap()
    );
    let id = owner.published_history_id().unwrap().unwrap();
    assert!(!owner.directory(&id).join("cell/VSR_ACTIVE").exists());
    drop(owner);
    let cold =
        LearnerHistoryStore::open(&target, PlacementGroupId(1), NodeId(3), NodeId(0)).unwrap();
    cold.published_state_token().unwrap();
    assert_eq!(
        cold.published_replay()
            .unwrap()
            .membership_lineage()
            .unwrap(),
        source.membership_lineage().unwrap()
    );
    drop(cold);
    // Keep the ordinary fixture's own publication valid as a separate owner.
    f.reopen(false);
}
#[test]
fn ordinary_finish_refuses_authentic_generation_substitution_for_its_retained_store() {
    let f = Fixture::new();
    let mut source = installed(&f.root);
    let snapshot = source.history_snapshot().unwrap();
    let target = f.root.join("misdirected");
    let mut owner =
        LearnerHistoryStore::open(&target, PlacementGroupId(1), NodeId(3), NodeId(0)).unwrap();
    let id = snapshot.descriptor.id().unwrap();
    owner.begin(snapshot.descriptor).unwrap();
    for (i, record) in snapshot.records.into_iter().enumerate() {
        owner.append(id, i as u64, record).unwrap();
    }
    let cell = owner.directory(&id).join("cell");
    copy_tree(&source.root.join("vsr-views"), &cell.join("vsr-views"));
    for name in ["VSR_ACTIVE", "VSR_PUBLISHED"] {
        fs::copy(source.root.join(name), cell.join(name)).unwrap();
    }
    // This is a real certified installation, not a malformed pointer fixture.
    let active = installation::publication::active(&cell, PlacementGroupId(1)).unwrap();
    assert_ne!(active.path, cell);
    assert!(active.plan.is_some());
    let before = inventory(&target);
    let error = owner.finish(id).unwrap_err();
    assert_eq!(
        error.message,
        "learner active generation differs from retained transfer store"
    );
    assert!(!target.join("ACTIVE").exists());
    assert_eq!(owner.published_history_id().unwrap_err().message, FENCED);
    // The only possible effect is the explicitly owned zero inner writer lock,
    // acquired before resolving the real installation. All history is identical.
    let mut after = inventory(&target);
    let lock = owner
        .directory(&id)
        .join("cell/.vsr-writer.lock")
        .strip_prefix(&target)
        .unwrap()
        .to_path_buf();
    let created = after
        .remove(&lock)
        .expect("actual owned writer lock creation");
    assert!(!created.directory);
    assert!(created.bytes.is_empty());
    assert_eq!(after, before);
}

fn complete_stage(f: &mut Fixture) -> [u8; 32] {
    commit(f.source.as_mut().unwrap(), 2);
    let snapshot = f.source.as_mut().unwrap().history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let owner = f.learner.as_mut().unwrap();
    assert_eq!(owner.begin(snapshot.descriptor.clone()).unwrap(), 0);
    for (i, record) in snapshot.records.into_iter().enumerate() {
        assert_eq!(owner.append(id, i as u64, record).unwrap(), i as u64 + 1);
    }
    assert_eq!(
        owner.stage.as_ref().unwrap().count,
        snapshot.descriptor.records
    );
    id
}
#[test]
fn ordinary_complete_finish_torn_tail_never_repairs_acknowledged_bytes_or_clears_fence() {
    let mut f = Fixture::new();
    let id = complete_stage(&mut f);
    let owner = f.learner.as_mut().unwrap();
    let active_before = fs::read(owner.root.join("ACTIVE")).unwrap();
    let segments = owner.directory(&id).join("cell/segments");
    let path = fs::read_dir(&segments)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "segment"))
        .unwrap();
    let original = fs::metadata(&path).unwrap();
    let length = original.len();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"torn").unwrap();
    file.sync_all().unwrap();
    let before = inventory(&owner.root);
    let error = owner.finish(id).unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "owned segment changed after validated WAL state"
    );
    assert_eq!(inventory(&owner.root), before);
    assert!(fs::read(&path).unwrap().ends_with(b"torn"));
    file.set_len(length).unwrap();
    file.sync_all().unwrap();
    drop(file);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let restored = fs::metadata(&path).unwrap();
        assert_eq!(
            (restored.dev(), restored.ino()),
            (original.dev(), original.ino())
        );
    }
    assert_eq!(fs::read(owner.root.join("ACTIVE")).unwrap(), active_before);
    fenced(owner);
    f.reopen(false);
    assert_eq!(f.owner().committed_entries().unwrap().len(), 1);
}
#[test]
fn ordinary_complete_finish_keeps_original_descriptor_through_same_byte_replacement() {
    let mut f = Fixture::new();
    let id = complete_stage(&mut f);
    let owner = f.learner.as_mut().unwrap();
    let path = owner.directory(&id).join("DESCRIPTOR");
    let saved = f.root.join("original-descriptor");
    let bytes = fs::read(&path).unwrap();
    let before = inventory(&owner.root);
    fs::rename(&path, &saved).unwrap();
    fs::write(&path, &bytes).unwrap();
    let error = owner.finish(id).unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(error.message, "learner publication metadata was replaced");
    fs::remove_file(&path).unwrap();
    fs::rename(saved, path).unwrap();
    assert_eq!(inventory(&owner.root), before);
    fenced(owner);
    f.reopen(false);
    assert_eq!(f.owner().committed_entries().unwrap().len(), 1);
}
