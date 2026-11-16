mod finish;

use super::*;

fn path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-learner-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn source(root: &Path) -> DurableVsrCell {
    DurableVsrCell::open(
        root,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}
fn learner(root: &Path) -> LearnerHistoryStore {
    LearnerHistoryStore::open(root, PlacementGroupId(1), NodeId(9), NodeId(0)).unwrap()
}
fn commit(cell: &mut DurableVsrCell, operation: u64) {
    let index = cell.next_index();
    cell.prepare(
        NodeId(0),
        VsrOperation::ReplicatedCommand {
            operation_id: OperationId(operation),
            payload: b"data".to_vec(),
        },
        4,
    )
    .unwrap();
    cell.acknowledge(NodeId(1), cell.view(), index);
    cell.commit(NodeId(0), index).unwrap();
    cell.remember_client_request(VsrClientRequestId::new("client", index.0), index)
        .unwrap();
}
fn stage(learner: &mut LearnerHistoryStore, snapshot: &VsrHistorySnapshot) -> [u8; 32] {
    let count = learner.begin(snapshot.descriptor.clone()).unwrap();
    let id = snapshot.descriptor.id().unwrap();
    for (ordinal, record) in snapshot.records.iter().enumerate().skip(count as usize) {
        assert_eq!(
            learner.append(id, ordinal as u64, record.clone()).unwrap(),
            ordinal as u64 + 1
        );
    }
    id
}
#[test]
fn history_transfer_resumes_after_restart_preserves_old_and_new_certificates_and_receipts() {
    let root = path("resume");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    assert!(matches!(
        source.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(1)]),
        VsrDecision::MembershipChanged { .. }
    ));
    commit(&mut source, 2);
    let snapshot = source.history_snapshot().unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let target = root.join("target");
    let mut learner = learner(&target);
    assert_eq!(learner.begin(snapshot.descriptor.clone()).unwrap(), 0);
    learner.append(id, 0, snapshot.records[0].clone()).unwrap();
    learner.append(id, 1, snapshot.records[1].clone()).unwrap();
    assert_eq!(
        learner.append(id, 1, snapshot.records[1].clone()).unwrap(),
        2
    );
    assert!(learner.finish(id).is_err());
    assert!(learner.committed_entries().unwrap().is_empty());
    drop(learner);
    let mut learner = super::tests::learner(&target);
    assert_eq!(learner.begin(snapshot.descriptor.clone()).unwrap(), 2);
    stage(&mut learner, &snapshot);
    assert_eq!(learner.finish(id).unwrap(), LogIndex(2));
    assert_eq!(learner.finish(id).unwrap(), LogIndex(2));
    assert_eq!(
        learner.begin(snapshot.descriptor.clone()).unwrap(),
        snapshot.descriptor.records
    );
    assert_eq!(
        learner.committed_entries().unwrap(),
        source.committed_prefix().unwrap()
    );
    assert_eq!(
        learner
            .published
            .as_ref()
            .unwrap()
            .cell
            .request_commit(&VsrClientRequestId::new("client", 2)),
        Some(LogIndex(2))
    );
    drop(learner);
    let learner = super::tests::learner(&target);
    assert_eq!(
        learner.committed_entries().unwrap(),
        source.committed_prefix().unwrap()
    );
}
#[test]
fn rejects_foreign_source_voting_role_gaps_conflicts_and_corruption_before_publication() {
    let root = path("reject");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let snapshot = source.history_snapshot().unwrap();
    let mut learner = learner(&root.join("learner"));
    let mut wrong = snapshot.descriptor.clone();
    wrong.probe.primary = NodeId(1);
    assert!(learner.begin(wrong).is_err());
    let mut wrong = snapshot.descriptor.clone();
    wrong.probe.voters.push(NodeId(9));
    assert!(learner.begin(wrong).is_err());
    learner.begin(snapshot.descriptor.clone()).unwrap();
    let id = snapshot.descriptor.id().unwrap();
    assert!(learner.append(id, 1, snapshot.records[1].clone()).is_err());
    learner.append(id, 0, snapshot.records[0].clone()).unwrap();
    let mut wrong = snapshot.records[0].clone();
    wrong.epoch = Epoch(77);
    assert!(learner.append(id, 0, wrong).is_err());
    let mut corrupted = source.history_snapshot().unwrap();
    corrupted.descriptor.digest = Digest::blake3_bytes(b"wrong");
    let id = stage(&mut learner, &corrupted);
    assert!(learner.finish(id).is_err());
    assert!(learner.committed_entries().unwrap().is_empty());
    assert!(!learner.root.join("ACTIVE").exists());
}
#[test]
fn valid_digest_cannot_authorize_invalid_certificate_and_prior_history_survives() {
    let root = path("certificates");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let snapshot = source.history_snapshot().unwrap();
    let mut learner = learner(&root.join("learner"));
    let id = stage(&mut learner, &snapshot);
    learner.finish(id).unwrap();
    commit(&mut source, 2);
    let mut corrupt = source.history_snapshot().unwrap();
    let record = corrupt
        .records
        .iter_mut()
        .rev()
        .find(|record| record.kind == WalRecordKind::Commit)
        .unwrap();
    let mut certificate = VsrCommit::decode(&record.payload).unwrap();
    certificate.acknowledgements = 1;
    certificate.certificate.signers = [NodeId(0)].into_iter().collect();
    record.payload = certificate.encode().unwrap();
    corrupt.descriptor.digest = transfer::history_digest(&corrupt.records).unwrap();
    let id = stage(&mut learner, &corrupt);
    assert!(learner.finish(id).is_err());
    assert_eq!(learner.committed_entries().unwrap().len(), 1);
    drop(learner);
    assert_eq!(
        super::tests::learner(&root.join("learner"))
            .committed_entries()
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn published_commits_and_receipts_cannot_be_replaced_or_regressed() {
    let root = path("extension");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let first = source.history_snapshot().unwrap();
    let mut learner = learner(&root.join("learner"));
    let id = stage(&mut learner, &first);
    learner.finish(id).unwrap();
    let mut missing_receipts = source.history_snapshot().unwrap();
    missing_receipts
        .records
        .retain(|record| !record.payload.starts_with(VSR_CLIENT_REQUEST_MAGIC));
    missing_receipts.descriptor.records = missing_receipts.records.len() as u64;
    missing_receipts.descriptor.digest =
        transfer::history_digest(&missing_receipts.records).unwrap();
    let id = stage(&mut learner, &missing_receipts);
    assert!(learner.finish(id).is_err());
    commit(&mut source, 2);
    let next = source.history_snapshot().unwrap();
    let id = stage(&mut learner, &next);
    learner.finish(id).unwrap();
    assert!(learner.begin(first.descriptor).is_err());
    assert_eq!(learner.committed_entries().unwrap().len(), 2);
    drop(learner);
    assert_eq!(
        super::tests::learner(&root.join("learner"))
            .committed_entries()
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn exclusive_ownership_identity_and_active_descriptor_corruption_fail_closed() {
    let root = path("ownership");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let snapshot = source.history_snapshot().unwrap();
    let target = root.join("learner");
    let mut learner = learner(&target);
    assert!(LearnerHistoryStore::open(&target, PlacementGroupId(1), NodeId(9), NodeId(0)).is_err());
    let id = stage(&mut learner, &snapshot);
    learner.finish(id).unwrap();
    let directory = learner.directory(&id);
    drop(learner);
    assert!(LearnerHistoryStore::open(&target, PlacementGroupId(2), NodeId(9), NodeId(0)).is_err());
    fs::write(directory.join("DESCRIPTOR"), b"broken").unwrap();
    assert!(LearnerHistoryStore::open(&target, PlacementGroupId(1), NodeId(9), NodeId(0)).is_err());
}
#[test]
fn history_codecs_reject_truncation_trailing_bytes_and_unknown_kinds() {
    let root = path("codec");
    let mut source = source(&root);
    commit(&mut source, 1);
    let snapshot = source.history_snapshot().unwrap();
    let encoded = snapshot.descriptor.encode().unwrap();
    assert_eq!(
        VsrHistoryDescriptor::decode(&encoded).unwrap(),
        snapshot.descriptor
    );
    for length in 0..encoded.len() {
        assert!(VsrHistoryDescriptor::decode(&encoded[..length]).is_err());
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(VsrHistoryDescriptor::decode(&trailing).is_err());
    for record in snapshot.records {
        let mut encoded = record.encode().unwrap();
        assert_eq!(VsrHistoryRecord::decode(&encoded).unwrap(), record);
        encoded[5] = 255;
        assert!(VsrHistoryRecord::decode(&encoded).is_err());
    }
}

#[test]
fn existing_source_requires_persisted_authority_and_preserves_ownership() {
    let root = path("existing-source");
    let directory = root.join("source");
    assert!(DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).is_err());
    assert!(!directory.exists());
    let mut source = source(&directory);
    commit(&mut source, 1);
    let expected = source.committed_prefix().unwrap();
    assert!(DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).is_err());
    drop(source);
    assert!(DurableVsrCell::open_existing(&directory, PlacementGroupId(2)).is_err());
    let source = DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).unwrap();
    assert_eq!(source.committed_prefix().unwrap(), expected);
    drop(source);
    fs::remove_file(directory.join("MANIFEST")).unwrap();
    assert!(DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).is_err());
    assert!(!directory.join("MANIFEST").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn replacement_cannot_rewrite_a_previously_published_configuration() {
    let root = path("configuration-conflict");
    let mut original = source(&root.join("original"));
    let mut conflicting = source(&root.join("conflicting"));
    commit(&mut original, 1);
    commit(&mut conflicting, 1);
    assert!(matches!(
        original.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(1)]),
        VsrDecision::MembershipChanged { .. }
    ));
    assert!(matches!(
        conflicting.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(2)]),
        VsrDecision::MembershipChanged { .. }
    ));
    let mut learner = learner(&root.join("learner"));
    let snapshot = original.history_snapshot().unwrap();
    let id = stage(&mut learner, &snapshot);
    learner.finish(id).unwrap();
    let conflict = conflicting.history_snapshot().unwrap();
    let id = stage(&mut learner, &conflict);
    assert!(
        learner
            .finish(id)
            .unwrap_err()
            .message
            .contains("configurations")
    );
    assert_eq!(
        learner.published.as_ref().unwrap().cell.replicas(),
        [NodeId(0), NodeId(1)]
    );
}

#[test]
fn torn_staged_record_resumes_from_durable_boundary_and_superseded_snapshots_are_reclaimed() {
    let root = path("torn-stage");
    let mut source = source(&root.join("source"));
    commit(&mut source, 1);
    let snapshot = source.history_snapshot().unwrap();
    let target = root.join("learner");
    let mut learner = learner(&target);
    let id = stage(&mut learner, &snapshot);
    let segment = learner
        .stage
        .as_ref()
        .unwrap()
        .store
        .root()
        .join("segments")
        .join(
            &learner
                .stage
                .as_ref()
                .unwrap()
                .store
                .active_segment()
                .file_name,
        );
    let length = fs::metadata(&segment).unwrap().len();
    drop(learner);
    OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_len(length - 5)
        .unwrap();
    let mut learner = super::tests::learner(&target);
    assert_eq!(
        learner.begin(snapshot.descriptor.clone()).unwrap(),
        snapshot.descriptor.records - 1
    );
    assert!(!target.join("ACTIVE").exists());
    stage(&mut learner, &snapshot);
    learner.finish(id).unwrap();
    let original_directory = learner.directory(&id);
    commit(&mut source, 2);
    let next = source.history_snapshot().unwrap();
    let id = stage(&mut learner, &next);
    assert!(original_directory.is_dir());
    learner.finish(id).unwrap();
    assert!(!original_directory.exists());
    let active = learner.directory(&id);
    assert!(active.is_dir());
    commit(&mut source, 3);
    let abandoned = source.history_snapshot().unwrap();
    learner.begin(abandoned.descriptor.clone()).unwrap();
    let abandoned_directory = learner.directory(&abandoned.descriptor.id().unwrap());
    drop(learner);
    let mut learner = super::tests::learner(&target);
    commit(&mut source, 4);
    let latest = source.history_snapshot().unwrap();
    learner.begin(latest.descriptor).unwrap();
    assert!(!abandoned_directory.exists());
    assert!(active.exists());
}

#[path = "replay_tests.rs"]
mod published_replay;

mod progress;
