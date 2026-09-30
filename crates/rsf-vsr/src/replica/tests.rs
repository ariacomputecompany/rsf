use super::*;
use std::path::{Path, PathBuf};

fn path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-replica-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn open(path: &Path) -> DurableVsrCell {
    DurableVsrCell::open(
        path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}

fn operation() -> VsrOperation {
    VsrOperation::ReplicatedCommand {
        operation_id: OperationId(91),
        payload: b"namespace=app\0SET key\0complete binary value\xff".to_vec(),
    }
}

fn prepare(cell: &mut DurableVsrCell) -> VsrPrepare {
    let operation = operation();
    let VsrOperation::ReplicatedCommand { payload, .. } = &operation else {
        unreachable!()
    };
    let VsrDecision::Prepared(prepare) = cell
        .prepare(NodeId(0), operation.clone(), payload.len() as u32)
        .unwrap()
    else {
        panic!("prepare")
    };
    prepare
}

fn commit(cell: &mut DurableVsrCell, prepare: &VsrPrepare) -> VsrCommit {
    cell.acknowledge(NodeId(1), prepare.view, prepare.index);
    let VsrDecision::Committed(commit) = cell.commit(NodeId(0), prepare.index).unwrap() else {
        panic!("commit")
    };
    commit
}

#[test]
fn follower_persists_full_payload_and_idempotent_certificates_across_reopen() {
    let root = path("roundtrip");
    let mut leader = open(&root.join("leader"));
    let mut follower = open(&root.join("follower"));
    let prepare = prepare(&mut leader);
    let wire = prepare.encode().unwrap();
    assert_eq!(VsrPrepare::decode(&wire).unwrap(), prepare);
    assert_eq!(
        follower
            .receive_prepare(NodeId(1), NodeId(0), VsrPrepare::decode(&wire).unwrap())
            .unwrap(),
        NodeId(1)
    );
    drop(follower);
    let mut follower = open(&root.join("follower"));
    assert_eq!(
        follower.prepared(LogIndex(1)).unwrap().operation,
        operation()
    );
    assert_eq!(follower.committed_count(), 0);
    follower
        .receive_prepare(NodeId(1), NodeId(0), prepare.clone())
        .unwrap();
    let commit = commit(&mut leader, &prepare);
    assert_eq!(
        VsrCommit::decode(&commit.encode().unwrap()).unwrap(),
        commit
    );
    follower
        .receive_commit(NodeId(1), NodeId(0), commit.clone())
        .unwrap();
    let before = SegmentStore::recover(root.join("follower"))
        .unwrap()
        .recovery
        .wal_records
        .len();
    follower
        .receive_commit(NodeId(1), NodeId(0), commit)
        .unwrap();
    follower
        .receive_prepare(NodeId(1), NodeId(0), prepare)
        .unwrap();
    assert_eq!(
        SegmentStore::recover(root.join("follower"))
            .unwrap()
            .recovery
            .wal_records
            .len(),
        before
    );
    drop(follower);
    let recovered = open(&root.join("follower"));
    assert_eq!(
        recovered.committed_operation(LogIndex(1)),
        Some(&operation())
    );
    assert_eq!(
        recovered.committed_prefix().unwrap(),
        leader.committed_prefix().unwrap()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn follower_rejects_spoofed_stale_conflicting_gapped_and_incomplete_messages() {
    let root = path("rejections");
    let mut leader = open(&root.join("leader"));
    let mut follower = open(&root.join("follower"));
    let prepare = prepare(&mut leader);
    assert!(
        follower
            .receive_prepare(NodeId(1), NodeId(2), prepare.clone())
            .is_err()
    );
    assert!(
        follower
            .receive_prepare(NodeId(7), NodeId(0), prepare.clone())
            .is_err()
    );
    let mut gap = prepare.clone();
    gap.index = LogIndex(2);
    assert!(follower.receive_prepare(NodeId(1), NodeId(0), gap).is_err());
    let mut wrong_length = prepare.clone();
    wrong_length.bytes += 1;
    assert!(
        follower
            .receive_prepare(NodeId(1), NodeId(0), wrong_length)
            .is_err()
    );
    follower
        .receive_prepare(NodeId(1), NodeId(0), prepare.clone())
        .unwrap();
    let mut conflict = prepare.clone();
    conflict.operation = VsrOperation::ReplicatedCommand {
        operation_id: OperationId(91),
        payload: vec![0; prepare.bytes as usize],
    };
    assert!(
        follower
            .receive_prepare(NodeId(1), NodeId(0), conflict)
            .is_err()
    );
    let certificate = commit(&mut leader, &prepare);
    let mut minority = certificate.clone();
    minority.certificate.signers.remove(&NodeId(1));
    minority.acknowledgements = 1;
    assert!(
        follower
            .receive_commit(NodeId(1), NodeId(0), minority)
            .is_err()
    );
    let mut corrupt = certificate.clone();
    corrupt.certificate.operation = VsrOperation::ProtocolOperation {
        operation_id: OperationId(92),
    };
    assert!(
        follower
            .receive_commit(NodeId(1), NodeId(0), corrupt)
            .is_err()
    );
    assert_eq!(follower.committed_tail(), LogIndex(0));
    follower.view_change(Epoch(2), NodeId(2));
    assert!(
        follower
            .receive_prepare(NodeId(1), NodeId(0), prepare)
            .is_err()
    );
    assert!(
        follower
            .receive_commit(NodeId(1), NodeId(0), certificate)
            .is_err()
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn commits_and_state_transfer_cannot_skip_or_replace_a_committed_prefix() {
    let root = path("prefix");
    let mut leader = open(&root.join("leader"));
    let first = prepare(&mut leader);
    let second = prepare(&mut leader);
    leader.acknowledge(NodeId(1), second.view, second.index);
    assert!(matches!(
        leader.commit(NodeId(0), second.index).unwrap(),
        VsrDecision::Rejected { .. }
    ));
    commit(&mut leader, &first);
    commit(&mut leader, &second);
    let entries = leader.committed_prefix().unwrap();
    let mut follower = open(&root.join("follower"));
    assert!(follower.catch_up_from(&entries[1..]).is_err());
    assert_eq!(follower.catch_up_from(&entries).unwrap(), 2);
    let mut conflict = entries[0].clone();
    conflict.prepare.operation = VsrOperation::ProtocolOperation {
        operation_id: OperationId(99),
    };
    conflict.commit.certificate.operation = conflict.prepare.operation.clone();
    assert!(follower.catch_up_from(&[conflict]).is_err());
    assert_eq!(follower.committed_prefix().unwrap(), entries);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn reprepare_discards_old_votes_and_survives_restart() {
    let root = path("reprepare");
    let mut cell = open(&root);
    let original = prepare(&mut cell);
    cell.acknowledge(NodeId(1), Epoch(1), original.index);
    cell.view_change(Epoch(2), NodeId(0));
    let new = cell.reprepare(NodeId(0), original.index).unwrap();
    assert_eq!(new.operation, original.operation);
    assert_eq!(new.view, Epoch(2));
    assert!(matches!(
        cell.commit(NodeId(0), original.index).unwrap(),
        VsrDecision::Rejected { .. }
    ));
    drop(cell);
    let mut recovered = open(&root);
    assert_eq!(recovered.prepared(original.index), Some(&new));
    assert!(matches!(
        recovered.commit(NodeId(0), original.index).unwrap(),
        VsrDecision::Rejected { .. }
    ));
    commit(&mut recovered, &new);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn failed_persistence_does_not_publish_commit_and_requires_reopen() {
    let root = path("failure");
    let displaced = root.with_extension("displaced");
    let mut cell = open(&root);
    let prepare = prepare(&mut cell);
    cell.acknowledge(NodeId(1), prepare.view, prepare.index);
    std::fs::rename(&root, &displaced).unwrap();
    std::fs::write(&root, b"not a directory").unwrap();
    assert!(cell.commit(NodeId(0), prepare.index).is_err());
    assert_eq!(cell.committed_count(), 0);
    std::fs::remove_file(&root).unwrap();
    std::fs::rename(&displaced, &root).unwrap();
    assert!(
        cell.commit(NodeId(0), prepare.index)
            .unwrap_err()
            .message
            .contains("reopen")
    );
    drop(cell);
    let mut recovered = open(&root);
    assert_eq!(recovered.committed_count(), 0);
    commit(&mut recovered, &prepare);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn duplicate_writer_is_rejected_until_first_owner_drops() {
    let root = path("writer");
    let first = open(&root);
    assert!(
        DurableVsrCell::open(
            &root,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)]
        )
        .is_err()
    );
    drop(first);
    drop(open(&root));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn durable_replica_rejects_unsynced_storage_before_creating_state() {
    let root = path("unsynced");
    let options = SegmentStoreOptions {
        log_options: rsf_storage::SegmentLogOptions {
            sync_policy: rsf_storage::SyncPolicy::None,
        },
        ..SegmentStoreOptions::default()
    };
    assert!(
        DurableVsrCell::open_with_store_options(
            &root,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
            options,
        )
        .is_err()
    );
    assert!(!root.exists());
}

#[test]
fn wire_decoders_reject_truncation_overflow_and_oversized_payloads() {
    let root = path("codec");
    let mut cell = open(&root);
    let prepare = prepare(&mut cell);
    let bytes = prepare.encode().unwrap();
    for end in 0..bytes.len() {
        assert!(VsrPrepare::decode(&bytes[..end]).is_err(), "length={end}");
    }
    let mut overflow = bytes.clone();
    overflow[50..58].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(VsrPrepare::decode(&overflow).is_err());
    let commit = commit(&mut cell, &prepare);
    let bytes = commit.encode().unwrap();
    for end in 0..bytes.len() {
        assert!(VsrCommit::decode(&bytes[..end]).is_err(), "length={end}");
    }
    let mut invalid = commit;
    invalid.certificate.pg = PlacementGroupId(7);
    assert!(invalid.encode().is_err());
    assert!(matches!(
        cell.prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(1),
                payload: vec![0; MAX_REPLICATED_COMMAND_BYTES + 1]
            },
            (MAX_REPLICATED_COMMAND_BYTES + 1) as u32
        )
        .unwrap(),
        VsrDecision::Rejected { .. }
    ));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn follower_process() {
    use std::io::Write;
    let Some(root) = std::env::var_os("RSF_TEST_FOLLOWER_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let prepare = VsrPrepare::decode(&std::fs::read(root.join("prepare.bin")).unwrap()).unwrap();
    let commit = VsrCommit::decode(&std::fs::read(root.join("commit.bin")).unwrap()).unwrap();
    let mut follower = open(&root.join("follower"));
    follower
        .receive_prepare(NodeId(1), NodeId(0), prepare)
        .unwrap();
    if std::env::var_os("RSF_TEST_COMMIT_BEFORE_KILL").is_some() {
        follower
            .receive_commit(NodeId(1), NodeId(0), commit)
            .unwrap();
    }
    println!("RSF_FOLLOWER_DURABLE");
    std::io::stdout().flush().unwrap();
    // The parent kills this process without running destructors.
    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(1));
    }
}

#[test]
fn acknowledged_payload_survives_forced_follower_process_termination() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    for commit_before_kill in [false, true] {
        let root = path("process_crash");
        let mut leader = open(&root.join("leader"));
        let prepare = prepare(&mut leader);
        let certificate = commit(&mut leader, &prepare);
        std::fs::write(root.join("prepare.bin"), prepare.encode().unwrap()).unwrap();
        std::fs::write(root.join("commit.bin"), certificate.encode().unwrap()).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "replica::tests::follower_process", "--nocapture"])
            .env("RSF_TEST_FOLLOWER_ROOT", &root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if commit_before_kill {
            command.env("RSF_TEST_COMMIT_BEFORE_KILL", "1");
        } else {
            command.env_remove("RSF_TEST_COMMIT_BEFORE_KILL");
        }
        let mut child = command.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if line.unwrap().contains("RSF_FOLLOWER_DURABLE") {
                    let _ = sender.send(());
                    return;
                }
            }
        });
        let acknowledged = receiver.recv_timeout(std::time::Duration::from_secs(10));
        let _ = child.kill();
        let output = child.wait_with_output().unwrap();
        reader.join().unwrap();
        assert!(
            acknowledged.is_ok(),
            "follower did not acknowledge: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !output.status.success(),
            "follower must be forcibly terminated"
        );
        let mut recovered = open(&root.join("follower"));
        assert_eq!(recovered.prepared(prepare.index), Some(&prepare));
        assert_eq!(recovered.committed_count(), usize::from(commit_before_kill));
        recovered
            .receive_commit(NodeId(1), NodeId(0), certificate)
            .unwrap();
        assert_eq!(
            recovered.committed_operation(prepare.index),
            Some(&operation())
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
