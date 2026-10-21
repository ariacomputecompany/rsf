use super::*;
use publication::Boundary;
use std::path::{Path, PathBuf};

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-install-{label}-{}-{}",
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
fn request() -> VsrViewChangeRequest {
    VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(1),
        target_view: Epoch(2),
        candidate: NodeId(1),
        voters: vec![NodeId(0), NodeId(1), NodeId(2)],
    }
}
fn prepare(cell: &mut DurableVsrCell, value: u64) -> VsrPrepare {
    let VsrDecision::Prepared(prepare) = cell
        .prepare(
            cell.primary(),
            VsrOperation::ProtocolOperation {
                operation_id: OperationId(value),
            },
            1,
        )
        .unwrap()
    else {
        panic!("prepare");
    };
    prepare
}

fn fixture(path: &Path) -> (DurableVsrCell, DurableVsrCell, Vec<VsrViewChangeSnapshot>) {
    let mut primary = open(&path.join("0"));
    let mut first = open(&path.join("1"));
    let mut second = open(&path.join("2"));
    let prepared = prepare(&mut primary, 1);
    first
        .receive_prepare(NodeId(1), NodeId(0), prepared.clone())
        .unwrap();
    second
        .receive_prepare(NodeId(2), NodeId(0), prepared.clone())
        .unwrap();
    primary.acknowledge(NodeId(1), Epoch(1), prepared.index);
    let VsrDecision::Committed(commit) = primary.commit(NodeId(0), prepared.index).unwrap() else {
        panic!("commit");
    };
    first.receive_commit(NodeId(1), NodeId(0), commit).unwrap();
    first
        .remember_client_request(VsrClientRequestId::new("original", 1), LogIndex(1))
        .unwrap();
    let pending = prepare(&mut primary, 2);
    second
        .receive_prepare(NodeId(2), NodeId(0), pending)
        .unwrap();
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(1), request())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(1), request())
            .unwrap(),
    ];
    (first, second, snapshots)
}

#[test]
fn selects_longest_accepted_history_merges_commits_and_receipts_and_restarts_twice() {
    let path = root("roundtrip");
    let (mut first, mut second, mut snapshots) = fixture(&path);
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    assert_eq!(plan.committed_tail(), LogIndex(1));
    assert_eq!(plan.accepted_tail(), LogIndex(2));
    snapshots.reverse();
    assert_eq!(
        VsrViewInstallation::select(&snapshots).unwrap().id(),
        plan.id()
    );
    first.install_view(&plan).unwrap();
    second.install_view(&plan).unwrap();
    assert_eq!(first.view(), Epoch(2));
    assert_eq!(
        first.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    assert!(
        first
            .commit(NodeId(1), LogIndex(2))
            .is_ok_and(|decision| matches!(decision, VsrDecision::Rejected { .. }))
    );
    let pending = first.reprepare(NodeId(1), LogIndex(2)).unwrap();
    second
        .receive_prepare(NodeId(2), NodeId(1), pending)
        .unwrap();
    first.acknowledge(NodeId(2), Epoch(2), LogIndex(2));
    let VsrDecision::Committed(commit) = first.commit(NodeId(1), LogIndex(2)).unwrap() else {
        panic!("new quorum");
    };
    second.receive_commit(NodeId(2), NodeId(1), commit).unwrap();
    first.install_view(&plan).unwrap(); // Lost installation reply must not roll back later writes.
    assert_eq!(first.committed_tail(), LogIndex(2));
    drop(first);
    drop(second);
    let mut first = open(&path.join("1"));
    let mut second = DurableVsrCell::open_existing(path.join("2"), PlacementGroupId(1)).unwrap();
    assert_eq!(
        first.committed_prefix().unwrap(),
        second.committed_prefix().unwrap()
    );
    let next = VsrViewChangeRequest {
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        ..request()
    };
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(2), next.clone())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(2), next)
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    first.install_view(&plan).unwrap();
    second.install_view(&plan).unwrap();
    drop(first);
    drop(second);
    let first = open(&path.join("1"));
    assert_eq!(first.view(), Epoch(3));
    assert_eq!(first.primary(), NodeId(2));
    assert_eq!(first.committed_count(), 2);
    assert_eq!(
        first.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
}

#[test]
fn rejects_missing_duplicate_corrupt_or_semantically_forged_quorum_reports() {
    let path = root("reject");
    let (_, _, mut snapshots) = fixture(&path);
    assert!(VsrViewInstallation::select(&[]).is_err());
    assert!(VsrViewInstallation::select(&snapshots[..1]).is_err());
    let original = snapshots[1].report.clone();
    snapshots[1].report.replica = snapshots[0].report.replica;
    assert!(VsrViewInstallation::select(&snapshots).is_err());
    snapshots[1].report = original;
    snapshots[1].report.accepted_tail = LogIndex(999);
    // Hash integrity alone passes; authoritative semantic replay must reject it.
    snapshots[1].validate_integrity().unwrap();
    assert!(VsrViewInstallation::select(&snapshots).is_err());
    snapshots[1].report.accepted_tail = LogIndex(2);
    snapshots[1].records[1].payload[0] ^= 1;
    assert!(VsrViewInstallation::select(&snapshots).is_err());
}

#[test]
fn local_publication_preserves_extra_receipts_and_rejects_superseded_plans() {
    let path = root("local-receipt");
    let (first, second, snapshots) = fixture(&path);
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    drop(first);
    drop(second);
    let mut primary = open(&path.join("0"));
    primary
        .remember_client_request(
            VsrClientRequestId::new("only-on-old-primary", 9),
            LogIndex(1),
        )
        .unwrap();
    primary
        .begin_view_change(NodeId(0), NodeId(1), request())
        .unwrap();
    primary.install_view(&plan).unwrap();
    assert_eq!(
        primary.request_commit(&VsrClientRequestId::new("only-on-old-primary", 9)),
        Some(LogIndex(1))
    );
    drop(primary);
    let mut primary = open(&path.join("0"));
    assert_eq!(primary.view(), Epoch(2));
    assert_eq!(
        primary.request_commit(&VsrClientRequestId::new("only-on-old-primary", 9)),
        Some(LogIndex(1))
    );
    primary.install_view(&plan).unwrap();
    let higher = VsrViewChangeRequest {
        previous_view: Epoch(2),
        target_view: Epoch(4),
        candidate: NodeId(2),
        ..request()
    };
    primary
        .begin_view_change(NodeId(0), NodeId(2), higher)
        .unwrap();
    assert!(primary.install_view(&plan).is_err());
}

#[test]
fn publication_failures_leave_old_fenced_state_or_complete_new_state_after_reopen() {
    for boundary in [Boundary::Staged, Boundary::Renamed, Boundary::Published] {
        let path = root("publication");
        let (mut first, _, snapshots) = fixture(&path);
        let plan = VsrViewInstallation::select(&snapshots).unwrap();
        let expected = match boundary {
            Boundary::Staged => Epoch(1),
            _ => Epoch(2),
        };
        assert!(
            first
                .install_view_with_hook(&plan, &mut |at| {
                    if std::mem::discriminant(&at) == std::mem::discriminant(&boundary) {
                        Err(corrupt("injected publication interruption"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert!(
            first
                .prepare(
                    NodeId(1),
                    VsrOperation::ProtocolOperation {
                        operation_id: OperationId(3)
                    },
                    1
                )
                .is_err()
        );
        drop(first);
        let mut recovered = open(&path.join("1"));
        assert_eq!(recovered.view(), expected);
        recovered.install_view(&plan).unwrap();
        assert_eq!(recovered.view(), Epoch(2));
        assert_eq!(recovered.committed_tail(), LogIndex(1));
    }
}

#[test]
fn semantically_invalid_prepares_and_forked_accepted_prefixes_do_not_form_a_plan() {
    for fork in [false, true] {
        let path = root("semantic-corruption");
        let (_, _, mut snapshots) = fixture(&path);
        let record = snapshots[1]
            .records
            .iter_mut()
            .find(|record| record.kind == WalRecordKind::Prepare)
            .unwrap();
        let mut prepare = VsrPrepare::decode(&record.payload).unwrap();
        if fork {
            prepare.operation = VsrOperation::ProtocolOperation {
                operation_id: OperationId(999),
            };
        } else {
            prepare.view = Epoch(999);
        }
        record.payload = prepare.encode().unwrap();
        snapshots[1].report.history.digest =
            transfer::history_digest(&snapshots[1].records).unwrap();
        snapshots[1].validate_integrity().unwrap();
        assert!(VsrViewInstallation::select(&snapshots).is_err());
    }
}

#[test]
fn invalid_active_pointer_missing_seal_or_missing_generation_fail_closed() {
    for damage in 0..3 {
        let path = root("generation-corruption");
        let (mut first, _, snapshots) = fixture(&path);
        first
            .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
            .unwrap();
        let directory = path.join("1");
        let generation = publication::active(&directory, PlacementGroupId(1))
            .unwrap()
            .path;
        drop(first);
        match damage {
            0 => {
                let mut pointer = std::fs::read(directory.join("VSR_ACTIVE")).unwrap();
                pointer[20] ^= 1;
                std::fs::write(directory.join("VSR_ACTIVE"), pointer).unwrap();
            }
            1 => std::fs::remove_file(generation.join("VSR_SEAL")).unwrap(),
            _ => std::fs::rename(&generation, directory.join("missing-generation")).unwrap(),
        }
        assert!(DurableVsrCell::open_existing(&directory, PlacementGroupId(1)).is_err());
        assert!(
            DurableVsrCell::open(
                &directory,
                PlacementGroupId(1),
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)]
            )
            .is_err()
        );
    }
}

#[test]
fn installed_history_reaches_nonvoting_learners_with_both_view_certificates() {
    let path = root("learner");
    let (mut first, _, snapshots) = fixture(&path);
    first
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    let snapshot = first.history_snapshot().unwrap();
    let mut learner = LearnerHistoryStore::open(
        path.join("learner"),
        PlacementGroupId(1),
        NodeId(9),
        NodeId(1),
    )
    .unwrap();
    learner.begin(snapshot.descriptor.clone()).unwrap();
    let id = snapshot.descriptor.id().unwrap();
    for (i, record) in snapshot.records.iter().enumerate() {
        learner.append(id, i as u64, record.clone()).unwrap();
    }
    assert_eq!(learner.finish(id).unwrap(), LogIndex(1));
    assert_eq!(
        learner.committed_entries().unwrap(),
        first.committed_prefix().unwrap()
    );
}

#[test]
fn abrupt_installation_process() {
    let Some(path) = std::env::var_os("RSF_INSTALL_CRASH_ROOT") else {
        return;
    };
    let path = PathBuf::from(path);
    let target = std::env::var("RSF_INSTALL_CRASH_AT").unwrap();
    let (mut first, _, snapshots) = fixture(&path);
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    first
        .install_view_with_hook(&plan, &mut |boundary| {
            let at = match boundary {
                Boundary::Staged => "staged",
                Boundary::Renamed => "renamed",
                Boundary::Published => "published",
            };
            if at == target {
                std::process::exit(73);
            }
            Ok(())
        })
        .unwrap();
    panic!("requested crash boundary was not reached");
}

#[test]
fn abrupt_process_exit_at_each_publication_boundary_recovers_a_complete_view() {
    for boundary in ["staged", "renamed", "published"] {
        let path = root("process-crash");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "installation::tests::abrupt_installation_process",
                "--nocapture",
            ])
            .env("RSF_INSTALL_CRASH_ROOT", &path)
            .env("RSF_INSTALL_CRASH_AT", boundary)
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73));
        let recovered = open(&path.join("1"));
        assert_eq!(
            recovered.view(),
            if boundary == "staged" {
                Epoch(1)
            } else {
                Epoch(2)
            }
        );
        assert_eq!(
            recovered.committed_operation(LogIndex(1)),
            Some(&VsrOperation::ProtocolOperation {
                operation_id: OperationId(1)
            })
        );
        if boundary == "staged" {
            assert!(recovered.view_change_promise().is_some());
        } else {
            assert!(recovered.view_change_promise().is_none());
        }
    }
}

#[test]
fn live_writer_rejects_a_missing_active_pointer_and_old_generations_are_reclaimed() {
    let path = root("pointer-ownership");
    let (mut first, _, snapshots) = fixture(&path);
    first
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    let directory = path.join("1");
    let active = publication::active(&directory, PlacementGroupId(1))
        .unwrap()
        .path;
    // Simulate an abandoned staging generation, with an owned directory name.
    let abandoned = directory.join("vsr-views").join("a".repeat(64));
    if abandoned != active {
        std::fs::create_dir_all(&abandoned).unwrap();
    }
    assert_eq!(first.reclaim_inactive_views().unwrap(), 1);
    assert!(active.exists());
    let pointer = std::fs::read(directory.join("VSR_ACTIVE")).unwrap();
    std::fs::remove_file(directory.join("VSR_ACTIVE")).unwrap();
    assert!(first.primary_probe().is_err());
    assert!(
        first
            .prepare(
                NodeId(1),
                VsrOperation::ProtocolOperation {
                    operation_id: OperationId(99)
                },
                1
            )
            .is_err()
    );
    std::fs::write(directory.join("VSR_ACTIVE"), pointer).unwrap();
    assert!(
        first
            .primary_probe()
            .unwrap_err()
            .message
            .contains("reopen")
    );
    drop(first);
    assert!(open(&directory).primary_probe().is_ok());
}

#[test]
fn mixed_normal_views_prefer_the_newest_view_over_a_longer_old_uncommitted_tail() {
    let path = root("mixed-normal");
    let (mut first, mut second, snapshots) = fixture(&path);
    first
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    // Only one replica installed view 2. The original primary continues making
    // local, unacknowledged proposals in view 1 while isolated from the quorum.
    let mut old = open(&path.join("0"));
    assert_eq!(prepare(&mut old, 3).index, LogIndex(3));
    let request = VsrViewChangeRequest {
        target_view: Epoch(3),
        candidate: NodeId(2),
        ..request()
    };
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(2), request.clone())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(2), request.clone())
            .unwrap(),
        old.begin_view_change(NodeId(0), NodeId(2), request)
            .unwrap(),
    ];
    assert_eq!(snapshots[0].report.history.probe.view, Epoch(2));
    assert_eq!(snapshots[2].report.accepted_tail, LogIndex(3));
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    assert_eq!(plan.accepted_tail(), LogIndex(2));
    assert_eq!(plan.committed_tail(), LogIndex(1));
    for cell in [&mut first, &mut second, &mut old] {
        cell.install_view(&plan).unwrap();
    }
    drop(first);
    drop(second);
    drop(old);
    for node in 0..3 {
        let cell = open(&path.join(node.to_string()));
        assert_eq!(cell.view(), Epoch(3));
        assert_eq!(cell.primary(), NodeId(2));
        assert!(cell.prepared(LogIndex(3)).is_none());
        assert_eq!(
            cell.request_commit(&VsrClientRequestId::new("original", 1)),
            Some(LogIndex(1))
        );
    }
}

#[test]
fn mixed_views_learn_an_old_certificate_without_reopening_its_sealed_view() {
    let path = root("late-certificate");
    let mut old = open(&path.join("0"));
    let mut first = open(&path.join("1"));
    let mut second = open(&path.join("2"));
    let p1 = prepare(&mut old, 1);
    first
        .receive_prepare(NodeId(1), NodeId(0), p1.clone())
        .unwrap();
    second
        .receive_prepare(NodeId(2), NodeId(0), p1.clone())
        .unwrap();
    old.acknowledge(NodeId(1), Epoch(1), LogIndex(1));
    let VsrDecision::Committed(c1) = old.commit(NodeId(0), LogIndex(1)).unwrap() else {
        panic!("commit one");
    };
    first.receive_commit(NodeId(1), NodeId(0), c1).unwrap();
    for n in [2, 3] {
        let pending = prepare(&mut old, n);
        second
            .receive_prepare(NodeId(2), NodeId(0), pending)
            .unwrap();
        old.acknowledge(NodeId(2), Epoch(1), LogIndex(n));
        old.commit(NodeId(0), LogIndex(n)).unwrap();
    }
    old.remember_client_request(VsrClientRequestId::new("late", 3), LogIndex(3))
        .unwrap();
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(1), request())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(1), request())
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    assert_eq!(plan.committed_tail(), LogIndex(1));
    first.install_view(&plan).unwrap();
    second.install_view(&plan).unwrap();
    let prepared = first.reprepare(NodeId(1), LogIndex(2)).unwrap();
    second
        .receive_prepare(NodeId(2), NodeId(1), prepared)
        .unwrap();
    first.acknowledge(NodeId(2), Epoch(2), LogIndex(2));
    let VsrDecision::Committed(commit) = first.commit(NodeId(1), LogIndex(2)).unwrap() else {
        panic!("view two commit");
    };
    second.receive_commit(NodeId(2), NodeId(1), commit).unwrap();
    let next = VsrViewChangeRequest {
        target_view: Epoch(3),
        ..request()
    };
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(1), next.clone())
            .unwrap(),
        old.begin_view_change(NodeId(0), NodeId(1), next.clone())
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    assert_eq!(plan.committed_tail(), LogIndex(3));
    second
        .begin_view_change(NodeId(2), NodeId(1), next)
        .unwrap();
    for cell in [&mut first, &mut old, &mut second] {
        cell.install_view(&plan).unwrap();
    }
    drop(first);
    drop(old);
    drop(second);
    let mut cell = open(&path.join("2"));
    assert_eq!(
        cell.cell
            .committed
            .iter()
            .map(|commit| commit.view)
            .collect::<Vec<_>>(),
        vec![Epoch(1), Epoch(2), Epoch(1)]
    );
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("late", 3)),
        Some(LogIndex(3))
    );
    let legitimate = &cell.committed_prefix().unwrap()[2];
    cell.history
        .validate(&legitimate.prepare, &legitimate.commit)
        .unwrap();
    let mut forged = cell.committed_prefix().unwrap()[2].clone();
    forged.prepare.operation = VsrOperation::ProtocolOperation {
        operation_id: OperationId(999),
    };
    forged.commit.certificate.operation = forged.prepare.operation.clone();
    assert!(
        cell.history
            .validate(&forged.prepare, &forged.commit)
            .is_err()
    );
    assert!(cell.catch_up_from(&[forged]).is_err());
    // A different old-view command is still forbidden. Installation authorized
    // the exact operation at index 3, not a blanket extension of view 1.
    let prepare = VsrPrepare {
        index: LogIndex(4),
        operation: VsrOperation::ProtocolOperation {
            operation_id: OperationId(4),
        },
        ..p1
    };
    let commit = VsrCommit {
        pg: prepare.pg,
        view: prepare.view,
        index: prepare.index,
        primary: prepare.primary,
        acknowledgements: 2,
        certificate: VsrCommitCertificate {
            pg: prepare.pg,
            view: prepare.view,
            index: prepare.index,
            primary: prepare.primary,
            operation: prepare.operation.clone(),
            signers: [NodeId(0), NodeId(2)].into(),
        },
    };
    assert!(
        cell.catch_up_from(&[VsrCommittedEntry { prepare, commit }])
            .is_err()
    );
    assert_eq!(cell.committed_tail(), LogIndex(3));
}

#[test]
fn legacy_installation_markers_and_commit_deltas_remain_replayable() {
    let path = root("legacy-marker");
    let (_, _, snapshots) = fixture(&path);
    let committed = snapshots[0].recover_state().unwrap().0.committed[0].clone();
    let mut records = snapshots[1].records.clone();
    let deltas = vec![
        VsrHistoryRecord {
            epoch: Epoch(1),
            kind: WalRecordKind::ViewInstallation,
            payload: codec::commit_delta(&committed).unwrap(),
        },
        VsrHistoryRecord {
            epoch: Epoch(1),
            kind: WalRecordKind::ViewInstallation,
            payload: codec::client_delta(&VsrClientRequestCommit {
                pg: PlacementGroupId(1),
                request: VsrClientRequestId::new("original", 1),
                index: LogIndex(1),
            })
            .unwrap(),
        },
    ];
    let marker = Installation {
        reports: snapshots
            .iter()
            .map(|snapshot| snapshot.report.clone())
            .collect(),
        source: NodeId(2),
        delta_count: 2,
        delta_digest: transfer::history_digest(&deltas).unwrap(),
    };
    records.extend(deltas);
    records.push(VsrHistoryRecord {
        epoch: Epoch(1),
        kind: WalRecordKind::ViewInstallation,
        payload: marker.encode_version(true).unwrap(),
    });
    let recovered = recover_records(&records).unwrap();
    assert_eq!(recovered.0.view, Epoch(2));
    assert_eq!(recovered.0.committed_tail(), LogIndex(1));
    assert_eq!(
        recovered.0.client_table[&VsrClientRequestId::new("original", 1)],
        LogIndex(1)
    );
    let legacy_id = transfer::history_digest(&records).unwrap().bytes;
    let legacy_plan = VsrViewInstallation {
        request: request(),
        records,
        recovered,
        id: legacy_id,
    };
    let mut first = open(&path.join("1"));
    first.install_view(&legacy_plan).unwrap();
    let upgraded_plan = VsrViewInstallation::select(&snapshots).unwrap();
    assert_ne!(legacy_plan.id(), upgraded_plan.id());
    first.install_view(&upgraded_plan).unwrap();
    assert_eq!(first.installed_plan, Some(legacy_id));
    let encoded = marker.encode().unwrap();
    for end in 0..encoded.len() {
        assert!(Installation::decode(&encoded[..end]).is_err());
    }
}

#[test]
fn equal_normal_views_allow_reprepare_headers_to_differ_for_the_same_operation() {
    let path = root("reprepare-prefix");
    let (mut first, mut second, snapshots) = fixture(&path);
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    first.install_view(&plan).unwrap();
    second.install_view(&plan).unwrap();
    first.reprepare(NodeId(1), LogIndex(2)).unwrap();
    let request = VsrViewChangeRequest {
        target_view: Epoch(3),
        ..request()
    };
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(1), request.clone())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(1), request)
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    first.install_view(&plan).unwrap();
    second.install_view(&plan).unwrap();
    assert_eq!(first.view(), Epoch(3));
    assert_eq!(first.committed_tail(), LogIndex(1));
    assert_eq!(
        first.reprepare(NodeId(1), LogIndex(2)).unwrap().view,
        Epoch(3)
    );
}

#[test]
fn a_newer_normal_view_cannot_discard_a_committed_conflict_from_an_older_voter() {
    let path = root("mixed-committed-conflict");
    let (mut first, _, snapshots) = fixture(&path);
    first
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    let mut fork = open(&path.join("fork"));
    for value in [1, 999] {
        let prepared = prepare(&mut fork, value);
        fork.acknowledge(NodeId(2), Epoch(1), prepared.index);
        fork.commit(NodeId(0), prepared.index).unwrap();
    }
    let next = VsrViewChangeRequest {
        target_view: Epoch(3),
        ..request()
    };
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(1), next.clone())
            .unwrap(),
        fork.begin_view_change(NodeId(0), NodeId(1), next).unwrap(),
    ];
    assert!(VsrViewInstallation::select(&snapshots).is_err());
}

#[test]
fn maximum_command_commit_learning_stays_within_history_record_bounds() {
    let path = root("maximum-learned-command");
    let mut old = open(&path.join("0"));
    let mut first = open(&path.join("1"));
    let mut second = open(&path.join("2"));
    let operation = VsrOperation::ReplicatedCommand {
        operation_id: OperationId(1),
        payload: vec![b'x'; MAX_REPLICATED_COMMAND_BYTES],
    };
    let VsrDecision::Prepared(prepared) = old
        .prepare(NodeId(0), operation, MAX_REPLICATED_COMMAND_BYTES as u32)
        .unwrap()
    else {
        panic!("prepare");
    };
    first
        .receive_prepare(NodeId(1), NodeId(0), prepared.clone())
        .unwrap();
    second
        .receive_prepare(NodeId(2), NodeId(0), prepared.clone())
        .unwrap();
    old.acknowledge(NodeId(2), Epoch(1), prepared.index);
    let VsrDecision::Committed(commit) = old.commit(NodeId(0), prepared.index).unwrap() else {
        panic!("commit");
    };
    second.receive_commit(NodeId(2), NodeId(0), commit).unwrap();
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(1), request())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(1), request())
            .unwrap(),
    ];
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    assert!(
        plan.records
            .iter()
            .any(|record| record.payload.starts_with(b"VSIL1"))
    );
    for record in &plan.records {
        assert!(record.encode().is_ok());
    }
    first.install_view(&plan).unwrap();
    first.history_snapshot().unwrap();
    drop(first);
    assert_eq!(open(&path.join("1")).committed_tail(), LogIndex(1));
}

#[test]
fn learned_commit_decoder_rejects_forged_evidence_without_mutating_the_log() {
    let path = root("learned-evidence-validation");
    let (_, _, snapshots) = fixture(&path);
    let original = snapshots[0].recover_state().unwrap().0.committed[0].clone();
    for case in 0..6 {
        let mut recovered = snapshots[1].recover_state().unwrap();
        let mut commit = original.clone();
        let mut bytes = 1;
        match case {
            0 => {
                commit.certificate.signers = [NodeId(0)].into();
                commit.acknowledgements = 1;
            }
            1 => {
                commit.certificate.operation = VsrOperation::ProtocolOperation {
                    operation_id: OperationId(999),
                }
            }
            2 => {
                commit.index = LogIndex(2);
                commit.certificate.index = LogIndex(2);
                commit.certificate.operation = VsrOperation::ProtocolOperation {
                    operation_id: OperationId(2),
                };
            }
            3 => {
                commit.view = Epoch(999);
                commit.certificate.view = Epoch(999);
            }
            4 => {
                commit.primary = NodeId(2);
                commit.certificate.primary = NodeId(2);
                commit.certificate.signers = [NodeId(0), NodeId(2)].into();
            }
            _ => bytes = 2,
        }
        let prepare = VsrPrepare {
            pg: commit.pg,
            view: commit.view,
            index: commit.index,
            primary: commit.primary,
            operation: commit.certificate.operation.clone(),
            bytes,
        };
        let payload = codec::learned_commit_delta(&prepare, &commit).unwrap();
        assert!(
            codec::apply_delta(&mut recovered.0, &mut recovered.1, &payload).is_err(),
            "invalid evidence case {case}"
        );
        assert_eq!(recovered.0.committed_tail(), LogIndex(0));
        assert!(!recovered.0.pending[&LogIndex(1)].committed);
        assert_eq!(
            recovered.0.pending[&LogIndex(1)].prepare.operation,
            VsrOperation::ProtocolOperation {
                operation_id: OperationId(1)
            }
        );
    }
}

#[test]
fn durable_installation_input_resumes_and_rejects_gaps_replacements_and_wrong_candidates() {
    let path = root("input-resume");
    let (mut first, second, snapshots) = fixture(&path);
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    assert_eq!(
        VsrInstallationInput::decode(&input.encode().unwrap()).unwrap(),
        input
    );
    let id = input.id().unwrap();
    let records = snapshots
        .iter()
        .flat_map(|s| s.records.iter())
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        first
            .begin_installation_input(NodeId(1), NodeId(0), input.clone())
            .is_err()
    );
    assert_eq!(
        first
            .begin_installation_input(NodeId(1), NodeId(1), input.clone())
            .unwrap(),
        0
    );
    assert!(
        first
            .append_installation_input(NodeId(1), id, 1, records[0].clone())
            .is_err()
    );
    assert!(
        first
            .append_installation_input(NodeId(2), id, 0, records[0].clone())
            .is_err()
    );
    let halfway = records.len() / 2;
    for (i, record) in records.iter().take(halfway).enumerate() {
        assert_eq!(
            first
                .append_installation_input(NodeId(1), id, i as u64, record.clone())
                .unwrap(),
            i as u64 + 1
        );
    }
    assert!(first.finish_installation_input(NodeId(1), id).is_err());
    drop(first);
    let mut first = open(&path.join("1"));
    assert_eq!(first.installation_input().unwrap(), Some(input.clone()));
    assert_eq!(
        first
            .append_installation_input(NodeId(1), id, 0, records[0].clone())
            .unwrap(),
        halfway as u64
    );
    let mut changed = records[0].clone();
    changed.payload.push(0);
    assert!(
        first
            .append_installation_input(NodeId(1), id, 0, changed)
            .is_err()
    );
    // Continue without another BEGIN after reopening the recipient.
    for (i, record) in records.iter().enumerate().skip(halfway) {
        first
            .append_installation_input(NodeId(1), id, i as u64, record.clone())
            .unwrap();
    }
    assert_eq!(
        first.finish_installation_input(NodeId(1), id).unwrap(),
        Epoch(2)
    );
    assert_eq!(first.committed_tail(), LogIndex(1));
    assert_eq!(LogIndex(first.cell.next_index - 1), LogIndex(2));
    drop(first);
    let mut first = open(&path.join("1"));
    assert_eq!(
        first.finish_installation_input(NodeId(1), id).unwrap(),
        Epoch(2)
    );
    assert_eq!(
        first
            .begin_installation_input(NodeId(1), NodeId(1), input)
            .unwrap(),
        records.len() as u64
    );
    let mut newer = request();
    newer.previous_view = Epoch(2);
    newer.target_view = Epoch(3);
    newer.candidate = NodeId(2);
    first
        .begin_view_change(NodeId(1), NodeId(2), newer)
        .unwrap();
    assert!(first.finish_installation_input(NodeId(1), id).is_err());
    drop(first);
    drop(second);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn installation_input_validates_all_histories_before_publication() {
    let path = root("input-corrupt");
    let (mut first, second, snapshots) = fixture(&path);
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    let id = input.id().unwrap();
    first
        .begin_installation_input(NodeId(1), NodeId(1), input)
        .unwrap();
    let mut records = snapshots
        .iter()
        .flat_map(|s| s.records.iter())
        .cloned()
        .collect::<Vec<_>>();
    records.last_mut().unwrap().payload[0] ^= 1;
    for (i, record) in records.into_iter().enumerate() {
        first
            .append_installation_input(NodeId(1), id, i as u64, record)
            .unwrap();
    }
    assert!(first.finish_installation_input(NodeId(1), id).is_err());
    assert_eq!(first.view(), Epoch(1));
    assert!(!path.join("1/VSR_ACTIVE").exists());
    drop(first);
    drop(second);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn installation_batches_recover_logical_ordinals_overlap_retries_and_reject_changed_prefixes() {
    let path = root("input-batches");
    let (mut first, second, snapshots) = fixture(&path);
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    let id = input.id().unwrap();
    let records = input
        .reports()
        .iter()
        .flat_map(|report| {
            snapshots
                .iter()
                .find(|snapshot| snapshot.report == *report)
                .unwrap()
                .records
                .iter()
                .cloned()
        })
        .collect::<Vec<_>>();
    first
        .begin_installation_input(NodeId(1), NodeId(1), input.clone())
        .unwrap();
    // Start with the old one-record WAL representation, then mix in batches.
    drop(first);
    let directory = path
        .join("1/vsr-install-input")
        .join(id.iter().map(|b| format!("{b:02x}")).collect::<String>());
    let mut store = SegmentStore::open(directory.join("records")).unwrap();
    store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(2),
            WalRecordKind::ViewInstallation,
            &records[0].encode().unwrap(),
        )
        .unwrap();
    drop(store);
    let mut first = open(&path.join("1"));
    assert_eq!(
        first
            .begin_installation_input(NodeId(1), NodeId(1), input.clone())
            .unwrap(),
        1
    );
    assert_eq!(
        first
            .append_installation_batch(
                NodeId(1),
                id,
                crate::VsrHistoryBatch {
                    ordinal: 0,
                    records: records[..3].to_vec()
                }
            )
            .unwrap(),
        3
    );
    drop(first);
    let mut first = open(&path.join("1"));
    assert_eq!(
        first
            .begin_installation_input(NodeId(1), NodeId(1), input)
            .unwrap(),
        3
    );
    let mut changed = records[..3].to_vec();
    changed[1].payload.push(0);
    assert!(
        first
            .append_installation_batch(
                NodeId(1),
                id,
                crate::VsrHistoryBatch {
                    ordinal: 0,
                    records: changed
                }
            )
            .is_err()
    );
    assert_eq!(
        first
            .append_installation_batch(
                NodeId(1),
                id,
                crate::VsrHistoryBatch {
                    ordinal: 1,
                    records: records[1..].to_vec()
                }
            )
            .unwrap(),
        records.len() as u64
    );
    let restored = first.installation_snapshots(id).unwrap();
    assert_eq!(restored.len(), snapshots.len());
    for (restored, original) in restored.iter().zip(&snapshots) {
        assert_eq!(restored.report, original.report);
        assert_eq!(restored.records, original.records);
    }
    let recovered = SegmentStore::recover(directory.join("records")).unwrap();
    assert_eq!(recovered.recovery.wal_records.len(), 3);
    assert_eq!(
        first.finish_installation_input(NodeId(1), id).unwrap(),
        Epoch(2)
    );
    drop(first);
    assert_eq!(open(&path.join("1")).committed_tail(), LogIndex(1));
    drop(second);
}

#[test]
fn torn_staged_batch_recovers_only_the_previous_logical_prefix_and_can_resume() {
    let path = root("torn-input-batch");
    let (mut first, second, snapshots) = fixture(&path);
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    let id = input.id().unwrap();
    let records = input
        .reports()
        .iter()
        .flat_map(|report| {
            snapshots
                .iter()
                .find(|s| s.report == *report)
                .unwrap()
                .records
                .iter()
                .cloned()
        })
        .collect::<Vec<_>>();
    first
        .begin_installation_input(NodeId(1), NodeId(1), input.clone())
        .unwrap();
    first
        .append_installation_input(NodeId(1), id, 0, records[0].clone())
        .unwrap();
    first
        .append_installation_batch(
            NodeId(1),
            id,
            crate::VsrHistoryBatch {
                ordinal: 1,
                records: records[1..].to_vec(),
            },
        )
        .unwrap();
    drop(first);
    let directory = path
        .join("1/vsr-install-input")
        .join(id.iter().map(|b| format!("{b:02x}")).collect::<String>())
        .join("records");
    let store = SegmentStore::open(&directory).unwrap();
    let segment = directory
        .join("segments")
        .join(&store.active_segment().file_name);
    drop(store);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(segment)
        .unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let mut first = open(&path.join("1"));
    assert_eq!(
        first
            .begin_installation_input(NodeId(1), NodeId(1), input)
            .unwrap(),
        1
    );
    assert!(first.finish_installation_input(NodeId(1), id).is_err());
    assert_eq!(first.view(), Epoch(1));
    first
        .append_installation_batch(
            NodeId(1),
            id,
            crate::VsrHistoryBatch {
                ordinal: 1,
                records: records[1..].to_vec(),
            },
        )
        .unwrap();
    assert_eq!(
        first.finish_installation_input(NodeId(1), id).unwrap(),
        Epoch(2)
    );
    assert_eq!(
        first.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    drop(second);
}

#[test]
fn missing_selected_installation_input_cannot_be_recreated_silently() {
    let path = root("input-missing");
    let (mut first, second, snapshots) = fixture(&path);
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    let id = input.id().unwrap();
    first
        .begin_installation_input(NodeId(1), NodeId(1), input.clone())
        .unwrap();
    first
        .append_installation_input(NodeId(1), id, 0, snapshots[0].records[0].clone())
        .unwrap();
    drop(first);
    let directory = path
        .join("1/vsr-install-input")
        .join(id.iter().map(|b| format!("{b:02x}")).collect::<String>());
    std::fs::remove_dir_all(directory.join("records")).unwrap();
    let mut first = open(&path.join("1"));
    assert!(first.installation_input().is_err());
    assert!(
        first
            .begin_installation_input(NodeId(1), NodeId(1), input)
            .is_err()
    );
    assert_eq!(first.view(), Epoch(1));
    drop(first);
    drop(second);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn a_new_input_reclaims_only_owned_inactive_uploads_after_durable_selection() {
    let path = root("input-reclaim");
    let (mut first, mut second, snapshots) = fixture(&path);
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    first
        .begin_installation_input(NodeId(1), NodeId(1), input)
        .unwrap();
    let base = path.join("1/vsr-install-input");
    std::fs::create_dir(base.join("operator-notes")).unwrap();
    let mut higher = request();
    higher.target_view = Epoch(3);
    let snapshots = vec![
        first
            .begin_view_change(NodeId(1), NodeId(1), higher.clone())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(1), higher)
            .unwrap(),
    ];
    let input = VsrInstallationInput::from_snapshots(&snapshots).unwrap();
    first
        .begin_installation_input(NodeId(1), NodeId(1), input)
        .unwrap();
    assert_eq!(std::fs::read_dir(base).unwrap().count(), 2);
    drop(first);
    drop(second);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn installed_authority_rejects_local_shortcuts_across_restart_but_accepts_next_quorum_view() {
    let path = root("authority-shortcuts");
    let (mut first, mut second, snapshots) = fixture(&path);
    let plan = VsrViewInstallation::select(&snapshots).unwrap();
    first.install_view(&plan).unwrap();
    second.install_view(&plan).unwrap();
    for reopen in [false, true] {
        if reopen {
            drop(first);
            first = DurableVsrCell::open_existing(path.join("1"), PlacementGroupId(1)).unwrap();
        }
        let before = SegmentStore::recover(first.store.root())
            .unwrap()
            .recovery
            .wal_records;
        for decision in [
            first.view_change(Epoch(3), NodeId(2)),
            first.timeout_election(),
            first.reconfigure(Epoch(3), NodeId(2), [NodeId(1), NodeId(2)]),
        ] {
            let VsrDecision::Rejected { reason, .. } = decision else {
                panic!("unilateral authority changed")
            };
            assert!(
                reason.contains("quorum") || reason.contains("certified"),
                "{reason}"
            );
        }
        assert_eq!(first.view(), Epoch(2));
        assert_eq!(first.primary(), NodeId(1));
        assert_eq!(first.replicas(), vec![NodeId(0), NodeId(1), NodeId(2)]);
        assert_eq!(first.committed_tail(), LogIndex(1));
        assert_eq!(
            SegmentStore::recover(first.store.root())
                .unwrap()
                .recovery
                .wal_records,
            before
        );
    }
    let request = VsrViewChangeRequest {
        pg: PlacementGroupId(1),
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        voters: first.replicas(),
    };
    let reports = [
        first
            .begin_view_change(NodeId(1), NodeId(2), request.clone())
            .unwrap(),
        second
            .begin_view_change(NodeId(2), NodeId(2), request)
            .unwrap(),
    ];
    let next = VsrViewInstallation::select(&reports).unwrap();
    first.install_view(&next).unwrap();
    second.install_view(&next).unwrap();
    let prepared = second.reprepare(NodeId(2), LogIndex(2)).unwrap();
    first
        .receive_prepare(NodeId(1), NodeId(2), prepared)
        .unwrap();
    second.acknowledge(NodeId(1), Epoch(3), LogIndex(2));
    let VsrDecision::Committed(commit) = second.commit(NodeId(2), LogIndex(2)).unwrap() else {
        panic!("new quorum cannot commit")
    };
    first.receive_commit(NodeId(1), NodeId(2), commit).unwrap();
    assert_eq!(first.view(), Epoch(3));
    assert_eq!(first.committed_tail(), LogIndex(2));
    assert_eq!(
        first.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    drop(first);
    drop(second);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn replay_rejects_unilateral_authority_records_after_quorum_installation() {
    for membership in [false, true] {
        let path = root(if membership {
            "forged-membership"
        } else {
            "forged-view"
        });
        let (mut first, second, snapshots) = fixture(&path);
        first
            .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
            .unwrap();
        let payload = if membership {
            encode_membership_change(&VsrMembershipChange {
                pg: PlacementGroupId(1),
                view: Epoch(3),
                primary: NodeId(2),
                committed_tail: LogIndex(1),
                old_replicas: [NodeId(0), NodeId(1), NodeId(2)].into(),
                new_replicas: [NodeId(1), NodeId(2)].into(),
            })
        } else {
            encode_view_change(&VsrViewChange {
                pg: PlacementGroupId(1),
                view: Epoch(3),
                primary: NodeId(2),
                committed_tail: LogIndex(1),
            })
        };
        // Valid checksums and legacy local-change encoding must not manufacture
        // authority after an installation, including state from older software.
        first
            .store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(2),
                WalRecordKind::Membership,
                &payload,
            )
            .unwrap();
        drop(first);
        let error = DurableVsrCell::open_existing(path.join("1"), PlacementGroupId(1)).unwrap_err();
        assert_eq!(error.code, rsf_core::ErrorCode::CorruptionDetected);
        assert!(
            error.message.contains("unilateral authority change"),
            "{}",
            error.message
        );
        drop(second);
        std::fs::remove_dir_all(path).unwrap();
    }
}
