//! Retention custody only: these tests do not activate membership or grant an
//! application acknowledgement. The real process test is intentionally opt-in
//! only through its exact child selector and never runs a publisher.
use super::*;

struct Prepared {
    fixture: Fixture,
    stages: Vec<MembershipStagingStore>,
    first: VsrMembershipStagedQuorum,
    alternate: VsrMembershipStagedQuorum,
}
fn prepared(path: &Path) -> Prepared {
    let fixture = fixture(path);
    let mut stages = (0..3)
        .map(|n| {
            MembershipStagingStore::stage(
                path.join(format!("stage-{n}")),
                NodeId(n as u64),
                plan(&fixture, false),
                &fixture.cells[n],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    stages.push(
        fixture
            .learner
            .stage_membership(path.join("stage-3"), plan(&fixture, true))
            .unwrap(),
    );
    let reports = stages
        .iter_mut()
        .map(|stage| stage.materialize(&mut |_| Ok(())).unwrap())
        .collect::<Vec<_>>();
    Prepared {
        fixture,
        stages,
        first: VsrMembershipStagedQuorum::from_reports(reports[..3].to_vec()).unwrap(),
        alternate: VsrMembershipStagedQuorum::from_reports(reports[1..].to_vec()).unwrap(),
    }
}

#[test]
fn membership_publication_input_codec_and_bound_local_stage_reject_tampering() {
    let path = root("publication-retention-codec");
    let mut p = prepared(&path);
    let local = p.stages[0].report().unwrap();
    let first = super::super::publication::encode(&local, &p.first).unwrap();
    assert_eq!(
        super::super::publication::decode(&first).unwrap(),
        (local.clone(), p.first.clone())
    );
    for length in 0..first.len() {
        assert!(super::super::publication::decode(&first[..length]).is_err());
    }
    let mut checksum = first.clone();
    *checksum.last_mut().unwrap() ^= 1;
    assert!(super::super::publication::decode(&checksum).is_err());
    let mut trailing = first.clone();
    trailing.push(0);
    assert!(super::super::publication::decode(&trailing).is_err());
    p.stages[0].retain_publication(&p.first).unwrap();
    p.stages[1].retain_publication(&p.first).unwrap();
    drop(p.stages.remove(0));
    drop(p.stages.remove(0));
    let target = path.join("stage-0");
    let alternate = super::super::publication::encode(&local, &p.alternate).unwrap();
    assert_ne!(alternate, first);
    // Equivalent proofs are accepted as retries, never as a replacement of the
    // first file already bound in V2 metadata.
    fs::write(target.join("PUBLICATION"), &alternate).unwrap();
    assert!(
        MembershipStagingStore::open_existing(&target, PlacementGroupId(1), NodeId(0)).is_err()
    );
    fs::write(target.join("PUBLICATION"), &first).unwrap();
    let target = path.join("stage-1");
    // Rebind both checksums consistently: the local report still identifies
    // stage-0 and must not be accepted under stage-1's independently sealed data.
    let mut meta = fs::read(target.join("META")).unwrap();
    let end = meta.len();
    meta[end - 64..end - 32].copy_from_slice(Digest::blake3_bytes(&first).as_bytes());
    let checksum = Digest::blake3_bytes(&meta[..end - 32]);
    meta[end - 32..].copy_from_slice(checksum.as_bytes());
    fs::write(target.join("PUBLICATION"), &first).unwrap();
    fs::write(target.join("META"), &meta).unwrap();
    assert!(
        MembershipStagingStore::open_existing(&target, PlacementGroupId(1), NodeId(1)).is_err()
    );
    assert_eq!(fs::read(target.join("PUBLICATION")).unwrap(), first);
    assert_eq!(fs::read(target.join("META")).unwrap(), meta);
    drop(p);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_publication_retention_preserves_first_quorum_and_requires_boot_materialization() {
    let path = root("publication-retention-reopen");
    let mut p = prepared(&path);
    let stage = p.stages.remove(0);
    let id = stage.stage_id();
    assert!(stage.publication_quorum().unwrap().is_none());
    let v1 = fs::read(path.join("stage-0/META")).unwrap();
    drop(stage);
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-0"), PlacementGroupId(1), NodeId(0))
            .unwrap();
    assert!(stage.retain_publication(&p.first).is_err());
    assert!(!path.join("stage-0/PUBLICATION").exists());
    assert_eq!(fs::read(path.join("stage-0/META")).unwrap(), v1);
    stage.materialize(&mut |_| Ok(())).unwrap();
    stage.retain_publication(&p.first).unwrap();
    let publication = fs::read(path.join("stage-0/PUBLICATION")).unwrap();
    let v2 = fs::read(path.join("stage-0/META")).unwrap();
    assert!(v1.starts_with(b"VSRMST1"));
    assert!(v2.starts_with(b"VSRMST2"));
    assert_eq!(v2.len(), v1.len() + 32);
    assert_eq!(stage.stage_id(), id);
    assert_ne!(p.first, p.alternate);
    // The alternate majority excludes this local node; its durable local stage
    // remains bound separately and the canonical chosen prefix is unchanged.
    stage.retain_publication(&p.alternate).unwrap();
    assert_eq!(
        fs::read(path.join("stage-0/PUBLICATION")).unwrap(),
        publication
    );
    assert_eq!(fs::read(path.join("stage-0/META")).unwrap(), v2);
    assert_eq!(stage.publication_quorum().unwrap(), Some(p.first.clone()));
    drop(stage);
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-0"), PlacementGroupId(1), NodeId(0))
            .unwrap();
    assert_eq!(stage.publication_quorum().unwrap(), Some(p.first.clone()));
    assert!(stage.report().is_err());
    assert!(stage.retain_publication(&p.alternate).is_err());
    stage.materialize(&mut |_| Ok(())).unwrap();
    stage.retain_publication(&p.alternate).unwrap();
    assert_eq!(
        fs::read(path.join("stage-0/PUBLICATION")).unwrap(),
        publication
    );
    assert_eq!(fs::read(path.join("stage-0/META")).unwrap(), v2);
    drop(stage);
    // The normal stage factory also retains the first chosen/retention bytes.
    let stage = MembershipStagingStore::stage(
        path.join("stage-0"),
        NodeId(0),
        plan(&p.fixture, true),
        &p.fixture.cells[0],
    )
    .unwrap();
    assert_eq!(stage.stage_id(), id);
    assert_eq!(stage.publication_quorum().unwrap(), Some(p.first));
    assert!(stage.report().is_err());
    drop(stage);
    drop(p.stages);
    drop(p.fixture);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_publication_retention_rejects_conflicting_authority_without_overwrite() {
    let path = root("publication-retention-conflicts");
    let mut p = prepared(&path);
    p.stages[0].retain_publication(&p.first).unwrap();
    let before = ["META", "PUBLICATION", "INSTALLED"]
        .map(|name| fs::read(path.join("stage-0").join(name)).unwrap());
    for field in [
        "proposal",
        "input",
        "plan",
        "committed",
        "accepted",
        "local-stage",
    ] {
        let mut reports = p.first.reports().to_vec();
        for report in &mut reports {
            match field {
                "proposal" => report.proposal.election.target_view.0 += 1,
                "input" => report.input[0] ^= 1,
                "plan" => report.plan[0] ^= 1,
                "committed" => report.committed_tail = LogIndex(0),
                "accepted" => report.accepted_tail.0 += 1,
                _ if report.replica == NodeId(0) => report.stage[0] ^= 1,
                _ => {}
            }
        }
        let conflicting = VsrMembershipStagedQuorum::from_reports(reports).unwrap();
        assert!(
            p.stages[0].retain_publication(&conflicting).is_err(),
            "{field}"
        );
        assert_eq!(
            ["META", "PUBLICATION", "INSTALLED"]
                .map(|name| fs::read(path.join("stage-0").join(name)).unwrap()),
            before,
        );
        assert_eq!(
            p.stages[0].publication_quorum().unwrap(),
            Some(p.first.clone())
        );
    }
    drop(p);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_publication_retention_corruption_or_missing_custody_never_recreates_authority() {
    let path = root("publication-retention-damage");
    let mut p = prepared(&path);
    p.stages[0].retain_publication(&p.first).unwrap();
    drop(p.stages.remove(0));
    let target = path.join("stage-0");
    let names = ["META", "PUBLICATION", "INSTALLED"];
    let intact = names.map(|name| fs::read(target.join(name)).unwrap());
    for damage in [
        "missing-publication",
        "corrupt-publication",
        "missing-receipt",
        "missing-meta",
        "missing-meta-and-receipt",
    ] {
        for (name, bytes) in names.iter().zip(&intact) {
            fs::write(target.join(name), bytes).unwrap();
        }
        match damage {
            "missing-publication" => fs::remove_file(target.join("PUBLICATION")).unwrap(),
            "corrupt-publication" => fs::write(target.join("PUBLICATION"), b"torn").unwrap(),
            "missing-receipt" => fs::remove_file(target.join("INSTALLED")).unwrap(),
            "missing-meta" => fs::remove_file(target.join("META")).unwrap(),
            _ => {
                fs::remove_file(target.join("META")).unwrap();
                fs::remove_file(target.join("INSTALLED")).unwrap();
            }
        }
        let before = names.map(|name| fs::read(target.join(name)).ok());
        for _ in 0..2 {
            assert!(
                MembershipStagingStore::open_existing(&target, PlacementGroupId(1), NodeId(0))
                    .is_err(),
                "{damage}"
            );
            assert!(
                MembershipStagingStore::stage(
                    &target,
                    NodeId(0),
                    plan(&p.fixture, false),
                    &p.fixture.cells[0]
                )
                .is_err(),
                "{damage}"
            );
            assert_eq!(
                names.map(|name| fs::read(target.join(name)).ok()),
                before,
                "{damage}"
            );
        }
    }
    drop(p);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn membership_publication_retention_io_failure_fences_handle_and_resumes_durable_input() {
    for boundary in ["PUBLICATION", "META"] {
        let path = root(&format!("publication-retention-failure-{boundary}"));
        let mut p = prepared(&path);
        let target = path.join("stage-0");
        let obstruction = target.join(format!(".{boundary}.tmp"));
        fs::create_dir(&obstruction).unwrap();
        assert!(p.stages[0].retain_publication(&p.first).is_err());
        assert!(p.stages[0].report().is_err());
        assert!(p.stages[0].publication_quorum().is_err());
        assert!(p.stages[0].materialize(&mut |_| Ok(())).is_err());
        let input = fs::read(target.join("PUBLICATION")).ok();
        assert_eq!(input.is_some(), boundary == "META");
        assert!(
            fs::read(target.join("META"))
                .unwrap()
                .starts_with(b"VSRMST1")
        );
        drop(p.stages.remove(0));
        fs::remove_dir(&obstruction).unwrap();
        let mut stage =
            MembershipStagingStore::open_existing(&target, PlacementGroupId(1), NodeId(0)).unwrap();
        assert_eq!(
            stage.publication_quorum().unwrap().is_some(),
            boundary == "META"
        );
        assert!(stage.retain_publication(&p.alternate).is_err());
        stage.materialize(&mut |_| Ok(())).unwrap();
        stage.retain_publication(&p.alternate).unwrap();
        if let Some(input) = input {
            assert_eq!(fs::read(target.join("PUBLICATION")).unwrap(), input);
            assert_eq!(stage.publication_quorum().unwrap(), Some(p.first.clone()));
        }
        assert!(
            fs::read(target.join("META"))
                .unwrap()
                .starts_with(b"VSRMST2")
        );
        assert!(!target.join("VSR_ACTIVE").exists());
        drop(stage);
        drop(p);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn membership_publication_retention_rejects_foreign_temporary_paths_and_resumes_regular_torn_files()
{
    #[cfg(not(unix))]
    let cases = ["directory", "regular"];
    #[cfg(unix)]
    let cases = ["directory", "regular", "symlink"];
    for kind in cases {
        let path = root(&format!("publication-retention-temporary-{kind}"));
        let mut p = prepared(&path);
        let target = path.join("stage-0");
        let temporary = target.join(".PUBLICATION.tmp");
        let outside = path.join("outside-target");
        fs::write(&outside, b"unrelated retained bytes").unwrap();
        match kind {
            "regular" => fs::write(&temporary, b"torn unacknowledged bytes").unwrap(),
            "directory" => fs::create_dir(&temporary).unwrap(),
            #[cfg(unix)]
            "symlink" => std::os::unix::fs::symlink(&outside, &temporary).unwrap(),
            _ => unreachable!(),
        }
        let before = ["META", "INSTALLED"].map(|name| fs::read(target.join(name)).unwrap());
        let result = p.stages[0].retain_publication(&p.first);
        assert_eq!(fs::read(&outside).unwrap(), b"unrelated retained bytes");
        if kind == "regular" {
            result.unwrap();
            assert!(!temporary.exists());
            assert_eq!(
                p.stages[0].publication_quorum().unwrap(),
                Some(p.first.clone())
            );
        } else {
            assert!(result.is_err());
            assert!(p.stages[0].report().is_err());
            assert!(p.stages[0].publication_quorum().is_err());
            assert!(!target.join("PUBLICATION").exists());
            assert_eq!(
                ["META", "INSTALLED"].map(|name| fs::read(target.join(name)).unwrap()),
                before,
            );
            let metadata = fs::symlink_metadata(&temporary).unwrap();
            assert_eq!(metadata.file_type().is_symlink(), kind == "symlink");
            assert_eq!(metadata.is_dir(), kind == "directory");
        }
        drop(p);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn membership_publication_retention_process_child() {
    let Some(path) = std::env::var_os("RSF_PUBLICATION_RETENTION_ROOT") else {
        return;
    };
    let path = PathBuf::from(path);
    let boundary = std::env::var("RSF_PUBLICATION_RETENTION_BOUNDARY").unwrap();
    let mut stage =
        MembershipStagingStore::open_existing(path.join("stage-3"), PlacementGroupId(1), NodeId(3))
            .unwrap();
    stage
        .materialize(&mut |entry| write_application(&path, entry))
        .unwrap();
    let quorum =
        VsrMembershipStagedQuorum::decode(&fs::read(path.join("quorum.bin")).unwrap()).unwrap();
    let ready = || {
        println!("RSF_PUBLICATION_RETENTION_READY={boundary}");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    };
    stage
        .retain_publication_with_hook(&quorum, || {
            if boundary == "input" {
                ready();
            }
        })
        .unwrap();
    assert_eq!(boundary, "metadata");
    ready();
}

#[test]
fn killed_membership_publication_retention_recovers_input_without_activating_or_acknowledging() {
    for boundary in ["input", "metadata"] {
        let path = root(&format!("publication-retention-kill-{boundary}"));
        let p = prepared(&path);
        fs::write(path.join("quorum.bin"), p.first.encode().unwrap()).unwrap();
        let expected = p.first.clone();
        drop(p);
        fs::remove_dir_all(path.join("old")).unwrap();
        fs::remove_dir_all(path.join("learner")).unwrap();
        for n in 0..4 {
            fs::remove_dir_all(path.join(format!("decision-{n}"))).unwrap();
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "membership::staging::store::tests::publication_tests::membership_publication_retention_process_child", "--nocapture"])
            .env("RSF_PUBLICATION_RETENTION_ROOT", &path)
            .env("RSF_PUBLICATION_RETENTION_BOUNDARY", boundary)
            .stdout(std::process::Stdio::piped()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufRead::lines(std::io::BufReader::new(stdout)) {
                let Ok(line) = line else {
                    break;
                };
                if line.contains(&format!("RSF_PUBLICATION_RETENTION_READY={boundary}")) {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let ready = receive.recv_timeout(std::time::Duration::from_secs(15));
        let killed = child.kill();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        assert!(
            ready.is_ok(),
            "child did not reach {boundary}: {ready:?}, {status}"
        );
        killed.unwrap();
        assert!(!status.success());
        #[cfg(unix)]
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(9)
        );
        let target = path.join("stage-3");
        assert!(!target.join("VSR_ACTIVE").exists());
        assert!(!path.join("voter-3").exists());
        let bytes = fs::read(target.join("PUBLICATION")).unwrap();
        let meta = fs::read(target.join("META")).unwrap();
        assert_eq!(meta.starts_with(b"VSRMST2"), boundary == "metadata");
        let mut stage =
            MembershipStagingStore::open_existing(&target, PlacementGroupId(1), NodeId(3)).unwrap();
        assert_eq!(stage.publication_quorum().unwrap(), Some(expected.clone()));
        assert!(stage.report().is_err());
        assert!(stage.retain_publication(&expected).is_err());
        fs::remove_dir_all(path.join("application")).unwrap();
        stage
            .materialize(&mut |entry| write_application(&path, entry))
            .unwrap();
        stage.retain_publication(&expected).unwrap();
        assert_eq!(fs::read(target.join("PUBLICATION")).unwrap(), bytes);
        assert!(
            fs::read(target.join("META"))
                .unwrap()
                .starts_with(b"VSRMST2")
        );
        assert!(!target.join("VSR_ACTIVE").exists());
        drop(stage);
        fs::remove_dir_all(path).unwrap();
    }
}
