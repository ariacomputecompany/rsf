use super::*;
use crate::bounded::tests::{root, snapshot};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, symlink};

fn spec() -> VsrBootstrapSpec {
    VsrBootstrapSpec::new(
        PlacementGroupId(7),
        Epoch(3),
        NodeId(1),
        [NodeId(2), NodeId(0), NodeId(1)],
    )
    .unwrap()
}
fn limits() -> VsrPrefixLimits {
    VsrPrefixLimits {
        max_records: 64,
        max_encoded_bytes: 1024 * 1024,
    }
}
fn error() -> RsfError {
    invalid_input("native publication refused (test callback)")
}
struct Template {
    manifest: Vec<u8>,
    frame: Vec<u8>,
}
fn ordinary(extra: bool) -> Template {
    let path = root("factory-ordinary-codec");
    let s = spec();
    let mut cell = DurableVsrCell::open(
        &path,
        s.pg(),
        s.view(),
        s.primary(),
        s.voters().iter().copied(),
    )
    .unwrap();
    if extra {
        assert!(matches!(
            cell.prepare(
                NodeId(1),
                VsrOperation::ProtocolOperation {
                    operation_id: OperationId(1)
                },
                1
            )
            .unwrap(),
            VsrDecision::Prepared(_)
        ));
    }
    let result = Template {
        manifest: fs::read(path.join("MANIFEST")).unwrap(),
        frame: fs::read(path.join("segments").join(INITIAL_SEGMENT)).unwrap(),
    };
    drop(cell);
    fs::remove_dir_all(path).unwrap();
    result
}
fn seed(target: &Path, template: &Template, end: usize) {
    fs::create_dir_all(target.join("segments")).unwrap();
    fs::write(target.join(AUXILIARY), []).unwrap();
    fs::write(target.join("MANIFEST"), &template.manifest).unwrap();
    fs::write(target.join("MANIFEST.lock"), []).unwrap();
    fs::write(
        target.join("segments").join(INITIAL_SEGMENT),
        &template.frame[..end],
    )
    .unwrap();
    fs::write(target.join("segments").join(INITIAL_SIDECAR), []).unwrap();
}
fn open(target: &Path) -> ProvisionalVsrVoter {
    ProvisionalVsrVoter::open(target, spec(), limits()).unwrap()
}
fn assert_bootstrap(cell: &DurableVsrCell) {
    assert_eq!(cell.view(), Epoch(3));
    assert_eq!(cell.primary(), NodeId(1));
    assert_eq!(cell.replicas(), [NodeId(0), NodeId(1), NodeId(2)]);
    assert_eq!(cell.committed_tail(), LogIndex(0));
    assert_eq!(cell.next_index(), LogIndex(1));
    assert!(cell.committed_prefix().unwrap().is_empty());
}

#[test]
fn provisional_vsr_spec_uses_canonical_encoder_and_rejects_invalid_policy_before_io() {
    let s = spec();
    let equivalent = VsrBootstrapSpec::new(
        s.pg(),
        s.view(),
        s.primary(),
        [NodeId(1), NodeId(2), NodeId(0)],
    )
    .unwrap();
    assert_eq!(s, equivalent);
    let model = VsrCell::new(s.pg(), s.view(), s.primary(), s.voters().iter().copied()).unwrap();
    assert_eq!(s.bytes, encode_bootstrap(&model));
    assert_eq!(
        s.bootstrap_digest(),
        rsf_core::Digest::blake3_bytes(&encode_bootstrap(&model)).bytes
    );
    for invalid in [
        VsrBootstrapSpec::new(s.pg(), Epoch(0), s.primary(), [NodeId(1)]),
        VsrBootstrapSpec::new(s.pg(), s.view(), s.primary(), []),
        VsrBootstrapSpec::new(s.pg(), s.view(), s.primary(), [NodeId(0)]),
        VsrBootstrapSpec::new(s.pg(), s.view(), s.primary(), [NodeId(1), NodeId(1)]),
    ] {
        assert_eq!(invalid.unwrap_err().code, ErrorCode::InvalidInput);
    }
    let path = root("factory-policy");
    let target = path.join("absent");
    for policy in [
        VsrPrefixLimits {
            max_records: 0,
            ..limits()
        },
        VsrPrefixLimits {
            max_records: 1,
            max_encoded_bytes: s.bytes.len() as u64 + 17,
        },
        VsrPrefixLimits {
            max_records: u64::MAX,
            max_encoded_bytes: u64::MAX,
        },
    ] {
        let failure = ProvisionalVsrVoter::open(&target, s.clone(), policy).unwrap_err();
        assert_eq!(failure.code, ErrorCode::InvalidInput);
        assert!(
            !target.exists(),
            "invalid finite policy must precede mkdir or lock creation"
        );
    }
    let exact = VsrPrefixLimits {
        max_records: 1,
        max_encoded_bytes: s.bytes.len() as u64 + 18,
    };
    let cell = ProvisionalVsrVoter::open(&target, s, exact)
        .unwrap()
        .finish_with(|event| {
            assert_eq!(event.limits(), exact);
            event.validate()
        })
        .unwrap();
    assert_bootstrap(&cell);
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn provisional_vsr_completion_consumes_same_actual_store_and_auxiliary_into_voter() {
    let path = root("factory-owner-transfer");
    let target = path.join("voter");
    let provisional = open(&target);
    let original = provisional.custody.clone();
    let lock = provisional.writer_lock.metadata().unwrap();
    let before = snapshot(&target);
    assert_eq!(
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(7), limits())
            .unwrap_err()
            .code,
        ErrorCode::QuorumUnavailable
    );
    let mut called = false;
    let cell = provisional
        .finish_with(|event| {
            assert!(!called);
            called = true;
            assert_eq!(event.bootstrap(), &spec());
            assert_eq!(event.limits(), limits());
            assert_eq!(event.path(), target);
            event.validate()?;
            assert_eq!(
                DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(7), limits())
                    .unwrap_err()
                    .code,
                ErrorCode::QuorumUnavailable
            );
            Ok(())
        })
        .unwrap();
    assert!(called);
    cell.store.validate_owned_custody(&original).unwrap();
    let actual = cell._writer_lock.metadata().unwrap();
    assert_eq!((actual.dev(), actual.ino()), (lock.dev(), lock.ino()));
    assert_eq!(
        snapshot(&target),
        before,
        "handoff must not append or reinitialize bootstrap"
    );
    assert_bootstrap(&cell);
    drop(cell);
    let cold =
        DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(7), limits()).unwrap();
    assert_bootstrap(&cold);
    assert_eq!(snapshot(&target), before);
    drop(cold);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn provisional_vsr_cold_reserved_prefixes_complete_ordinary_codec_bytes_without_replacement() {
    let template = ordinary(false);
    let points = [
        0,
        1,
        4,
        23,
        24,
        template.frame.len() - 1,
        template.frame.len(),
    ];
    for end in points {
        let path = root("factory-cold-prefix");
        let target = path.join("voter");
        seed(&target, &template, end);
        let segment = target.join("segments").join(INITIAL_SEGMENT);
        let inode = fs::metadata(&segment).unwrap().ino();
        let provisional = open(&target);
        assert_eq!(fs::read(&segment).unwrap(), template.frame);
        assert_eq!(fs::metadata(&segment).unwrap().ino(), inode);
        let cell = provisional.finish_with(|event| event.validate()).unwrap();
        assert_bootstrap(&cell);
        assert_eq!(fs::read(&segment).unwrap(), template.frame);
        assert_eq!(fs::metadata(&segment).unwrap().ino(), inode);
        drop(cell);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn provisional_vsr_foreign_history_markers_aliases_and_hardlinks_are_not_repaired() {
    let template = ordinary(false);
    let extra = ordinary(true);
    for damage in [
        "foreign-bootstrap",
        "extra-wal",
        "checksum",
        "VSR_ACTIVE",
        "VSR_PROMOTION",
        "VSR_PUBLISHED",
        "foreign-child",
        "segment-alias",
        "lock-hardlink",
    ] {
        let path = root("factory-foreign");
        let target = path.join("voter");
        seed(
            &target,
            if damage == "extra-wal" {
                &extra
            } else {
                &template
            },
            if damage == "extra-wal" {
                extra.frame.len()
            } else {
                template.frame.len()
            },
        );
        let segment = target.join("segments").join(INITIAL_SEGMENT);
        let requested = if damage == "foreign-bootstrap" {
            VsrBootstrapSpec::new(
                PlacementGroupId(7),
                Epoch(4),
                NodeId(1),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .unwrap()
        } else {
            spec()
        };
        match damage {
            "checksum" => {
                let mut bytes = fs::read(&segment).unwrap();
                *bytes.last_mut().unwrap() ^= 1;
                fs::write(&segment, bytes).unwrap();
            }
            "segment-alias" => {
                let held = path.join("original-segment");
                fs::rename(&segment, &held).unwrap();
                symlink(&held, &segment).unwrap();
            }
            "lock-hardlink" => {
                fs::hard_link(target.join(AUXILIARY), path.join("auxiliary-link")).unwrap();
            }
            "foreign-bootstrap" | "extra-wal" => {}
            name => fs::write(target.join(name), b"foreign authority").unwrap(),
        }
        let before = snapshot(&path);
        let failure = ProvisionalVsrVoter::open(&target, requested, limits()).unwrap_err();
        assert_eq!(
            failure.code,
            ErrorCode::CorruptionDetected,
            "{damage}: {failure:?}"
        );
        assert_eq!(snapshot(&path), before, "{damage} must remain unchanged");
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn provisional_vsr_failed_or_unwound_callback_returns_no_owner_and_allows_only_cold_retry() {
    for unwind in [false, true] {
        let path = root("factory-callback-failure");
        let target = path.join("voter");
        let provisional = open(&target);
        let before = snapshot(&target);
        let called = std::cell::Cell::new(false);
        if unwind {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                provisional.finish_with(|event| {
                    called.set(true);
                    event.validate().unwrap();
                    panic!("injected native callback unwind");
                })
            }));
            let panic = result.expect_err("callback unwind must not return a voter");
            assert_eq!(
                panic.downcast_ref::<&str>(),
                Some(&"injected native callback unwind")
            );
        } else {
            let failure = provisional
                .finish_with(|event| {
                    called.set(true);
                    event.validate()?;
                    Err(error())
                })
                .unwrap_err();
            assert_eq!(failure.code, ErrorCode::InvalidInput);
            assert_eq!(
                failure.message,
                "native publication refused (test callback)"
            );
        }
        assert!(called.get());
        assert_eq!(snapshot(&target), before);
        // The failed wrapper was consumed. A separately authorized cold Reserved
        // construction gets new ownership; this is not warm catch-and-reopen.
        let cell = open(&target).finish_with(|event| event.validate()).unwrap();
        assert_bootstrap(&cell);
        assert_eq!(snapshot(&target), before);
        drop(cell);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn provisional_vsr_completion_rejects_original_file_substitution_for_ok_and_error_callbacks() {
    for name in [
        AUXILIARY,
        "MANIFEST",
        "MANIFEST.lock",
        "segments/000000000000000001.segment",
        "segments/000000000000000001.lock",
    ] {
        for callback_error in [false, true] {
            let path = root("factory-completion-owner");
            let target = path.join("voter");
            let provisional = open(&target);
            let file = target.join(name);
            let displaced = path.join("original-held-file");
            let bytes = fs::read(&file).unwrap();
            let failure = provisional
                .finish_with(|event| {
                    event.validate()?;
                    fs::rename(&file, &displaced).unwrap();
                    fs::copy(&displaced, &file).unwrap();
                    assert_ne!(
                        fs::metadata(&file).unwrap().ino(),
                        fs::metadata(&displaced).unwrap().ino()
                    );
                    if callback_error { Err(error()) } else { Ok(()) }
                })
                .unwrap_err();
            assert_eq!(
                failure.code,
                ErrorCode::CorruptionDetected,
                "{name}: {failure:?}"
            );
            assert_ne!(
                failure.message,
                "native publication refused (test callback)"
            );
            assert_eq!(fs::read(&file).unwrap(), bytes);
            assert_eq!(fs::read(&displaced).unwrap(), bytes);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn provisional_vsr_observed_custody_failure_stays_sticky_after_original_inode_restoration() {
    let path = root("factory-sticky-restoration");
    let target = path.join("voter");
    let provisional = open(&target);
    let before = snapshot(&target);
    let file = target.join(AUXILIARY);
    let original = fs::metadata(&file).unwrap().ino();
    let displaced = path.join("actual-original-lock");
    let failure = provisional
        .finish_with(|event| {
            event.validate()?;
            fs::rename(&file, &displaced).unwrap();
            fs::write(&file, []).unwrap();
            assert_eq!(
                event.validate().unwrap_err().code,
                ErrorCode::CorruptionDetected
            );
            fs::remove_file(&file).unwrap();
            fs::rename(&displaced, &file).unwrap();
            assert_eq!(fs::metadata(&file).unwrap().ino(), original);
            let sticky = event.validate().unwrap_err();
            assert_eq!(sticky.code, ErrorCode::CorruptionDetected);
            assert_eq!(
                sticky.message,
                "provisional voter custody failed; cold reserved recovery required"
            );
            Ok(())
        })
        .unwrap_err();
    assert_eq!(failure.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        failure.message,
        "provisional voter custody failed; cold reserved recovery required"
    );
    assert_eq!(snapshot(&target), before);
    let cell = open(&target).finish_with(|event| event.validate()).unwrap();
    assert_bootstrap(&cell);
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn provisional_vsr_completion_reads_actual_bytes_after_callback_even_with_matching_metadata() {
    let path = root("factory-readback");
    let target = path.join("voter");
    let provisional = open(&target);
    let file = target.join("segments").join(INITIAL_SEGMENT);
    let mut corrupt_bytes = Vec::new();
    let failure = provisional
        .finish_with(|event| {
            event.validate()?;
            let modified = fs::metadata(&file).unwrap().modified().unwrap();
            corrupt_bytes = fs::read(&file).unwrap();
            *corrupt_bytes.last_mut().unwrap() ^= 1;
            fs::write(&file, &corrupt_bytes).unwrap();
            OpenOptions::new()
                .write(true)
                .open(&file)
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(modified))
                .unwrap();
            // Establish that a metadata-only postcheck would pass. The final error
            // must come from actual parsing, not this deliberate mutation's mtime.
            event.owner.validate_owner().unwrap();
            Ok(())
        })
        .unwrap_err();
    assert_eq!(failure.code, ErrorCode::CorruptionDetected);
    assert!(failure.message.starts_with("checksum mismatch at offset 0"));
    assert_eq!(fs::read(&file).unwrap(), corrupt_bytes);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn provisional_vsr_initialized_loss_uses_strict_existing_noncreating_recovery() {
    for missing in [
        "whole",
        AUXILIARY,
        "MANIFEST",
        "MANIFEST.lock",
        "segments/000000000000000001.segment",
        "segments/000000000000000001.lock",
    ] {
        let path = root("factory-initialized-loss");
        let target = path.join("voter");
        drop(open(&target).finish_with(|event| event.validate()).unwrap());
        if missing == "whole" {
            fs::remove_dir_all(&target).unwrap();
        } else {
            fs::remove_file(target.join(missing)).unwrap();
        }
        let before = snapshot(&path);
        let failure = DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(7), limits())
            .unwrap_err();
        if missing == "whole" || missing == AUXILIARY {
            assert_eq!(failure.code, ErrorCode::CorruptionDetected);
        } else {
            assert_eq!(failure.code, ErrorCode::InvalidInput);
            assert!(failure.message.starts_with("storage Recover failed:"));
        }
        assert_eq!(
            snapshot(&path),
            before,
            "strict recovery must not invoke the provisional factory"
        );
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn provisional_vsr_original_directory_and_auxiliary_guard_span_factory_boundaries() {
    for at in [Boundary::Root, Boundary::Auxiliary, Boundary::Storage] {
        let path = root("factory-own-boundaries");
        let target = path.join("voter");
        let mut reached = false;
        let failure = ProvisionalVsrVoter::open_with(&target, spec(), limits(), &mut |phase| {
            if phase == at {
                reached = true;
                if phase == Boundary::Root {
                    fs::rename(&target, path.join("original-root")).unwrap();
                    fs::create_dir(&target).unwrap();
                } else {
                    let file = if phase == Boundary::Auxiliary {
                        target.join(AUXILIARY)
                    } else {
                        target.join("MANIFEST")
                    };
                    let original = path.join("original-held");
                    fs::rename(&file, &original).unwrap();
                    fs::copy(&original, &file).unwrap();
                }
            }
            Ok(())
        })
        .unwrap_err();
        assert!(reached);
        assert_eq!(failure.code, ErrorCode::CorruptionDetected);
        assert!(!target.join("VSR_ACTIVE").exists());
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn provisional_vsr_process_child() {
    let Some(root) = std::env::var_os("RSF_PROVISIONAL_VSR_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let at = std::env::var("RSF_PROVISIONAL_VSR_PHASE").unwrap();
    let ready = |name: &str| {
        if name == at {
            println!("RSF_PROVISIONAL_VSR_READY={name}");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        }
    };
    let owner = ProvisionalVsrVoter::open_with(&root, spec(), limits(), &mut |phase| {
        ready(match phase {
            Boundary::Root => "root",
            Boundary::Auxiliary => "auxiliary",
            Boundary::Storage => "storage",
        });
        Ok(())
    })
    .unwrap();
    owner
        .finish_with(|event| {
            event.validate()?;
            ready("completion");
            Ok(())
        })
        .unwrap();
    panic!("requested child boundary did not fire");
}

#[test]
fn provisional_vsr_actual_process_kill_releases_nonvoting_owner_for_cold_reserved_resume() {
    for at in ["root", "auxiliary", "storage", "completion"] {
        let path = root("factory-process-kill");
        let target = path.join("voter");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "bounded::factory::tests::provisional_vsr_process_child",
                "--nocapture",
            ])
            .env("RSF_PROVISIONAL_VSR_ROOT", &target)
            .env("RSF_PROVISIONAL_VSR_PHASE", at)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufRead::lines(std::io::BufReader::new(stdout)) {
                let Ok(line) = line else {
                    break;
                };
                if line.contains(&format!("RSF_PROVISIONAL_VSR_READY={at}")) {
                    let _ = send.send(());
                    break;
                }
            }
        });
        let reached = receive.recv_timeout(std::time::Duration::from_secs(15));
        let killed = child.kill();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        assert!(
            reached.is_ok(),
            "child did not reach {at}: {reached:?}, {status}"
        );
        killed.unwrap();
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(9)
        );
        let cell = open(&target).finish_with(|event| event.validate()).unwrap();
        assert_bootstrap(&cell);
        drop(cell);
        let cold =
            DurableVsrCell::open_existing_bounded(&target, PlacementGroupId(7), limits()).unwrap();
        assert_bootstrap(&cold);
        drop(cold);
        fs::remove_dir_all(path).unwrap();
    }
}
