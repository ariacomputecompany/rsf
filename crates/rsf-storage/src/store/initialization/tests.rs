use super::factory::{Boundary, resume_with_hook};
use super::*;
use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt, symlink};

struct Fixture(PathBuf);
impl Fixture {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "rsf-empty-factory-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn root(&self) -> PathBuf {
        self.0.join("records")
    }
    fn segment(&self) -> PathBuf {
        self.root().join(SEGMENT_DIR).join(segment_file_name(1))
    }
    fn empty_segment(&self) {
        fs::create_dir_all(self.root().join(SEGMENT_DIR)).unwrap();
        fs::write(self.root().join("MANIFEST.lock"), []).unwrap();
        File::create(self.segment()).unwrap().sync_all().unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn inventory(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, output: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let metadata = fs::symlink_metadata(path).unwrap();
        let relative = path.strip_prefix(root).unwrap().to_path_buf();
        if metadata.file_type().is_symlink() {
            output.insert(
                relative,
                format!("link:{}", fs::read_link(path).unwrap().display()).into_bytes(),
            );
        } else if metadata.is_dir() {
            output.insert(relative, b"directory".to_vec());
            for entry in fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), output);
            }
        } else {
            output.insert(relative, fs::read(path).unwrap());
        }
    }
    let mut output = BTreeMap::new();
    visit(root, root, &mut output);
    output
}
fn assert_rejected_without_change(fixture: &Fixture) {
    let before = inventory(&fixture.0);
    assert!(SegmentStore::initialization_pending(fixture.root()).is_err());
    assert!(SegmentStore::resume_empty_initialization(fixture.root()).is_err());
    assert_eq!(inventory(&fixture.0), before);
}
fn assert_published_empty(root: &Path) {
    assert!(!SegmentStore::initialization_pending(root).unwrap());
    assert_eq!(
        fs::read(root.join(MANIFEST_FILE)).unwrap(),
        SegmentManifest::initial().encode().as_bytes()
    );
    let recovered = SegmentStore::recover_existing(root).unwrap();
    assert!(recovered.recovery.records.is_empty());
    assert!(recovered.recovery.wal_records.is_empty());
    assert!(recovered.recovery.maintenance_records.is_empty());
}

#[test]
fn exact_factory_prefixes_are_read_only_until_explicit_resume() {
    let expected = SegmentManifest::initial().encode();
    for phase in 0..8 {
        let fixture = Fixture::new("prefix");
        match phase {
            0 => {}
            1 => fs::create_dir(fixture.root()).unwrap(),
            2 => {
                fs::create_dir(fixture.root()).unwrap();
                fs::write(fixture.root().join("MANIFEST.lock"), []).unwrap();
            }
            3 => fs::create_dir_all(fixture.root().join(SEGMENT_DIR)).unwrap(),
            _ => {
                fixture.empty_segment();
                if phase >= 5 {
                    let count = match phase {
                        5 => 0,
                        6 => expected.len() / 2,
                        _ => expected.len(),
                    };
                    fs::write(
                        fixture.root().join("MANIFEST.tmp"),
                        &expected.as_bytes()[..count],
                    )
                    .unwrap();
                }
            }
        }
        let before = inventory(&fixture.0);
        let segment_identity = (phase >= 4).then(|| {
            let metadata = fs::metadata(fixture.segment()).unwrap();
            (metadata.dev(), metadata.ino())
        });
        assert!(SegmentStore::initialization_pending(fixture.root()).unwrap());
        assert_eq!(
            inventory(&fixture.0),
            before,
            "classification created or rewrote files"
        );
        SegmentStore::resume_empty_initialization(fixture.root()).unwrap();
        if let Some(identity) = segment_identity {
            let metadata = fs::metadata(fixture.segment()).unwrap();
            assert_eq!(
                (metadata.dev(), metadata.ino()),
                identity,
                "initial segment was replaced"
            );
        }
        assert_published_empty(&fixture.root());
    }
}

#[test]
fn manifestless_empty_segment_stays_invalid_to_generic_recovery() {
    let fixture = Fixture::new("strict");
    fixture.empty_segment();
    let before = inventory(&fixture.0);
    assert!(SegmentStore::recover_existing(fixture.root()).is_err());
    assert!(SegmentStore::open(fixture.root()).is_err());
    assert!(SegmentStore::recover(fixture.root()).is_err());
    assert!(SegmentStore::recover_or_quarantine(fixture.root()).is_err());
    assert_eq!(inventory(&fixture.0), before);
    SegmentStore::resume_empty_initialization(fixture.root()).unwrap();
    assert_published_empty(&fixture.root());
}

#[test]
fn final_manifest_presence_never_enters_initialization_and_options_are_not_inferred() {
    let fixture = Fixture::new("options");
    SegmentStore::resume_empty_initialization(fixture.root()).unwrap();
    let options = SegmentStoreOptions {
        max_segment_bytes: 4096,
        log_options: SegmentLogOptions {
            sync_policy: crate::SyncPolicy::Full,
        },
    };
    let mut store = SegmentStore::open_with_options(fixture.root(), options).unwrap();
    store
        .append(b"data under explicitly supplied nondefault options")
        .unwrap();
    drop(store);
    let before = inventory(&fixture.0);
    assert!(!SegmentStore::initialization_pending(fixture.root()).unwrap());
    assert!(SegmentStore::resume_empty_initialization(fixture.root()).is_err());
    assert_eq!(inventory(&fixture.0), before);
    fs::write(
        fixture.root().join(MANIFEST_FILE),
        b"corrupt final manifest",
    )
    .unwrap();
    let before = inventory(&fixture.0);
    assert!(
        !SegmentStore::initialization_pending(fixture.root()).unwrap(),
        "false is not a health claim"
    );
    assert!(SegmentStore::recover_existing(fixture.root()).is_err());
    assert!(SegmentStore::resume_empty_initialization(fixture.root()).is_err());
    assert_eq!(inventory(&fixture.0), before);
}

#[test]
fn nonempty_foreign_and_noncanonical_factory_states_are_never_repaired() {
    for fault in [
        "segment-byte",
        "partial-header",
        "extra-segment",
        "segment-lock",
        "foreign-root",
        "quarantine",
        "bad-temp",
        "long-temp",
        "temp-without-segment",
        "nonzero-lock",
        "segment-directory",
    ] {
        let fixture = Fixture::new(fault);
        fixture.empty_segment();
        match fault {
            "segment-byte" => fs::write(fixture.segment(), [0]).unwrap(),
            "partial-header" => fs::write(fixture.segment(), b"RSF1").unwrap(),
            "extra-segment" => fs::write(
                fixture.root().join(SEGMENT_DIR).join(segment_file_name(2)),
                [],
            )
            .unwrap(),
            "segment-lock" => fs::write(
                fixture
                    .root()
                    .join(SEGMENT_DIR)
                    .join("000000000000000001.lock"),
                [],
            )
            .unwrap(),
            "foreign-root" => fs::write(fixture.root().join("foreign"), b"preserve").unwrap(),
            "quarantine" => fs::create_dir(fixture.root().join("quarantine")).unwrap(),
            "bad-temp" => fs::write(
                fixture.root().join("MANIFEST.tmp"),
                b"rsf-segment-manifest 2\n",
            )
            .unwrap(),
            "long-temp" => fs::write(
                fixture.root().join("MANIFEST.tmp"),
                format!("{}extra", SegmentManifest::initial().encode()),
            )
            .unwrap(),
            "temp-without-segment" => {
                fs::remove_file(fixture.segment()).unwrap();
                fs::write(fixture.root().join("MANIFEST.tmp"), b"r").unwrap();
            }
            "nonzero-lock" => {
                fs::write(fixture.root().join("MANIFEST.lock"), b"foreign lock data").unwrap()
            }
            "segment-directory" => {
                fs::remove_file(fixture.segment()).unwrap();
                fs::create_dir(fixture.segment()).unwrap();
            }
            _ => unreachable!(),
        }
        assert_rejected_without_change(&fixture);
    }
    let fixture = Fixture::new("real-wal");
    let mut store = SegmentStore::open(fixture.root()).unwrap();
    store
        .append_next_wal(
            PlacementGroupId(1),
            rsf_core::Epoch(1),
            WalRecordKind::Prepare,
            b"acknowledged WAL",
        )
        .unwrap();
    drop(store);
    fs::remove_file(fixture.root().join(MANIFEST_FILE)).unwrap();
    fs::remove_file(
        fixture
            .root()
            .join(SEGMENT_DIR)
            .join("000000000000000001.lock"),
    )
    .unwrap();
    assert_rejected_without_change(&fixture);
}

#[test]
fn symlink_and_hardlink_factory_files_cannot_redirect_reads_or_writes() {
    for route in ["root", "segments", "segment", "temporary", "lock", "final"] {
        let fixture = Fixture::new(route);
        fixture.empty_segment();
        let foreign = fixture.0.join("foreign");
        if matches!(route, "root" | "segments") {
            fs::create_dir(&foreign).unwrap();
            fs::write(foreign.join("sentinel"), b"do not touch").unwrap();
        } else {
            fs::write(&foreign, b"do not touch").unwrap();
        }
        let destination = match route {
            "root" => {
                fs::remove_dir_all(fixture.root()).unwrap();
                fixture.root()
            }
            "segments" => {
                fs::remove_dir_all(fixture.root().join(SEGMENT_DIR)).unwrap();
                fixture.root().join(SEGMENT_DIR)
            }
            "segment" => {
                fs::remove_file(fixture.segment()).unwrap();
                fixture.segment()
            }
            "temporary" => fixture.root().join("MANIFEST.tmp"),
            "lock" => {
                fs::remove_file(fixture.root().join("MANIFEST.lock")).unwrap();
                fixture.root().join("MANIFEST.lock")
            }
            "final" => fixture.root().join(MANIFEST_FILE),
            _ => unreachable!(),
        };
        symlink(&foreign, destination).unwrap();
        assert_rejected_without_change(&fixture);
        if route == "root" {
            assert!(SegmentStore::initialization_pending(fixture.root().join(".")).is_err());
            assert!(SegmentStore::resume_empty_initialization(fixture.root().join(".")).is_err());
        }
    }
    for route in ["segment", "temporary", "lock"] {
        let fixture = Fixture::new("hardlink");
        fixture.empty_segment();
        let foreign = fixture.0.join("foreign");
        fs::write(&foreign, []).unwrap();
        let destination = match route {
            "segment" => fixture.segment(),
            "temporary" => fixture.root().join("MANIFEST.tmp"),
            _ => fixture.root().join("MANIFEST.lock"),
        };
        if destination.exists() {
            fs::remove_file(&destination).unwrap();
        }
        fs::hard_link(foreign, destination).unwrap();
        assert_rejected_without_change(&fixture);
    }
}

const BOUNDARIES: [Boundary; 8] = [
    Boundary::RootSynced,
    Boundary::LockAcquired,
    Boundary::SegmentsSynced,
    Boundary::SegmentSynced,
    Boundary::TemporaryCreated,
    Boundary::TemporarySynced,
    Boundary::ManifestRenamed,
    Boundary::ManifestSynced,
];

#[test]
fn every_factory_interruption_resumes_or_is_already_published_without_data_reset() {
    for boundary in BOUNDARIES {
        let fixture = Fixture::new("interrupt");
        assert!(
            resume_with_hook(&fixture.root(), |at| {
                if at == boundary {
                    Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        "injected factory interruption",
                    ))
                } else {
                    Ok(())
                }
            })
            .is_err()
        );
        if SegmentStore::initialization_pending(fixture.root()).unwrap() {
            SegmentStore::resume_empty_initialization(fixture.root()).unwrap();
        }
        assert_published_empty(&fixture.root());
    }
}

#[test]
fn empty_factory_requires_existing_parent_and_excludes_an_active_manifest_owner() {
    let fixture = Fixture::new("owner");
    let missing_parent = fixture.0.join("absent/records");
    let before = inventory(&fixture.0);
    assert!(SegmentStore::initialization_pending(&missing_parent).is_err());
    assert!(SegmentStore::resume_empty_initialization(&missing_parent).is_err());
    assert_eq!(inventory(&fixture.0), before);
    fixture.empty_segment();
    let owner = SegmentLock::acquire(&fixture.root().join(MANIFEST_FILE)).unwrap();
    let before = inventory(&fixture.0);
    assert!(SegmentStore::resume_empty_initialization(fixture.root()).is_err());
    assert_eq!(inventory(&fixture.0), before);
    drop(owner);
    SegmentStore::resume_empty_initialization(fixture.root()).unwrap();
}

#[test]
fn initialization_kill_child() {
    let Ok(root) = std::env::var("RSF_EMPTY_FACTORY_CHILD_ROOT") else {
        return;
    };
    let index: usize = std::env::var("RSF_EMPTY_FACTORY_CHILD_BOUNDARY")
        .unwrap()
        .parse()
        .unwrap();
    let root = PathBuf::from(root);
    resume_with_hook(&root, |boundary| {
        if boundary == BOUNDARIES[index] {
            let marker = root.parent().unwrap().join("factory-paused");
            let mut file = File::create(&marker).unwrap();
            file.write_all(&[index as u8]).unwrap();
            file.sync_all().unwrap();
            File::open(root.parent().unwrap())
                .unwrap()
                .sync_all()
                .unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(())
    })
    .unwrap();
    panic!("child did not reach requested boundary");
}

#[test]
fn killed_factory_owner_releases_custody_and_preserves_resumable_physical_state() {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for index in 0..BOUNDARIES.len() {
        let fixture = Fixture::new("killed");
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::initialization::tests::initialization_kill_child",
                    "--nocapture",
                ])
                .env("RSF_EMPTY_FACTORY_CHILD_ROOT", fixture.root())
                .env("RSF_EMPTY_FACTORY_CHILD_BOUNDARY", index.to_string())
                // Keep child harness output in diagnostics: inherited stdout
                // can split an unrelated parent test's name/result line.
                .stdout(std::io::stderr())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + std::time::Duration::from_secs(20);
        while !fixture.0.join("factory-paused").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "factory child exited early"
            );
            assert!(
                Instant::now() < deadline,
                "factory child did not reach boundary"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        if SegmentStore::initialization_pending(fixture.root()).unwrap() {
            SegmentStore::resume_empty_initialization(fixture.root()).unwrap();
        }
        assert_published_empty(&fixture.root());
    }
}
