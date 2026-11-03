use super::*;
use std::fs;

struct Root(PathBuf);
impl Root {
    fn new(label: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "rsf-log-publication-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )))
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("segment publication fixture retained: {}", self.0.display());
        } else {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
fn full() -> SegmentLogOptions {
    SegmentLogOptions {
        sync_policy: SyncPolicy::Full,
    }
}
fn open_with_barrier(
    path: &Path,
    publish: impl FnOnce(&Path) -> io::Result<()>,
) -> RsfResult<SegmentLog> {
    // Match the public open path, including recovery which can create the file.
    let lock = SegmentLock::acquire(path)?;
    SegmentLog::recover_unlocked(path)?;
    SegmentLog::open_locked_with_publication(path.to_path_buf(), lock, full(), publish)
}
fn assert_lock_released(path: &Path) {
    let external = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .unwrap();
    external
        .try_lock()
        .expect("failed publication leaked writer ownership");
    external.unlock().unwrap();
}

#[test]
fn newly_created_segment_is_present_before_publication_and_failure_returns_no_writer() {
    let root = Root::new("new");
    let path = root.0.join("nested/segment");
    let mut reached = false;
    let error = open_with_barrier(&path, |opened| {
        reached = true;
        assert_eq!(opened, path);
        assert!(opened.is_file());
        assert_eq!(fs::metadata(opened)?.len(), 0);
        Err(io::Error::other("injected file publication failure"))
    })
    .unwrap_err();
    assert!(reached);
    assert!(error.message.contains("Sync"));
    assert!(error.message.contains("injected"));
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    assert_lock_released(&path);

    let mut retried = false;
    let mut log = open_with_barrier(&path, |opened| {
        retried = true;
        assert!(opened.is_file());
        durable_directory::sync_file_publication(opened)
    })
    .unwrap();
    assert!(retried, "existing-file retry skipped publication");
    log.append(b"first acknowledged payload").unwrap();
    drop(log);
    assert_eq!(
        SegmentLog::recover(&path).unwrap().records[0].payload,
        b"first acknowledged payload"
    );
}

#[test]
fn existing_segment_failure_preserves_wal_and_releases_lock_before_retry() {
    let root = Root::new("existing");
    let path = root.0.join("segment");
    let mut log = SegmentLog::open_with_options(&path, full()).unwrap();
    let (_, first) = log
        .append_wal(WalAppend {
            pg: PlacementGroupId(7),
            epoch: Epoch(3),
            index: LogIndex(1),
            kind: WalRecordKind::Membership,
            previous_hash: [0; 32],
            payload: b"authority",
        })
        .unwrap();
    drop(log);
    let bytes = fs::read(&path).unwrap();
    assert!(
        open_with_barrier(&path, |opened| {
            assert_eq!(fs::read(opened)?, bytes);
            Err(io::Error::other("injected existing-file barrier failure"))
        })
        .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_lock_released(&path);
    let mut log = SegmentLog::open_with_options(&path, full()).unwrap();
    log.append_wal(WalAppend {
        pg: PlacementGroupId(7),
        epoch: Epoch(3),
        index: LogIndex(2),
        kind: WalRecordKind::Membership,
        previous_hash: first.record_hash,
        payload: b"promise",
    })
    .unwrap();
    drop(log);
    let retained = SegmentLog::recover(&path).unwrap();
    assert_eq!(retained.wal_records.len(), 2);
    assert_eq!(retained.wal_records[0], first);
    assert_eq!(retained.wal_records[1].payload, b"promise");
}

#[test]
fn direct_open_locked_creation_also_executes_publication_before_returning() {
    let root = Root::new("locked");
    let path = root.0.join("segment");
    let lock = SegmentLock::acquire(&path).unwrap();
    assert!(!path.exists());
    let mut reached = false;
    assert!(
        SegmentLog::open_locked_with_publication(path.clone(), lock, full(), |opened| {
            reached = true;
            assert!(opened.is_file());
            Err(io::Error::other(
                "injected open-created file barrier failure",
            ))
        })
        .is_err()
    );
    assert!(reached);
    assert_lock_released(&path);
}

#[test]
fn generic_manifestless_segment_rejection_still_preserves_residue() {
    let root = Root::new("manifestless");
    let segments = root.0.join("segments");
    fs::create_dir_all(&segments).unwrap();
    let residue = segments.join("000000000000000001.segment");
    fs::write(&residue, b"unowned retained bytes").unwrap();
    assert!(SegmentStore::open(&root.0).is_err());
    assert!(!root.0.join("MANIFEST").exists());
    assert_eq!(fs::read(residue).unwrap(), b"unowned retained bytes");
}
