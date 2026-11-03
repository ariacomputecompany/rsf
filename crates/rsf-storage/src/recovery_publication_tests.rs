use super::*;
use std::fs;

struct Root(PathBuf);
impl Root {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "rsf-recovery-publication-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        )))
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "recovery publication fixture retained: {}",
                self.0.display()
            );
        } else {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
fn attempts() -> usize {
    durable_directory::sync_attempts()
}

#[test]
fn strict_existing_replay_does_not_publish_directory_ancestry() {
    let root = Root::new("existing");
    let mut store = SegmentStore::open(&root.0).unwrap();
    store
        .append_next_wal(
            PlacementGroupId(7),
            Epoch(3),
            WalRecordKind::Membership,
            b"retained authority",
        )
        .unwrap();
    drop(store);
    let before = attempts();
    for _ in 0..3 {
        let recovered = SegmentStore::recover_existing(&root.0).unwrap();
        assert_eq!(
            recovered.recovery.wal_records[0].payload,
            b"retained authority"
        );
    }
    assert_eq!(
        attempts(),
        before,
        "read scans repeated factory publication barriers"
    );
    drop(SegmentStore::open(&root.0).unwrap());
    assert!(
        attempts() > before,
        "writable reopen omitted its custody boundary"
    );
}

#[test]
fn create_capable_store_recovery_publishes_fresh_and_existing_routes() {
    let root = Root::new("store-create");
    let target = root.0.join("outer/store");
    let before = attempts();
    assert!(
        SegmentStore::recover(&target)
            .unwrap()
            .recovery
            .wal_records
            .is_empty()
    );
    assert!(attempts() > before);
    assert!(target.join("MANIFEST").is_file());
    for quarantine in [false, true] {
        let before = attempts();
        let recovered = if quarantine {
            SegmentStore::recover_or_quarantine(&target)
        } else {
            SegmentStore::recover(&target)
        }
        .unwrap();
        assert!(recovered.recovery.wal_records.is_empty());
        assert!(
            attempts() > before,
            "create-capable recovery inferred existing means published"
        );
    }
}

#[test]
fn public_segment_recovery_publishes_created_file_and_existing_retry() {
    let root = Root::new("log-create");
    let file = root.0.join("outer/log");
    for _ in 0..2 {
        let before = attempts();
        assert!(SegmentLog::recover(&file).unwrap().records.is_empty());
        assert!(file.is_file());
        assert!(
            attempts() > before,
            "public create-capable recovery omitted post-file publication"
        );
    }
}

#[test]
fn owned_segment_recovery_never_recreates_a_missing_acknowledged_file() {
    let root = Root::new("owned-log");
    let file = root.0.join("log");
    let mut log = SegmentLog::open_with_options(
        &file,
        SegmentLogOptions {
            sync_policy: SyncPolicy::Full,
        },
    )
    .unwrap();
    log.append(b"acknowledged").unwrap();
    let before = attempts();
    assert_eq!(
        log.recover_current().unwrap().records[0].payload,
        b"acknowledged"
    );
    assert_eq!(attempts(), before);
    fs::remove_file(&file).unwrap();
    assert!(log.recover_current().is_err());
    assert!(
        !file.exists(),
        "owned recovery recreated lost custody as an empty file"
    );
    assert_eq!(attempts(), before);
}

#[test]
fn ephemeral_segment_lock_does_not_grant_durable_directory_publication() {
    let root = Root::new("lock");
    let file = root.0.join("outer/log");
    let before = attempts();
    let lock = SegmentLock::acquire(&file).unwrap();
    assert_eq!(attempts(), before);
    assert!(file.with_extension("lock").is_file());
    assert!(!file.exists());
    drop(lock);
    drop(SegmentLog::open(&file).unwrap());
    assert!(
        attempts() > before,
        "data writer did not independently publish custody"
    );
}

#[test]
fn strict_existing_recovery_keeps_missing_store_and_segment_fail_closed() {
    let root = Root::new("missing");
    let before = attempts();
    assert!(SegmentStore::recover_existing(&root.0).is_err());
    assert!(!root.0.exists());
    assert_eq!(attempts(), before);
    let store = SegmentStore::open(&root.0).unwrap();
    drop(store);
    let segment = fs::read_dir(root.0.join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension() == Some(std::ffi::OsStr::new("segment")))
        .unwrap();
    fs::remove_file(&segment).unwrap();
    let before = attempts();
    assert!(SegmentStore::recover_existing(&root.0).is_err());
    assert!(!segment.exists());
    assert_eq!(attempts(), before);
}
