use super::*;
use crate::{Epoch, SyncPolicy, sync_observation};
use std::sync::atomic::{AtomicU64, Ordering};

fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = std::env::temp_dir().join(format!(
            "rsf-owned-active-renewal-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!("cannot reserve renewal fixture: {error}"),
        }
    }
}

fn options(policy: SyncPolicy) -> SegmentStoreOptions {
    SegmentStoreOptions {
        log_options: SegmentLogOptions {
            sync_policy: policy,
        },
        max_segment_bytes: 1,
    }
}
fn fixture(path: &Path, policy: SyncPolicy) -> SegmentStore {
    let mut store = SegmentStore::open_with_options(path, options(policy)).unwrap();
    for payload in [b"first".as_slice(), b"second", b"active"] {
        store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Prepare,
                payload,
            )
            .unwrap();
    }
    store
}
fn cursor(store: &SegmentStore) -> String {
    format!("{:?}", store.cursor)
}

#[test]
fn active_renewal_syncs_only_actual_nonempty_descriptor_and_preserves_cursor_without_reads() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("exact");
        let mut store = fixture(&path, policy);
        assert_eq!(store.manifest.segments.len(), 3);
        let stamp = store.capture_owned_custody().unwrap();
        let before = cursor(&store);
        let bytes = fs::read(&store.active.path).unwrap();
        let manifest = fs::read(path.join(MANIFEST_FILE)).unwrap();
        let reads = crate::reader::payload_reads();
        let observed = sync_observation::watch(&path, None);
        store.sync_owned_active_segment(&stamp).unwrap();
        let events = observed.events();
        assert_eq!(events.len(), 1);
        assert!(events[0].completed);
        assert_eq!(events[0].path, store.active.path);
        assert_eq!(events[0].policy, policy);
        assert_eq!(cursor(&store), before);
        assert_eq!(crate::reader::payload_reads(), reads);
        assert_eq!(fs::read(&store.active.path).unwrap(), bytes);
        assert_eq!(fs::read(path.join(MANIFEST_FILE)).unwrap(), manifest);
        store.validate_owned_custody(&stamp).unwrap();
        drop(observed);
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn active_renewal_real_sync_boundary_failure_returns_no_success_and_cannot_clear_prior_latch() {
    for policy in [SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("failure");
        let mut store = fixture(&path, policy);
        let stamp = store.capture_owned_custody().unwrap();
        let active = store.active.path.clone();
        let bytes = fs::read(&active).unwrap();
        let observed = sync_observation::watch(&path, Some(0));
        let error = store.sync_owned_active_segment(&stamp).unwrap_err();
        assert!(error.message.contains("injected append sync failure"));
        assert!(store.write_failed && store.cursor.is_none());
        assert!(store.sync_owned_active_segment(&stamp).is_err());
        let events = observed.events();
        assert_eq!(events.len(), 1);
        assert!(!events[0].completed);
        assert_eq!(events[0].policy, policy);
        assert_eq!(fs::read(&active).unwrap(), bytes);
        drop(observed);
        drop(store);
        // Simulated syscall-boundary failure is uncertain durability, not
        // power loss. Cold reopen must still renew the visible retained bytes.
        let mut reopened = SegmentStore::open_with_options(&path, options(policy)).unwrap();
        let original = reopened.capture_owned_custody().unwrap();
        let observed = sync_observation::watch(&path, None);
        reopened.sync_owned_closed_segments(&original).unwrap();
        reopened.sync_owned_active_segment(&original).unwrap();
        assert_eq!(observed.events().len(), 3);
        assert!(observed.events().iter().all(|event| event.completed));
        assert_eq!(fs::read(&active).unwrap(), bytes);
        drop(observed);
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn active_renewal_injected_sync_unwind_keeps_owner_fenced_without_claiming_syscall_completion() {
    let path = root("panic");
    let mut store = fixture(&path, SyncPolicy::Full);
    let stamp = store.capture_owned_custody().unwrap();
    let bytes = fs::read(&store.active.path).unwrap();
    let observed = sync_observation::watch_panic(&path, 0);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = store.sync_owned_active_segment(&stamp);
    }));
    assert!(result.is_err());
    assert!(store.write_failed);
    assert!(store.sync_owned_active_segment(&stamp).is_err());
    assert!(store.capture_owned_custody().is_err());
    let events = observed.events();
    assert_eq!(events.len(), 1);
    assert!(!events[0].completed);
    assert_eq!(fs::read(&store.active.path).unwrap(), bytes);
    drop(observed);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn active_renewal_incomplete_bytes_on_held_inode_reject_without_tail_repair() {
    let path = root("held-torn");
    let mut store = fixture(&path, SyncPolicy::Data);
    let stamp = store.capture_owned_custody().unwrap();
    let active = store.active.path.clone();
    OpenOptions::new()
        .append(true)
        .open(&active)
        .unwrap()
        .write_all(b"RSF1-torn")
        .unwrap();
    let damaged = fs::read(&active).unwrap();
    let reads = crate::reader::payload_reads();
    let observed = sync_observation::watch(&path, None);
    assert!(store.sync_owned_active_segment(&stamp).is_err());
    assert!(store.write_failed && store.cursor.is_none());
    assert_eq!(fs::read(&active).unwrap(), damaged);
    assert_eq!(crate::reader::payload_reads(), reads);
    assert!(observed.events().is_empty());
    drop(observed);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn active_renewal_changed_torn_tail_never_scans_or_repairs_and_restoration_stays_fenced() {
    let path = root("torn");
    let mut store = fixture(&path, SyncPolicy::Data);
    let stamp = store.capture_owned_custody().unwrap();
    let active = store.active.path.clone();
    let saved = path.join("original-active");
    fs::rename(&active, &saved).unwrap();
    let mut damaged = fs::read(&saved).unwrap();
    damaged.extend_from_slice(b"RSF1-torn");
    fs::write(&active, &damaged).unwrap();
    let reads = crate::reader::payload_reads();
    let observed = sync_observation::watch(&path, None);
    assert!(store.sync_owned_active_segment(&stamp).is_err());
    assert!(store.write_failed && store.cursor.is_none());
    assert_eq!(fs::read(&active).unwrap(), damaged);
    assert_eq!(crate::reader::payload_reads(), reads);
    assert!(observed.events().is_empty());
    // Restore the actual original inode, not a same-byte new file, before
    // checking sticky failure. Renewal must not clear an already-failed owner.
    fs::remove_file(&active).unwrap();
    fs::rename(&saved, &active).unwrap();
    assert!(store.sync_owned_active_segment(&stamp).is_err());
    assert!(observed.events().is_empty());
    drop(observed);
    drop(store);
    let mut reopened = SegmentStore::open(&path).unwrap();
    let stamp = reopened.capture_owned_custody().unwrap();
    reopened.sync_owned_active_segment(&stamp).unwrap();
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn active_renewal_oversized_manifest_rejects_without_parser_read_refresh_or_repair() {
    let path = root("manifest");
    let mut store = fixture(&path, SyncPolicy::Data);
    let stamp = store.capture_owned_custody().unwrap();
    let manifest = path.join(MANIFEST_FILE);
    let saved = path.join("original-manifest");
    fs::rename(&manifest, &saved).unwrap();
    let huge = 128 * 1024 * 1024;
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&manifest)
        .unwrap()
        .set_len(huge)
        .unwrap();
    let reads = crate::reader::payload_reads();
    let observed = sync_observation::watch(&path, None);
    assert!(store.sync_owned_active_segment(&stamp).is_err());
    assert_eq!(fs::metadata(&manifest).unwrap().len(), huge);
    assert_eq!(crate::reader::payload_reads(), reads);
    assert!(observed.events().is_empty());
    fs::remove_file(&manifest).unwrap();
    fs::rename(saved, &manifest).unwrap();
    assert!(store.sync_owned_active_segment(&stamp).is_err());
    assert!(store.write_failed && store.cursor.is_none());
    drop(observed);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn active_renewal_empty_and_none_policy_never_claim_a_data_flush_or_adopt_foreign_stamp() {
    for policy in [SyncPolicy::None, SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("empty");
        let mut store = SegmentStore::open_with_options(&path, options(policy)).unwrap();
        let stamp = store.capture_owned_custody().unwrap();
        let before = cursor(&store);
        let observed = sync_observation::watch(&path, None);
        let result = store.sync_owned_active_segment(&stamp);
        if policy == SyncPolicy::None {
            assert_eq!(result.unwrap_err().code, ErrorCode::InvalidInput);
        } else {
            result.unwrap();
        }
        assert!(!store.write_failed);
        assert_eq!(cursor(&store), before);
        assert!(observed.events().is_empty());
        drop(observed);
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
    let left = root("left");
    let right = root("right");
    let mut a = fixture(&left, SyncPolicy::Data);
    let b = fixture(&right, SyncPolicy::Data);
    let foreign = b.capture_owned_custody().unwrap();
    assert!(
        a.sync_owned_active_segment(&foreign)
            .unwrap_err()
            .message
            .contains("another store handle")
    );
    assert!(a.write_failed && a.cursor.is_none());
    drop(a);
    drop(b);
    fs::remove_dir_all(left).unwrap();
    fs::remove_dir_all(right).unwrap();
}
