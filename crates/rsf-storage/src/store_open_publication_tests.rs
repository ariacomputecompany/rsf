//! Actual public store opens, directory syncs, lock release and WAL recovery.
//! Injected errors occur only after successful sync; they are not power cuts.
use super::*;
use crate::durable_directory::store_open_observation::{self as observed, Event};
use std::fs;
use std::os::unix::fs::{MetadataExt, symlink};
use std::sync::atomic::{AtomicU64, Ordering};

struct Root(PathBuf);
impl Root {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        loop {
            let path = std::env::temp_dir().join(format!(
                "rsf-store-open-publication-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("cannot reserve fixture: {error}"),
            }
        }
    }
    fn target(&self) -> PathBuf {
        self.0.join("container/store")
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "store-open publication fixture retained: {}",
                self.0.display()
            );
        } else {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn options() -> SegmentStoreOptions {
    SegmentStoreOptions {
        log_options: SegmentLogOptions {
            sync_policy: SyncPolicy::Full,
        },
        ..SegmentStoreOptions::default()
    }
}
fn route(target: &Path) -> Vec<PathBuf> {
    let lexical = if target.is_absolute() {
        target.to_path_buf()
    } else {
        std::env::current_dir().unwrap().join(target)
    }
    .join("segments");
    let physical = fs::canonicalize(&lexical).unwrap();
    let mut paths = physical
        .ancestors()
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    if lexical != physical {
        paths.extend(lexical.ancestors().map(Path::to_path_buf));
    }
    paths
}
fn file_route(file: &Path) -> Vec<PathBuf> {
    assert!(file.is_absolute());
    let physical = fs::canonicalize(file)
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let lexical = file.parent().unwrap();
    let mut paths = physical
        .ancestors()
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    if lexical != physical {
        paths.extend(lexical.ancestors().map(Path::to_path_buf));
    }
    paths
}
fn paths(events: &[Event]) -> Vec<PathBuf> {
    events.iter().map(|event| event.directory.clone()).collect()
}
fn repeated_route(route: &[PathBuf], cycles: usize) -> Vec<PathBuf> {
    (0..cycles).flat_map(|_| route.iter().cloned()).collect()
}
fn complete_routes(events: &[Event], target: &Path, cycles: usize) {
    assert_eq!(paths(events), repeated_route(&route(target), cycles));
}
fn phases(events: &[Event], target: &Path, fresh: bool) {
    let one = route(target).len();
    let final_start = if fresh { events.len() - one } else { 0 };
    let manifest = fs::read(target.join("MANIFEST")).unwrap();
    for (index, event) in events.iter().enumerate() {
        if index < final_start {
            assert!(
                event.manifest.is_none(),
                "premanifest route published early"
            );
            assert!(
                event.segments.is_empty(),
                "initial segment preceded its route"
            );
        } else {
            assert_eq!(event.manifest.as_deref(), Some(manifest.as_slice()));
            assert_eq!(event.segments.len(), 1, "final route needs actual segment");
            assert!(target.join("segments").join(&event.segments[0].0).is_file());
        }
    }
}
fn open_observed(target: &Path) -> (SegmentStore, SegmentStoreRecovery, Vec<Event>) {
    let watch = observed::watch(target, None);
    let (store, recovery) = SegmentStore::open_recovered(target, options()).unwrap();
    let events = watch.events();
    drop(watch);
    (store, recovery, events)
}
fn append(store: &mut SegmentStore, payload: &[u8]) -> rsf_core::storage::WalRecord {
    store
        .append_next_wal(
            PlacementGroupId(7),
            Epoch(3),
            WalRecordKind::Prepare,
            payload,
        )
        .unwrap()
        .2
}
struct Prefix {
    records: Vec<rsf_core::storage::WalRecord>,
    manifest: Vec<u8>,
    segment: PathBuf,
    bytes: Vec<u8>,
}
fn seed(target: &Path) -> Prefix {
    let mut store = SegmentStore::open_with_options(target, options()).unwrap();
    let records = vec![
        append(&mut store, b"acknowledged one"),
        append(&mut store, b"acknowledged two"),
    ];
    let segment = target
        .join("segments")
        .join(&store.active_segment().file_name);
    drop(store);
    Prefix {
        records,
        manifest: fs::read(target.join("MANIFEST")).unwrap(),
        bytes: fs::read(&segment).unwrap(),
        segment,
    }
}
fn assert_prefix(target: &Path, expected: &Prefix) {
    assert_eq!(
        fs::read(target.join("MANIFEST")).unwrap(),
        expected.manifest
    );
    assert_eq!(fs::read(&expected.segment).unwrap(), expected.bytes);
}
// Keep the original descriptors alive across the operation so an unlinked
// inode cannot be recycled into a replacement that happens to compare equal.
struct OriginalLock {
    path: PathBuf,
    file: File,
    identity: (u64, u64),
}
fn original_locks(target: &Path) -> Vec<OriginalLock> {
    let mut candidates = vec![target.join("MANIFEST.lock")];
    candidates.extend(
        fs::read_dir(target.join("segments"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "lock")
            }),
    );
    let original = candidates
        .into_iter()
        .map(|path| {
            let named = fs::symlink_metadata(&path).unwrap();
            assert!(named.is_file() && !named.file_type().is_symlink());
            let file = OpenOptions::new().write(true).open(&path).unwrap();
            let held = file.metadata().unwrap();
            let identity = (named.dev(), named.ino());
            assert_eq!((held.dev(), held.ino()), identity);
            OriginalLock {
                path,
                file,
                identity,
            }
        })
        .collect::<Vec<_>>();
    assert!(
        original.len() >= 2,
        "seed must provide manifest and segment locks"
    );
    original
}
fn original_locks_released(original: Vec<OriginalLock>) {
    for OriginalLock {
        path,
        file,
        identity,
    } in original
    {
        let named = fs::symlink_metadata(&path).unwrap();
        assert!(named.is_file() && !named.file_type().is_symlink());
        assert_eq!(
            (named.dev(), named.ino()),
            identity,
            "original lock replaced"
        );
        let lock = ExclusiveFileLock::acquire(file)
            .expect("failed open must release the retained original lock inode");
        let held = lock.metadata().unwrap();
        assert_eq!((held.dev(), held.ino()), identity);
        drop(lock);
        let after = fs::symlink_metadata(&path).unwrap();
        assert!(after.is_file() && !after.file_type().is_symlink());
        assert_eq!((after.dev(), after.ino()), identity);
        assert_eq!(fs::read(path).unwrap(), b"");
    }
}
fn released_locks(target: &Path) -> usize {
    let mut candidates = vec![target.join("MANIFEST.lock")];
    candidates.extend(
        fs::read_dir(target.join("segments"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "lock")
            }),
    );
    let mut count = 0;
    for path in candidates {
        let before = match fs::symlink_metadata(&path) {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => panic!("actual lock metadata failed: {error}"),
        };
        assert!(before.is_file() && !before.file_type().is_symlink());
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        let lock = ExclusiveFileLock::acquire(file)
            .expect("failed public open must release actual lock inode");
        let held = lock.metadata().unwrap();
        assert_eq!((held.dev(), held.ino()), (before.dev(), before.ino()));
        drop(lock);
        let after = fs::metadata(&path).unwrap();
        assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
        assert_eq!(fs::read(path).unwrap(), b"");
        count += 1;
    }
    count
}
fn uncertainty(error: &rsf_core::RsfError) {
    assert_eq!(error.code, rsf_core::ErrorCode::InvalidInput);
    assert!(
        error
            .message
            .ends_with("injected successful-sync return uncertainty (test only)"),
        "{error:?}"
    );
}
fn discovered_cycles() -> (usize, usize, usize) {
    let fixture = Root::new("discover");
    let target = fixture.target();
    let (store, recovery, fresh) = open_observed(&target);
    assert!(recovery.recovery.wal_records.is_empty());
    phases(&fresh, &target, true);
    drop(store);
    let (store, _, existing) = open_observed(&target);
    phases(&existing, &target, false);
    drop(store);
    let count = route(&target).len();
    assert_eq!(fresh.len() % count, 0);
    assert_eq!(existing.len() % count, 0);
    complete_routes(&fresh, &target, fresh.len() / count);
    complete_routes(&existing, &target, existing.len() / count);
    (fresh.len() / count, existing.len() / count, count)
}

#[test]
fn fresh_and_existing_store_open_use_two_and_one_complete_publication_routes() {
    for alias in [false, true] {
        let fixture = Root::new("route-budget");
        let target = if alias {
            crate::create_dir_all_synced(fixture.0.join("physical/deep")).unwrap();
            crate::create_dir_all_synced(fixture.0.join("logical")).unwrap();
            symlink(
                fixture.0.join("physical/deep"),
                fixture.0.join("logical/link"),
            )
            .unwrap();
            // Provision the alias outside the measured operation. ParentDir
            // remains filesystem-resolved, never lexically collapsed.
            File::open(fixture.0.join("logical"))
                .unwrap()
                .sync_all()
                .unwrap();
            fixture.0.join("logical/link/../new/store")
        } else {
            fixture.target()
        };
        let (mut store, recovery, fresh) = open_observed(&target);
        assert!(recovery.recovery.wal_records.is_empty());
        phases(&fresh, &target, true);
        let record = append(&mut store, b"real Full-sync payload");
        drop(store);
        let (mut store, recovery, existing) = open_observed(&target);
        phases(&existing, &target, false);
        assert_eq!(recovery.recovery.wal_records, vec![record.clone()]);
        let next = append(&mut store, b"next actual payload");
        drop(store);
        assert_eq!(
            SegmentStore::recover_existing(&target)
                .unwrap()
                .recovery
                .wal_records,
            vec![record, next]
        );
        // Observe both actual opens and complete WAL equality before the
        // budget assertion, so original-source red records both 3N and 2N.
        let one = route(&target).len();
        assert_eq!(
            (fresh.len(), existing.len()),
            (2 * one, one),
            "fresh/existing successful full-route sync counts; one route has {one} actual directories"
        );
        complete_routes(&fresh, &target, 2);
        complete_routes(&existing, &target, 1);
    }
}

#[test]
fn fresh_store_open_uncertainty_after_each_real_sync_returns_no_owner_and_retries_full_route() {
    let (fresh_cycles, existing_cycles, depth) = discovered_cycles();
    for fail_at in 0..fresh_cycles * depth {
        let fixture = Root::new("fresh-uncertainty");
        let target = fixture.target();
        let watch = observed::watch(&target, Some(fail_at));
        let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
        let events = watch.events();
        drop(watch);
        uncertainty(&error);
        assert_eq!(events.len(), fail_at + 1);
        let actual_route = route(&target);
        assert_eq!(actual_route.len(), depth);
        assert_eq!(
            paths(&events),
            repeated_route(&actual_route, fresh_cycles)[..=fail_at]
        );
        let has_manifest = target.join("MANIFEST").is_file();
        let final_route = fail_at >= (fresh_cycles - 1) * depth;
        assert_eq!(has_manifest, final_route);
        assert_eq!(events.last().unwrap().manifest.is_some(), has_manifest);
        if has_manifest {
            assert_eq!(events.last().unwrap().segments.len(), 1);
            assert!(released_locks(&target) >= 2);
        } else {
            assert!(
                events
                    .iter()
                    .all(|event| event.manifest.is_none() && event.segments.is_empty())
            );
            released_locks(&target);
        }
        let (mut store, recovered, retry) = open_observed(&target);
        complete_routes(
            &retry,
            &target,
            if has_manifest {
                existing_cycles
            } else {
                fresh_cycles
            },
        );
        phases(&retry, &target, !has_manifest);
        assert!(recovered.recovery.wal_records.is_empty());
        let record = append(&mut store, b"retry is a real usable writer");
        drop(store);
        assert_eq!(
            SegmentStore::recover_existing(&target)
                .unwrap()
                .recovery
                .wal_records,
            vec![record]
        );
    }
}

#[test]
fn existing_store_open_uncertainty_releases_original_locks_and_preserves_every_acknowledged_byte() {
    let (_, existing_cycles, depth) = discovered_cycles();
    for fail_at in 0..existing_cycles * depth {
        let fixture = Root::new("existing-uncertainty");
        let target = fixture.target();
        let prefix = seed(&target);
        let original = original_locks(&target);
        let watch = observed::watch(&target, Some(fail_at));
        let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
        let events = watch.events();
        drop(watch);
        uncertainty(&error);
        assert_eq!(events.len(), fail_at + 1);
        assert_eq!(
            paths(&events),
            repeated_route(&route(&target), existing_cycles)[..=fail_at]
        );
        assert!(
            events
                .iter()
                .all(|event| event.manifest.as_ref() == Some(&prefix.manifest)
                    && event.segments.len() == 1
                    && event.segments[0].1 == prefix.bytes.len() as u64)
        );
        assert_prefix(&target, &prefix);
        assert!(released_locks(&target) >= 2);
        original_locks_released(original);
        let (store, recovered, retry) = open_observed(&target);
        complete_routes(&retry, &target, existing_cycles);
        assert_eq!(recovered.recovery.wal_records, prefix.records);
        assert_prefix(&target, &prefix);
        drop(store);
    }
}

#[test]
fn torn_unacknowledged_tail_is_repaired_before_final_uncertainty_without_losing_acknowledged_prefix()
 {
    let (_, existing_cycles, depth) = discovered_cycles();
    for position in 0..depth {
        let fixture = Root::new("torn-before-final-route");
        let target = fixture.target();
        let prefix = seed(&target);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&prefix.segment)
            .unwrap();
        file.write_all(&[0xa5]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert_eq!(
            fs::metadata(&prefix.segment).unwrap().len(),
            prefix.bytes.len() as u64 + 1
        );
        // Select the final route from actual discovery on both original and
        // corrected source; early duplicate routes must not mask the repair.
        let fail_at = (existing_cycles - 1) * depth + position;
        let original = original_locks(&target);
        let watch = observed::watch(&target, Some(fail_at));
        let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
        let events = watch.events();
        drop(watch);
        uncertainty(&error);
        assert_eq!(events.len(), fail_at + 1);
        assert_eq!(
            events.last().unwrap().segments[0].1,
            prefix.bytes.len() as u64
        );
        assert_prefix(&target, &prefix);
        assert!(released_locks(&target) >= 2);
        original_locks_released(original);
        let (mut store, recovered, retry) = open_observed(&target);
        complete_routes(&retry, &target, existing_cycles);
        assert_eq!(recovered.recovery.wal_records, prefix.records);
        let next = append(&mut store, b"after repaired unacknowledged suffix");
        drop(store);
        let mut expected = prefix.records;
        expected.push(next);
        assert_eq!(
            SegmentStore::recover_existing(&target)
                .unwrap()
                .recovery
                .wal_records,
            expected
        );
    }
}

#[test]
fn double_alias_store_renews_resolved_directory_before_recovery_and_final_target_routes() {
    let fixture = Root::new("double-alias");
    let physical_store = fixture.0.join("physical/deep/store");
    let prefix = seed(&physical_store);
    crate::create_dir_all_synced(fixture.0.join("foreign/deep")).unwrap();
    crate::create_dir_all_synced(fixture.0.join("logical")).unwrap();
    let foreign = fixture.0.join("foreign/deep/retained-wal");
    fs::rename(&prefix.segment, &foreign).unwrap();
    symlink(&foreign, &prefix.segment).unwrap();
    let target = fixture.0.join("logical/store");
    symlink(&physical_store, &target).unwrap();
    // These alias routes are provisioned, not created by the measured factory.
    crate::durable_directory::sync_file_publication(&foreign).unwrap();
    File::open(prefix.segment.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    File::open(fixture.0.join("logical"))
        .unwrap()
        .sync_all()
        .unwrap();
    let active = target
        .join("segments")
        .join(prefix.segment.file_name().unwrap());
    let directory_route = route(&target);
    let final_route = file_route(&active);
    let hidden_ancestor = fs::canonicalize(fixture.0.join("physical/deep")).unwrap();
    assert!(directory_route.contains(&hidden_ancestor));
    assert!(
        !final_route.contains(&hidden_ancestor),
        "fixture must exercise the omitted physical route"
    );
    let expected = [directory_route.clone(), final_route.clone()].concat();
    let (store, recovered, events) = open_observed(&target);
    assert_eq!(paths(&events), expected);
    assert_eq!(recovered.recovery.wal_records, prefix.records);
    assert_eq!(fs::read(&foreign).unwrap(), prefix.bytes);
    drop(store);

    let mut file = OpenOptions::new().append(true).open(&foreign).unwrap();
    file.write_all(&[0xa5]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let torn = fs::read(&foreign).unwrap();
    let fail_at = directory_route
        .iter()
        .position(|path| path == &hidden_ancestor)
        .unwrap();
    let original = original_locks(&target);
    let watch = observed::watch(&target, Some(fail_at));
    let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
    let events = watch.events();
    drop(watch);
    uncertainty(&error);
    assert_eq!(paths(&events), directory_route[..=fail_at]);
    assert_eq!(events.last().unwrap().segments[0].1, torn.len() as u64);
    assert_eq!(
        fs::read(&foreign).unwrap(),
        torn,
        "fallback failure must precede any tail repair"
    );
    assert_eq!(fs::read(target.join("MANIFEST")).unwrap(), prefix.manifest);
    assert!(released_locks(&target) >= 2);
    original_locks_released(original);
    assert_eq!(fs::read_link(&target).unwrap(), physical_store);
    assert_eq!(fs::read_link(&prefix.segment).unwrap(), foreign);

    let (mut store, recovered, retry) = open_observed(&target);
    assert_eq!(paths(&retry), expected);
    assert_eq!(recovered.recovery.wal_records, prefix.records);
    assert_eq!(fs::read(&foreign).unwrap(), prefix.bytes);
    // The fallback route sees the torn suffix; only after that barrier does
    // ordinary recovery repair it before the final file-publication route.
    assert!(
        retry[..directory_route.len()]
            .iter()
            .all(|event| event.segments[0].1 == torn.len() as u64)
    );
    assert!(
        retry[directory_route.len()..]
            .iter()
            .all(|event| event.segments[0].1 == prefix.bytes.len() as u64)
    );
    let next = append(&mut store, b"double alias remains an actual writer");
    drop(store);
    let mut expected_records = prefix.records;
    expected_records.push(next);
    assert_eq!(
        SegmentStore::recover_existing(&target)
            .unwrap()
            .recovery
            .wal_records,
        expected_records
    );
    assert_eq!(fs::read_link(&target).unwrap(), physical_store);
    assert_eq!(fs::read_link(&prefix.segment).unwrap(), foreign);
}
