use super::*;
use crate::{Epoch, SyncPolicy, WalGroupEntry, sync_observation};
use std::sync::atomic::{AtomicU64, Ordering};
mod faults;
mod flow;
mod rotation;

fn root(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let path = std::env::temp_dir().join(format!(
            "rsf-owned-group-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => panic!("cannot reserve owned group fixture: {error}"),
        }
    }
}
fn options(policy: SyncPolicy, limit: u64) -> SegmentStoreOptions {
    SegmentStoreOptions {
        log_options: SegmentLogOptions {
            sync_policy: policy,
        },
        max_segment_bytes: limit,
    }
}
fn record(payload: &[u8]) -> RsfResult<WalGroupEntry<&[u8]>> {
    Ok(WalGroupEntry {
        epoch: Epoch(1),
        kind: WalRecordKind::Prepare,
        payload,
    })
}
fn seeded(path: &Path, policy: SyncPolicy, limit: u64) -> SegmentStore {
    let mut store = SegmentStore::open_with_options(path, options(policy, limit)).unwrap();
    store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(1),
            WalRecordKind::Prepare,
            b"retained",
        )
        .unwrap();
    store
}
fn content(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    files.insert(
        PathBuf::from(MANIFEST_FILE),
        fs::read(path.join(MANIFEST_FILE)).unwrap(),
    );
    for entry in fs::read_dir(path.join(SEGMENT_DIR)).unwrap() {
        let file = entry.unwrap().path();
        if file.extension().is_some_and(|value| value == "segment") {
            files.insert(
                file.strip_prefix(path).unwrap().to_path_buf(),
                fs::read(file).unwrap(),
            );
        }
    }
    files
}
fn input_error() -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, "injected producer rejection")
}
