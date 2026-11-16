//! Complete pre-crash V1 native layout for membership fixtures. This is fixture
//! construction, not evidence that native identity publication ran successfully.
use std::io::Write;
use std::path::Path;

pub(super) fn provision(root: &Path, cluster: &str, node: u64) {
    rsf_storage::create_dir_all_synced(root).unwrap();
    assert!(
        std::fs::read_dir(root).unwrap().next().is_none(),
        "legacy identity fixture requires an empty namespace"
    );
    let lock_path = root.join(".peer.lock");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
        .unwrap();
    file.sync_all().unwrap();
    let guard = rsf_storage::ExclusiveFileLock::acquire(file).unwrap();
    let held = guard.metadata().unwrap();
    let named = std::fs::symlink_metadata(&lock_path).unwrap();
    assert!(held.is_file() && named.is_file());
    assert_eq!(held.len(), 0);
    assert_eq!(named.len(), 0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!((held.dev(), held.ino()), (named.dev(), named.ino()));
        assert_eq!(named.nlink(), 1);
    }
    // Old PeerService::open acquired this same real lock before binding V1.
    // Keep it held while constructing the complete durable pre-crash layout.
    let bytes = serde_json::to_vec(&serde_json::json!({
        "version": 1, "cluster": cluster, "node": node,
    }))
    .unwrap();
    let mut identity = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("IDENTITY.json"))
        .unwrap();
    identity.write_all(&bytes).unwrap();
    identity.sync_all().unwrap();
    rsf_storage::sync_existing_directory(root).unwrap();
    assert_eq!(std::fs::read(root.join("IDENTITY.json")).unwrap(), bytes);
    assert_eq!(std::fs::read_dir(root).unwrap().count(), 2);
    drop(guard);
}
