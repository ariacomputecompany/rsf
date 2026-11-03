//! Claim fixture namespaces before any store creates files or acquires locks.
use std::path::{Path, PathBuf};

pub(super) fn reserve(parent: &Path, name: &str, timestamp: u128) -> PathBuf {
    let mut suffix = 0u64;
    loop {
        let path = parent.join(format!(
            "rsf-peer-{name}-{}-{timestamp}-{suffix}",
            std::process::id(),
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            // The clock may repeat, another test may be allocating concurrently,
            // or an earlier process may have left custody behind. Never reuse it.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                suffix = suffix
                    .checked_add(1)
                    .expect("fixture root suffix exhausted");
            }
            Err(error) => panic!("cannot reserve fixture root {}: {error}", path.display()),
        }
    }
}

#[test]
fn concurrent_same_name_same_clock_fixtures_preserve_existing_custody_and_isolation() {
    let parent = super::root("root-reservation-regression");
    // Hold the clock constant so every caller necessarily encounters the same
    // candidates. Existing directory and file custody must both remain intact.
    let existing = reserve(&parent, "accepted-stage-import", 7);
    std::fs::write(existing.join("sentinel"), b"retained directory").unwrap();
    let file = reserve(&parent, "accepted-stage-import", 7);
    std::fs::remove_dir(&file).unwrap();
    std::fs::write(&file, b"retained file").unwrap();
    let barrier = std::sync::Barrier::new(8);
    let allocated = std::thread::scope(|scope| {
        let handles = (0u64..8)
            .map(|owner| {
                let parent = &parent;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let path = reserve(parent, "accepted-stage-import", 7);
                    std::fs::write(path.join("owner"), owner.to_le_bytes()).unwrap();
                    (owner, path)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    let unique = allocated
        .iter()
        .map(|(_, path)| path)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), allocated.len());
    for (owner, path) in &allocated {
        assert_ne!(path, &existing);
        assert_ne!(path, &file);
        assert_eq!(
            std::fs::read(path.join("owner")).unwrap(),
            owner.to_le_bytes()
        );
    }
    // Normal cleanup of one fixture cannot remove another fixture's state.
    std::fs::remove_dir_all(&allocated[0].1).unwrap();
    for (owner, path) in &allocated[1..] {
        assert_eq!(
            std::fs::read(path.join("owner")).unwrap(),
            owner.to_le_bytes()
        );
    }
    assert_eq!(
        std::fs::read(existing.join("sentinel")).unwrap(),
        b"retained directory"
    );
    assert_eq!(std::fs::read(&file).unwrap(), b"retained file");
    std::fs::remove_dir_all(parent).unwrap();
}
