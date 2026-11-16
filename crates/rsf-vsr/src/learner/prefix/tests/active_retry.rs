//! The current-ACTIVE shortcut must check the pending owner it will drop.
use super::*;

fn inventory(path: &Path) -> std::collections::BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn walk(
        base: &Path,
        path: &Path,
        out: &mut std::collections::BTreeMap<PathBuf, Option<Vec<u8>>>,
    ) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(base).unwrap().to_path_buf();
            if entry.file_type().unwrap().is_dir() {
                out.insert(relative, None);
                walk(base, &path, out);
            } else {
                out.insert(relative, Some(fs::read(path).unwrap()));
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk(path, path, &mut out);
    out
}

#[test]
#[cfg(unix)]
fn current_active_begin_cannot_drop_lost_or_replaced_warm_stage_custody() {
    for replacement in [false, true] {
        let mut fixture = Fixture::new("active-retry-warm-loss");
        let published = fixture.four.published.as_ref().unwrap().descriptor.clone();
        let active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
        let export = fixture.advance();
        let id = export.descriptor().id().unwrap();
        assert_eq!(
            fixture
                .four
                .begin_prefix(export.descriptor().clone(), limits())
                .unwrap(),
            0
        );
        let directory = fixture.four.directory(&id);
        let lock = active_segment_lock(&directory.join("cell"));
        let held = directory.join("original-lock");
        fs::rename(&lock, &held).unwrap();
        if replacement {
            fs::write(&lock, []).unwrap();
        }
        let before = inventory(&fixture.root.join("four"));
        assert!(
            fixture
                .four
                .begin_prefix(published.clone(), limits())
                .is_err()
        );
        assert_eq!(
            inventory(&fixture.root.join("four")),
            before,
            "no reclamation, recreation or ACK"
        );
        assert_eq!(
            fixture
                .four
                .stage
                .as_ref()
                .unwrap()
                .descriptor
                .id()
                .unwrap(),
            id
        );
        assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), active);
        if replacement {
            fs::remove_file(&lock).unwrap();
        }
        fs::rename(&held, &lock).unwrap();
        let restored = inventory(&fixture.root.join("four"));
        assert!(
            fixture
                .four
                .begin_prefix(published, limits())
                .unwrap_err()
                .message
                .contains("learner durability failed")
        );
        assert!(fixture.four.published_history_id().is_err());
        assert_eq!(
            inventory(&fixture.root.join("four")),
            restored,
            "restoration must not revive warm owner"
        );
        let root = fixture.root.clone();
        drop(fixture);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn healthy_current_active_begin_may_reclaim_provisional_stage_after_validation() {
    let mut fixture = Fixture::new("active-retry-healthy");
    let published = fixture.four.published.as_ref().unwrap().descriptor.clone();
    let active = fs::read(fixture.root.join("four/ACTIVE")).unwrap();
    let export = fixture.advance();
    let id = export.descriptor().id().unwrap();
    assert_eq!(
        fixture
            .four
            .begin_prefix(export.descriptor().clone(), limits())
            .unwrap(),
        0
    );
    let directory = fixture.four.directory(&id);
    assert!(directory.is_dir());
    assert_eq!(
        fixture
            .four
            .begin_prefix(published.clone(), limits())
            .unwrap(),
        published.records
    );
    assert!(fixture.four.stage.is_none());
    assert!(!directory.exists());
    assert_eq!(fs::read(fixture.root.join("four/ACTIVE")).unwrap(), active);
    assert_eq!(
        fixture
            .four
            .finish_prefix(published.id().unwrap(), |preview| {
                assert_eq!(preview.committed_tail(), LogIndex(1));
                preview.validate_live()
            })
            .unwrap(),
        LogIndex(1)
    );
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
