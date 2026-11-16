use super::*;
use std::os::unix::fs::symlink;

fn root(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "rsf-stage-lock-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn stage_lock_creation_never_follows_a_symlink_substituted_after_precheck() {
    for existing in [false, true] {
        let root = root(if existing { "existing" } else { "missing" });
        let path = root.join(".staging.lock");
        let saved = root.join("previous-lock");
        let foreign = root.join("foreign-target");
        if existing {
            fs::write(&path, b"original").unwrap();
        }
        let result = Owner::acquire_with(&root, |checked| {
            assert_eq!(checked, path);
            if existing {
                fs::rename(checked, &saved).unwrap();
            }
            symlink(&foreign, checked).unwrap();
        });
        assert!(result.is_err());
        assert!(
            !foreign.exists(),
            "rejected lock open created its substituted target"
        );
        assert!(
            fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        if existing {
            assert_eq!(fs::read(saved).unwrap(), b"original");
        }
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn stage_lock_rejects_existing_symlink_and_hardlink_without_changing_target() {
    for hardlink in [false, true] {
        let root = root(if hardlink { "hardlink" } else { "symlink" });
        let target = root.join("foreign-target");
        let path = root.join(".staging.lock");
        fs::write(&target, b"foreign bytes").unwrap();
        if hardlink {
            fs::hard_link(&target, &path).unwrap();
        } else {
            symlink(&target, &path).unwrap();
        }
        assert!(Owner::acquire(&root).is_err());
        assert_eq!(fs::read(target).unwrap(), b"foreign bytes");
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn stage_owner_keeps_original_descriptor_locked_and_rejects_renamed_path_replacement() {
    let root = root("renamed-owner");
    let path = root.join(".staging.lock");
    let saved = root.join("previous-lock");
    let owner = Owner::acquire(&root).unwrap();
    fs::rename(&path, &saved).unwrap();
    assert!(owner.validate().is_err());
    let open = |path: &Path| OpenOptions::new().write(true).open(path).unwrap();
    assert!(
        rsf_storage::ExclusiveFileLock::acquire(open(&saved)).is_err(),
        "rename released the owner's original descriptor lock"
    );
    fs::write(&path, b"").unwrap();
    let replacement = rsf_storage::ExclusiveFileLock::acquire(open(&path)).unwrap();
    assert!(
        owner.validate().is_err(),
        "a separately lockable path replaced the owner"
    );
    drop(replacement);
    fs::remove_file(&path).unwrap();
    fs::rename(&saved, &path).unwrap();
    owner.validate().unwrap();
    // Owner only compares identity. The outer MembershipStagingStore latches
    // observed loss; the real-stage lock matrix separately proves no revival.
    assert!(rsf_storage::ExclusiveFileLock::acquire(open(&path)).is_err());
    drop(owner);
    let next = Owner::acquire(&root).unwrap();
    next.validate().unwrap();
    drop(next);
    fs::remove_dir_all(root).unwrap();
}
