use super::*;
use crate::durable_directory::fresh_publication::observation::{self as fault, Phase};
use std::cell::RefCell;
use std::os::unix::fs::PermissionsExt;
use std::rc::Rc;

fn segment(target: &Path) -> PathBuf {
    fs::read_dir(target.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|v| v == "segment"))
        .unwrap()
}
fn locks(target: &Path) -> Vec<OriginalLock> {
    if fs::read_dir(target.join("segments"))
        .unwrap()
        .any(|e| e.unwrap().path().extension().is_some_and(|v| v == "lock"))
    {
        return original_locks(target);
    }
    let path = target.join("MANIFEST.lock");
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    let m = file.metadata().unwrap();
    vec![OriginalLock {
        path,
        file,
        identity: (m.dev(), m.ino()),
    }]
}
fn usable(target: &Path, existing: bool) {
    let (mut store, recovered, events) = open_observed(target);
    assert!(recovered.recovery.wal_records.is_empty());
    if existing {
        complete_routes(&events, target, 1);
    } else {
        complete_fresh(&events, target);
    }
    let record = append(&mut store, b"cold retry after physical fault");
    drop(store);
    assert_eq!(
        SegmentStore::recover_existing(target)
            .unwrap()
            .recovery
            .wal_records,
        vec![record]
    );
}
fn custody(error: &rsf_core::RsfError) {
    assert!(
        error
            .message
            .contains("fresh store publication lost original"),
        "{error:?}"
    );
}

#[test]
fn fresh_route_replacement_never_adopts_new_ancestors_and_cold_retry_uses_original_locks() {
    for phase in [
        Phase::RoutesPublished,
        Phase::FinalOpened,
        Phase::FinalSynced,
    ] {
        for level in ["container", "container/store", "container/store/segments"] {
            let root = Root::new("fresh-route-substitution");
            let target = root.target();
            let replaced = root.0.join(level);
            let saved = root.0.join("original-directory");
            let state = Rc::new(RefCell::new(None));
            let copy = Rc::clone(&state);
            let (t, p, save) = (target.clone(), replaced.clone(), saved.clone());
            let watch = fault::watch(move |at| {
                if at != phase {
                    return Ok(());
                }
                assert!(copy.borrow().is_none());
                let held = File::open(&p)?;
                let originals = locks(&t);
                let manifest = fs::read(t.join("MANIFEST")).ok();
                fs::rename(&p, &save)?;
                fs::create_dir_all(t.join("segments"))?;
                assert_ne!(held.metadata()?.ino(), fs::metadata(&p)?.ino());
                *copy.borrow_mut() = Some((held, originals, manifest));
                Ok(())
            });
            let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
            drop(watch);
            custody(&error);
            let (held, originals, manifest) = state
                .borrow_mut()
                .take()
                .expect("physical substitution ran");
            let expected_manifest = if level == "container/store/segments" {
                manifest.clone()
            } else {
                None
            };
            assert_eq!(
                fs::read(target.join("MANIFEST")).ok(),
                expected_manifest,
                "replacement namespace was mutated"
            );
            assert!(
                fs::read_dir(target.join("segments"))
                    .unwrap()
                    .next()
                    .is_none()
            );
            fs::remove_dir_all(&replaced).unwrap();
            fs::rename(&saved, &replaced).unwrap();
            assert_eq!(
                held.metadata().unwrap().ino(),
                fs::metadata(&replaced).unwrap().ino()
            );
            assert_eq!(fs::read(target.join("MANIFEST")).ok(), manifest);
            original_locks_released(originals);
            usable(&target, phase != Phase::RoutesPublished);
        }
    }
}

#[test]
fn fresh_final_segment_and_lock_substitutions_return_no_owner_after_real_publication() {
    for phase in [Phase::FinalOpened, Phase::FinalSynced] {
        for kind in ["segment", "segment-alias", "manifest-lock", "active-lock"] {
            let root = Root::new("fresh-file-substitution");
            let target = root.target();
            let saved = root.0.join("original-file");
            let state = Rc::new(RefCell::new(None));
            let copy = Rc::clone(&state);
            let (t, save) = (target.clone(), saved.clone());
            let watch = fault::watch(move |at| {
                if at != phase {
                    return Ok(());
                }
                let originals = locks(&t);
                let path = match kind {
                    "manifest-lock" => t.join("MANIFEST.lock"),
                    "active-lock" => segment(&t).with_extension("lock"),
                    _ => segment(&t),
                };
                let held = File::open(&path)?;
                fs::rename(&path, &save)?;
                if kind == "segment-alias" {
                    symlink(&save, &path)?;
                } else {
                    fs::write(&path, b"")?;
                }
                *copy.borrow_mut() = Some((path, held, originals));
                Ok(())
            });
            let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
            drop(watch);
            custody(&error);
            let (path, held, originals) = state.borrow_mut().take().expect("substitution ran");
            assert_eq!(fs::read(&path).unwrap(), b"");
            fs::remove_file(&path).unwrap();
            fs::rename(saved, &path).unwrap();
            assert_eq!(
                held.metadata().unwrap().ino(),
                fs::metadata(&path).unwrap().ino()
            );
            original_locks_released(originals);
            usable(&target, true);
        }
    }
}

#[test]
fn fresh_writer_descriptor_cannot_hide_behind_restored_original_segment_name() {
    let root = Root::new("fresh-writer-descriptor");
    let target = root.target();
    let saved = root.0.join("original-segment");
    let foreign = root.0.join("foreign-segment");
    let state = Rc::new(RefCell::new((0, None)));
    let copy = Rc::clone(&state);
    let (t, save, other) = (target.clone(), saved.clone(), foreign.clone());
    let watch = fault::watch(move |phase| {
        if phase == Phase::BeforeFileOpen {
            let path = segment(&t);
            let held = File::open(&path)?;
            let originals = locks(&t);
            fs::rename(&path, &save)?;
            fs::write(&path, b"")?;
            *copy.borrow_mut() = (1, Some((path, held, originals)));
        } else if phase == Phase::FinalOpened {
            let mut state = copy.borrow_mut();
            assert_eq!(state.0, 1);
            let (path, _, _) = state.1.as_ref().unwrap();
            fs::rename(path, &other)?;
            fs::rename(&save, path)?;
            state.0 = 2;
        }
        Ok(())
    });
    let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
    drop(watch);
    custody(&error);
    let mut state = state.borrow_mut();
    assert_eq!(
        state.0, 2,
        "foreign writer was opened before original name restoration"
    );
    let (path, held, originals) = state.1.take().unwrap();
    assert_eq!(
        held.metadata().unwrap().ino(),
        fs::metadata(path).unwrap().ino()
    );
    assert_ne!(
        held.metadata().unwrap().ino(),
        fs::metadata(foreign).unwrap().ino()
    );
    original_locks_released(originals);
    usable(&target, true);
}

#[test]
fn fresh_same_target_alias_replacement_is_not_new_publication_authority() {
    for phase in [
        Phase::RoutesPublished,
        Phase::FinalOpened,
        Phase::FinalSynced,
    ] {
        let root = Root::new("fresh-link-substitution");
        let physical = root.0.join("physical");
        fs::create_dir(&physical).unwrap();
        let alias = root.0.join("alias");
        symlink(&physical, &alias).unwrap();
        let saved = root.0.join("original-link");
        let target = alias.join("store");
        let state = Rc::new(RefCell::new(None));
        let copy = Rc::clone(&state);
        let (t, a, p, save) = (
            target.clone(),
            alias.clone(),
            physical.clone(),
            saved.clone(),
        );
        let watch = fault::watch(move |at| {
            if at != phase {
                return Ok(());
            }
            let originals = locks(&t);
            let before = fs::symlink_metadata(&a)?;
            fs::rename(&a, &save)?;
            symlink(&p, &a)?;
            assert_ne!(before.ino(), fs::symlink_metadata(&a)?.ino());
            *copy.borrow_mut() = Some(originals);
            Ok(())
        });
        custody(&SegmentStore::open_recovered(&target, options()).unwrap_err());
        drop(watch);
        let originals = state
            .borrow_mut()
            .take()
            .expect("same-target link substitution ran");
        fs::remove_file(&alias).unwrap();
        fs::rename(saved, &alias).unwrap();
        original_locks_released(originals);
        usable(&target, phase != Phase::RoutesPublished);
    }
    // ParentDir and a preprovisioned alias stay valid without lexical collapse.
    let root = Root::new("fresh-parent-components");
    fs::create_dir_all(root.0.join("physical/child")).unwrap();
    symlink(root.0.join("physical"), root.0.join("alias")).unwrap();
    let target = root.0.join("alias/child/../store");
    usable(&target, false);
}

#[test]
fn fresh_publication_rechecks_current_directory_access_without_replacing_original_custody() {
    for phase in [
        Phase::RoutesPublished,
        Phase::FinalOpened,
        Phase::FinalSynced,
    ] {
        let root = Root::new("fresh-permission");
        let target = root.target();
        let ancestor = root.0.join("container");
        let state = Rc::new(RefCell::new(None));
        let copy = Rc::clone(&state);
        let (t, dir) = (target.clone(), ancestor.clone());
        let watch = fault::watch(move |at| {
            if at != phase {
                return Ok(());
            }
            let originals = locks(&t);
            let old = fs::metadata(&dir)?.permissions();
            *copy.borrow_mut() = Some((old, originals));
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o111))?;
            let error = File::open(&dir)
                .expect_err("qualification needs real unprivileged permission enforcement");
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            Ok(())
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            SegmentStore::open_recovered(&target, options())
        }));
        drop(watch);
        let (permissions, originals) = state.borrow_mut().take().expect("access fault ran");
        fs::set_permissions(&ancestor, permissions).unwrap();
        let error = result.unwrap().unwrap_err();
        assert!(
            error.message.to_lowercase().contains("permission denied"),
            "{error:?}"
        );
        original_locks_released(originals);
        usable(&target, phase != Phase::RoutesPublished);
    }
}

#[test]
fn fresh_publication_errors_and_unwinds_release_original_locks_without_residue_repair() {
    for phase in [
        Phase::RoutesPublished,
        Phase::SegmentBound,
        Phase::BeforeFileOpen,
        Phase::FinalOpened,
        Phase::FinalSynced,
    ] {
        for unwind in [false, true] {
            let root = Root::new("fresh-error-unwind");
            let target = root.target();
            let state = Rc::new(RefCell::new(None));
            let copy = Rc::clone(&state);
            let t = target.clone();
            let watch = fault::watch(move |at| {
                if at != phase {
                    return Ok(());
                }
                *copy.borrow_mut() = Some(locks(&t));
                if unwind {
                    panic!("fresh publication scheduled unwind");
                }
                Err(io::Error::other("fresh publication scheduled refusal"))
            });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                SegmentStore::open_recovered(&target, options())
            }));
            drop(watch);
            let originals = state
                .borrow_mut()
                .take()
                .expect("requested failure boundary ran");
            if unwind {
                assert_eq!(
                    result.unwrap_err().downcast_ref::<&str>(),
                    Some(&"fresh publication scheduled unwind")
                );
            } else {
                assert!(
                    result
                        .unwrap()
                        .unwrap_err()
                        .message
                        .ends_with("fresh publication scheduled refusal")
                );
            }
            original_locks_released(originals);
            if phase == Phase::SegmentBound {
                assert!(!target.join("MANIFEST").exists());
                let path = segment(&target);
                let original = File::open(&path).unwrap();
                let bytes = fs::read(&path).unwrap();
                let error = SegmentStore::open_recovered(&target, options()).unwrap_err();
                assert!(
                    error
                        .message
                        .contains("segment files exist without a manifest")
                );
                assert_eq!(fs::read(&path).unwrap(), bytes);
                assert_eq!(
                    original.metadata().unwrap().ino(),
                    fs::metadata(&path).unwrap().ino()
                );
                assert!(!target.join("MANIFEST").exists());
            } else {
                usable(&target, phase != Phase::RoutesPublished);
            }
        }
    }
}
