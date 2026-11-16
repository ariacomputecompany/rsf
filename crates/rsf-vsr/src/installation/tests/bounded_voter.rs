use super::*;
use crate::bounded::{
    Boundary as OpenBoundary,
    tests::{limits, open_with_hook, snapshot},
};
use std::fs;

fn installed(label: &str) -> PathBuf {
    let path = crate::bounded::tests::root(label);
    let (mut first, second, snapshots) = fixture(&path);
    first
        .install_view(&VsrViewInstallation::select(&snapshots).unwrap())
        .unwrap();
    drop(first);
    drop(second);
    path
}
fn target(path: &Path) -> PathBuf {
    path.join("1")
}

#[test]
fn bounded_installed_voter_preserves_sealed_prefix_receipt_tail_and_later_promise() {
    let path = installed("bounded-installed");
    let root = target(&path);
    let mut cell =
        DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap();
    assert!(cell.has_installed_view());
    assert_eq!(cell.view(), Epoch(2));
    assert_eq!(cell.primary(), NodeId(1));
    assert_eq!(cell.committed_tail(), LogIndex(1));
    assert_eq!(cell.next_index(), LogIndex(3));
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    let prefix = cell.committed_prefix().unwrap();
    let pending = cell.prepared(LogIndex(2)).unwrap().clone();
    let next = VsrViewChangeRequest {
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        ..request()
    };
    cell.begin_view_change(NodeId(1), NodeId(2), next.clone())
        .unwrap();
    drop(cell);
    let cell = DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap();
    assert_eq!(cell.view_change_promise(), Some(&next));
    assert_eq!(cell.committed_prefix().unwrap(), prefix);
    assert_eq!(cell.prepared(LogIndex(2)), Some(&pending));
    assert!(cell.primary_probe().is_err());
    assert!(cell.state_token().is_err());
    assert!(cell.recovery_state_token().is_ok());
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_valid_active_completes_only_missing_marker_and_canonical_temporary_prefix() {
    for prefix in [None, Some(0), Some(1), Some(43), Some(44)] {
        let path = installed("bounded-marker");
        let root = target(&path);
        let published = root.join("VSR_PUBLISHED");
        let expected = fs::read(&published).unwrap();
        assert_eq!(expected.len(), 44);
        fs::remove_file(&published).unwrap();
        if let Some(prefix) = prefix {
            fs::write(root.join(".VSR_PUBLISHED.tmp"), &expected[..prefix]).unwrap();
        }
        let mut prepared = 0;
        let mut completed = 0;
        let cell = open_with_hook(&root, &mut |boundary| {
            match boundary {
                OpenBoundary::MarkerPrepared => prepared += 1,
                OpenBoundary::MarkerPublished => completed += 1,
                _ => {}
            }
            Ok(())
        })
        .unwrap();
        assert_eq!((prepared, completed), (1, 1));
        assert_eq!(fs::read(&published).unwrap(), expected);
        assert!(!root.join(".VSR_PUBLISHED.tmp").exists());
        assert_eq!(cell.view(), Epoch(2));
        assert_eq!(cell.committed_tail(), LogIndex(1));
        drop(cell);
        drop(DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap());
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_foreign_or_aliased_publication_temporary_is_never_truncated_or_followed() {
    for alias in [false, true] {
        let path = installed("bounded-marker-foreign");
        let root = target(&path);
        fs::remove_file(root.join("VSR_PUBLISHED")).unwrap();
        let victim = path.join("foreign");
        fs::write(&victim, b"retain these unrelated bytes").unwrap();
        if alias {
            std::os::unix::fs::symlink(&victim, root.join(".VSR_PUBLISHED.tmp")).unwrap();
        } else {
            fs::write(root.join(".VSR_PUBLISHED.tmp"), b"foreign").unwrap();
        }
        let before = snapshot(&path);
        let error = DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits())
            .unwrap_err();
        assert!(
            error.message.contains(if alias {
                "regular file"
            } else {
                "canonical expected prefix"
            }),
            "{error:?}"
        );
        assert_eq!(snapshot(&path), before);
        assert_eq!(fs::read(victim).unwrap(), b"retain these unrelated bytes");
        assert!(!root.join("VSR_PUBLISHED").exists());
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_captured_pointer_or_held_writer_lock_loss_after_replay_cannot_complete_marker() {
    for name in ["VSR_ACTIVE", ".vsr-writer.lock"] {
        let path = installed("bounded-open-guard");
        let root = target(&path);
        fs::remove_file(root.join("VSR_PUBLISHED")).unwrap();
        let captured = root.join(name);
        let saved = root.join("saved-original-metadata");
        let mut reached = false;
        let error = open_with_hook(&root, &mut |boundary| {
            if boundary == OpenBoundary::Replayed {
                reached = true;
                fs::rename(&captured, &saved).unwrap();
                if name == ".vsr-writer.lock" {
                    // Same bytes, different inode: the actual held original
                    // descriptor must not authorize this replacement name.
                    fs::write(&captured, b"").unwrap();
                }
            }
            Ok(())
        })
        .unwrap_err();
        assert!(reached);
        assert!(error.message.contains("bounded voter"), "{error:?}");
        assert!(!root.join("VSR_PUBLISHED").exists());
        assert!(!root.join(".VSR_PUBLISHED.tmp").exists());
        if name == ".vsr-writer.lock" {
            fs::remove_file(&captured).unwrap();
        }
        fs::rename(saved, &captured).unwrap();
        drop(DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap());
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_prepared_marker_replacement_cannot_publish_or_modify_foreign_file() {
    let path = installed("bounded-temp-guard");
    let root = target(&path);
    fs::remove_file(root.join("VSR_PUBLISHED")).unwrap();
    let victim = path.join("victim");
    fs::write(&victim, b"untouched").unwrap();
    let temporary = root.join(".VSR_PUBLISHED.tmp");
    let saved = root.join("saved-marker");
    let mut reached = false;
    let error = open_with_hook(&root, &mut |boundary| {
        if boundary == OpenBoundary::MarkerPrepared {
            reached = true;
            fs::rename(&temporary, &saved).unwrap();
            std::os::unix::fs::symlink(&victim, &temporary).unwrap();
        }
        Ok(())
    })
    .unwrap_err();
    assert!(reached);
    assert!(error.message.contains("regular file"));
    assert_eq!(fs::read(&victim).unwrap(), b"untouched");
    assert!(!root.join("VSR_PUBLISHED").exists());
    fs::remove_file(&temporary).unwrap();
    fs::rename(saved, &temporary).unwrap();
    drop(DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_published_marker_error_returns_no_owner_and_cold_retry_retains_exact_history() {
    let path = installed("bounded-published-error");
    let root = target(&path);
    fs::remove_file(root.join("VSR_PUBLISHED")).unwrap();
    let mut reached = false;
    let error = open_with_hook(&root, &mut |boundary| {
        if boundary == OpenBoundary::MarkerPublished {
            reached = true;
            return Err(corrupt("injected after durable marker"));
        }
        Ok(())
    })
    .unwrap_err();
    assert!(reached);
    assert_eq!(error.message, "injected after durable marker");
    assert!(root.join("VSR_PUBLISHED").is_file());
    let cell = DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap();
    assert_eq!(cell.committed_tail(), LogIndex(1));
    assert_eq!(cell.next_index(), LogIndex(3));
    assert_eq!(
        cell.request_commit(&VsrClientRequestId::new("original", 1)),
        Some(LogIndex(1))
    );
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_installed_missing_damaged_or_aliased_active_metadata_never_repairs() {
    for fault in [
        "missing-pointer",
        "truncated-pointer",
        "trailing-pointer",
        "sparse-pointer",
        "wrong-pg",
        "missing-seal",
        "bad-seal",
        "bad-marker",
        "alias-generation",
        "alias-container",
    ] {
        let path = installed("bounded-meta");
        let root = target(&path);
        let active = publication::active(&root, PlacementGroupId(1)).unwrap();
        match fault {
            "missing-pointer" => fs::remove_file(root.join("VSR_ACTIVE")).unwrap(),
            "truncated-pointer" => {
                let p = root.join("VSR_ACTIVE");
                let mut b = fs::read(&p).unwrap();
                b.pop();
                fs::write(p, b).unwrap();
            }
            "trailing-pointer" => {
                use std::io::Write;
                fs::OpenOptions::new()
                    .append(true)
                    .open(root.join("VSR_ACTIVE"))
                    .unwrap()
                    .write_all(b"x")
                    .unwrap();
            }
            "sparse-pointer" => fs::OpenOptions::new()
                .write(true)
                .open(root.join("VSR_ACTIVE"))
                .unwrap()
                .set_len(1024 * 1024)
                .unwrap(),
            "wrong-pg" => {
                let p = root.join("VSR_ACTIVE");
                let mut b = fs::read(&p).unwrap();
                b[4..12].copy_from_slice(&9_u64.to_le_bytes());
                let n = b.len() - 32;
                let digest = Digest::blake3_bytes(&b[..n]);
                b[n..].copy_from_slice(digest.as_bytes());
                fs::write(p, b).unwrap();
            }
            "missing-seal" => fs::remove_file(active.path.join("VSR_SEAL")).unwrap(),
            "bad-seal" => fs::write(active.path.join("VSR_SEAL"), b"bad seal").unwrap(),
            "bad-marker" => fs::write(root.join("VSR_PUBLISHED"), b"bad marker").unwrap(),
            "alias-generation" => {
                let saved = path.join("saved-generation");
                fs::rename(&active.path, &saved).unwrap();
                std::os::unix::fs::symlink(saved, &active.path).unwrap();
            }
            "alias-container" => {
                let views = root.join("vsr-views");
                let saved = path.join("saved-views");
                fs::rename(&views, &saved).unwrap();
                std::os::unix::fs::symlink(saved, &views).unwrap();
            }
            _ => unreachable!(),
        }
        let before = snapshot(&path);
        assert!(
            DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).is_err(),
            "{fault}"
        );
        assert_eq!(snapshot(&path), before, "{fault}");
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_returned_installed_owner_latches_observed_pointer_loss_with_same_inode_restore() {
    let path = installed("bounded-live-pointer");
    let root = target(&path);
    let cell = DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap();
    let pointer = root.join("VSR_ACTIVE");
    let saved = root.join("saved-pointer");
    fs::rename(&pointer, &saved).unwrap();
    assert!(cell.recovery_state_token().is_err());
    fs::rename(saved, &pointer).unwrap();
    assert!(
        cell.recovery_state_token()
            .unwrap_err()
            .message
            .contains("VSR durability failed")
    );
    drop(cell);
    drop(DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_installed_visible_suffix_renews_before_marker_and_failed_hook_cannot_advance_it() {
    let path = installed("bounded-visible-generation");
    let root = target(&path);
    let cell = DurableVsrCell::open_existing_bounded(&root, PlacementGroupId(1), limits()).unwrap();
    let (root, prepare) = crate::bounded::tests::visible_prepare(cell);
    let marker = root.join("VSR_PUBLISHED");
    let expected = fs::read(&marker).unwrap();
    fs::remove_file(&marker).unwrap();
    let mut before = false;
    let mut after = false;
    let error = open_with_hook(&root, &mut |boundary| {
        if boundary == OpenBoundary::BeforeRenewal {
            before = true;
            return Err(corrupt("injected before renewal"));
        }
        if boundary == OpenBoundary::Renewed {
            after = true;
        }
        Ok(())
    })
    .unwrap_err();
    assert!(before);
    assert!(!after);
    assert_eq!(error.message, "injected before renewal");
    assert!(!marker.exists());
    assert!(!root.join(".VSR_PUBLISHED.tmp").exists());
    let mut phases = Vec::new();
    let cell = open_with_hook(&root, &mut |boundary| {
        phases.push(boundary);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        phases,
        vec![
            OpenBoundary::Replayed,
            OpenBoundary::BeforeRenewal,
            OpenBoundary::AfterClosedRenewal,
            OpenBoundary::Renewed,
            OpenBoundary::MarkerPrepared,
            OpenBoundary::MarkerPublished
        ]
    );
    assert_eq!(cell.prepared(prepare.index), Some(&prepare));
    assert_eq!(fs::read(&marker).unwrap(), expected);
    assert_eq!(cell.committed_tail(), LogIndex(1));
    drop(cell);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_active_renewal_rejects_new_incomplete_tail_without_repair_or_marker() {
    use std::io::Write;
    let path = installed("bounded-no-tail-repair");
    let root = target(&path);
    let active = publication::active(&root, PlacementGroupId(1)).unwrap();
    let recovery = SegmentStore::recover_existing(&active.path).unwrap();
    let segment = active
        .path
        .join("segments")
        .join(&recovery.segments.last().unwrap().file_name);
    fs::remove_file(root.join("VSR_PUBLISHED")).unwrap();
    let mut damaged = None;
    let mut renewed = false;
    let error = open_with_hook(&root, &mut |boundary| {
        if boundary == OpenBoundary::AfterClosedRenewal {
            // Mutate the actual held file after closed renewal. Generic empty
            // append may recover/truncate this tail before its final check;
            // strict active renewal must reject without entering that path.
            fs::OpenOptions::new()
                .append(true)
                .open(&segment)
                .unwrap()
                .write_all(b"RSF1-torn")
                .unwrap();
            damaged = Some(fs::read(&segment).unwrap());
        }
        if boundary == OpenBoundary::Renewed {
            renewed = true;
        }
        Ok(())
    })
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert!(!renewed);
    assert_eq!(
        fs::read(&segment).unwrap(),
        damaged.expect("actual renewal boundary reached")
    );
    assert!(!root.join("VSR_PUBLISHED").exists());
    assert!(!root.join(".VSR_PUBLISHED.tmp").exists());
    fs::remove_dir_all(path).unwrap();
}
