use super::*;
use std::cell::Cell;
use std::rc::Rc;

#[test]
fn owned_group_observed_physical_fault_stays_failed_when_iterator_drop_restores_original_inode() {
    struct Restore {
        manifest: PathBuf,
        saved: PathBuf,
        bytes: Vec<u8>,
        called: bool,
    }
    impl Iterator for Restore {
        type Item = RsfResult<WalGroupEntry<Vec<u8>>>;
        fn next(&mut self) -> Option<Self::Item> {
            assert!(!self.called);
            self.called = true;
            fs::rename(&self.manifest, &self.saved).unwrap();
            fs::write(&self.manifest, &self.bytes).unwrap();
            Some(Err(input_error()))
        }
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            if self.called {
                fs::remove_file(&self.manifest).unwrap();
                fs::rename(&self.saved, &self.manifest).unwrap();
            }
        }
    }
    let path = root("restore-on-error");
    let mut store = seeded(&path, SyncPolicy::Data, 0);
    let original = store.capture_owned_custody().unwrap();
    let before = content(&path);
    let manifest = path.join(MANIFEST_FILE);
    let producer = Restore {
        bytes: fs::read(&manifest).unwrap(),
        manifest,
        saved: path.join("original-manifest"),
        called: false,
    };
    let error = store
        .try_append_owned_wal_group(&original, PlacementGroupId(1), producer)
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert!(error.message.contains("custody"), "{error:?}");
    assert_eq!(content(&path), before);
    assert!(!path.join("original-manifest").exists());
    assert!(store.write_failed && store.cursor.is_none());
    assert!(
        store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Prepare,
                b"forbidden"
            )
            .is_err()
    );
    drop(store);
    let mut reopened = SegmentStore::open(&path).unwrap();
    let stamp = reopened.capture_owned_custody().unwrap();
    reopened
        .try_append_owned_wal_group(&stamp, PlacementGroupId(1), [record(b"valid cold retry")])
        .unwrap();
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_group_payload_drop_fault_is_detected_before_next_producer_poll() {
    struct Payload {
        bytes: Vec<u8>,
        manifest: PathBuf,
        saved: PathBuf,
    }
    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            fs::rename(&self.manifest, &self.saved).unwrap();
        }
    }
    let path = root("payload-drop");
    let mut store = seeded(&path, SyncPolicy::Full, 0);
    let original = store.capture_owned_custody().unwrap();
    let manifest = path.join(MANIFEST_FILE);
    let saved = path.join("manifest.saved");
    let polls = Rc::new(Cell::new(0));
    let called = polls.clone();
    let mut payload = Some(Payload {
        bytes: b"written before Drop".to_vec(),
        manifest: manifest.clone(),
        saved: saved.clone(),
    });
    let producer = std::iter::from_fn(move || {
        called.set(called.get() + 1);
        payload.take().map(|payload| {
            Ok(WalGroupEntry {
                epoch: Epoch(1),
                kind: WalRecordKind::Prepare,
                payload,
            })
        })
    });
    let observed = sync_observation::watch(&path, None);
    let error = store
        .try_append_owned_wal_group(&original, PlacementGroupId(1), producer)
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(
        error.message.starts_with("storage Recover failed:"),
        "{error:?}"
    );
    assert_eq!(
        polls.get(),
        1,
        "next producer executed after payload Drop lost custody"
    );
    assert!(
        observed.events().is_empty(),
        "no final flush may precede detecting Drop loss"
    );
    assert!(saved.is_file() && !manifest.exists());
    fs::rename(saved, manifest).unwrap();
    assert!(store.write_failed && store.cursor.is_none());
    drop(observed);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_group_iterator_drop_occurs_before_final_sync_and_its_loss_cannot_be_adopted() {
    struct LoseOnDrop {
        path: PathBuf,
        saved: PathBuf,
        position: usize,
    }
    impl Iterator for LoseOnDrop {
        type Item = RsfResult<WalGroupEntry<&'static [u8]>>;
        fn next(&mut self) -> Option<Self::Item> {
            self.position += 1;
            if self.position == 1 {
                Some(record(b"visible prefix"))
            } else {
                None
            }
        }
    }
    impl Drop for LoseOnDrop {
        fn drop(&mut self) {
            assert_eq!(self.position, 2);
            fs::rename(&self.path, &self.saved).unwrap();
        }
    }
    let path = root("iterator-drop");
    let mut store = seeded(&path, SyncPolicy::Data, 0);
    let original = store.capture_owned_custody().unwrap();
    let active = store.active.path.clone();
    let before = fs::metadata(&active).unwrap().len();
    let manifest = path.join(MANIFEST_FILE);
    let saved = path.join("manifest.saved");
    let observed = sync_observation::watch(&path, None);
    let error = store
        .try_append_owned_wal_group(
            &original,
            PlacementGroupId(1),
            LoseOnDrop {
                path: manifest.clone(),
                saved: saved.clone(),
                position: 0,
            },
        )
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidInput);
    assert!(
        error.message.starts_with("storage Recover failed:"),
        "{error:?}"
    );
    assert!(observed.events().is_empty());
    assert!(fs::metadata(&active).unwrap().len() > before);
    assert!(store.write_failed && store.cursor.is_none());
    fs::rename(saved, manifest).unwrap();
    drop(observed);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_group_late_input_error_checks_drop_custody_and_returns_physical_failure() {
    struct Late {
        closed: PathBuf,
        saved: PathBuf,
        position: usize,
    }
    impl Iterator for Late {
        type Item = RsfResult<WalGroupEntry<&'static [u8]>>;
        fn next(&mut self) -> Option<Self::Item> {
            self.position += 1;
            Some(if self.position == 1 {
                record(b"first")
            } else {
                Err(input_error())
            })
        }
    }
    impl Drop for Late {
        fn drop(&mut self) {
            assert_eq!(self.position, 2);
            fs::rename(&self.closed, &self.saved).unwrap();
            fs::write(&self.closed, fs::read(&self.saved).unwrap()).unwrap();
        }
    }
    let path = root("late-drop");
    let mut store = seeded(&path, SyncPolicy::Data, 1);
    store
        .append_next_wal(
            PlacementGroupId(1),
            Epoch(1),
            WalRecordKind::Prepare,
            b"second",
        )
        .unwrap();
    let original = store.capture_owned_custody().unwrap();
    let closed = path
        .join(SEGMENT_DIR)
        .join(&store.manifest.segments[0].file_name);
    let saved = path.join("original-closed");
    let error = store
        .try_append_owned_wal_group(
            &original,
            PlacementGroupId(1),
            Late {
                closed: closed.clone(),
                saved: saved.clone(),
                position: 0,
            },
        )
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_ne!(error.message, "injected producer rejection");
    assert!(store.write_failed && store.cursor.is_none());
    fs::remove_file(closed.clone()).unwrap();
    fs::rename(saved, closed).unwrap();
    assert!(store.capture_owned_custody().is_err());
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_group_preexisting_torn_tail_or_oversized_manifest_never_enters_refresh_or_repair() {
    for fault in ["tail", "manifest"] {
        let path = root("preflight-fault");
        let mut store = seeded(&path, SyncPolicy::Full, 0);
        let original = store.capture_owned_custody().unwrap();
        let target = if fault == "tail" {
            store.active.path.clone()
        } else {
            path.join(MANIFEST_FILE)
        };
        if fault == "tail" {
            OpenOptions::new()
                .append(true)
                .open(&target)
                .unwrap()
                .write_all(b"RSF1-torn")
                .unwrap();
        } else {
            OpenOptions::new()
                .write(true)
                .open(&target)
                .unwrap()
                .set_len(128 * 1024 * 1024)
                .unwrap();
        }
        let length = fs::metadata(&target).unwrap().len();
        let bytes = (fault == "tail").then(|| fs::read(&target).unwrap());
        let reads = crate::reader::payload_reads();
        let observed = sync_observation::watch(&path, None);
        let error = store
            .try_append_owned_wal_group(
                &original,
                PlacementGroupId(1),
                [record(b"must not append")],
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::CorruptionDetected);
        assert_eq!(fs::metadata(&target).unwrap().len(), length);
        if let Some(bytes) = bytes {
            assert_eq!(fs::read(&target).unwrap(), bytes);
        }
        assert_eq!(crate::reader::payload_reads(), reads);
        assert!(observed.events().is_empty());
        assert!(store.write_failed && store.cursor.is_none());
        // Payload counter proves no WAL read; length-before-read in owned
        // custody separately establishes the MANIFEST allocation bound.
        drop(observed);
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}
