use super::*;
use crate::{Epoch, SyncPolicy, sync_observation};
use std::os::unix::fs::symlink;

pub(super) fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-bounded-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
pub(super) fn limits() -> SegmentReadLimits {
    SegmentReadLimits {
        max_manifest_bytes: 4096,
        max_records: 100,
        max_encoded_bytes: 1024 * 1024,
        max_record_bytes: 256 * 1024,
    }
}
pub(super) fn allow(_: u64) -> Result<(), SegmentReadLimit> {
    Ok(())
}
fn options(policy: SyncPolicy, max_segment_bytes: u64) -> SegmentStoreOptions {
    SegmentStoreOptions {
        log_options: SegmentLogOptions {
            sync_policy: policy,
        },
        max_segment_bytes,
    }
}
fn append(store: &mut SegmentStore, pg: u64, payload: &[u8]) -> WalRecord {
    store
        .append_next_wal(
            PlacementGroupId(pg),
            Epoch(1),
            WalRecordKind::Prepare,
            payload,
        )
        .unwrap()
        .2
}
fn active(store: &SegmentStore) -> PathBuf {
    store
        .root
        .join(SEGMENT_DIR)
        .join(&store.active_segment().file_name)
}
pub(super) fn inventory(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, at: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            if fs::symlink_metadata(&path).unwrap().is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}
fn limit(error: SegmentReadError, kind: SegmentReadLimit, records: u64) -> SegmentReadCursor {
    match error {
        SegmentReadError::LimitExceeded {
            kind: actual,
            cursor,
        } => {
            assert_eq!(actual, kind);
            assert_eq!(cursor.physical_records(), records);
            cursor
        }
        other => panic!("expected typed capacity, got {other:?}"),
    }
}
fn failure(error: SegmentReadError) -> RsfError {
    match error {
        SegmentReadError::Failure(error) => error,
        other => panic!("expected failure, got {other:?}"),
    }
}

#[test]
fn bounded_shared_recovery_matches_generic_mixed_records_across_rotations() {
    let path = root("mixed");
    let mut store = SegmentStore::open_with_options(&path, options(SyncPolicy::Data, 180)).unwrap();
    store.append(b"raw record").unwrap();
    store
        .append_maintenance(MaintenanceRecord::retention(
            PlacementGroupId(1),
            LogIndex(0),
            b"note",
        ))
        .unwrap();
    for (pg, payload) in [(1, b"a".as_slice()), (2, b"other"), (1, b"last")] {
        append(&mut store, pg, payload);
    }
    assert!(store.segments().len() > 1);
    let stamp = store.capture_owned_custody().unwrap();
    let before = inventory(&path);
    let (bounded, read) = SegmentStore::recover_immutable_bounded(&path, limits(), allow).unwrap();
    read.validate().unwrap();
    let ordinary = store.recover_current().unwrap();
    assert_eq!(bounded, ordinary);
    let mut seen = Vec::new();
    let SegmentScanOutcome::Complete(cursor) = store
        .scan_owned_prefix(&stamp, limits(), allow, |wal| {
            if wal.pg == PlacementGroupId(1) {
                seen.push(wal.clone());
            }
            Ok(SegmentScanControl::Continue)
        })
        .unwrap()
    else {
        panic!("complete scan")
    };
    assert_eq!(seen.len(), 2);
    assert_eq!(
        cursor.physical_records(),
        5,
        "raw, maintenance and filtered PG all count"
    );
    assert_eq!(cursor.encoded_bytes(), ordinary.recovery.truncated_to);
    assert_eq!(inventory(&path), before);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_record_and_aggregate_caps_count_filtered_frames_before_allocation() {
    let path = root("physical-caps");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 2, b"filtered");
    append(&mut store, 1, b"target");
    let stamp = store.capture_owned_custody().unwrap();
    let first = wal_record_disk_bytes(8).unwrap();
    let second = wal_record_disk_bytes(6).unwrap();
    for (policy, kind) in [
        (
            SegmentReadLimits {
                max_records: 1,
                ..limits()
            },
            SegmentReadLimit::Records,
        ),
        (
            SegmentReadLimits {
                max_encoded_bytes: first + second - 1,
                ..limits()
            },
            SegmentReadLimit::EncodedBytes,
        ),
    ] {
        let before = reader::payload_reads();
        let mut targets = 0;
        let cursor = limit(
            store
                .scan_owned_prefix(&stamp, policy, allow, |wal| {
                    if wal.pg == PlacementGroupId(1) {
                        targets += 1;
                    }
                    Ok(SegmentScanControl::Continue)
                })
                .unwrap_err(),
            kind,
            1,
        );
        assert_eq!(targets, 0);
        assert_eq!(cursor.encoded_bytes(), first);
        assert_eq!(
            reader::payload_reads().0 - before.0,
            1,
            "over-budget second payload was not allocated"
        );
    }
    let exact = SegmentReadLimits {
        max_records: 2,
        max_encoded_bytes: first + second,
        max_record_bytes: first,
        ..limits()
    };
    assert!(matches!(
        store
            .scan_owned_prefix(&stamp, exact, allow, |_| Ok(SegmentScanControl::Continue))
            .unwrap(),
        SegmentScanOutcome::Complete(_)
    ));
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_caller_budget_tightens_every_frame_before_payload_allocation() {
    let path = root("caller-cap");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 2, b"skip");
    append(&mut store, 1, b"target");
    let stamp = store.capture_owned_custody().unwrap();
    let overhead = wal_record_disk_bytes(0).unwrap();
    for cap in [45, 46] {
        // (18+4) filtered + (18+6) target
        let before = reader::payload_reads();
        let mut charge = 0_u64;
        let mut selected = 0;
        let result = store.scan_owned_prefix(
            &stamp,
            limits(),
            |disk| {
                let next = charge
                    .checked_add(disk.saturating_sub(overhead))
                    .and_then(|n| n.checked_add(18))
                    .ok_or(SegmentReadLimit::CallerBytes)?;
                if next > cap {
                    return Err(SegmentReadLimit::CallerBytes);
                }
                charge = next;
                Ok(())
            },
            |wal| {
                if wal.pg == PlacementGroupId(1) {
                    selected += 1;
                }
                Ok(SegmentScanControl::Continue)
            },
        );
        if cap == 45 {
            limit(result.unwrap_err(), SegmentReadLimit::CallerBytes, 1);
            assert_eq!(selected, 0);
            assert_eq!(reader::payload_reads().0 - before.0, 1);
        } else {
            assert!(matches!(result.unwrap(), SegmentScanOutcome::Complete(_)));
            assert_eq!(selected, 1);
            assert_eq!(charge, cap);
        }
    }
    // An admission callback cannot waive the independent storage cap.
    let mut admitted = 0;
    limit(
        store
            .scan_owned_prefix(
                &stamp,
                SegmentReadLimits {
                    max_records: 1,
                    ..limits()
                },
                |_| {
                    admitted += 1;
                    Ok(())
                },
                |_| Ok(SegmentScanControl::Continue),
            )
            .unwrap_err(),
        SegmentReadLimit::Records,
        1,
    );
    assert_eq!(admitted, 1);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_sparse_record_and_manifest_limits_precede_payload_allocation() {
    let path = root("sparse");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 1, b"a");
    let segment = active(&store);
    drop(store);
    // A complete physically present sparse frame with a huge declared payload;
    // its checksum is deliberately not valid. Capacity must stop before reading
    // or allocating it, while a generous policy would still reject corruption.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&segment)
        .unwrap();
    use std::io::{Seek, SeekFrom};
    let length = 8 * 1024 * 1024_u64;
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&length.to_le_bytes()).unwrap();
    file.set_len(length + crate::HEADER_LEN as u64).unwrap();
    drop(file);
    let before = reader::payload_reads();
    limit(
        SegmentStore::recover_immutable_bounded(&path, limits(), allow).unwrap_err(),
        SegmentReadLimit::RecordBytes,
        0,
    );
    assert_eq!(reader::payload_reads(), before);
    let manifest_bytes = fs::metadata(path.join(MANIFEST_FILE)).unwrap().len() as usize;
    limit(
        SegmentStore::recover_immutable_bounded(
            &path,
            SegmentReadLimits {
                max_manifest_bytes: manifest_bytes - 1,
                ..limits()
            },
            allow,
        )
        .unwrap_err(),
        SegmentReadLimit::ManifestBytes,
        0,
    );
    assert_eq!(reader::payload_reads(), before);
    assert_eq!(
        fs::metadata(&segment).unwrap().len(),
        length + crate::HEADER_LEN as u64
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_prefix_stop_reads_only_the_complete_selected_prefix() {
    let path = root("stop");
    let mut store = SegmentStore::open(&path).unwrap();
    let first = append(&mut store, 1, b"boundary");
    append(&mut store, 1, &vec![9; 64 * 1024]);
    let stamp = store.capture_owned_custody().unwrap();
    let before = reader::payload_reads();
    let SegmentScanOutcome::Stopped(cursor) = store
        .scan_owned_prefix(&stamp, limits(), allow, |wal| {
            assert_eq!(wal, &first);
            Ok(SegmentScanControl::Stop)
        })
        .unwrap()
    else {
        panic!("expected complete-record stop")
    };
    assert_eq!(cursor.physical_records(), 1);
    assert_eq!(cursor.encoded_bytes(), wal_record_disk_bytes(8).unwrap());
    assert_eq!(reader::payload_reads().0 - before.0, 1);
    assert_eq!(
        reader::payload_reads().1 - before.1,
        crate::WAL_HEADER_LEN as u64 + 8
    );
    store.validate_owned_custody(&stamp).unwrap();
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_custody_change_overrides_visitor_error_stop_and_admission_limit() {
    for at_admission in [false, true] {
        for stop in [false, true] {
            let path = root("callback-custody");
            let mut store = SegmentStore::open(&path).unwrap();
            append(&mut store, 1, b"one");
            let stamp = store.capture_owned_custody().unwrap();
            let segment = active(&store);
            let replace = || {
                fs::rename(&segment, path.join("old-inode")).unwrap();
                fs::copy(path.join("old-inode"), &segment).unwrap();
            };
            let error = store
                .scan_owned_prefix(
                    &stamp,
                    limits(),
                    |_| {
                        if at_admission {
                            replace();
                            return Err(SegmentReadLimit::CallerBytes);
                        }
                        Ok(())
                    },
                    |_| {
                        replace();
                        if stop {
                            Ok(SegmentScanControl::Stop)
                        } else {
                            Err(RsfError::new(ErrorCode::InvalidInput, "visitor rejection"))
                        }
                    },
                )
                .unwrap_err();
            let error = failure(error);
            assert_eq!(error.code, ErrorCode::CorruptionDetected);
            assert_ne!(error.message, "visitor rejection");
            assert!(store.validate_owned_custody(&stamp).is_err());
            drop(store);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn bounded_strict_recovery_preserves_checksum_chain_and_epoch_errors() {
    for fault in ["checksum", "chain", "index", "epoch"] {
        let path = root(fault);
        let mut store = SegmentStore::open(&path).unwrap();
        let first = append(&mut store, 1, b"one");
        let segment = active(&store);
        drop(store);
        if fault == "checksum" {
            let mut bytes = fs::read(&segment).unwrap();
            *bytes.last_mut().unwrap() ^= 1;
            fs::write(&segment, bytes).unwrap();
        } else {
            let mut log = SegmentLog::open(&segment).unwrap();
            let input = WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(if fault == "epoch" { 0 } else { 1 }),
                index: LogIndex(if fault == "index" { 3 } else { 2 }),
                kind: WalRecordKind::Prepare,
                previous_hash: if fault == "chain" {
                    [0; 32]
                } else {
                    first.record_hash
                },
                payload: b"two",
            };
            // Construct a checksum-valid frame with an invalid chain relation.
            // The guarded WAL append correctly refuses this input, so it
            // cannot produce the malformed recovery fixture.
            let payload_hash = digest(input.payload);
            let record_hash = wal_record_hash(
                input.pg,
                input.epoch,
                input.index,
                input.kind,
                &input.previous_hash,
                &payload_hash,
                input.payload,
            );
            let encoded = encode_wal_record(input, payload_hash, record_hash).unwrap();
            assert!(crate::decode_wal_record(&encoded).unwrap().is_some());
            log.append(&encoded).unwrap();
        }
        let before = inventory(&path);
        let bounded =
            failure(SegmentStore::recover_immutable_bounded(&path, limits(), allow).unwrap_err());
        let generic = SegmentStore::recover_existing(&path).unwrap_err();
        let (code, message) = match fault {
            "checksum" => (ErrorCode::CorruptionDetected, "checksum mismatch"),
            "chain" => (ErrorCode::CorruptionDetected, "wal hash-chain mismatch"),
            "index" => (
                ErrorCode::CorruptionDetected,
                "wal index 3 is not monotonic",
            ),
            "epoch" => (ErrorCode::StaleEpoch, "wal epoch 0 moved backward"),
            _ => unreachable!(),
        };
        assert_eq!(bounded.code, code, "{fault}");
        assert!(bounded.message.starts_with(message), "{fault}: {bounded:?}");
        assert_eq!(bounded.code, generic.code, "{fault}");
        assert_eq!(bounded.message, generic.message, "{fault}");
        assert_eq!(inventory(&path), before);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_strict_writer_rejects_torn_tail_without_repair_then_generic_can_repair() {
    let path = root("torn");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 1, b"one");
    let segment = active(&store);
    drop(store);
    let good = fs::read(&segment).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&segment)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    let before = inventory(&path);
    let error = failure(
        SegmentStore::open_existing_recovered_bounded(&path, Default::default(), limits(), allow)
            .unwrap_err(),
    );
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(inventory(&path), before);
    let ordinary = SegmentStore::open(&path).unwrap();
    assert_eq!(fs::read(&segment).unwrap(), good);
    drop(ordinary);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_strict_writer_requires_existing_regular_data_and_sidecars() {
    for target in ["manifest", "segment", "store-lock", "active-lock"] {
        for fault in ["missing", "symlink", "hardlink"] {
            let path = root("strict-path");
            let mut store = SegmentStore::open(&path).unwrap();
            append(&mut store, 1, b"one");
            let segment = active(&store);
            drop(store);
            let target = match target {
                "manifest" => path.join(MANIFEST_FILE),
                "segment" => segment.clone(),
                "store-lock" => path.join("MANIFEST.lock"),
                _ => segment.with_extension("lock"),
            };
            let saved = path.join("saved");
            fs::rename(&target, &saved).unwrap();
            if fault == "symlink" {
                symlink(&saved, &target).unwrap();
            }
            if fault == "hardlink" {
                fs::hard_link(&saved, &target).unwrap();
            }
            let before = inventory(&path);
            assert!(
                matches!(
                    SegmentStore::open_existing_recovered_bounded(
                        &path,
                        Default::default(),
                        limits(),
                        allow
                    ),
                    Err(SegmentReadError::Failure(_))
                ),
                "{target:?} {fault}"
            );
            assert_eq!(inventory(&path), before);
            if fault == "missing" {
                assert!(!target.exists());
            }
            fs::remove_dir_all(path).unwrap();
        }
    }
    let missing = root("missing-root");
    assert!(
        SegmentStore::open_existing_recovered_bounded(
            &missing,
            Default::default(),
            limits(),
            allow
        )
        .is_err()
    );
    assert!(!missing.exists());
}

#[test]
fn bounded_strict_writer_rejects_replaced_name_of_reentrant_held_lock() {
    let path = root("held-lock");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 1, b"one");
    let lock = path.join("MANIFEST.lock");
    fs::rename(&lock, path.join("old-lock")).unwrap();
    fs::write(&lock, b"").unwrap();
    let before = inventory(&path);
    let error = failure(
        SegmentStore::open_existing_recovered_bounded(&path, Default::default(), limits(), allow)
            .unwrap_err(),
    );
    assert!(error.message.contains("held lock"), "{error:?}");
    assert_eq!(inventory(&path), before);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_strict_writer_uses_recovered_cursor_and_preserves_append_sync_policy() {
    for policy in [SyncPolicy::None, SyncPolicy::Data, SyncPolicy::Full] {
        let path = root("writer-cursor");
        let settings = options(policy, 200);
        let mut store = SegmentStore::open_with_options(&path, settings).unwrap();
        append(&mut store, 1, b"one");
        drop(store);
        let before = inventory(&path);
        let barriers = crate::durable_directory::sync_attempts();
        let observed = sync_observation::watch(&path, None);
        let (mut store, recovered) =
            SegmentStore::open_existing_recovered_bounded(&path, settings, limits(), allow)
                .unwrap();
        assert_eq!(recovered.recovery.wal_records.len(), 1);
        assert_eq!(inventory(&path), before);
        assert!(
            crate::durable_directory::sync_attempts() > barriers,
            "strict owner must publish existing names/ancestors"
        );
        assert!(
            observed.events().is_empty(),
            "opening is not implicit retained-data renewal"
        );
        let old = store.capture_owned_custody().unwrap();
        let before = reader::payload_reads();
        let next = append(&mut store, 1, b"two");
        assert_eq!(next.index, LogIndex(2));
        assert_eq!(
            reader::payload_reads(),
            before,
            "actual bounded recovery seeded the writer cursor"
        );
        let events = observed.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].policy, policy);
        assert!(events[0].completed);
        assert!(store.validate_owned_custody(&old).is_err());
        store.capture_owned_custody().unwrap();
        drop(observed);
        drop(store);
        assert_eq!(
            SegmentStore::recover_existing(&path)
                .unwrap()
                .recovery
                .wal_records
                .len(),
            2
        );
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_compacted_prefix_is_typed_unsupported_without_payload_reads() {
    let path = root("compacted");
    let mut store = SegmentStore::open(&path).unwrap();
    let record = append(&mut store, 1, b"one");
    let segment = active(&store);
    drop(store);
    // Real canonical compacted representation: prefix plus empty active segment.
    let mut manifest =
        SegmentManifest::decode(&fs::read(path.join(MANIFEST_FILE)).unwrap()).unwrap();
    manifest.compacted_prefixes.insert(
        PlacementGroupId(1),
        crate::PlacementGroupPrefix {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(1),
            record_hash: record.record_hash,
        },
    );
    fs::write(path.join(MANIFEST_FILE), manifest.encode()).unwrap();
    fs::write(segment, b"").unwrap();
    let before = reader::payload_reads();
    assert!(matches!(
        SegmentStore::recover_immutable_bounded(&path, limits(), allow),
        Err(SegmentReadError::CompactedPrefixUnsupported)
    ));
    assert!(matches!(
        SegmentStore::open_existing_recovered_bounded(&path, Default::default(), limits(), allow),
        Err(SegmentReadError::CompactedPrefixUnsupported)
    ));
    assert_eq!(reader::payload_reads(), before);
    let recovered = SegmentStore::recover_existing(&path).unwrap();
    assert_eq!(
        recovered
            .recovery
            .pg_tail(PlacementGroupId(1))
            .unwrap()
            .index,
        LogIndex(1)
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_writer_admission_rejection_cannot_create_a_partial_success() {
    let path = root("open-admission");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 1, b"one");
    append(&mut store, 1, b"two");
    drop(store);
    let before = inventory(&path);
    let mut admitted = 0;
    limit(
        SegmentStore::open_existing_recovered_bounded(&path, Default::default(), limits(), |_| {
            admitted += 1;
            if admitted == 2 {
                Err(SegmentReadLimit::CallerBytes)
            } else {
                Ok(())
            }
        })
        .unwrap_err(),
        SegmentReadLimit::CallerBytes,
        1,
    );
    assert_eq!(inventory(&path), before);
    let (_, recovered) =
        SegmentStore::open_existing_recovered_bounded(&path, Default::default(), limits(), allow)
            .unwrap();
    assert_eq!(recovered.recovery.wal_records.len(), 2);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn bounded_size_helpers_and_nonzero_policy_reject_overflow_without_io() {
    assert_eq!(
        wal_record_disk_bytes(3).unwrap(),
        crate::HEADER_LEN as u64 + crate::WAL_HEADER_LEN as u64 + 3
    );
    assert!(wal_record_disk_bytes(u64::MAX).is_err());
    assert!(append_only_manifest_byte_bound(u64::MAX).is_err());
    let path = root("invalid-policy");
    let before = reader::payload_reads();
    for policy in [
        SegmentReadLimits {
            max_records: 0,
            ..limits()
        },
        SegmentReadLimits {
            max_encoded_bytes: 0,
            ..limits()
        },
        SegmentReadLimits {
            max_record_bytes: 0,
            ..limits()
        },
        SegmentReadLimits {
            max_manifest_bytes: 0,
            ..limits()
        },
    ] {
        assert_eq!(
            failure(SegmentStore::recover_immutable_bounded(&path, policy, allow).unwrap_err())
                .code,
            ErrorCode::InvalidInput
        );
    }
    assert!(!path.exists());
    assert_eq!(reader::payload_reads(), before);
}

#[test]
fn bounded_shared_parser_preserves_supported_checksum_formats_and_rejects_bad_headers() {
    for algorithm in [
        crate::ChecksumAlgorithm::Fnv1a64,
        crate::ChecksumAlgorithm::Blake3_64,
    ] {
        let path = root("checksum-format");
        let mut store = SegmentStore::open(&path).unwrap();
        append(&mut store, 1, b"valid");
        let segment = active(&store);
        drop(store);
        let mut bytes = fs::read(&segment).unwrap();
        bytes[6..8].copy_from_slice(&algorithm.to_u16().to_le_bytes());
        let checksum = algorithm.checksum(&bytes[crate::HEADER_LEN..]);
        bytes[16..24].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&segment, &bytes).unwrap();
        let (bounded, _) = SegmentStore::recover_immutable_bounded(&path, limits(), allow).unwrap();
        assert_eq!(bounded, SegmentStore::recover_existing(&path).unwrap());
        assert_eq!(
            bounded.recovery.records[0].header.checksum_algorithm,
            algorithm
        );
        for field in [0, 4, 6] {
            let mut damaged = bytes.clone();
            damaged[field] = 0xff;
            fs::write(&segment, &damaged).unwrap();
            let before = reader::payload_reads();
            let error = failure(
                SegmentStore::recover_immutable_bounded(&path, limits(), allow).unwrap_err(),
            );
            assert_eq!(error.code, ErrorCode::CorruptionDetected);
            assert_eq!(reader::payload_reads(), before);
            assert_eq!(fs::read(&segment).unwrap(), damaged);
        }
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_cold_admission_cannot_certify_replacement_or_mask_it_as_capacity() {
    for writer in [false, true] {
        let path = root("cold-callback");
        let mut store = SegmentStore::open(&path).unwrap();
        append(&mut store, 1, b"one");
        let segment = active(&store);
        drop(store);
        let admit = |_| {
            fs::rename(&segment, path.join("previous")).unwrap();
            fs::copy(path.join("previous"), &segment).unwrap();
            Err(SegmentReadLimit::CallerBytes)
        };
        let error = if writer {
            SegmentStore::open_existing_recovered_bounded(
                &path,
                Default::default(),
                limits(),
                admit,
            )
            .unwrap_err()
        } else {
            SegmentStore::recover_immutable_bounded(&path, limits(), admit).unwrap_err()
        };
        assert_eq!(failure(error).code, ErrorCode::CorruptionDetected);
        // Rejection drops any attempted lock references. A fresh qualified open
        // can validate the replacement independently; old read never certified it.
        let (store, _) = SegmentStore::open_existing_recovered_bounded(
            &path,
            Default::default(),
            limits(),
            allow,
        )
        .unwrap();
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn bounded_owned_custody_rejects_lost_or_replaced_actual_held_sidecars() {
    for manifest in [true, false] {
        for replace in [false, true] {
            let path = root("warm-sidecar");
            let mut store = SegmentStore::open(&path).unwrap();
            append(&mut store, 1, b"owned");
            let stamp = store.capture_owned_custody().unwrap();
            let lock = if manifest {
                path.join("MANIFEST.lock")
            } else {
                active(&store).with_extension("lock")
            };
            fs::rename(&lock, path.join("original-lock")).unwrap();
            if replace {
                fs::write(&lock, b"").unwrap();
            }
            let before = inventory(&path);
            assert!(store.validate_owned_custody(&stamp).is_err());
            assert!(
                store.capture_owned_custody().is_err(),
                "fresh capture must not adopt replacement exclusion"
            );
            assert!(matches!(
                store.scan_owned_prefix(&stamp, limits(), allow, |_| Ok(SegmentScanControl::Stop)),
                Err(SegmentReadError::Failure(_))
            ));
            assert_eq!(inventory(&path), before);
            drop(store);
            fs::remove_dir_all(path).unwrap();
        }
    }
}

#[test]
fn owned_full_recovery_reads_real_mixed_bytes_and_preserves_writer_custody() {
    let path = root("owned-full-real");
    let mut store = SegmentStore::open_with_options(&path, options(SyncPolicy::Data, 180)).unwrap();
    store.append(b"raw frame").unwrap();
    store
        .append_maintenance(MaintenanceRecord::retention(
            PlacementGroupId(1),
            LogIndex(0),
            b"retained note",
        ))
        .unwrap();
    let first = append(&mut store, 1, b"first");
    let other = append(&mut store, 2, b"other group");
    let last = append(&mut store, 1, b"last");
    assert!(store.segments().len() > 1);
    let custody = store.capture_owned_custody().unwrap();
    let before = inventory(&path);
    let payloads = reader::payload_reads();
    let syncs = sync_observation::watch(&path, None);
    let recovered = store
        .recover_owned_immutable_bounded(&custody, limits(), allow)
        .unwrap();
    assert_eq!(reader::payload_reads().0 - payloads.0, 5);
    assert!(
        syncs.events().is_empty(),
        "readback must not append or renew data"
    );
    drop(syncs);
    assert_eq!(recovered.recovery.wal_records, vec![first, other, last]);
    assert_eq!(recovered.recovery.records.len(), 5);
    assert_eq!(recovered.recovery.maintenance_records.len(), 1);
    assert_eq!(inventory(&path), before);
    store.validate_owned_custody(&custody).unwrap();
    let (immutable, _) = SegmentStore::recover_immutable_bounded(&path, limits(), allow).unwrap();
    assert_eq!(recovered, immutable);
    let payloads = reader::payload_reads();
    limit(
        store
            .recover_owned_immutable_bounded(
                &custody,
                SegmentReadLimits {
                    max_records: 1,
                    ..limits()
                },
                allow,
            )
            .unwrap_err(),
        SegmentReadLimit::Records,
        1,
    );
    assert_eq!(reader::payload_reads().0 - payloads.0, 1);
    assert_eq!(inventory(&path), before);
    store.validate_owned_custody(&custody).unwrap();
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_full_recovery_rejects_replaced_owner_files_before_payload_reads() {
    for damage in ["manifest", "segment", "manifest-lock", "segment-lock"] {
        let path = root(damage);
        let mut store = SegmentStore::open(&path).unwrap();
        append(&mut store, 1, b"owned bytes");
        let custody = store.capture_owned_custody().unwrap();
        let segment = active(&store);
        let target = match damage {
            "manifest" => path.join(MANIFEST_FILE),
            "segment" => segment.clone(),
            "manifest-lock" => path.join(MANIFEST_FILE).with_extension("lock"),
            _ => segment.with_extension("lock"),
        };
        let displaced = path.join("actual-held-inode");
        fs::rename(&target, &displaced).unwrap();
        fs::copy(&displaced, &target).unwrap();
        let before = inventory(&path);
        let payloads = reader::payload_reads();
        let error = failure(
            store
                .recover_owned_immutable_bounded(&custody, limits(), allow)
                .unwrap_err(),
        );
        assert_eq!(error.code, ErrorCode::CorruptionDetected);
        assert_eq!(
            reader::payload_reads(),
            payloads,
            "replacement must not become a new snapshot"
        );
        assert_eq!(
            inventory(&path),
            before,
            "recovery must not repair or remove a replacement"
        );
        drop(store);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn owned_full_recovery_original_custody_failure_overrides_admission_limit() {
    let path = root("owned-full-admission");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 1, b"real frame");
    let custody = store.capture_owned_custody().unwrap();
    let segment = active(&store);
    let mut called = false;
    let error = failure(
        store
            .recover_owned_immutable_bounded(&custody, limits(), |_| {
                assert!(!called);
                called = true;
                let held = path.join("original-active");
                fs::rename(&segment, &held).unwrap();
                fs::copy(&held, &segment).unwrap();
                Err(SegmentReadLimit::CallerBytes)
            })
            .unwrap_err(),
    );
    assert!(called, "real parser must reach the admission boundary");
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        fs::read(&segment).unwrap(),
        fs::read(path.join("original-active")).unwrap()
    );
    let before = inventory(&path);
    assert_eq!(
        store.validate_owned_custody(&custody).unwrap_err().code,
        ErrorCode::CorruptionDetected
    );
    assert_eq!(inventory(&path), before);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn owned_full_recovery_checks_actual_checksum_even_when_metadata_still_matches() {
    let path = root("owned-full-checksum");
    let mut store = SegmentStore::open(&path).unwrap();
    append(&mut store, 1, b"physical checksum evidence");
    let custody = store.capture_owned_custody().unwrap();
    let segment = active(&store);
    let modified = fs::metadata(&segment).unwrap().modified().unwrap();
    let mut bytes = fs::read(&segment).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&segment, &bytes).unwrap();
    // Deliberately bypass the metadata-only guard to prove that this operation
    // parses disk bytes. This is not a guarantee against ongoing hostile writes.
    OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(modified))
        .unwrap();
    store.validate_owned_custody(&custody).unwrap();
    let payloads = reader::payload_reads();
    let error = failure(
        store
            .recover_owned_immutable_bounded(&custody, limits(), allow)
            .unwrap_err(),
    );
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert!(error.message.starts_with("checksum mismatch at offset 0"));
    assert_eq!(reader::payload_reads().0 - payloads.0, 1);
    assert_eq!(
        fs::read(&segment).unwrap(),
        bytes,
        "checksum failure must not repair"
    );
    drop(store);
    fs::remove_dir_all(path).unwrap();
}
