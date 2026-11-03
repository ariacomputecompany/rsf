use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

mod coordination;
mod custody;
pub use custody::{SegmentReadCustody, SegmentStoreCustody};
mod group;
mod initialization;
mod wal;
use coordination::{StoreGate, lock_gate, store_gate};
pub use group::{WalGroupEntry, WalGroupRecord};

use rsf_core::storage::{WalRecord, WalRecordKind};
use rsf_core::{ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult};

use crate::{
    HEADER_LEN, MaintenanceKind, MaintenanceRecord, PlacementGroupPrefix, Recovery, SegmentLock,
    SegmentLog, SegmentLogOptions, StorageIoOperation, WalAppend, WalTail, digest,
    encode_existing_wal_record, encode_maintenance_record, encode_wal_record, storage_error,
    wal_record_hash,
};

const MANIFEST_FILE: &str = "MANIFEST";
const SEGMENT_DIR: &str = "segments";
const MANIFEST_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentStoreOptions {
    pub log_options: SegmentLogOptions,
    pub max_segment_bytes: u64,
}

impl Default for SegmentStoreOptions {
    fn default() -> Self {
        Self {
            log_options: SegmentLogOptions::default(),
            max_segment_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentDescriptor {
    pub id: u64,
    pub file_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SegmentManifest {
    active_id: u64,
    segments: Vec<SegmentDescriptor>,
    compacted_prefixes: BTreeMap<PlacementGroupId, PlacementGroupPrefix>,
}

#[derive(Debug)]
pub struct SegmentStore {
    root: PathBuf,
    manifest: SegmentManifest,
    active: SegmentLog,
    _lock: SegmentLock,
    options: SegmentStoreOptions,
    write_failed: bool,
    gate: StoreGate,
    cursor: Option<WalCursor>,
    observer: std::sync::Arc<()>,
    external_changes: u64,
}

#[derive(Debug)]
struct WalCursor {
    tails: BTreeMap<PlacementGroupId, WalTail>,
    stamp: Vec<SegmentStamp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SegmentStamp {
    id: u64,
    bytes: u64,
    modified: SystemTime,
}

/// Opaque identity of a validated physical store state. Comparing revisions is
/// bounded by segment count; changed disk state is recovered and validated
/// before a new revision is returned. It is not a durable checkpoint.
#[derive(Clone, Debug)]
pub struct SegmentStoreRevision {
    manifest: SegmentManifest,
    stamp: Vec<SegmentStamp>,
    observer: std::sync::Arc<()>,
    external_changes: u64,
}
impl PartialEq for SegmentStoreRevision {
    fn eq(&self, other: &Self) -> bool {
        self.same_writer_epoch(other)
            && self.manifest == other.manifest
            && self.stamp == other.stamp
    }
}
impl Eq for SegmentStoreRevision {}
impl SegmentStoreRevision {
    /// True only for the same observed handle without an intervening refresh
    /// caused by another writer or a changed physical layout.
    pub fn same_writer_epoch(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.observer, &other.observer)
            && self.external_changes == other.external_changes
    }
}

impl SegmentStore {
    pub fn open(root: impl AsRef<Path>) -> RsfResult<Self> {
        Self::open_with_options(root, SegmentStoreOptions::default())
    }

    pub fn open_with_options(
        root: impl AsRef<Path>,
        options: SegmentStoreOptions,
    ) -> RsfResult<Self> {
        Self::open_recovered(root, options).map(|(store, _)| store)
    }

    /// Open the writer and return the recovery already validated while opening
    /// it. Startup consumers can build their logical state from these records
    /// without immediately scanning the same segments again.
    pub fn open_recovered(
        root: impl AsRef<Path>,
        options: SegmentStoreOptions,
    ) -> RsfResult<(Self, SegmentStoreRecovery)> {
        let root = root.as_ref().to_path_buf();
        crate::create_dir_all_synced(root.join(SEGMENT_DIR))
            .map_err(|err| storage_error(StorageIoOperation::Open, err))?;

        let gate = store_gate(&root)?;
        let opening_gate = gate.clone();
        let _guard = lock_gate(&opening_gate)?;
        let lock = SegmentLock::acquire(&root.join(MANIFEST_FILE))?;
        let manifest = read_or_create_manifest(&root)?;
        let recovery = recover_manifest(&root, &manifest)?;
        let active_descriptor = manifest.active_segment()?;
        let active = SegmentLog::open_after_external_recovery(
            root.join(SEGMENT_DIR).join(&active_descriptor.file_name),
            options.log_options,
        )?;
        let cursor = Some(WalCursor {
            tails: recovery.recovery.pg_tails.clone(),
            stamp: segment_stamp(&root, &manifest)?,
        });

        Ok((
            Self {
                root,
                manifest,
                active,
                _lock: lock,
                options,
                write_failed: false,
                gate,
                cursor,
                observer: std::sync::Arc::new(()),
                external_changes: 0,
            },
            recovery,
        ))
    }

    /// Observe a validated WAL tail without replaying unchanged segments.
    pub fn wal_tail(&mut self, pg: PlacementGroupId) -> RsfResult<Option<WalTail>> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        self.wal_tail_owned(pg)
    }

    /// Detect writes, rotations, truncation, or compaction from another handle.
    /// Own successful appends keep the validated physical cursor current.
    pub fn revision(&mut self) -> RsfResult<SegmentStoreRevision> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        self.wal_tail_owned(PlacementGroupId(0))?;
        Ok(SegmentStoreRevision {
            manifest: self.manifest.clone(),
            observer: self.observer.clone(),
            external_changes: self.external_changes,
            stamp: self
                .cursor
                .as_ref()
                .expect("validated cursor")
                .stamp
                .clone(),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn segments(&self) -> &[SegmentDescriptor] {
        &self.manifest.segments
    }

    pub fn active_segment(&self) -> &SegmentDescriptor {
        self.manifest
            .active_segment()
            .expect("active segment validated during open")
    }

    pub fn append(&mut self, payload: &[u8]) -> RsfResult<(u64, u64)> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        let length = record_disk_len(payload.len())?;
        self.cursor = None;
        self.write_failed = true;
        let result = (|| {
            self.rotate_if_needed(length)?;
            let id = self.manifest.active_id;
            let offset = self.active.append(payload)?;
            Ok((id, offset))
        })();
        self.write_failed = result.is_err();
        result
    }

    pub fn append_wal(&mut self, input: WalAppend<'_>) -> RsfResult<(u64, u64, WalRecord)> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        let tail = self.wal_tail_owned(input.pg)?;
        self.append_at_tail(input, tail)
    }

    /// Derive the physical index and hash chain from one validated recovery.
    /// Callers with their own logical indices should encode those in the
    /// payload; they need not recover the physical WAL a second time.
    pub fn append_next_wal(
        &mut self,
        pg: PlacementGroupId,
        epoch: rsf_core::Epoch,
        kind: WalRecordKind,
        payload: &[u8],
    ) -> RsfResult<(u64, u64, WalRecord)> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        let tail = self.wal_tail_owned(pg)?;
        let index = tail
            .map_or(Some(1), |tail| tail.index.0.checked_add(1))
            .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "WAL index exhausted"))?;
        self.append_at_tail(
            WalAppend {
                pg,
                epoch,
                index: LogIndex(index),
                kind,
                previous_hash: tail.map_or([0; 32], |tail| tail.record_hash),
                payload,
            },
            tail,
        )
    }

    fn append_at_tail(
        &mut self,
        input: WalAppend<'_>,
        tail: Option<WalTail>,
    ) -> RsfResult<(u64, u64, WalRecord)> {
        let wal::Frame {
            encoded,
            payload_hash,
            record_hash,
        } = wal::encode_at_tail(input, tail)?;
        let length = record_disk_len(encoded.len())?;
        self.write_failed = true;
        let result = (|| {
            self.rotate_if_needed(length)?;
            Ok((self.manifest.active_id, self.active.append(&encoded)?))
        })();
        self.write_failed = result.is_err();
        let (segment_id, offset) = result?;

        // Publish the in-memory tail only after append and its configured sync
        // succeed. Physical stamps invalidate it after another writer, recovery
        // truncation, or an out-of-band file change; reads still verify checksums.
        let stamp = match segment_stamp(&self.root, &self.manifest) {
            Ok(stamp) => stamp,
            Err(err) => {
                self.write_failed = true;
                return Err(err);
            }
        };
        let cursor = self
            .cursor
            .as_mut()
            .expect("WAL cursor validated before append");
        cursor.tails.insert(
            input.pg,
            WalTail {
                epoch: input.epoch,
                index: input.index,
                record_hash,
            },
        );
        cursor.stamp = stamp;

        Ok((
            segment_id,
            offset,
            WalRecord {
                pg: input.pg,
                epoch: input.epoch,
                index: input.index,
                kind: input.kind,
                previous_hash: input.previous_hash,
                payload: input.payload.to_vec(),
                payload_hash,
                record_hash,
            },
        ))
    }

    pub fn append_maintenance(
        &mut self,
        record: MaintenanceRecord<'_>,
    ) -> RsfResult<MaintenanceRecord<'static>> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        self.cursor = None;
        let result = self.active.append_maintenance(record);
        self.write_failed = result.is_err();
        result
    }

    pub fn scan(&self, filter: SegmentScanFilter) -> RsfResult<SegmentScan> {
        let recovered = self.recover_current()?;
        let wal_records = recovered
            .recovery
            .wal_records
            .into_iter()
            .filter(|record| filter.pg.is_none_or(|pg| record.pg == pg))
            .collect::<Vec<_>>();

        Ok(SegmentScan {
            segments: recovered.segments,
            wal_records,
            compacted_prefixes: recovered.recovery.compacted_prefixes,
        })
    }

    pub fn benchmark_append_replay(
        &mut self,
        payloads: &[&[u8]],
    ) -> RsfResult<SegmentStoreBenchmark> {
        let segments_before = self.manifest.segments.len();
        let append_start = Instant::now();
        for payload in payloads {
            self.append(payload)?;
        }
        let append_elapsed = append_start.elapsed();

        let replay_start = Instant::now();
        let recovery = self.recover_current()?;
        let replay_elapsed = replay_start.elapsed();

        Ok(SegmentStoreBenchmark {
            appended_records: payloads.len(),
            recovered_records: recovery.recovery.records.len(),
            recovered_wal_records: recovery.recovery.wal_records.len(),
            segments_before,
            segments_after: recovery.segments.len(),
            bytes: payloads.iter().map(|payload| payload.len() as u64).sum(),
            append_nanos: append_elapsed.as_nanos(),
            replay_nanos: replay_elapsed.as_nanos(),
        })
    }

    pub fn truncate_uncommitted_tails(&mut self) -> RsfResult<TruncateReport> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        let recovered = self.recover_owned()?;
        let mut latest_commit = recovered.recovery.compacted_prefixes.clone();
        for record in &recovered.recovery.wal_records {
            if matches!(
                record.kind,
                WalRecordKind::Commit
                    | WalRecordKind::Membership
                    | WalRecordKind::ViewPromise
                    | WalRecordKind::ViewInstallation
                    | WalRecordKind::MembershipPromise
            ) {
                latest_commit.insert(
                    record.pg,
                    PlacementGroupPrefix {
                        pg: record.pg,
                        epoch: record.epoch,
                        index: record.index,
                        record_hash: record.record_hash,
                    },
                );
            }
        }

        let mut retained_wal_records = Vec::new();
        let mut dropped_wal_records = 0_usize;
        for record in &recovered.recovery.wal_records {
            if latest_commit
                .get(&record.pg)
                .is_some_and(|prefix| record.index.0 <= prefix.index.0)
            {
                retained_wal_records.push(record.clone());
            } else {
                dropped_wal_records += 1;
            }
        }

        let old_segments = self.manifest.segments.clone();
        let old_bytes = segment_bytes(&self.root, &old_segments)?;
        let new_id = self.manifest.next_id()?;
        let new_file_name = segment_file_name(new_id);
        let final_path = self.root.join(SEGMENT_DIR).join(&new_file_name);
        let note = format!("dropped-uncommitted-wal-records:{dropped_wal_records}");
        let marker = MaintenanceRecord {
            kind: MaintenanceKind::Compaction,
            pg: PlacementGroupId(0),
            retained_through: LogIndex(0),
            reclaimed_bytes: 0,
            note: std::borrow::Cow::Owned(note.into_bytes()),
        };

        self.write_failed = true;
        self.cursor = None;
        rewrite_single_segment(
            &self.root,
            self.options.log_options,
            new_id,
            &retained_wal_records,
            &recovered.recovery.maintenance_records,
            Some(marker),
        )?;

        let new_manifest = SegmentManifest {
            active_id: new_id,
            segments: vec![SegmentDescriptor {
                id: new_id,
                file_name: new_file_name.clone(),
            }],
            compacted_prefixes: self.manifest.compacted_prefixes.clone(),
        };
        write_manifest(&self.root, &new_manifest)?;
        let _ = recover_manifest(&self.root, &new_manifest)?;

        self.active =
            SegmentLog::open_after_external_recovery(&final_path, self.options.log_options)?;
        self.manifest = new_manifest;
        self.write_failed = false;

        let segments_dir = self.root.join(SEGMENT_DIR);
        for segment in &old_segments {
            if segment.file_name != new_file_name {
                let _ = fs::remove_file(segments_dir.join(&segment.file_name));
            }
        }

        let new_bytes = fs::metadata(&final_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        Ok(TruncateReport {
            old_segments: old_segments.len(),
            new_segments: self.manifest.segments.len(),
            dropped_wal_records,
            retained_wal_records: retained_wal_records.len(),
            reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
        })
    }

    pub fn compact(
        &mut self,
        retained_through: BTreeMap<PlacementGroupId, LogIndex>,
    ) -> RsfResult<CompactionReport> {
        self.ensure_writable()?;
        let gate = self.gate.clone();
        let _guard = lock_gate(&gate)?;
        self.refresh_layout()?;
        let recovered = self.recover_owned()?;
        let mut compacted_prefixes = self.manifest.compacted_prefixes.clone();
        let mut retained_wal_records = Vec::new();
        let mut dropped_wal_records = 0_usize;

        for (pg, index) in &retained_through {
            if compacted_prefixes
                .get(pg)
                .is_some_and(|prefix| prefix.covers(*index))
            {
                continue;
            }
            let base = recovered
                .recovery
                .wal_records
                .iter()
                .find(|record| {
                    record.pg == *pg
                        && record.index == *index
                        && record.kind == WalRecordKind::Commit
                })
                .ok_or_else(|| {
                    RsfError::new(
                        ErrorCode::InvalidInput,
                        format!(
                            "retention point for pg {} index {} is not a recovered commit record",
                            pg.0, index.0
                        ),
                    )
                })?;
            compacted_prefixes.insert(
                *pg,
                PlacementGroupPrefix {
                    pg: *pg,
                    epoch: base.epoch,
                    index: base.index,
                    record_hash: base.record_hash,
                },
            );
        }

        for record in &recovered.recovery.wal_records {
            if retained_through
                .get(&record.pg)
                .is_some_and(|index| record.index.0 <= index.0)
            {
                dropped_wal_records += 1;
            } else {
                retained_wal_records.push(record.clone());
            }
        }

        let old_segments = self.manifest.segments.clone();
        let old_bytes = segment_bytes(&self.root, &old_segments)?;
        let new_id = self.manifest.next_id()?;
        let new_file_name = segment_file_name(new_id);
        let temp_file_name = format!("{new_file_name}.compact");
        let segments_dir = self.root.join(SEGMENT_DIR);
        let temp_path = segments_dir.join(&temp_file_name);
        let final_path = segments_dir.join(&new_file_name);
        self.write_failed = true;
        self.cursor = None;
        let _ = fs::remove_file(&temp_path);
        let _ = fs::remove_file(&final_path);

        let mut compacted_log =
            SegmentLog::open_with_options(&temp_path, self.options.log_options)?;
        for record in &retained_wal_records {
            compacted_log.append(&encode_existing_wal_record(record)?)?;
        }
        for record in &recovered.recovery.maintenance_records {
            compacted_log.append(&encode_maintenance_record(record.clone())?)?;
        }

        let retained_high_water = retained_through
            .iter()
            .map(|(pg, index)| format!("pg:{}@{}", pg.0, index.0))
            .collect::<Vec<_>>()
            .join(",");
        let new_bytes_before_marker = fs::metadata(&temp_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let reclaimed_bytes = old_bytes.saturating_sub(new_bytes_before_marker);
        compacted_log.append(&encode_maintenance_record(MaintenanceRecord {
            kind: MaintenanceKind::Compaction,
            pg: retained_through
                .keys()
                .next()
                .copied()
                .unwrap_or(PlacementGroupId(0)),
            retained_through: retained_through
                .values()
                .max()
                .copied()
                .unwrap_or(LogIndex(0)),
            reclaimed_bytes,
            note: std::borrow::Cow::Owned(retained_high_water.into_bytes()),
        })?)?;
        drop(compacted_log);

        fs::rename(&temp_path, &final_path)
            .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
        sync_dir(&segments_dir)?;

        let new_manifest = SegmentManifest {
            active_id: new_id,
            segments: vec![SegmentDescriptor {
                id: new_id,
                file_name: new_file_name.clone(),
            }],
            compacted_prefixes,
        };
        write_manifest(&self.root, &new_manifest)?;

        let _ = recover_manifest(&self.root, &new_manifest)?;
        self.active =
            SegmentLog::open_after_external_recovery(&final_path, self.options.log_options)?;
        self.manifest = new_manifest;
        self.write_failed = false;

        for segment in &old_segments {
            if segment.file_name != new_file_name {
                let _ = fs::remove_file(segments_dir.join(&segment.file_name));
            }
        }

        let new_bytes = fs::metadata(&final_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        Ok(CompactionReport {
            old_segments: old_segments.len(),
            new_segments: self.manifest.segments.len(),
            dropped_wal_records,
            retained_wal_records: retained_wal_records.len(),
            reclaimed_bytes: old_bytes.saturating_sub(new_bytes),
        })
    }

    pub fn recover(root: impl AsRef<Path>) -> RsfResult<SegmentStoreRecovery> {
        let root = root.as_ref();
        // This entry point may initialize a store. Lock acquisition itself is
        // not a publication barrier, including on existing-path retries.
        crate::create_dir_all_synced(root)
            .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
        let _lock = SegmentLock::acquire(&root.join(MANIFEST_FILE))?;
        let gate = store_gate(root)?;
        let _guard = lock_gate(&gate)?;
        let manifest = read_or_create_manifest(root)?;
        recover_manifest(root, &manifest)
    }

    /// Recover existing state without initializing an absent store. This reads
    /// and validates bytes (and may repair a torn active tail); it does not
    /// publish namespace entries or grant durable ownership. Callers returning
    /// custody acknowledgements must establish their own publication boundary.
    pub fn recover_existing(root: impl AsRef<Path>) -> RsfResult<SegmentStoreRecovery> {
        let root = root.as_ref();
        // Check the manifest before acquiring a lock, whose sidecar is created
        // by SegmentLock. An absent root or manifest remains absent.
        fs::metadata(root.join(MANIFEST_FILE))
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?;
        let _lock = SegmentLock::acquire(&root.join(MANIFEST_FILE))?;
        let gate = store_gate(root)?;
        let _guard = lock_gate(&gate)?;
        let bytes = fs::read(root.join(MANIFEST_FILE))
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?;
        let manifest = SegmentManifest::decode(&bytes)?;
        recover_manifest(root, &manifest)
    }

    pub fn recover_or_quarantine(root: impl AsRef<Path>) -> RsfResult<SegmentStoreRecovery> {
        let root = root.as_ref();
        // This entry point may initialize a store. Lock acquisition itself is
        // not a publication barrier, including on existing-path retries.
        crate::create_dir_all_synced(root)
            .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
        let _lock = SegmentLock::acquire(&root.join(MANIFEST_FILE))?;
        let gate = store_gate(root)?;
        let _guard = lock_gate(&gate)?;
        let manifest = read_or_create_manifest(root)?;
        recover_manifest_or_quarantine(root, &manifest)
    }

    pub fn recover_current(&self) -> RsfResult<SegmentStoreRecovery> {
        let _guard = lock_gate(&self.gate)?;
        let bytes = fs::read(self.root.join(MANIFEST_FILE))
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?;
        let manifest = SegmentManifest::decode(&bytes)?;
        recover_manifest(&self.root, &manifest)
    }

    fn recover_owned(&self) -> RsfResult<SegmentStoreRecovery> {
        recover_manifest(&self.root, &self.manifest)
    }

    fn wal_tail_owned(&mut self, pg: PlacementGroupId) -> RsfResult<Option<WalTail>> {
        let stamp = segment_stamp(&self.root, &self.manifest)?;
        if self
            .cursor
            .as_ref()
            .is_none_or(|cursor| cursor.stamp != stamp)
        {
            self.external_changes = self.external_changes.checked_add(1).ok_or_else(|| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "store observer revision exhausted",
                )
            })?;
            let recovered = self.recover_owned()?;
            self.cursor = Some(WalCursor {
                tails: recovered.recovery.pg_tails,
                // Recovery may have truncated an incomplete active tail.
                stamp: segment_stamp(&self.root, &self.manifest)?,
            });
        }
        Ok(self
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.tails.get(&pg))
            .copied())
    }

    fn refresh_layout(&mut self) -> RsfResult<()> {
        let bytes = fs::read(self.root.join(MANIFEST_FILE))
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?;
        let manifest = SegmentManifest::decode(&bytes)?;
        if manifest != self.manifest {
            // Another same-process writer may have rotated or compacted the
            // store. Its published layout is authoritative, including the next
            // segment id. Never reclaim a file based on a stale local manifest.
            recover_manifest(&self.root, &manifest)?;
            let path = self
                .root
                .join(SEGMENT_DIR)
                .join(&manifest.active_segment()?.file_name);
            let active = SegmentLog::open_after_external_recovery(path, self.options.log_options)?;
            self.active = active;
            self.manifest = manifest;
            self.cursor = None;
        }
        Ok(())
    }

    fn ensure_writable(&self) -> RsfResult<()> {
        if self.write_failed {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "segment store durability failed; reopen before further writes",
            ));
        }
        Ok(())
    }

    fn rotation_required(&self, pending_bytes: u64) -> bool {
        if self.options.max_segment_bytes == 0 {
            return false;
        }
        let current_len = fs::metadata(self.active.path())
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        current_len != 0 && current_len + pending_bytes > self.options.max_segment_bytes
    }

    fn rotate_if_needed(&mut self, pending_bytes: u64) -> RsfResult<()> {
        if self.rotation_required(pending_bytes) {
            self.rotate()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> RsfResult<()> {
        let next_id = self.manifest.next_id()?;
        let mut manifest = self.manifest.clone();
        manifest.segments.push(SegmentDescriptor {
            id: next_id,
            file_name: segment_file_name(next_id),
        });
        manifest.active_id = next_id;
        let path = self.root.join(SEGMENT_DIR).join(segment_file_name(next_id));
        // A previous interrupted rotation may have left an unpublished file.
        // The current manifest does not reference this id, so it is disposable.
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(storage_error(StorageIoOperation::Open, err)),
        }
        create_durable_segment(&self.root, &manifest.segments.last().unwrap().file_name)?;
        let active = SegmentLog::open_after_external_recovery(&path, self.options.log_options)?;
        write_manifest(&self.root, &manifest)?;
        self.active = active;
        self.manifest = manifest;
        Ok(())
    }
}

fn segment_stamp(root: &Path, manifest: &SegmentManifest) -> RsfResult<Vec<SegmentStamp>> {
    manifest
        .segments
        .iter()
        .map(|segment| {
            let metadata = fs::metadata(root.join(SEGMENT_DIR).join(&segment.file_name))
                .map_err(|err| storage_error(StorageIoOperation::Recover, err))?;
            Ok(SegmentStamp {
                id: segment.id,
                bytes: metadata.len(),
                modified: metadata
                    .modified()
                    .map_err(|err| storage_error(StorageIoOperation::Recover, err))?,
            })
        })
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentStoreRecovery {
    pub segments: Vec<SegmentDescriptor>,
    pub recovery: Recovery,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentScanFilter {
    pub pg: Option<PlacementGroupId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentScan {
    pub segments: Vec<SegmentDescriptor>,
    pub wal_records: Vec<WalRecord>,
    pub compacted_prefixes: BTreeMap<PlacementGroupId, PlacementGroupPrefix>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentStoreBenchmark {
    pub appended_records: usize,
    pub recovered_records: usize,
    pub recovered_wal_records: usize,
    pub segments_before: usize,
    pub segments_after: usize,
    pub bytes: u64,
    pub append_nanos: u128,
    pub replay_nanos: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionReport {
    pub old_segments: usize,
    pub new_segments: usize,
    pub dropped_wal_records: usize,
    pub retained_wal_records: usize,
    pub reclaimed_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncateReport {
    pub old_segments: usize,
    pub new_segments: usize,
    pub dropped_wal_records: usize,
    pub retained_wal_records: usize,
    pub reclaimed_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineReport {
    pub segment: SegmentDescriptor,
    pub quarantine_path: PathBuf,
}

impl SegmentManifest {
    fn initial() -> Self {
        Self {
            active_id: 1,
            segments: vec![SegmentDescriptor {
                id: 1,
                file_name: segment_file_name(1),
            }],
            compacted_prefixes: BTreeMap::new(),
        }
    }

    fn active_segment(&self) -> RsfResult<&SegmentDescriptor> {
        self.segments
            .iter()
            .find(|segment| segment.id == self.active_id)
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("manifest active segment {} is missing", self.active_id),
                )
            })
    }

    fn next_id(&self) -> RsfResult<u64> {
        self.segments
            .iter()
            .map(|segment| segment.id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "segment id exhausted"))
    }

    fn encode(&self) -> String {
        let mut encoded = format!("rsf-segment-manifest {MANIFEST_VERSION}\n");
        encoded.push_str(&format!("active {}\n", self.active_id));
        for segment in &self.segments {
            encoded.push_str(&format!("segment {} {}\n", segment.id, segment.file_name));
        }
        for prefix in self.compacted_prefixes.values() {
            encoded.push_str(&format!(
                "base {} {} {} {}\n",
                prefix.pg.0,
                prefix.epoch.0,
                prefix.index.0,
                encode_hash(&prefix.record_hash)
            ));
        }
        encoded
    }

    fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let text = std::str::from_utf8(bytes).map_err(|_| {
            RsfError::new(ErrorCode::CorruptionDetected, "manifest is not valid utf-8")
        })?;
        let mut lines = text.lines();
        let header = lines
            .next()
            .ok_or_else(|| RsfError::new(ErrorCode::CorruptionDetected, "manifest is empty"))?;
        if header != format!("rsf-segment-manifest {MANIFEST_VERSION}") {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "unsupported segment manifest version",
            ));
        }

        let active = lines.next().ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "manifest has no active segment",
            )
        })?;
        let Some(active_id) = active.strip_prefix("active ") else {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "manifest active line is malformed",
            ));
        };
        let active_id = active_id.parse::<u64>().map_err(|_| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "manifest active segment id is invalid",
            )
        })?;

        let mut segments = Vec::new();
        let mut compacted_prefixes = BTreeMap::new();
        for line in lines {
            if let Some(rest) = line.strip_prefix("segment ") {
                let mut parts = rest.splitn(2, ' ');
                let id = parts
                    .next()
                    .and_then(|part| part.parse::<u64>().ok())
                    .ok_or_else(|| {
                        RsfError::new(ErrorCode::CorruptionDetected, "segment id is invalid")
                    })?;
                let file_name = parts.next().ok_or_else(|| {
                    RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "segment file name is missing",
                    )
                })?;
                if id == 0
                    || file_name != segment_file_name(id)
                    || segments
                        .last()
                        .is_some_and(|previous: &SegmentDescriptor| previous.id >= id)
                {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "manifest requires increasing unique segment ids and canonical file names",
                    ));
                }
                segments.push(SegmentDescriptor {
                    id,
                    file_name: file_name.to_string(),
                });
                continue;
            }
            if let Some(rest) = line.strip_prefix("base ") {
                let parts = rest.split(' ').collect::<Vec<_>>();
                if parts.len() != 4 {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "manifest base line is malformed",
                    ));
                }
                let pg = PlacementGroupId(parse_manifest_u64(parts[0], "base pg")?);
                let prefix = PlacementGroupPrefix {
                    pg,
                    epoch: rsf_core::Epoch(parse_manifest_u64(parts[1], "base epoch")?),
                    index: LogIndex(parse_manifest_u64(parts[2], "base index")?),
                    record_hash: decode_hash(parts[3])?,
                };
                if compacted_prefixes.insert(pg, prefix).is_some() {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "manifest contains duplicate compacted placement-group prefixes",
                    ));
                }
                continue;
            }
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("manifest line is malformed: {line}"),
            ));
        }
        if segments.is_empty() {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "manifest has no segments",
            ));
        }
        if segments
            .last()
            .is_some_and(|segment| segment.id != active_id)
        {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "manifest active segment must be the last segment",
            ));
        }
        let manifest = Self {
            active_id,
            segments,
            compacted_prefixes,
        };
        let _ = manifest.active_segment()?;
        Ok(manifest)
    }
}

fn parse_manifest_u64(value: &str, label: &str) -> RsfResult<u64> {
    value.parse::<u64>().map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("manifest {label} is invalid"),
        )
    })
}

fn read_or_create_manifest(root: &Path) -> RsfResult<SegmentManifest> {
    let path = root.join(MANIFEST_FILE);
    match fs::read(&path) {
        Ok(bytes) => SegmentManifest::decode(&bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // A caller may have allocated the root immediately before opening.
            // Persist that root's link before the initial manifest can be used.
            let parent = root
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            sync_dir(parent)?;
            crate::create_dir_all_synced(root.join(SEGMENT_DIR))
                .map_err(|err| storage_error(StorageIoOperation::Open, err))?;
            if fs::read_dir(root.join(SEGMENT_DIR))
                .map_err(|err| storage_error(StorageIoOperation::Recover, err))?
                .next()
                .is_some()
            {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "segment files exist without a manifest; restore the manifest before recovery",
                ));
            }
            let manifest = SegmentManifest::initial();
            create_durable_segment(root, &manifest.active_segment()?.file_name)?;
            write_manifest(root, &manifest)?;
            Ok(manifest)
        }
        Err(err) => Err(storage_error(StorageIoOperation::Recover, err)),
    }
}

fn create_durable_segment(root: &Path, name: &str) -> RsfResult<()> {
    let directory = root.join(SEGMENT_DIR);
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join(name))
        .map_err(|err| storage_error(StorageIoOperation::Open, err))?;
    file.sync_all()
        .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
    sync_dir(&directory)
}

fn recover_manifest(root: &Path, manifest: &SegmentManifest) -> RsfResult<SegmentStoreRecovery> {
    recover_manifest_with_handler(root, manifest, None)
}

fn recover_manifest_or_quarantine(
    root: &Path,
    manifest: &SegmentManifest,
) -> RsfResult<SegmentStoreRecovery> {
    recover_manifest_with_handler(root, manifest, Some(quarantine_segment))
}

type CorruptSegmentHandler =
    fn(&Path, &SegmentDescriptor, &RsfError) -> RsfResult<QuarantineReport>;

fn recover_manifest_with_handler(
    root: &Path,
    manifest: &SegmentManifest,
    on_corrupt_segment: Option<CorruptSegmentHandler>,
) -> RsfResult<SegmentStoreRecovery> {
    let mut records = Vec::new();
    let mut wal_records = Vec::new();
    let mut maintenance_records = Vec::new();
    let mut pg_tails = manifest
        .compacted_prefixes
        .iter()
        .map(|(pg, prefix)| {
            (
                *pg,
                crate::WalTail {
                    epoch: prefix.epoch,
                    index: prefix.index,
                    record_hash: prefix.record_hash,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut truncated_to = 0;

    for segment in &manifest.segments {
        let segment_path = root.join(SEGMENT_DIR).join(&segment.file_name);
        let segment_recovery = match SegmentLog::recover_existing_with_tails(
            &root.join(SEGMENT_DIR).join(&segment.file_name),
            pg_tails,
            segment.id == manifest.active_id,
        ) {
            Ok(recovery) => recovery,
            Err(err) if err.code == ErrorCode::CorruptionDetected => {
                if let Some(handler) = on_corrupt_segment {
                    let report = handler(root, segment, &err)?;
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        format!(
                            "quarantined corrupt segment {} at {}; recovery remains failed closed: {}",
                            segment_path.display(),
                            report.quarantine_path.display(),
                            err.message
                        ),
                    ));
                }
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        records.extend(segment_recovery.records);
        wal_records.extend(segment_recovery.wal_records);
        maintenance_records.extend(segment_recovery.maintenance_records);
        pg_tails = segment_recovery.pg_tails;
        truncated_to += segment_recovery.truncated_to;
    }

    Ok(SegmentStoreRecovery {
        segments: manifest.segments.clone(),
        recovery: Recovery {
            records,
            wal_records,
            maintenance_records,
            pg_tails,
            compacted_prefixes: manifest.compacted_prefixes.clone(),
            truncated_to,
        },
    })
}

fn quarantine_segment(
    root: &Path,
    segment: &SegmentDescriptor,
    _err: &RsfError,
) -> RsfResult<QuarantineReport> {
    let quarantine_dir = root.join("quarantine");
    fs::create_dir_all(&quarantine_dir)
        .map_err(|err| storage_error(StorageIoOperation::Open, err))?;
    // This child is created after the owner-root barrier. Publish its link
    // before moving the only retained corrupt segment into it, including retry.
    sync_dir(root)?;
    let source = root.join(SEGMENT_DIR).join(&segment.file_name);
    let quarantine_path = quarantine_dir.join(format!("{}.corrupt", segment.file_name));
    let _ = fs::remove_file(&quarantine_path);
    fs::rename(&source, &quarantine_path)
        .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
    sync_dir(&quarantine_dir)?;
    sync_dir(&root.join(SEGMENT_DIR))?;
    Ok(QuarantineReport {
        segment: segment.clone(),
        quarantine_path,
    })
}

fn write_manifest(root: &Path, manifest: &SegmentManifest) -> RsfResult<()> {
    fs::create_dir_all(root.join(SEGMENT_DIR))
        .map_err(|err| storage_error(StorageIoOperation::Open, err))?;
    let manifest_path = root.join(MANIFEST_FILE);
    let tmp_path = root.join("MANIFEST.tmp");
    let mut file =
        File::create(&tmp_path).map_err(|err| storage_error(StorageIoOperation::Open, err))?;
    file.write_all(manifest.encode().as_bytes())
        .map_err(|err| storage_error(StorageIoOperation::Append, err))?;
    file.sync_all()
        .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
    drop(file);
    fs::rename(&tmp_path, &manifest_path)
        .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
    sync_dir(root)?;
    Ok(())
}

fn sync_dir(path: &Path) -> RsfResult<()> {
    let dir = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|err| storage_error(StorageIoOperation::Open, err))?;
    dir.sync_all()
        .map_err(|err| storage_error(StorageIoOperation::Sync, err))
}

fn segment_bytes(root: &Path, segments: &[SegmentDescriptor]) -> RsfResult<u64> {
    segments.iter().try_fold(0_u64, |total, segment| {
        let len = fs::metadata(root.join(SEGMENT_DIR).join(&segment.file_name))
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?
            .len();
        Ok(total + len)
    })
}

fn rewrite_single_segment(
    root: &Path,
    options: SegmentLogOptions,
    new_id: u64,
    wal_records: &[WalRecord],
    maintenance_records: &[MaintenanceRecord<'static>],
    marker: Option<MaintenanceRecord<'_>>,
) -> RsfResult<()> {
    let segments_dir = root.join(SEGMENT_DIR);
    let new_file_name = segment_file_name(new_id);
    let temp_file_name = format!("{new_file_name}.rewrite");
    let temp_path = segments_dir.join(&temp_file_name);
    let final_path = segments_dir.join(&new_file_name);
    let _ = fs::remove_file(&temp_path);
    let _ = fs::remove_file(&final_path);

    let mut log = SegmentLog::open_with_options(&temp_path, options)?;
    for record in wal_records {
        log.append(&encode_existing_wal_record(record)?)?;
    }
    for record in maintenance_records {
        log.append(&encode_maintenance_record(record.clone())?)?;
    }
    if let Some(marker) = marker {
        log.append(&encode_maintenance_record(marker)?)?;
    }
    drop(log);

    fs::rename(&temp_path, &final_path)
        .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
    sync_dir(&segments_dir)
}

fn record_disk_len(payload_len: usize) -> RsfResult<u64> {
    let payload_len = u64::try_from(payload_len).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "payload length does not fit in segment size accounting",
        )
    })?;
    Ok(HEADER_LEN as u64 + payload_len)
}

fn segment_file_name(id: u64) -> String {
    format!("{id:018}.segment")
}

fn encode_hash(hash: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in hash {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn decode_hash(encoded: &str) -> RsfResult<[u8; 32]> {
    if encoded.len() != 64 || !encoded.is_ascii() {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "manifest hash has invalid length",
        ));
    }
    let mut hash = [0_u8; 32];
    for (index, slot) in hash.iter_mut().enumerate() {
        let start = index * 2;
        *slot = u8::from_str_radix(&encoded[start..start + 2], 16).map_err(|_| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "manifest hash is invalid hex",
            )
        })?;
    }
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rsf_core::storage::WalRecordKind;
    use rsf_core::{Epoch, LogIndex, PlacementGroupId};

    use super::*;
    use crate::SyncPolicy;

    #[test]
    fn interleaved_writers_cannot_replace_another_writers_published_segment() {
        let root = test_root("interleaved-writer-rotation");
        let options = SegmentStoreOptions {
            max_segment_bytes: 1,
            ..Default::default()
        };
        let mut first = SegmentStore::open_with_options(&root, options).unwrap();
        let mut second = SegmentStore::open_with_options(&root, options).unwrap();
        first
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Prepare,
                b"prepared",
            )
            .unwrap();
        first
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Commit,
                b"acknowledged",
            )
            .unwrap();
        second
            .append_next_wal(
                PlacementGroupId(2),
                Epoch(1),
                WalRecordKind::Commit,
                b"other group",
            )
            .unwrap();
        let records = SegmentStore::recover(&root).unwrap().recovery.wal_records;
        assert_eq!(records.len(), 3);
        assert!(records.iter().any(|record| record.pg == PlacementGroupId(1)
            && record.index == LogIndex(2)
            && record.payload == b"acknowledged"));
        drop(first);
        drop(second);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_root_writers_preserve_both_placement_group_chains() {
        let root = test_root("concurrent-root-writers");
        let options = SegmentStoreOptions {
            max_segment_bytes: 256,
            log_options: SegmentLogOptions {
                sync_policy: SyncPolicy::None,
            },
        };
        let first = SegmentStore::open_with_options(&root, options).unwrap();
        let second = SegmentStore::open_with_options(&root, options).unwrap();
        std::thread::scope(|scope| {
            let workers = [first, second]
                .into_iter()
                .enumerate()
                .map(|(group, mut store)| {
                    scope.spawn(move || {
                        for index in 1..=16 {
                            let (_, _, record) = store
                                .append_next_wal(
                                    PlacementGroupId(group as u64),
                                    Epoch(1),
                                    WalRecordKind::Commit,
                                    b"value",
                                )
                                .unwrap();
                            assert_eq!(record.index, LogIndex(index));
                        }
                    })
                })
                .collect::<Vec<_>>();
            for worker in workers {
                worker.join().unwrap();
            }
        });
        let records = SegmentStore::recover(&root).unwrap().recovery.wal_records;
        assert_eq!(records.len(), 32);
        for group in 0..2 {
            assert_eq!(
                records
                    .iter()
                    .filter(|record| record.pg == PlacementGroupId(group))
                    .map(|record| record.index.0)
                    .collect::<Vec<_>>(),
                (1..=16).collect::<Vec<_>>()
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_cached_wal_tail_does_not_hide_changed_corrupt_disk_records() {
        let root = test_root("cursor-corruption");
        let mut store = SegmentStore::open(&root).unwrap();
        store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Commit,
                b"acknowledged",
            )
            .unwrap();
        let path = root
            .join(SEGMENT_DIR)
            .join(&store.active_segment().file_name);
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, &bytes).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))
            .unwrap();
        let error = store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Commit,
                b"must not append",
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::CorruptionDetected);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn writer_open_returns_the_recovery_used_to_establish_its_tail() {
        let root = test_root("open_recovered");
        let (mut store, recovered) =
            SegmentStore::open_recovered(&root, Default::default()).unwrap();
        assert!(recovered.recovery.wal_records.is_empty());
        let (_, _, first) = store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Prepare,
                b"first",
            )
            .unwrap();
        drop(store);
        let (mut store, recovered) =
            SegmentStore::open_recovered(&root, Default::default()).unwrap();
        assert_eq!(recovered.recovery.wal_records, vec![first.clone()]);
        let (_, _, second) = store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(1),
                WalRecordKind::Commit,
                b"second",
            )
            .unwrap();
        assert_eq!(second.previous_hash, first.record_hash);
        assert_eq!(recovered.recovery.wal_records.len(), 1);
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn next_wal_append_preserves_per_group_chains_across_rotation_and_restart() {
        let root = test_root("append_next_wal");
        let options = SegmentStoreOptions {
            max_segment_bytes: 1,
            ..Default::default()
        };
        let mut store = SegmentStore::open_with_options(&root, options).unwrap();
        let (_, _, first) = store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(2),
                WalRecordKind::Prepare,
                b"first",
            )
            .unwrap();
        let (_, _, other) = store
            .append_next_wal(
                PlacementGroupId(2),
                Epoch(1),
                WalRecordKind::Prepare,
                b"other",
            )
            .unwrap();
        assert_eq!(first.index, LogIndex(1));
        assert_eq!(other.index, LogIndex(1));
        assert_eq!(other.previous_hash, [0; 32]);
        assert_eq!(
            store
                .append_next_wal(
                    PlacementGroupId(1),
                    Epoch(1),
                    WalRecordKind::Commit,
                    b"stale"
                )
                .unwrap_err()
                .code,
            ErrorCode::StaleEpoch
        );
        drop(store);
        let mut store = SegmentStore::open_with_options(&root, options).unwrap();
        let (_, _, committed) = store
            .append_next_wal(
                PlacementGroupId(1),
                Epoch(2),
                WalRecordKind::Commit,
                b"committed",
            )
            .unwrap();
        assert_eq!(committed.index, LogIndex(2));
        assert_eq!(committed.previous_hash, first.record_hash);
        assert_eq!(
            store.recover_current().unwrap().recovery.wal_records.len(),
            3
        );
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manifest_rejects_ambiguous_history_and_hashes_without_panicking() {
        let segment = |id| format!("segment {id} {}\n", segment_file_name(id));
        for (active, body) in [
            (1, format!("{}{}", segment(1), segment(1))),
            (1, format!("{}{}", segment(2), segment(1))),
            (1, format!("{}{}", segment(1), segment(2))),
            (0, segment(0)),
            (1, "segment 1 wrong.segment\n".into()),
            (
                1,
                format!(
                    "{}base 1 1 1 {}\nbase 1 1 1 {}\n",
                    segment(1),
                    "ab".repeat(32),
                    "ab".repeat(32)
                ),
            ),
            (
                1,
                format!("{}base 1 1 1 aé{}\n", segment(1), "a".repeat(61)),
            ),
        ] {
            let text = format!("rsf-segment-manifest 1\nactive {active}\n{body}");
            assert!(SegmentManifest::decode(text.as_bytes()).is_err(), "{text}");
        }
        let manifest = SegmentManifest {
            active_id: u64::MAX,
            segments: vec![SegmentDescriptor {
                id: u64::MAX,
                file_name: segment_file_name(u64::MAX),
            }],
            compacted_prefixes: BTreeMap::new(),
        };
        assert!(manifest.next_id().is_err());
    }

    #[test]
    fn missing_manifest_cannot_reinitialize_an_existing_store() {
        let root = test_root("missing_manifest");
        let mut store = SegmentStore::open(&root).unwrap();
        store.append(b"acknowledged data").unwrap();
        drop(store);
        fs::remove_file(root.join(MANIFEST_FILE)).unwrap();

        assert!(SegmentStore::open(&root).is_err());
        assert!(SegmentStore::recover(&root).is_err());
        assert!(SegmentStore::recover_or_quarantine(&root).is_err());
        assert!(!root.join(MANIFEST_FILE).exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_manifest_segment_fails_closed_without_recreating_it() {
        let root = test_root("missing_segment");
        let mut store = SegmentStore::open(&root).unwrap();
        store.append(b"acknowledged data").unwrap();
        let segment = root
            .join(SEGMENT_DIR)
            .join(&store.active_segment().file_name);
        drop(store);
        fs::remove_file(&segment).unwrap();

        assert!(SegmentStore::recover(&root).is_err());
        assert!(SegmentStore::open(&root).is_err());
        assert!(!segment.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn torn_sealed_segment_is_corruption_and_is_never_truncated() {
        let root = test_root("torn_sealed_segment");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                max_segment_bytes: 1,
                ..Default::default()
            },
        )
        .unwrap();
        store.append(b"sealed acknowledged record").unwrap();
        store.append(b"active record").unwrap();
        let path = root.join(SEGMENT_DIR).join(&store.segments()[0].file_name);
        drop(store);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        let length = file.metadata().unwrap().len() - 3;
        file.set_len(length).unwrap();
        drop(file);
        assert!(SegmentStore::recover(&root).is_err());
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
        assert!(SegmentStore::open(&root).is_err());
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn existing_recovery_never_bootstraps_missing_state() {
        let root = test_root("existing_recovery");
        assert!(SegmentStore::recover_existing(&root).is_err());
        assert!(!root.exists());
        fs::create_dir_all(&root).unwrap();
        assert!(SegmentStore::recover_existing(&root).is_err());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        drop(SegmentStore::open(&root).unwrap());
        assert!(
            SegmentStore::recover_existing(&root)
                .unwrap()
                .recovery
                .records
                .is_empty()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_rotation_fences_writes_and_reopen_discards_unpublished_segments() {
        let root = test_root("failed_rotation");
        let options = SegmentStoreOptions {
            max_segment_bytes: 1,
            ..Default::default()
        };
        let mut store = SegmentStore::open_with_options(&root, options).unwrap();
        store.append(b"committed before failure").unwrap();
        let manifest = fs::read(root.join(MANIFEST_FILE)).unwrap();
        fs::create_dir(root.join("MANIFEST.tmp")).unwrap();
        assert!(store.append(b"uncertain").is_err());
        assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), manifest);
        fs::remove_dir(root.join("MANIFEST.tmp")).unwrap();
        assert!(
            store
                .append(b"must be fenced")
                .unwrap_err()
                .message
                .contains("reopen")
        );
        assert!(store.compact(BTreeMap::new()).is_err());
        assert!(store.truncate_uncommitted_tails().is_err());
        drop(store);
        let mut store = SegmentStore::open_with_options(&root, options).unwrap();
        store.append(b"after restart").unwrap();
        let records = store.recover_current().unwrap().recovery.records;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"committed before failure");
        assert_eq!(records[1].payload, b"after restart");
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejected_stale_epoch_does_not_write_or_fence_the_store() {
        let root = test_root("stale_epoch");
        let mut store = SegmentStore::open(&root).unwrap();
        let (_, _, first) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(2),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"first",
            })
            .unwrap();
        assert_eq!(
            store
                .append_wal(WalAppend {
                    pg: PlacementGroupId(1),
                    epoch: Epoch(1),
                    index: LogIndex(2),
                    kind: WalRecordKind::Commit,
                    previous_hash: first.record_hash,
                    payload: b"stale",
                })
                .unwrap_err()
                .code,
            ErrorCode::StaleEpoch
        );
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(2),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: first.record_hash,
                payload: b"valid",
            })
            .unwrap();
        assert_eq!(
            store.recover_current().unwrap().recovery.wal_records.len(),
            2
        );
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn segment_store_initializes_directory_manifest_and_active_segment() {
        let root = test_root("segment_store_initializes_directory_manifest_and_active_segment");
        let store = SegmentStore::open(&root).unwrap();

        assert_eq!(store.segments().len(), 1);
        assert_eq!(store.active_segment().id, 1);
        assert!(root.join(MANIFEST_FILE).exists());
        assert!(
            root.join(SEGMENT_DIR)
                .join("000000000000000001.segment")
                .exists()
        );

        drop(store);
        let recovery = SegmentStore::recover(&root).unwrap();
        assert_eq!(recovery.segments.len(), 1);
        assert!(recovery.recovery.records.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn segment_store_recovers_hash_chain_across_rotated_segments() {
        let root = test_root("segment_store_recovers_hash_chain_across_rotated_segments");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();

        let (first_segment, _, first) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(7),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        let (second_segment, _, commit) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(7),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: first.record_hash,
                payload: b"commit",
            })
            .unwrap();
        assert_ne!(first_segment, second_segment);
        drop(store);

        let recovery = SegmentStore::recover(&root).unwrap();
        assert_eq!(recovery.segments.len(), 2);
        assert_eq!(recovery.recovery.wal_records.len(), 2);
        assert_eq!(
            recovery.recovery.committed_prefix(PlacementGroupId(7)),
            Some(crate::PlacementGroupPrefix {
                pg: PlacementGroupId(7),
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: commit.record_hash,
            })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn segment_store_recovers_maintenance_records_across_restart() {
        let root = test_root("segment_store_recovers_maintenance_records_across_restart");
        let mut store = SegmentStore::open(&root).unwrap();
        store
            .append_maintenance(MaintenanceRecord::retention(
                PlacementGroupId(1),
                LogIndex(9),
                b"retain",
            ))
            .unwrap();
        drop(store);

        let recovery = SegmentStore::recover(&root).unwrap();
        assert_eq!(recovery.recovery.maintenance_records.len(), 1);
        assert_eq!(
            recovery.recovery.maintenance_records[0].retained_through,
            LogIndex(9)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scan_filters_wal_records_by_placement_group_across_segments() {
        let root = test_root("scan_filters_wal_records_by_placement_group_across_segments");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();
        let (_, _, pg1_prepare) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(21),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"pg1-prepare",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(21),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: pg1_prepare.record_hash,
                payload: b"pg1-commit",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(22),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"pg2-prepare",
            })
            .unwrap();

        let scan = store
            .scan(SegmentScanFilter {
                pg: Some(PlacementGroupId(21)),
            })
            .unwrap();
        assert!(scan.segments.len() > 1);
        assert_eq!(scan.wal_records.len(), 2);
        assert!(
            scan.wal_records
                .iter()
                .all(|record| record.pg == PlacementGroupId(21))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn store_append_replay_benchmark_reports_multi_segment_recovery() {
        let root = test_root("store_append_replay_benchmark_reports_multi_segment_recovery");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 40,
            },
        )
        .unwrap();
        let payloads = (0..128)
            .map(|index| format!("payload-{index:03}"))
            .collect::<Vec<_>>();
        let payload_refs = payloads
            .iter()
            .map(|payload| payload.as_bytes())
            .collect::<Vec<_>>();

        let report = store.benchmark_append_replay(&payload_refs).unwrap();
        assert_eq!(report.appended_records, 128);
        assert_eq!(report.recovered_records, 128);
        assert!(report.segments_after > report.segments_before);
        assert_eq!(
            report.bytes,
            payload_refs
                .iter()
                .map(|payload| payload.len() as u64)
                .sum()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compaction_rewrites_segments_and_preserves_chain_across_restart() {
        let root = test_root("compaction_rewrites_segments_and_preserves_chain_across_restart");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();

        let (_, _, prepare) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(3),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        let (_, _, commit) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(3),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: prepare.record_hash,
                payload: b"commit",
            })
            .unwrap();
        let (_, _, retained) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(3),
                epoch: Epoch(1),
                index: LogIndex(3),
                kind: WalRecordKind::Prepare,
                previous_hash: commit.record_hash,
                payload: b"retained",
            })
            .unwrap();
        assert!(store.segments().len() > 1);

        let report = store
            .compact(BTreeMap::from([(PlacementGroupId(3), LogIndex(2))]))
            .unwrap();
        assert_eq!(report.dropped_wal_records, 2);
        assert_eq!(report.retained_wal_records, 1);
        assert_eq!(store.segments().len(), 1);
        drop(store);

        let mut reopened = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1024,
            },
        )
        .unwrap();
        let recovery = reopened.recover_current().unwrap();
        assert_eq!(recovery.segments.len(), 1);
        assert_eq!(recovery.recovery.wal_records.len(), 1);
        assert_eq!(
            recovery.recovery.committed_prefix(PlacementGroupId(3)),
            Some(crate::PlacementGroupPrefix {
                pg: PlacementGroupId(3),
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: commit.record_hash,
            })
        );
        assert_eq!(
            recovery
                .recovery
                .pg_tail(PlacementGroupId(3))
                .unwrap()
                .record_hash,
            retained.record_hash
        );

        let (_, _, final_commit) = reopened
            .append_wal(WalAppend {
                pg: PlacementGroupId(3),
                epoch: Epoch(1),
                index: LogIndex(4),
                kind: WalRecordKind::Commit,
                previous_hash: retained.record_hash,
                payload: b"final",
            })
            .unwrap();
        drop(reopened);

        let recovery = SegmentStore::recover(&root).unwrap();
        assert_eq!(
            recovery
                .recovery
                .pg_tail(PlacementGroupId(3))
                .unwrap()
                .record_hash,
            final_commit.record_hash
        );
        assert_eq!(
            recovery.recovery.committed_prefix(PlacementGroupId(3)),
            Some(crate::PlacementGroupPrefix {
                pg: PlacementGroupId(3),
                epoch: Epoch(1),
                index: LogIndex(4),
                record_hash: final_commit.record_hash,
            })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compaction_can_reclaim_entire_committed_prefix() {
        let root = test_root("compaction_can_reclaim_entire_committed_prefix");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();
        let (_, _, prepare) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(5),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        let (_, _, commit) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(5),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: prepare.record_hash,
                payload: b"commit",
            })
            .unwrap();

        store
            .compact(BTreeMap::from([(PlacementGroupId(5), LogIndex(2))]))
            .unwrap();
        drop(store);

        let mut reopened = SegmentStore::open(&root).unwrap();
        let recovery = reopened.recover_current().unwrap();
        assert!(recovery.recovery.wal_records.is_empty());
        assert_eq!(
            recovery
                .recovery
                .pg_tail(PlacementGroupId(5))
                .unwrap()
                .record_hash,
            commit.record_hash
        );
        assert_eq!(
            recovery.recovery.committed_prefix(PlacementGroupId(5)),
            Some(crate::PlacementGroupPrefix {
                pg: PlacementGroupId(5),
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: commit.record_hash,
            })
        );

        reopened
            .append_wal(WalAppend {
                pg: PlacementGroupId(5),
                epoch: Epoch(1),
                index: LogIndex(3),
                kind: WalRecordKind::Prepare,
                previous_hash: commit.record_hash,
                payload: b"after-compact",
            })
            .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn truncate_uncommitted_tails_preserves_committed_prefix() {
        let root = test_root("truncate_uncommitted_tails_preserves_committed_prefix");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();
        let (_, _, prepare) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(31),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        let (_, _, commit) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(31),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: prepare.record_hash,
                payload: b"commit",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(31),
                epoch: Epoch(1),
                index: LogIndex(3),
                kind: WalRecordKind::Prepare,
                previous_hash: commit.record_hash,
                payload: b"uncommitted",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(32),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"drop-all-uncommitted",
            })
            .unwrap();

        let report = store.truncate_uncommitted_tails().unwrap();
        assert_eq!(report.dropped_wal_records, 2);
        assert_eq!(report.retained_wal_records, 2);
        drop(store);

        let mut reopened = SegmentStore::open(&root).unwrap();
        let recovery = reopened.recover_current().unwrap();
        assert_eq!(recovery.recovery.wal_records.len(), 2);
        assert_eq!(
            recovery.recovery.committed_prefix(PlacementGroupId(31)),
            Some(crate::PlacementGroupPrefix {
                pg: PlacementGroupId(31),
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: commit.record_hash,
            })
        );
        assert!(recovery.recovery.pg_tail(PlacementGroupId(32)).is_none());

        reopened
            .append_wal(WalAppend {
                pg: PlacementGroupId(31),
                epoch: Epoch(1),
                index: LogIndex(3),
                kind: WalRecordKind::Prepare,
                previous_hash: commit.record_hash,
                payload: b"after-truncate",
            })
            .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn truncate_uncommitted_tails_preserves_membership_metadata_prefix() {
        let root = test_root("truncate_uncommitted_tails_preserves_membership_metadata_prefix");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();
        let (_, _, membership) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(0),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Membership,
                previous_hash: [0; 32],
                payload: b"membership",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(0),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Prepare,
                previous_hash: membership.record_hash,
                payload: b"uncommitted-metadata-tail",
            })
            .unwrap();

        let report = store.truncate_uncommitted_tails().unwrap();
        assert_eq!(report.dropped_wal_records, 1);
        assert_eq!(report.retained_wal_records, 1);
        drop(store);

        let recovery = SegmentStore::recover(&root).unwrap().recovery;
        assert_eq!(recovery.wal_records.len(), 1);
        assert_eq!(recovery.wal_records[0].kind, WalRecordKind::Membership);
        assert_eq!(
            recovery.pg_tail(PlacementGroupId(0)).unwrap().record_hash,
            membership.record_hash
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovery_rejects_broken_compacted_prefix_hash() {
        let root = test_root("recovery_rejects_broken_compacted_prefix_hash");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();
        let (_, _, prepare) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(11),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        let (_, _, commit) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(11),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: prepare.record_hash,
                payload: b"commit",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(11),
                epoch: Epoch(1),
                index: LogIndex(3),
                kind: WalRecordKind::Prepare,
                previous_hash: commit.record_hash,
                payload: b"retained",
            })
            .unwrap();
        store
            .compact(BTreeMap::from([(PlacementGroupId(11), LogIndex(2))]))
            .unwrap();
        drop(store);

        let manifest_path = root.join(MANIFEST_FILE);
        let manifest = fs::read_to_string(&manifest_path).unwrap();
        let corrupted = manifest
            .lines()
            .map(|line| {
                if line.starts_with("base 11 ") {
                    let mut parts = line.split(' ').collect::<Vec<_>>();
                    parts[4] = "0000000000000000000000000000000000000000000000000000000000000000";
                    parts.join(" ")
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(&manifest_path, corrupted).unwrap();

        let err = SegmentStore::recover(&root).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("wal hash-chain mismatch"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovery_rejects_cross_segment_retained_record_chain_corruption() {
        let root = test_root("recovery_rejects_cross_segment_retained_record_chain_corruption");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();
        let (_, _, first) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(12),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"first",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(12),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: first.record_hash,
                payload: b"second",
            })
            .unwrap();
        let second_segment = store.segments()[1].clone();
        drop(store);

        corrupt_wal_previous_hash_and_rechecksum(
            &root.join(SEGMENT_DIR).join(second_segment.file_name),
        );

        let err = SegmentStore::recover(&root).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("wal record hash mismatch"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recover_or_quarantine_isolates_corrupt_segment_and_fails_closed() {
        let root = test_root("recover_or_quarantine_isolates_corrupt_segment_and_fails_closed");
        let mut store = SegmentStore::open_with_options(
            &root,
            SegmentStoreOptions {
                log_options: SegmentLogOptions {
                    sync_policy: SyncPolicy::None,
                },
                max_segment_bytes: 1,
            },
        )
        .unwrap();
        let (_, _, first) = store
            .append_wal(WalAppend {
                pg: PlacementGroupId(13),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"first",
            })
            .unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(13),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: first.record_hash,
                payload: b"second",
            })
            .unwrap();
        let corrupt_segment = store.segments()[0].clone();
        drop(store);

        let corrupt_path = root.join(SEGMENT_DIR).join(&corrupt_segment.file_name);
        let mut bytes = fs::read(&corrupt_path).unwrap();
        bytes[0] ^= 0xff;
        fs::write(&corrupt_path, bytes).unwrap();

        let err = SegmentStore::recover_or_quarantine(&root).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("quarantined corrupt segment"));
        assert!(!corrupt_path.exists());
        assert!(
            root.join("quarantine")
                .join(format!("{}.corrupt", corrupt_segment.file_name))
                .exists()
        );

        let err = SegmentStore::recover(&root).unwrap_err();
        assert!(matches!(
            err.code,
            ErrorCode::CorruptionDetected | ErrorCode::InvalidInput
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compaction_rejects_retention_point_that_is_not_commit() {
        let root = test_root("compaction_rejects_retention_point_that_is_not_commit");
        let mut store = SegmentStore::open(&root).unwrap();
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(8),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();

        let err = store
            .compact(BTreeMap::from([(PlacementGroupId(8), LogIndex(1))]))
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn segment_store_rejects_corrupt_manifest() {
        let root = test_root("segment_store_rejects_corrupt_manifest");
        fs::create_dir_all(root.join(SEGMENT_DIR)).unwrap();
        fs::write(root.join(MANIFEST_FILE), b"not a manifest").unwrap();

        let err = SegmentStore::open(&root).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        let _ = fs::remove_dir_all(root);
    }

    fn test_root(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-{name}-{unique}"))
    }

    fn corrupt_wal_previous_hash_and_rechecksum(path: &Path) {
        let mut bytes = fs::read(path).unwrap();
        let payload_start = HEADER_LEN;
        let previous_hash_start = payload_start + 29;
        bytes[previous_hash_start] ^= 1;
        let payload = &bytes[payload_start..];
        let checksum = crate::DEFAULT_CHECKSUM_ALGORITHM.checksum(payload);
        bytes[16..24].copy_from_slice(&checksum.to_le_bytes());
        fs::write(path, bytes).unwrap();
    }
    #[test]
    fn observed_revisions_distinguish_own_appends_foreign_writes_and_corruption() {
        let root = test_root("observed-revisions");
        let mut first = SegmentStore::open(&root).unwrap();
        let initial = first.revision().unwrap();
        first
            .append_next_wal(PlacementGroupId(1), Epoch(1), WalRecordKind::Commit, b"own")
            .unwrap();
        let own = first.revision().unwrap();
        assert_ne!(initial, own);
        assert!(own.same_writer_epoch(&initial));
        let mut second = SegmentStore::open(&root).unwrap();
        second
            .append_next_wal(
                PlacementGroupId(2),
                Epoch(1),
                WalRecordKind::Commit,
                b"foreign",
            )
            .unwrap();
        let foreign = first.revision().unwrap();
        assert!(!foreign.same_writer_epoch(&own));
        assert_eq!(
            first.wal_tail(PlacementGroupId(1)).unwrap().unwrap().index,
            LogIndex(1)
        );
        let path = root
            .join(SEGMENT_DIR)
            .join(&first.active_segment().file_name);
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, &bytes).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH))
            .unwrap();
        assert_eq!(
            first.revision().unwrap_err().code,
            ErrorCode::CorruptionDetected
        );
        drop(first);
        drop(second);
        fs::remove_dir_all(root).unwrap();
    }
}
