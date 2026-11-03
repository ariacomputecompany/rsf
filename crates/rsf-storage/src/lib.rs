#![forbid(unsafe_code)]

mod durable_directory;
pub use durable_directory::create_dir_all_synced;

mod exclusive_lock;
pub use exclusive_lock::ExclusiveFileLock;

mod catalog;
mod store;
#[cfg(test)]
mod sync_observation;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

pub use catalog::{DurableManifestCatalog, PublishedManifest};
use rsf_core::storage::{WalRecord, WalRecordKind};
use rsf_core::{Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult};
use rsf_manifest::{ExtentManifest, ManifestKind};
pub use store::{
    CompactionReport, QuarantineReport, SegmentDescriptor, SegmentScan, SegmentScanFilter,
    SegmentStore, SegmentStoreBenchmark, SegmentStoreCustody, SegmentStoreOptions,
    SegmentStoreRecovery, SegmentStoreRevision, TruncateReport, WalGroupRecord,
};

fn sync_segment(file: &File, path: &Path, policy: SyncPolicy) -> RsfResult<()> {
    #[cfg(test)]
    let observed = sync_observation::before(path, policy)?;
    #[cfg(not(test))]
    let _ = path;
    let result = match policy {
        SyncPolicy::None => Ok(()),
        SyncPolicy::Data => file
            .sync_data()
            .map_err(|err| storage_error(StorageIoOperation::Sync, err)),
        SyncPolicy::Full => file
            .sync_all()
            .map_err(|err| storage_error(StorageIoOperation::Sync, err)),
    };
    #[cfg(test)]
    sync_observation::after(observed, result.is_ok());
    result
}

const MAGIC: [u8; 4] = *b"RSF1";
const SEGMENT_RECORD_VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 2 + 8 + 8;
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;
const DEFAULT_CHECKSUM_ALGORITHM: ChecksumAlgorithm = ChecksumAlgorithm::Blake3_64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumAlgorithm {
    Blake3_64,
    Fnv1a64,
}

impl ChecksumAlgorithm {
    pub fn to_u16(self) -> u16 {
        match self {
            Self::Blake3_64 => 2,
            Self::Fnv1a64 => 1,
        }
    }

    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::Fnv1a64),
            2 => Some(Self::Blake3_64),
            _ => None,
        }
    }

    pub fn checksum(self, bytes: &[u8]) -> u64 {
        match self {
            Self::Blake3_64 => blake3_u64(bytes),
            Self::Fnv1a64 => fnv1a64(bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentRecordHeader {
    pub version: u16,
    pub checksum_algorithm: ChecksumAlgorithm,
    pub payload_len: u64,
    pub checksum: u64,
}

#[derive(Debug)]
pub struct SegmentLog {
    path: PathBuf,
    file: File,
    _lock: SegmentLock,
    options: SegmentLogOptions,
}

impl SegmentLog {
    pub fn open(path: impl AsRef<Path>) -> RsfResult<Self> {
        Self::open_with_options(path, SegmentLogOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: SegmentLogOptions,
    ) -> RsfResult<Self> {
        let path = path.as_ref().to_path_buf();
        let lock = SegmentLock::acquire(&path)?;
        let _ = Self::recover_unlocked(&path)?;
        Self::open_locked(path, lock, options)
    }

    pub(crate) fn open_after_external_recovery(
        path: impl AsRef<Path>,
        options: SegmentLogOptions,
    ) -> RsfResult<Self> {
        let path = path.as_ref().to_path_buf();
        let lock = SegmentLock::acquire(&path)?;
        Self::open_locked(path, lock, options)
    }

    fn open_locked(
        path: PathBuf,
        lock: SegmentLock,
        options: SegmentLogOptions,
    ) -> RsfResult<Self> {
        Self::open_locked_with_publication(
            path,
            lock,
            options,
            durable_directory::sync_file_publication,
        )
    }

    fn open_locked_with_publication(
        path: PathBuf,
        lock: SegmentLock,
        options: SegmentLogOptions,
        publish: impl FnOnce(&Path) -> io::Result<()>,
    ) -> RsfResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|err| storage_error(StorageIoOperation::Open, err))?;
        // Existing-file retries also need this barrier: a visible segment name
        // can remain after an interrupted creation. Failure drops file and lock;
        // no writable handle escapes before the name is durably published.
        publish(&path).map_err(|err| storage_error(StorageIoOperation::Sync, err))?;

        Ok(Self {
            path,
            file,
            _lock: lock,
            options,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn recover_current(&self) -> RsfResult<Recovery> {
        // This handle already owns a published segment. Missing custody cannot
        // become a fresh empty file during an append/recovery read.
        Self::recover_existing_with_tails(&self.path, BTreeMap::new(), true)
    }

    pub fn append(&mut self, payload: &[u8]) -> RsfResult<u64> {
        let offset = self.append_unsynced(payload)?;
        self.sync_after_append()?;
        Ok(offset)
    }

    // Private framing primitive. The store-owned durable group is the only
    // alternate caller; it must flush before returning or rotating this file.
    fn append_unsynced(&mut self, payload: &[u8]) -> RsfResult<u64> {
        let offset = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|err| storage_error(StorageIoOperation::Append, err))?;
        let len = u64::try_from(payload.len()).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "payload length does not fit in segment record header",
            )
        })?;
        let checksum_algorithm = DEFAULT_CHECKSUM_ALGORITHM;
        let checksum = checksum_algorithm.checksum(payload);

        self.file
            .write_all(&MAGIC)
            .map_err(|err| storage_error(StorageIoOperation::Append, err))?;
        self.file
            .write_all(&SEGMENT_RECORD_VERSION.to_le_bytes())
            .map_err(|err| storage_error(StorageIoOperation::Append, err))?;
        self.file
            .write_all(&checksum_algorithm.to_u16().to_le_bytes())
            .map_err(|err| storage_error(StorageIoOperation::Append, err))?;
        self.file
            .write_all(&len.to_le_bytes())
            .map_err(|err| storage_error(StorageIoOperation::Append, err))?;
        self.file
            .write_all(&checksum.to_le_bytes())
            .map_err(|err| storage_error(StorageIoOperation::Append, err))?;
        self.file
            .write_all(payload)
            .map_err(|err| storage_error(StorageIoOperation::Append, err))?;

        Ok(offset)
    }

    pub fn append_wal(&mut self, input: WalAppend<'_>) -> RsfResult<(u64, WalRecord)> {
        let recovery = self.recover_current()?;
        let tail = recovery.pg_tails.get(&input.pg).copied();
        let expected_index = tail.map_or(1, |tail| tail.index.0 + 1);
        if input.index.0 != expected_index {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "wal index {} does not match next placement-group index {expected_index} for pg {}",
                    input.index.0, input.pg.0
                ),
            ));
        }

        let expected_previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        if input.previous_hash != expected_previous_hash {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "wal previous hash does not match recovered placement-group tail for pg {}",
                    input.pg.0
                ),
            ));
        }

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
        let encoded = encode_wal_record(input, payload_hash, record_hash)?;
        let offset = self.append(&encoded)?;

        Ok((
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
        let owned = record.to_owned();
        let encoded = encode_maintenance_record(record)?;
        let _ = self.append(&encoded)?;
        Ok(owned)
    }

    pub fn recover(path: impl AsRef<Path>) -> RsfResult<Recovery> {
        let path = path.as_ref();
        let _lock = SegmentLock::acquire(path)?;
        let recovery = Self::recover_unlocked(path)?;
        // Public recovery can create a file. Publish that name even when this
        // is a retry of an interrupted visible creation before returning data.
        durable_directory::sync_file_publication(path)
            .map_err(|err| storage_error(StorageIoOperation::Sync, err))?;
        Ok(recovery)
    }

    fn recover_unlocked(path: &Path) -> RsfResult<Recovery> {
        Self::recover_unlocked_with_tails(path, BTreeMap::new())
    }

    pub(crate) fn recover_unlocked_with_tails(
        path: &Path,
        initial_pg_tails: BTreeMap<PlacementGroupId, WalTail>,
    ) -> RsfResult<Recovery> {
        Self::recover_file(path, initial_pg_tails, true, true)
    }

    // Manifest-referenced files are already published durable state. Creating
    // an absent file here would conceal disk loss as an empty recovered log.
    pub(crate) fn recover_existing_with_tails(
        path: &Path,
        initial_pg_tails: BTreeMap<PlacementGroupId, WalTail>,
        repair_tail: bool,
    ) -> RsfResult<Recovery> {
        Self::recover_file(path, initial_pg_tails, false, repair_tail)
    }

    fn recover_file(
        path: &Path,
        initial_pg_tails: BTreeMap<PlacementGroupId, WalTail>,
        create: bool,
        repair_tail: bool,
    ) -> RsfResult<Recovery> {
        let mut file = OpenOptions::new()
            .create(create)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?;
        let file_len = file
            .metadata()
            .map_err(|err| storage_error(StorageIoOperation::Recover, err))?
            .len();

        let mut offset = 0_u64;
        let mut records = Vec::new();
        let mut wal_records = Vec::new();
        let mut maintenance_records = Vec::new();
        let mut pg_tails = initial_pg_tails;
        let mut header = [0_u8; HEADER_LEN];

        loop {
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    truncate_recovery_tail(&file, offset, repair_tail)?;
                    return Ok(Recovery {
                        records,
                        wal_records,
                        maintenance_records,
                        pg_tails,
                        compacted_prefixes: BTreeMap::new(),
                        truncated_to: offset,
                    });
                }
                Err(err) => return Err(storage_error(StorageIoOperation::Recover, err)),
            }

            if header[0..4] != MAGIC {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("invalid segment magic at offset {offset}"),
                ));
            }

            let version = u16::from_le_bytes(header[4..6].try_into().expect("slice length"));
            if version != SEGMENT_RECORD_VERSION {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "unsupported segment record version {version} at offset {offset}; expected {SEGMENT_RECORD_VERSION}"
                    ),
                ));
            }
            let checksum_algorithm_id =
                u16::from_le_bytes(header[6..8].try_into().expect("slice length"));
            let checksum_algorithm =
                ChecksumAlgorithm::from_u16(checksum_algorithm_id).ok_or_else(|| {
                    RsfError::new(
                        ErrorCode::CorruptionDetected,
                        format!(
                            "unsupported checksum algorithm {checksum_algorithm_id} at offset {offset}"
                        ),
                    )
                })?;
            let len = u64::from_le_bytes(header[8..16].try_into().expect("slice length"));
            let expected_checksum =
                u64::from_le_bytes(header[16..24].try_into().expect("slice length"));
            // A corrupt or torn length header must not allocate untrusted
            // quantities of memory before discovering the missing payload.
            if len
                > file_len
                    .saturating_sub(offset)
                    .saturating_sub(HEADER_LEN as u64)
            {
                truncate_recovery_tail(&file, offset, repair_tail)?;
                return Ok(Recovery {
                    records,
                    wal_records,
                    maintenance_records,
                    pg_tails,
                    compacted_prefixes: BTreeMap::new(),
                    truncated_to: offset,
                });
            }
            let payload_len = usize::try_from(len).map_err(|_| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("record length too large at offset {offset}"),
                )
            })?;
            let mut payload = vec![0_u8; payload_len];

            match file.read_exact(&mut payload) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    truncate_recovery_tail(&file, offset, repair_tail)?;
                    return Ok(Recovery {
                        records,
                        wal_records,
                        maintenance_records,
                        pg_tails,
                        compacted_prefixes: BTreeMap::new(),
                        truncated_to: offset,
                    });
                }
                Err(err) => return Err(storage_error(StorageIoOperation::Recover, err)),
            }

            let actual_checksum = checksum_algorithm.checksum(&payload);
            if actual_checksum != expected_checksum {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "checksum mismatch at offset {offset} using {:?}",
                        checksum_algorithm
                    ),
                ));
            }

            let segment_record = SegmentRecord {
                offset,
                header: SegmentRecordHeader {
                    version,
                    checksum_algorithm,
                    payload_len: len,
                    checksum: expected_checksum,
                },
                payload: payload.clone(),
            };

            if let Some(record) = decode_maintenance_record(&payload)? {
                maintenance_records.push(record);
            } else if let Some(wal_record) = decode_wal_record(&payload)? {
                let tail = pg_tails.get(&wal_record.pg).copied();
                let expected_previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
                if wal_record.previous_hash != expected_previous_hash {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        format!(
                            "wal hash-chain mismatch for pg {} at offset {offset}",
                            wal_record.pg.0
                        ),
                    ));
                }
                let expected_index = tail.map_or(1, |tail| tail.index.0 + 1);
                if wal_record.index.0 != expected_index {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        format!(
                            "wal index {} is not monotonic for pg {}; expected {expected_index}",
                            wal_record.index.0, wal_record.pg.0
                        ),
                    ));
                }
                if let Some(tail) = tail
                    && wal_record.epoch.0 < tail.epoch.0
                {
                    return Err(RsfError::new(
                        ErrorCode::StaleEpoch,
                        format!(
                            "wal epoch {} moved backward for pg {} from epoch {}",
                            wal_record.epoch.0, wal_record.pg.0, tail.epoch.0
                        ),
                    ));
                }
                pg_tails.insert(
                    wal_record.pg,
                    WalTail {
                        epoch: wal_record.epoch,
                        index: wal_record.index,
                        record_hash: wal_record.record_hash,
                    },
                );
                wal_records.push(wal_record);
            }

            records.push(segment_record);
            offset += HEADER_LEN as u64 + len;
        }
    }

    fn sync_after_append(&mut self) -> RsfResult<()> {
        sync_segment(&self.file, &self.path, self.options.sync_policy)
    }

    pub fn benchmark_append_replay(
        &mut self,
        payloads: &[&[u8]],
    ) -> RsfResult<AppendReplayBenchmark> {
        let append_start = Instant::now();
        for payload in payloads {
            self.append(payload)?;
        }
        let append_elapsed = append_start.elapsed();

        let replay_start = Instant::now();
        let recovery = self.recover_current()?;
        let replay_elapsed = replay_start.elapsed();

        Ok(AppendReplayBenchmark {
            appended_records: payloads.len(),
            recovered_records: recovery.records.len(),
            recovered_wal_records: recovery.wal_records.len(),
            bytes: payloads.iter().map(|payload| payload.len() as u64).sum(),
            append_nanos: append_elapsed.as_nanos(),
            replay_nanos: replay_elapsed.as_nanos(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendReplayBenchmark {
    pub appended_records: usize,
    pub recovered_records: usize,
    pub recovered_wal_records: usize,
    pub bytes: u64,
    pub append_nanos: u128,
    pub replay_nanos: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentLogOptions {
    pub sync_policy: SyncPolicy,
}

impl Default for SegmentLogOptions {
    fn default() -> Self {
        Self {
            sync_policy: SyncPolicy::Data,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPolicy {
    None,
    Data,
    Full,
}

#[derive(Debug)]
pub(crate) struct SegmentLock {
    path: PathBuf,
}

// Recovery reenters a store's lock within this process. Keep the owning file in
// the registry until the final handle drops; the kernel releases it on crash.
struct SegmentLockOwner {
    references: usize,
    _file: File,
}

impl Drop for SegmentLockOwner {
    fn drop(&mut self) {
        // A concurrent process spawn can briefly inherit this descriptor before
        // exec closes it. Explicit unlock releases ownership at the last local
        // handle, even during that interval.
        let _ = self._file.unlock();
    }
}

impl SegmentLock {
    fn acquire(segment_path: &Path) -> RsfResult<Self> {
        let lock_path = segment_path.with_extension("lock");
        if let Some(parent) = lock_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            // A sidecar lock establishes exclusion, not durable data custody.
            // Create-capable factories/recovery and writable handles publish
            // their own data paths explicitly; read scans must not repeat that
            // publication merely to acquire this ephemeral ownership lock.
            std::fs::create_dir_all(parent)
                .map_err(|err| storage_error(StorageIoOperation::Lock, err))?;
        }
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|err| storage_error(StorageIoOperation::Lock, err))?;
        let lock_path = std::fs::canonicalize(&lock_path)
            .map_err(|err| storage_error(StorageIoOperation::Lock, err))?;
        let mut registry = lock_registry().lock().map_err(|_| {
            RsfError::new(ErrorCode::InvalidInput, "segment lock registry is poisoned")
        })?;
        if let Some(owner) = registry.get_mut(&lock_path) {
            owner.references += 1;
            return Ok(Self { path: lock_path });
        }
        file.try_lock().map_err(|err| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "segment lock already held or unavailable at {}: {err}",
                    lock_path.display()
                ),
            )
        })?;
        registry.insert(
            lock_path.clone(),
            SegmentLockOwner {
                references: 1,
                _file: file,
            },
        );
        Ok(Self { path: lock_path })
    }
}

impl Drop for SegmentLock {
    fn drop(&mut self) {
        let Ok(mut registry) = lock_registry().lock() else {
            return;
        };
        let Some(owner) = registry.get_mut(&self.path) else {
            return;
        };
        owner.references -= 1;
        if owner.references == 0 {
            registry.remove(&self.path);
            // Never unlink: another process may already have opened this inode.
        }
    }
}

fn lock_registry() -> &'static Mutex<BTreeMap<PathBuf, SegmentLockOwner>> {
    static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, SegmentLockOwner>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalAppend<'a> {
    pub pg: PlacementGroupId,
    pub epoch: Epoch,
    pub index: LogIndex,
    pub kind: WalRecordKind,
    pub previous_hash: [u8; 32],
    pub payload: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceKind {
    Retention,
    Compaction,
}

impl MaintenanceKind {
    fn to_u8(self) -> u8 {
        match self {
            Self::Retention => 1,
            Self::Compaction => 2,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Retention),
            2 => Some(Self::Compaction),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceRecord<'a> {
    pub kind: MaintenanceKind,
    pub pg: PlacementGroupId,
    pub retained_through: LogIndex,
    pub reclaimed_bytes: u64,
    pub note: Cow<'a, [u8]>,
}

impl<'a> MaintenanceRecord<'a> {
    pub fn retention(pg: PlacementGroupId, retained_through: LogIndex, note: &'a [u8]) -> Self {
        Self {
            kind: MaintenanceKind::Retention,
            pg,
            retained_through,
            reclaimed_bytes: 0,
            note: Cow::Borrowed(note),
        }
    }

    pub fn compaction(
        pg: PlacementGroupId,
        retained_through: LogIndex,
        reclaimed_bytes: u64,
        note: &'a [u8],
    ) -> Self {
        Self {
            kind: MaintenanceKind::Compaction,
            pg,
            retained_through,
            reclaimed_bytes,
            note: Cow::Borrowed(note),
        }
    }

    fn to_owned(&self) -> MaintenanceRecord<'static> {
        MaintenanceRecord {
            kind: self.kind,
            pg: self.pg,
            retained_through: self.retained_through,
            reclaimed_bytes: self.reclaimed_bytes,
            note: Cow::Owned(self.note.to_vec()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Recovery {
    pub records: Vec<SegmentRecord>,
    pub wal_records: Vec<WalRecord>,
    pub maintenance_records: Vec<MaintenanceRecord<'static>>,
    pub pg_tails: BTreeMap<PlacementGroupId, WalTail>,
    pub compacted_prefixes: BTreeMap<PlacementGroupId, PlacementGroupPrefix>,
    pub truncated_to: u64,
}

impl Recovery {
    pub fn pg_tail(&self, pg: PlacementGroupId) -> Option<WalTail> {
        self.pg_tails.get(&pg).copied()
    }

    pub fn pg_log(&self, pg: PlacementGroupId) -> PlacementGroupLog {
        let records = self
            .wal_records
            .iter()
            .filter(|record| record.pg == pg)
            .cloned()
            .collect::<Vec<_>>();
        let committed_tail = records
            .iter()
            .rev()
            .find(|record| record.kind == WalRecordKind::Commit)
            .map(WalTail::from);

        PlacementGroupLog {
            pg,
            tail: self.pg_tail(pg),
            committed_tail,
            records,
        }
    }

    pub fn committed_prefix(&self, pg: PlacementGroupId) -> Option<PlacementGroupPrefix> {
        self.pg_log(pg)
            .committed_prefix()
            .or_else(|| self.compacted_prefixes.get(&pg).copied())
    }

    pub fn committed_prefixes(&self) -> Vec<PlacementGroupPrefix> {
        self.pg_tail_summaries()
            .into_iter()
            .filter_map(|summary| self.committed_prefix(summary.pg))
            .collect()
    }

    pub fn prove_manifest_coverage(
        &self,
        manifest: &ExtentManifest,
        required_prefixes: &BTreeMap<PlacementGroupId, LogIndex>,
    ) -> RsfResult<ManifestCoverage> {
        let mut entries_by_surface = BTreeMap::new();
        let mut covered_entries = Vec::new();

        for entry in manifest.entries() {
            let required_index =
                required_prefixes
                    .get(&entry.placement_group)
                    .ok_or_else(|| {
                        RsfError::new(
                            ErrorCode::InvalidInput,
                            format!(
                                "manifest entry {} has no required prefix for pg {}",
                                entry.object, entry.placement_group.0
                            ),
                        )
                    })?;
            let prefix = self
                .committed_prefix(entry.placement_group)
                .ok_or_else(|| {
                    RsfError::new(
                        ErrorCode::InvalidInput,
                        format!(
                            "manifest entry {} references pg {} without committed prefix",
                            entry.object, entry.placement_group.0
                        ),
                    )
                })?;
            if !prefix.covers(*required_index) {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!(
                        "manifest entry {} requires pg {} prefix {} beyond committed prefix {}",
                        entry.object, entry.placement_group.0, required_index.0, prefix.index.0
                    ),
                ));
            }

            *entries_by_surface.entry(entry.kind).or_default() += 1;
            covered_entries.push(CoveredManifestEntry {
                kind: entry.kind,
                object: entry.object.clone(),
                pg: entry.placement_group,
                version: entry.version.0,
                required_index: *required_index,
                committed_prefix: prefix,
            });
        }

        Ok(ManifestCoverage {
            total_entries: covered_entries.len(),
            entries_by_surface,
            covered_entries,
        })
    }

    pub fn pg_tail_summaries(&self) -> Vec<PlacementGroupTailSummary> {
        self.pg_tails
            .iter()
            .map(|(pg, tail)| PlacementGroupTailSummary {
                pg: *pg,
                tail: *tail,
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalTail {
    pub epoch: Epoch,
    pub index: LogIndex,
    pub record_hash: [u8; 32],
}

impl From<&WalRecord> for WalTail {
    fn from(record: &WalRecord) -> Self {
        Self {
            epoch: record.epoch,
            index: record.index,
            record_hash: record.record_hash,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementGroupLog {
    pub pg: PlacementGroupId,
    pub records: Vec<WalRecord>,
    pub tail: Option<WalTail>,
    pub committed_tail: Option<WalTail>,
}

impl PlacementGroupLog {
    pub fn committed_prefix(&self) -> Option<PlacementGroupPrefix> {
        self.committed_tail.map(|tail| PlacementGroupPrefix {
            pg: self.pg,
            epoch: tail.epoch,
            index: tail.index,
            record_hash: tail.record_hash,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementGroupPrefix {
    pub pg: PlacementGroupId,
    pub epoch: Epoch,
    pub index: LogIndex,
    pub record_hash: [u8; 32],
}

impl PlacementGroupPrefix {
    pub fn covers(self, index: LogIndex) -> bool {
        index.0 <= self.index.0
    }

    pub fn compare(self, other: Self) -> PrefixRelation {
        if self.pg != other.pg {
            return PrefixRelation::DifferentPlacementGroup;
        }
        if self.record_hash == other.record_hash
            && self.epoch == other.epoch
            && self.index == other.index
        {
            return PrefixRelation::Same;
        }
        if self.epoch == other.epoch && self.index.0 > other.index.0 {
            return PrefixRelation::Ahead {
                by: self.index.0 - other.index.0,
            };
        }
        if self.epoch == other.epoch && self.index.0 < other.index.0 {
            return PrefixRelation::Behind {
                by: other.index.0 - self.index.0,
            };
        }
        PrefixRelation::Diverged
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrefixRelation {
    Same,
    Ahead { by: u64 },
    Behind { by: u64 },
    Diverged,
    DifferentPlacementGroup,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoveredManifestEntry {
    pub kind: ManifestKind,
    pub object: String,
    pub pg: PlacementGroupId,
    pub version: u64,
    pub required_index: LogIndex,
    pub committed_prefix: PlacementGroupPrefix,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestCoverage {
    pub total_entries: usize,
    pub entries_by_surface: BTreeMap<ManifestKind, usize>,
    pub covered_entries: Vec<CoveredManifestEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementGroupTailSummary {
    pub pg: PlacementGroupId,
    pub tail: WalTail,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentRecord {
    pub offset: u64,
    pub header: SegmentRecordHeader,
    pub payload: Vec<u8>,
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

fn blake3_u64(bytes: &[u8]) -> u64 {
    let hash = blake3::hash(bytes);
    u64::from_le_bytes(hash.as_bytes()[0..8].try_into().expect("slice length"))
}

#[cfg(test)]
fn checksum(bytes: &[u8]) -> u64 {
    DEFAULT_CHECKSUM_ALGORITHM.checksum(bytes)
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn wal_record_hash(
    pg: PlacementGroupId,
    epoch: Epoch,
    index: LogIndex,
    kind: WalRecordKind,
    previous_hash: &[u8; 32],
    payload_hash: &[u8; 32],
    payload: &[u8],
) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(8 + 8 + 8 + 1 + 32 + 32 + payload.len());
    bytes.extend_from_slice(&pg.0.to_le_bytes());
    bytes.extend_from_slice(&epoch.0.to_le_bytes());
    bytes.extend_from_slice(&index.0.to_le_bytes());
    bytes.push(kind.to_u8());
    bytes.extend_from_slice(previous_hash);
    bytes.extend_from_slice(payload_hash);
    bytes.extend_from_slice(payload);
    digest(&bytes)
}

fn encode_wal_record(
    input: WalAppend<'_>,
    payload_hash: [u8; 32],
    record_hash: [u8; 32],
) -> RsfResult<Vec<u8>> {
    let payload_len = u64::try_from(input.payload.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "wal payload length does not fit in record",
        )
    })?;
    let mut encoded =
        Vec::with_capacity(4 + 8 + 8 + 8 + 1 + 32 + 32 + 32 + 8 + input.payload.len());
    encoded.extend_from_slice(b"WAL1");
    encoded.extend_from_slice(&input.pg.0.to_le_bytes());
    encoded.extend_from_slice(&input.epoch.0.to_le_bytes());
    encoded.extend_from_slice(&input.index.0.to_le_bytes());
    encoded.push(input.kind.to_u8());
    encoded.extend_from_slice(&input.previous_hash);
    encoded.extend_from_slice(&payload_hash);
    encoded.extend_from_slice(&record_hash);
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(input.payload);
    Ok(encoded)
}

pub(crate) fn encode_existing_wal_record(record: &WalRecord) -> RsfResult<Vec<u8>> {
    let payload_len = u64::try_from(record.payload.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "wal payload length does not fit in record",
        )
    })?;
    let mut encoded =
        Vec::with_capacity(4 + 8 + 8 + 8 + 1 + 32 + 32 + 32 + 8 + record.payload.len());
    encoded.extend_from_slice(b"WAL1");
    encoded.extend_from_slice(&record.pg.0.to_le_bytes());
    encoded.extend_from_slice(&record.epoch.0.to_le_bytes());
    encoded.extend_from_slice(&record.index.0.to_le_bytes());
    encoded.push(record.kind.to_u8());
    encoded.extend_from_slice(&record.previous_hash);
    encoded.extend_from_slice(&record.payload_hash);
    encoded.extend_from_slice(&record.record_hash);
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&record.payload);
    Ok(encoded)
}

fn decode_wal_record(bytes: &[u8]) -> RsfResult<Option<WalRecord>> {
    if !bytes.starts_with(b"WAL1") {
        return Ok(None);
    }

    const WAL_HEADER_LEN: usize = 4 + 8 + 8 + 8 + 1 + 32 + 32 + 32 + 8;
    if bytes.len() < WAL_HEADER_LEN {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "wal record is shorter than header",
        ));
    }

    let pg = PlacementGroupId(read_u64(bytes, 4)?);
    let epoch = Epoch(read_u64(bytes, 12)?);
    let index = LogIndex(read_u64(bytes, 20)?);
    let kind = WalRecordKind::from_u8(bytes[28]).ok_or_else(|| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("unknown wal record kind {}", bytes[28]),
        )
    })?;
    let previous_hash = read_hash(bytes, 29)?;
    let payload_hash = read_hash(bytes, 61)?;
    let record_hash = read_hash(bytes, 93)?;
    let payload_len = usize::try_from(read_u64(bytes, 125)?).map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "wal payload length does not fit in memory",
        )
    })?;
    let payload = bytes[WAL_HEADER_LEN..].to_vec();

    if payload.len() != payload_len {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "wal payload length mismatch",
        ));
    }

    if digest(&payload) != payload_hash {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "wal payload hash mismatch",
        ));
    }

    let actual_record_hash = wal_record_hash(
        pg,
        epoch,
        index,
        kind,
        &previous_hash,
        &payload_hash,
        &payload,
    );
    if actual_record_hash != record_hash {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "wal record hash mismatch",
        ));
    }

    Ok(Some(WalRecord {
        pg,
        epoch,
        index,
        kind,
        previous_hash,
        payload,
        payload_hash,
        record_hash,
    }))
}

pub(crate) fn encode_maintenance_record(record: MaintenanceRecord<'_>) -> RsfResult<Vec<u8>> {
    let note_len = u64::try_from(record.note.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "maintenance note length does not fit in record",
        )
    })?;
    let mut encoded = Vec::with_capacity(4 + 1 + 8 + 8 + 8 + 8 + record.note.len());
    encoded.extend_from_slice(b"MNT1");
    encoded.push(record.kind.to_u8());
    encoded.extend_from_slice(&record.pg.0.to_le_bytes());
    encoded.extend_from_slice(&record.retained_through.0.to_le_bytes());
    encoded.extend_from_slice(&record.reclaimed_bytes.to_le_bytes());
    encoded.extend_from_slice(&note_len.to_le_bytes());
    encoded.extend_from_slice(&record.note);
    Ok(encoded)
}

fn decode_maintenance_record(bytes: &[u8]) -> RsfResult<Option<MaintenanceRecord<'static>>> {
    if !bytes.starts_with(b"MNT1") {
        return Ok(None);
    }
    const MAINTENANCE_HEADER_LEN: usize = 4 + 1 + 8 + 8 + 8 + 8;
    if bytes.len() < MAINTENANCE_HEADER_LEN {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "maintenance record is shorter than header",
        ));
    }
    let kind = MaintenanceKind::from_u8(bytes[4]).ok_or_else(|| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("unknown maintenance record kind {}", bytes[4]),
        )
    })?;
    let note_len = usize::try_from(read_u64(bytes, 29)?).map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "maintenance note length does not fit in memory",
        )
    })?;
    let note = bytes[MAINTENANCE_HEADER_LEN..].to_vec();
    if note.len() != note_len {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "maintenance note length mismatch",
        ));
    }
    Ok(Some(MaintenanceRecord {
        kind,
        pg: PlacementGroupId(read_u64(bytes, 5)?),
        retained_through: LogIndex(read_u64(bytes, 13)?),
        reclaimed_bytes: read_u64(bytes, 21)?,
        note: Cow::Owned(note),
    }))
}

fn read_u64(bytes: &[u8], start: usize) -> RsfResult<u64> {
    let end = start + 8;
    let value = bytes.get(start..end).ok_or_else(|| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "record ended while reading u64",
        )
    })?;
    Ok(u64::from_le_bytes(value.try_into().expect("slice length")))
}

fn read_hash(bytes: &[u8], start: usize) -> RsfResult<[u8; 32]> {
    let end = start + 32;
    let value = bytes.get(start..end).ok_or_else(|| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "record ended while reading hash",
        )
    })?;
    Ok(value.try_into().expect("slice length"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageIoOperation {
    Open,
    Lock,
    Append,
    Sync,
    Recover,
    Truncate,
}

fn truncate_recovery_tail(file: &File, offset: u64, repair_tail: bool) -> RsfResult<()> {
    let length = file
        .metadata()
        .map_err(|err| storage_error(StorageIoOperation::Recover, err))?
        .len();
    if length == offset {
        return Ok(());
    }
    if !repair_tail {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "torn record in sealed segment at offset {offset}; refusing to truncate durable history"
            ),
        ));
    }
    file.set_len(offset)
        .map_err(|err| storage_error(StorageIoOperation::Truncate, err))?;
    file.sync_all()
        .map_err(|err| storage_error(StorageIoOperation::Sync, err))
}

fn storage_error(operation: StorageIoOperation, err: io::Error) -> RsfError {
    let code = match err.kind() {
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof | io::ErrorKind::WriteZero => {
            ErrorCode::CorruptionDetected
        }
        _ => ErrorCode::InvalidInput,
    };
    RsfError::new(code, format!("storage {operation:?} failed: {err}"))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use rsf_core::{CommitVersion, Digest};
    use rsf_manifest::{ByteRange, ManifestEntry, ManifestObject};

    #[test]
    fn append_and_recover_records_in_order() {
        let path = test_path("append_and_recover_records_in_order");
        let mut log = SegmentLog::open(&path).unwrap();

        assert_eq!(log.append(b"first").unwrap(), 0);
        assert_eq!(log.append(b"second").unwrap(), 29);
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        assert_eq!(recovery.truncated_to, 59);
        assert_eq!(recovery.records[0].payload, b"first");
        assert_eq!(recovery.records[0].header.version, SEGMENT_RECORD_VERSION);
        assert_eq!(
            recovery.records[0].header.checksum_algorithm,
            ChecksumAlgorithm::Blake3_64
        );
        assert_eq!(recovery.records[1].payload, b"second");
        assert!(recovery.wal_records.is_empty());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_reads_legacy_fnv_checksum_records_without_writing_new_ones() {
        let path = test_path("recovery_reads_legacy_fnv_checksum_records_without_writing_new_ones");
        let mut file = File::create(&path).unwrap();
        let payload = b"legacy";
        write_segment_header_with_algorithm(
            &mut file,
            payload.len() as u64,
            ChecksumAlgorithm::Fnv1a64.checksum(payload),
            ChecksumAlgorithm::Fnv1a64,
        )
        .unwrap();
        file.write_all(payload).unwrap();
        drop(file);

        let recovery = SegmentLog::recover(&path).unwrap();
        assert_eq!(recovery.records.len(), 1);
        assert_eq!(
            recovery.records[0].header.checksum_algorithm,
            ChecksumAlgorithm::Fnv1a64
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_truncates_torn_tail_record() {
        let path = test_path("recovery_truncates_torn_tail_record");
        let mut log = SegmentLog::open(&path).unwrap();
        assert_eq!(log.append(b"stable").unwrap(), 0);
        drop(log);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        write_segment_header(&mut file, 100, 1).unwrap();
        file.write_all(b"partial").unwrap();
        drop(file);

        let recovery = SegmentLog::recover(&path).unwrap();
        assert_eq!(recovery.records.len(), 1);
        assert_eq!(recovery.records[0].payload, b"stable");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            recovery.truncated_to
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_checks_payload_length_before_allocating_from_a_torn_header() {
        let path = test_path("untrusted_record_length");
        let mut log = SegmentLog::open(&path).unwrap();
        log.append(b"stable").unwrap();
        drop(log);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        write_segment_header(&mut file, u64::MAX, 0).unwrap();
        drop(file);
        let recovery = SegmentLog::recover(&path).unwrap();
        assert_eq!(recovery.records.len(), 1);
        assert_eq!(recovery.records[0].payload, b"stable");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            recovery.truncated_to
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn append_wal_recovers_typed_hash_chained_records() {
        let path = test_path("append_wal_recovers_typed_hash_chained_records");
        let mut log = SegmentLog::open(&path).unwrap();

        let (_, first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(7),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"table:create users",
            })
            .unwrap();
        let (_, second) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(7),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: first.record_hash,
                payload: b"commit:1",
            })
            .unwrap();
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        assert_eq!(recovery.wal_records.len(), 2);
        assert_eq!(recovery.wal_records[0].pg, PlacementGroupId(7));
        assert_eq!(recovery.wal_records[0].kind, WalRecordKind::Prepare);
        assert_eq!(recovery.wal_records[1].previous_hash, first.record_hash);
        assert_eq!(
            recovery.pg_tail(PlacementGroupId(7)),
            Some(WalTail {
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: second.record_hash,
            })
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn interleaved_placement_groups_keep_independent_hash_tails() {
        let path = test_path("interleaved_placement_groups_keep_independent_hash_tails");
        let mut log = SegmentLog::open(&path).unwrap();

        let (_, pg1_first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"pg1:first",
            })
            .unwrap();
        let (_, pg2_first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(2),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"pg2:first",
            })
            .unwrap();
        let (_, pg1_second) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: pg1_first.record_hash,
                payload: b"pg1:commit",
            })
            .unwrap();
        let (_, pg2_second) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(2),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: pg2_first.record_hash,
                payload: b"pg2:commit",
            })
            .unwrap();
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        assert_eq!(recovery.wal_records.len(), 4);
        assert_eq!(
            recovery.pg_tail(PlacementGroupId(1)),
            Some(WalTail {
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: pg1_second.record_hash,
            })
        );
        assert_eq!(
            recovery.pg_tail(PlacementGroupId(2)),
            Some(WalTail {
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: pg2_second.record_hash,
            })
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_wal_allows_index_one_for_new_pg_after_other_pg_records() {
        let path = test_path("append_wal_allows_index_one_for_new_pg_after_other_pg_records");
        let mut log = SegmentLog::open(&path).unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(1),
            kind: WalRecordKind::Prepare,
            previous_hash: [0; 32],
            payload: b"pg1:first",
        })
        .unwrap();

        let (_, pg2_first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(2),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"pg2:first",
            })
            .unwrap();

        assert_eq!(pg2_first.index, LogIndex(1));
        assert_eq!(pg2_first.previous_hash, [0; 32]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn pg_log_filters_records_and_reports_tail_summary() {
        let path = test_path("pg_log_filters_records_and_reports_tail_summary");
        let mut log = SegmentLog::open(&path).unwrap();

        let (_, pg1_first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"pg1:first",
            })
            .unwrap();
        let (_, pg1_commit) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: pg1_first.record_hash,
                payload: b"pg1:commit",
            })
            .unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(2),
            epoch: Epoch(1),
            index: LogIndex(1),
            kind: WalRecordKind::Prepare,
            previous_hash: [0; 32],
            payload: b"pg2:first",
        })
        .unwrap();
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        let pg1 = recovery.pg_log(PlacementGroupId(1));
        assert_eq!(pg1.pg, PlacementGroupId(1));
        assert_eq!(pg1.records.len(), 2);
        assert!(
            pg1.records
                .iter()
                .all(|record| record.pg == PlacementGroupId(1))
        );
        assert_eq!(
            pg1.tail,
            Some(WalTail {
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: pg1_commit.record_hash,
            })
        );
        assert_eq!(pg1.committed_tail, pg1.tail);

        let summaries = recovery.pg_tail_summaries();
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].pg, PlacementGroupId(1));
        assert_eq!(summaries[1].pg, PlacementGroupId(2));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn committed_prefixes_compare_without_scanning_payloads() {
        let path = test_path("committed_prefixes_compare_without_scanning_payloads");
        let mut log = SegmentLog::open(&path).unwrap();

        let (_, first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        let (_, second) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: first.record_hash,
                payload: b"commit",
            })
            .unwrap();
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        let prefix = recovery.committed_prefix(PlacementGroupId(1)).unwrap();
        assert_eq!(
            prefix,
            PlacementGroupPrefix {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: second.record_hash,
            }
        );
        assert!(prefix.covers(LogIndex(1)));
        assert!(prefix.covers(LogIndex(2)));
        assert!(!prefix.covers(LogIndex(3)));
        assert_eq!(prefix.compare(prefix), PrefixRelation::Same);
        assert_eq!(
            prefix.compare(PlacementGroupPrefix {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                record_hash: first.record_hash,
            }),
            PrefixRelation::Ahead { by: 1 }
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovered_committed_prefixes_prove_manifest_coverage() {
        let path = test_path("recovered_committed_prefixes_prove_manifest_coverage");
        let mut log = SegmentLog::open(&path).unwrap();

        let (_, pg1_prepare) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::RelationalMutation,
                previous_hash: [0; 32],
                payload: b"relational-page",
            })
            .unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: pg1_prepare.record_hash,
            payload: b"commit-relational",
        })
        .unwrap();
        let (_, pg2_prepare) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(2),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::ObjectManifest,
                previous_hash: [0; 32],
                payload: b"object-part",
            })
            .unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(2),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: pg2_prepare.record_hash,
            payload: b"commit-object",
        })
        .unwrap();
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        let manifest = ExtentManifest::new(vec![
            entry(
                ManifestKind::RelationalPage,
                "sql/users/page/1",
                CommitVersion(2),
                PlacementGroupId(1),
                1,
            ),
            entry(
                ManifestKind::ObjectPart,
                "object/media/hero.png/part/1",
                CommitVersion(2),
                PlacementGroupId(2),
                2,
            ),
        ])
        .unwrap();

        let coverage = recovery
            .prove_manifest_coverage(&manifest, &required_prefixes([(1, 2), (2, 2)]))
            .unwrap();
        assert_eq!(coverage.total_entries, 2);
        assert_eq!(
            coverage
                .entries_by_surface
                .get(&ManifestKind::RelationalPage),
            Some(&1)
        );
        assert_eq!(
            coverage.entries_by_surface.get(&ManifestKind::ObjectPart),
            Some(&1)
        );
        assert!(
            coverage
                .covered_entries
                .iter()
                .all(|entry| entry.committed_prefix.covers(entry.required_index))
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn manifest_coverage_rejects_entries_beyond_committed_prefix() {
        let path = test_path("manifest_coverage_rejects_entries_beyond_committed_prefix");
        let mut log = SegmentLog::open(&path).unwrap();

        let (_, prepare) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::RelationalMutation,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: prepare.record_hash,
            payload: b"commit",
        })
        .unwrap();
        drop(log);

        let manifest = ExtentManifest::new(vec![entry(
            ManifestKind::RelationalWal,
            "sql/users/wal/too-new",
            CommitVersion(3),
            PlacementGroupId(1),
            1,
        )])
        .unwrap();

        let err = SegmentLog::recover(&path)
            .unwrap()
            .prove_manifest_coverage(&manifest, &required_prefixes([(1, 3)]))
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn pg_log_for_missing_group_is_empty() {
        let path = test_path("pg_log_for_missing_group_is_empty");
        let mut log = SegmentLog::open(&path).unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(1),
            kind: WalRecordKind::Prepare,
            previous_hash: [0; 32],
            payload: b"first",
        })
        .unwrap();
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        let missing = recovery.pg_log(PlacementGroupId(99));
        assert_eq!(missing.pg, PlacementGroupId(99));
        assert!(missing.records.is_empty());
        assert_eq!(missing.tail, None);
        assert_eq!(missing.committed_tail, None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_wal_rejects_non_monotonic_index_for_pg() {
        let path = test_path("append_wal_rejects_non_monotonic_index_for_pg");
        let mut log = SegmentLog::open(&path).unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(1),
            kind: WalRecordKind::Prepare,
            previous_hash: [0; 32],
            payload: b"first",
        })
        .unwrap();

        let err = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Commit,
                previous_hash: [0; 32],
                payload: b"duplicate-index",
            })
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_wal_rejects_wrong_previous_hash() {
        let path = test_path("append_wal_rejects_wrong_previous_hash");
        let mut log = SegmentLog::open(&path).unwrap();
        let (_, first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"first",
            })
            .unwrap();

        let mut wrong_hash = first.record_hash;
        wrong_hash[0] ^= 1;
        let err = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: wrong_hash,
                payload: b"second",
            })
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_rejects_wal_chain_corruption() {
        let path = test_path("recovery_rejects_wal_chain_corruption");
        let mut log = SegmentLog::open(&path).unwrap();
        let (_, first) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"first",
            })
            .unwrap();
        log.append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: first.record_hash,
            payload: b"second",
        })
        .unwrap();
        drop(log);

        let mut bytes = std::fs::read(&path).unwrap();
        let second_payload_start = (HEADER_LEN + 133 + b"first".len()) + HEADER_LEN;
        let second_previous_hash_start = second_payload_start + 29;
        bytes[second_previous_hash_start] ^= 1;
        let payload_start = second_payload_start;
        let payload_end = bytes.len();
        let new_checksum = checksum(&bytes[payload_start..payload_end]);
        let checksum_start = second_payload_start - HEADER_LEN + 16;
        bytes[checksum_start..checksum_start + 8].copy_from_slice(&new_checksum.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();

        let err = SegmentLog::recover(&path).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_rejects_checksum_corruption() {
        let path = test_path("recovery_rejects_checksum_corruption");
        let mut log = SegmentLog::open(&path).unwrap();
        log.append(b"stable").unwrap();
        drop(log);

        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();

        let err = SegmentLog::recover(&path).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_rejects_unsupported_record_version() {
        let path = test_path("recovery_rejects_unsupported_record_version");
        let mut log = SegmentLog::open(&path).unwrap();
        log.append(b"stable").unwrap();
        drop(log);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[4..6].copy_from_slice(&999_u16.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();

        let err = SegmentLog::recover(&path).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("unsupported segment record version"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_rejects_unsupported_checksum_algorithm() {
        let path = test_path("recovery_rejects_unsupported_checksum_algorithm");
        let mut log = SegmentLog::open(&path).unwrap();
        log.append(b"stable").unwrap();
        drop(log);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[6..8].copy_from_slice(&777_u16.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();

        let err = SegmentLog::recover(&path).unwrap_err();
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("unsupported checksum algorithm"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn maintenance_records_do_not_change_committed_prefix() {
        let path = test_path("maintenance_records_do_not_change_committed_prefix");
        let mut log = SegmentLog::open(&path).unwrap();
        let (_, prepare) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0; 32],
                payload: b"prepare",
            })
            .unwrap();
        let (_, commit) = log
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: prepare.record_hash,
                payload: b"commit",
            })
            .unwrap();
        log.append_maintenance(MaintenanceRecord::retention(
            PlacementGroupId(1),
            LogIndex(2),
            b"retain committed prefix",
        ))
        .unwrap();
        log.append_maintenance(MaintenanceRecord::compaction(
            PlacementGroupId(1),
            LogIndex(2),
            4096,
            b"compact obsolete records",
        ))
        .unwrap();
        drop(log);

        let recovery = SegmentLog::recover(&path).unwrap();
        assert_eq!(recovery.maintenance_records.len(), 2);
        assert_eq!(
            recovery.committed_prefix(PlacementGroupId(1)),
            Some(PlacementGroupPrefix {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                record_hash: commit.record_hash,
            })
        );
        assert_eq!(
            recovery.pg_tail(PlacementGroupId(1)).unwrap().index,
            LogIndex(2)
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn segment_lock_allows_same_process_reentrant_handles() {
        let path = test_path("segment_lock_allows_same_process_reentrant_handles");
        let log = SegmentLog::open(&path).unwrap();

        let second = SegmentLog::open(&path).unwrap();
        assert!(std::fs::metadata(path.with_extension("lock")).is_ok());
        drop(second);
        assert!(std::fs::metadata(path.with_extension("lock")).is_ok());

        drop(log);
        assert!(std::fs::metadata(path.with_extension("lock")).is_ok());
        drop(SegmentLog::open(&path).unwrap());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn external_segment_lock_blocks_open_and_recovery() {
        let path = test_path("external_segment_lock_blocks_open_and_recovery");
        let external = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("lock"))
            .unwrap();
        external.try_lock().unwrap();

        let err = SegmentLog::open(&path).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("segment lock already held"));

        let err = SegmentLog::recover(&path).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("segment lock already held"));

        drop(external);
        drop(SegmentLog::open(&path).unwrap());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sync_policy_modes_append_and_recover() {
        for policy in [SyncPolicy::None, SyncPolicy::Data, SyncPolicy::Full] {
            let path = test_path(&format!("sync_policy_{policy:?}"));
            let mut log = SegmentLog::open_with_options(
                &path,
                SegmentLogOptions {
                    sync_policy: policy,
                },
            )
            .unwrap();
            log.append(b"payload").unwrap();
            drop(log);

            let recovery = SegmentLog::recover(&path).unwrap();
            assert_eq!(recovery.records.len(), 1);
            assert_eq!(recovery.records[0].payload, b"payload");

            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn storage_io_error_classifies_corruption_kinds() {
        let corrupt = storage_error(
            StorageIoOperation::Recover,
            io::Error::new(io::ErrorKind::InvalidData, "bad bytes"),
        );
        assert_eq!(corrupt.code, ErrorCode::CorruptionDetected);
        assert!(corrupt.message.contains("Recover"));

        let invalid = storage_error(
            StorageIoOperation::Open,
            io::Error::new(io::ErrorKind::PermissionDenied, "no permission"),
        );
        assert_eq!(invalid.code, ErrorCode::InvalidInput);
        assert!(invalid.message.contains("Open"));
    }

    #[test]
    fn append_replay_benchmark_reports_record_counts() {
        let path = test_path("append_replay_benchmark_reports_record_counts");
        let mut log = SegmentLog::open_with_options(
            &path,
            SegmentLogOptions {
                sync_policy: SyncPolicy::None,
            },
        )
        .unwrap();
        let report = log
            .benchmark_append_replay(&[b"one".as_slice(), b"two".as_slice(), b"three".as_slice()])
            .unwrap();

        assert_eq!(report.appended_records, 3);
        assert_eq!(report.recovered_records, 3);
        assert_eq!(report.recovered_wal_records, 0);
        assert_eq!(report.bytes, 11);

        let _ = std::fs::remove_file(path);
    }

    fn test_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-{name}-{unique}.segment"))
    }

    fn write_segment_header(file: &mut File, payload_len: u64, checksum: u64) -> io::Result<()> {
        write_segment_header_with_algorithm(file, payload_len, checksum, DEFAULT_CHECKSUM_ALGORITHM)
    }

    fn write_segment_header_with_algorithm(
        file: &mut File,
        payload_len: u64,
        checksum: u64,
        algorithm: ChecksumAlgorithm,
    ) -> io::Result<()> {
        file.write_all(&MAGIC)?;
        file.write_all(&SEGMENT_RECORD_VERSION.to_le_bytes())?;
        file.write_all(&algorithm.to_u16().to_le_bytes())?;
        file.write_all(&payload_len.to_le_bytes())?;
        file.write_all(&checksum.to_le_bytes())?;
        Ok(())
    }

    fn entry(
        kind: ManifestKind,
        object: &str,
        version: CommitVersion,
        pg: PlacementGroupId,
        seed: u8,
    ) -> ManifestEntry {
        ManifestEntry::new(
            kind,
            ManifestObject::new("app", object),
            version,
            Digest::blake3_bytes(format!("storage-manifest-extent:{object}:{seed}").as_bytes()),
            pg,
            ByteRange::new(u64::from(seed) * 1024, 512),
            Digest::blake3_bytes(format!("storage-manifest-checksum:{object}:{seed}").as_bytes()),
        )
    }

    fn required_prefixes<const N: usize>(
        prefixes: [(u64, u64); N],
    ) -> BTreeMap<PlacementGroupId, LogIndex> {
        prefixes
            .into_iter()
            .map(|(pg, index)| (PlacementGroupId(pg), LogIndex(index)))
            .collect()
    }
}

#[cfg(test)]
mod segment_publication_tests;

#[cfg(test)]
mod recovery_publication_tests;
