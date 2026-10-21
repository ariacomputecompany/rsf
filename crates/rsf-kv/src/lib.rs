#![forbid(unsafe_code)]

mod journal;

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    Bytes, CommitConsistency, CommitEvidence, CommitVersion, Epoch, ErrorCode, LogIndex,
    PlacementGroupId, RsfError, RsfResult, storage::WalRecordKind,
};
use rsf_protocol::{
    ClientRequestId, CommitCertificate, ConsistencyMode, FabricCommit, FabricOperation,
    FabricRouterPort,
};
use rsf_storage::SegmentStore;

const KV_WAL_MAGIC: &[u8; 5] = b"RSFKV";
const KV_WAL_VERSION: u8 = 1;
const KV_SNAPSHOT_MAGIC: &[u8; 6] = b"RSFKVS";
const KV_SNAPSHOT_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvVersionedValue {
    pub version: CommitVersion,
    pub value: Bytes,
    pub deleted: bool,
}

#[derive(Clone, Debug, Default)]
pub struct KvEngine {
    data: BTreeMap<Bytes, Vec<KvVersionedValue>>,
    changes: Vec<KvChange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvChange {
    pub version: CommitVersion,
    pub key: Bytes,
    pub value: Option<Bytes>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadSnapshot {
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvBatch {
    pub client_request: Option<ClientRequestId>,
    pub snapshot: ReadSnapshot,
    pub mutations: Vec<KvMutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvCommitResult {
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KvMutation {
    Set { key: Bytes, value: Bytes },
    Clear { key: Bytes },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvCompactionOutput {
    pub highest_version: CommitVersion,
    pub retained_versions: usize,
    pub dropped_versions: usize,
    pub retained_tombstones: usize,
    pub snapshot_log_index: LogIndex,
}

#[derive(Debug)]
pub struct DurableKvEngine {
    engine: KvEngine,
    store: SegmentStore,
    epoch: Epoch,
    journal: journal::Journal,
    _writer_lock: rsf_storage::ExclusiveFileLock,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveredKvBatch {
    version: CommitVersion,
    protocol_log_index: LogIndex,
    mutations: Vec<KvMutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveredKvSnapshot {
    highest_version: CommitVersion,
    data: BTreeMap<Bytes, Vec<KvVersionedValue>>,
}

impl KvEngine {
    pub fn snapshot(&self, router: &impl FabricRouterPort) -> ReadSnapshot {
        ReadSnapshot {
            version: router.latest_version(),
        }
    }

    pub fn set(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> RsfResult<CommitVersion> {
        let key = key.into();
        let snapshot = self.snapshot(router);
        self.commit_batch(
            router,
            pg,
            KvBatch {
                client_request: None,
                snapshot,
                mutations: vec![KvMutation::Set {
                    key,
                    value: value.into(),
                }],
            },
        )
    }

    pub fn commit_batch(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        batch: KvBatch,
    ) -> RsfResult<CommitVersion> {
        Ok(self.commit_batch_with_evidence(router, pg, batch)?.version)
    }

    pub fn commit_batch_with_evidence(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        batch: KvBatch,
    ) -> RsfResult<KvCommitResult> {
        if let Some(request) = &batch.client_request
            && let Some(certificate) = router.request_certificate(request)
        {
            return Ok(KvCommitResult {
                version: certificate.version,
                evidence: commit_evidence(certificate),
                replayed: true,
            });
        }

        let keys = batch
            .mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect::<Vec<_>>();
        for key in &keys {
            self.ensure_write_not_conflicted(key, batch.snapshot.version)?;
        }
        let operation = FabricOperation::KvBatch { keys };
        ensure_operation_matches_router_route(router, pg, &operation)?;

        if let Some(request) = batch.client_request {
            let result = router.commit_request(pg, request, operation)?;
            if !result.replayed {
                self.apply_batch_at(result.certificate.version, batch.mutations);
            }
            return Ok(KvCommitResult {
                version: result.certificate.version,
                evidence: commit_evidence(result.certificate),
                replayed: result.replayed,
            });
        }

        let commit = router.commit(pg, operation)?;
        self.apply_batch_at(commit.version, batch.mutations);
        Ok(KvCommitResult {
            version: commit.version,
            evidence: fabric_commit_evidence(commit),
            replayed: false,
        })
    }

    pub fn commit_batch_routed(
        &mut self,
        router: &mut impl FabricRouterPort,
        batch: KvBatch,
    ) -> RsfResult<KvCommitResult> {
        if let Some(request) = &batch.client_request
            && let Some(certificate) = router.request_certificate(request)
        {
            return Ok(KvCommitResult {
                version: certificate.version,
                evidence: commit_evidence(certificate),
                replayed: true,
            });
        }

        let keys = batch
            .mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect::<Vec<_>>();
        for key in &keys {
            self.ensure_write_not_conflicted(key, batch.snapshot.version)?;
        }

        let operation = FabricOperation::KvBatch { keys };
        let pg = router.route_operation(&operation).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "router does not expose placement routing for kv batch",
            )
        })?;

        if let Some(request) = batch.client_request {
            let result = router.commit_request(pg, request, operation)?;
            if !result.replayed {
                self.apply_batch_at(result.certificate.version, batch.mutations);
            }
            return Ok(KvCommitResult {
                version: result.certificate.version,
                evidence: commit_evidence(result.certificate),
                replayed: result.replayed,
            });
        }

        let commit = router.commit(pg, operation)?;
        self.apply_batch_at(commit.version, batch.mutations);
        Ok(KvCommitResult {
            version: commit.version,
            evidence: fabric_commit_evidence(commit),
            replayed: false,
        })
    }

    pub fn apply_committed_set(
        &mut self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        version: CommitVersion,
    ) {
        let key = key.into();
        let value = value.into();
        self.data
            .entry(key.clone())
            .or_default()
            .push(KvVersionedValue {
                version,
                value: value.clone(),
                deleted: false,
            });
        self.changes.push(KvChange {
            version,
            key,
            value: Some(value),
        });
    }

    pub fn get(&self, key: &[u8]) -> Option<&KvVersionedValue> {
        let value = self.latest_any(key)?;
        (!value.deleted).then_some(value)
    }

    pub fn get_at(&self, key: &[u8], snapshot: ReadSnapshot) -> Option<&KvVersionedValue> {
        let value = self
            .data
            .get(key)?
            .iter()
            .rev()
            .find(|value| value.version <= snapshot.version)?;
        (!value.deleted).then_some(value)
    }

    pub fn apply_committed_clear(&mut self, key: impl Into<Bytes>, version: CommitVersion) {
        let key = key.into();
        self.data
            .entry(key.clone())
            .or_default()
            .push(KvVersionedValue {
                version,
                value: Vec::new(),
                deleted: true,
            });
        self.changes.push(KvChange {
            version,
            key,
            value: None,
        });
    }

    /// Apply a batch atomically after validating all key versions and mutations.
    /// Rejection leaves MVCC history and change delivery unchanged. Identical
    /// same-version mutations (including duplicates within a batch) are no-ops.
    pub fn apply_committed_batch(
        &mut self,
        version: CommitVersion,
        mutations: Vec<KvMutation>,
    ) -> RsfResult<()> {
        // Validate the whole batch against the unchanged base before publishing
        // either MVCC versions or change events. Only borrowed keys are retained.
        let mut proposed = BTreeMap::new();
        for mutation in &mutations {
            if let Some(previous) = proposed.insert(mutation.key(), mutation)
                && previous != mutation
            {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "committed kv batch contains conflicting mutations for one key",
                ));
            }
            if let Some(value) = self.latest_any(mutation.key()) {
                if value.version > version {
                    return Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        "cannot apply committed kv mutation below latest key version",
                    ));
                }
                if value.version == version && !mutation_matches_value(mutation, value) {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "committed kv mutation conflicts with existing value at same version",
                    ));
                }
            }
        }
        for mutation in mutations {
            if self
                .latest_any(mutation.key())
                .is_some_and(|value| value.version == version)
            {
                continue;
            }
            match mutation {
                KvMutation::Set { key, value } => self.apply_committed_set(key, value, version),
                KvMutation::Clear { key } => self.apply_committed_clear(key, version),
            }
        }
        Ok(())
    }

    pub fn scan_prefix_at(
        &self,
        prefix: &[u8],
        snapshot: ReadSnapshot,
    ) -> Vec<(Bytes, KvVersionedValue)> {
        self.data
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| {
                self.get_at(key, snapshot)
                    .map(|value| (key.clone(), value.clone()))
            })
            .collect()
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Bytes, KvVersionedValue)> {
        self.data
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| self.get(key).map(|value| (key.clone(), value.clone())))
            .collect()
    }

    pub fn scan_prefix_versions(&self, prefix: &[u8]) -> Vec<(Bytes, Vec<KvVersionedValue>)> {
        self.data
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, versions)| (key.clone(), versions.clone()))
            .collect()
    }

    pub fn changes_since(&self, version: CommitVersion) -> Vec<KvChange> {
        self.changes
            .iter()
            .filter(|change| change.version > version)
            .cloned()
            .collect()
    }

    pub fn visible_key_count(&self) -> usize {
        self.data
            .keys()
            .filter(|key| self.get(key).is_some())
            .count()
    }

    pub fn compact(
        &mut self,
        gc_before: CommitVersion,
        active_snapshots: &[ReadSnapshot],
    ) -> KvCompactionOutput {
        let before = self.version_count();
        let mut compacted = BTreeMap::new();
        let mut retained_versions = 0;
        let mut retained_tombstones = 0;

        for (key, versions) in &self.data {
            let mut retained = BTreeMap::<CommitVersion, KvVersionedValue>::new();
            if let Some(latest) = versions.last()
                && (!latest.deleted || latest.version > gc_before)
            {
                retained.insert(latest.version, latest.clone());
            }
            for snapshot in active_snapshots {
                if let Some(value) = versions
                    .iter()
                    .rev()
                    .find(|value| value.version <= snapshot.version)
                {
                    retained.insert(value.version, value.clone());
                }
            }

            let retained = retained.into_values().collect::<Vec<_>>();
            if !retained.is_empty() {
                retained_tombstones += retained.iter().filter(|value| value.deleted).count();
                retained_versions += retained.len();
                compacted.insert(key.clone(), retained);
            }
        }

        self.data = compacted;

        KvCompactionOutput {
            highest_version: self.highest_version(),
            retained_versions,
            dropped_versions: before.saturating_sub(retained_versions),
            retained_tombstones,
            snapshot_log_index: LogIndex(0),
        }
    }

    fn replace_data(&mut self, snapshot: RecoveredKvSnapshot) {
        self.data = snapshot.data;
        self.changes
            .retain(|change| change.version > snapshot.highest_version);
        for (key, versions) in &self.data {
            for value in versions {
                self.changes.push(KvChange {
                    version: value.version,
                    key: key.clone(),
                    value: (!value.deleted).then(|| value.value.clone()),
                });
            }
        }
        self.changes.sort_by_key(|change| change.version);
    }

    fn version_count(&self) -> usize {
        self.data.values().map(Vec::len).sum()
    }

    fn highest_version(&self) -> CommitVersion {
        self.data
            .values()
            .flat_map(|versions| versions.iter().map(|value| value.version))
            .max()
            .unwrap_or(CommitVersion(0))
    }

    fn ensure_write_not_conflicted(
        &self,
        key: &[u8],
        read_version: CommitVersion,
    ) -> RsfResult<()> {
        if self
            .latest_any(key)
            .is_some_and(|value| value.version > read_version)
        {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "write conflict against newer committed version",
            ));
        }
        Ok(())
    }

    fn latest_any(&self, key: &[u8]) -> Option<&KvVersionedValue> {
        self.data.get(key).and_then(|versions| versions.last())
    }

    fn apply_batch_at(&mut self, version: CommitVersion, mutations: Vec<KvMutation>) {
        for mutation in mutations {
            match mutation {
                KvMutation::Set { key, value } => self.apply_committed_set(key, value, version),
                KvMutation::Clear { key } => self.apply_committed_clear(key, version),
            }
        }
    }
}

impl DurableKvEngine {
    /// Recover locally applied values and unresolved intents. Call `reconcile`
    /// with the authoritative router before serving application reads.
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        Self::open_journal(path.as_ref(), epoch)
    }

    pub fn snapshot(&self, router: &impl FabricRouterPort) -> ReadSnapshot {
        self.engine.snapshot(router)
    }

    pub fn commit_batch(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        batch: KvBatch,
    ) -> RsfResult<KvCommitResult> {
        self.commit_durable(router, pg, batch)
    }

    pub fn commit_batch_routed(
        &mut self,
        router: &mut impl FabricRouterPort,
        batch: KvBatch,
    ) -> RsfResult<KvCommitResult> {
        let operation = journal::operation(&batch.mutations);
        let pg = router.route_operation(&operation).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "router does not expose placement routing for kv batch",
            )
        })?;
        self.commit_durable(router, pg, batch)
    }

    pub fn get(&self, key: &[u8]) -> Option<&KvVersionedValue> {
        self.engine.get(key)
    }

    pub fn get_at(&self, key: &[u8], snapshot: ReadSnapshot) -> Option<&KvVersionedValue> {
        self.engine.get_at(key, snapshot)
    }

    pub fn scan_prefix_at(
        &self,
        prefix: &[u8],
        snapshot: ReadSnapshot,
    ) -> Vec<(Bytes, KvVersionedValue)> {
        self.engine.scan_prefix_at(prefix, snapshot)
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Bytes, KvVersionedValue)> {
        self.engine.scan_prefix(prefix)
    }

    pub fn scan_prefix_versions(&self, prefix: &[u8]) -> Vec<(Bytes, Vec<KvVersionedValue>)> {
        self.engine.scan_prefix_versions(prefix)
    }

    pub fn changes_since(&self, version: CommitVersion) -> Vec<KvChange> {
        self.engine.changes_since(version)
    }

    pub fn visible_key_count(&self) -> usize {
        self.engine.visible_key_count()
    }

    pub fn compact(
        &mut self,
        pg: PlacementGroupId,
        gc_before: CommitVersion,
        active_snapshots: &[ReadSnapshot],
    ) -> RsfResult<KvCompactionOutput> {
        self.ensure_healthy()?;
        if self.pending_request_count() != 0 {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "resolve pending KV intents before compaction",
            ));
        }
        let mut candidate = self.engine.clone();
        let mut output = candidate.compact(gc_before, active_snapshots);
        let payload = encode_kv_snapshot_payload(&candidate)?;
        output.snapshot_log_index = self.append_record(pg, WalRecordKind::Snapshot, &payload)?;
        self.engine = candidate;
        Ok(output)
    }
}

fn encode_kv_wal_payload(
    version: CommitVersion,
    protocol_log_index: LogIndex,
    mutations: &[KvMutation],
) -> RsfResult<Vec<u8>> {
    let mutation_count = u32::try_from(mutations.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "kv wal mutation count does not fit in record",
        )
    })?;
    let mut out = Vec::new();
    out.extend_from_slice(KV_WAL_MAGIC);
    out.push(KV_WAL_VERSION);
    out.extend_from_slice(&version.0.to_le_bytes());
    out.extend_from_slice(&protocol_log_index.0.to_le_bytes());
    out.extend_from_slice(&mutation_count.to_le_bytes());
    for mutation in mutations {
        match mutation {
            KvMutation::Set { key, value } => {
                out.push(1);
                write_bytes(key, &mut out)?;
                write_bytes(value, &mut out)?;
            }
            KvMutation::Clear { key } => {
                out.push(2);
                write_bytes(key, &mut out)?;
                write_bytes(&[], &mut out)?;
            }
        }
    }
    Ok(out)
}

fn decode_kv_wal_payload(bytes: &[u8]) -> RsfResult<RecoveredKvBatch> {
    if !bytes.starts_with(KV_WAL_MAGIC) {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "kv wal record has invalid magic",
        ));
    }
    let mut offset = KV_WAL_MAGIC.len();
    let version_tag = read_u8(bytes, &mut offset)?;
    if version_tag != KV_WAL_VERSION {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "kv wal record has unsupported version",
        ));
    }
    let version = CommitVersion(read_u64(bytes, &mut offset)?);
    let protocol_log_index = LogIndex(read_u64(bytes, &mut offset)?);
    let mutation_count = read_u32(bytes, &mut offset)? as usize;
    if mutation_count > bytes.len().saturating_sub(offset) / 9 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "kv mutation count exceeds payload bounds",
        ));
    }
    let mut mutations = Vec::with_capacity(mutation_count);
    for _ in 0..mutation_count {
        let tag = read_u8(bytes, &mut offset)?;
        let key = read_bytes(bytes, &mut offset)?;
        let value = read_bytes(bytes, &mut offset)?;
        let mutation = match tag {
            1 => KvMutation::Set { key, value },
            2 => {
                if !value.is_empty() {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "kv clear record must not carry a value",
                    ));
                }
                KvMutation::Clear { key }
            }
            _ => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "kv wal record has unknown mutation tag",
                ));
            }
        };
        mutations.push(mutation);
    }
    if offset != bytes.len() {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "kv wal record has trailing bytes",
        ));
    }
    Ok(RecoveredKvBatch {
        version,
        protocol_log_index,
        mutations,
    })
}

fn encode_kv_snapshot_payload(engine: &KvEngine) -> RsfResult<Vec<u8>> {
    let key_count = u32::try_from(engine.data.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "kv snapshot key count does not fit in record",
        )
    })?;
    let mut out = Vec::new();
    out.extend_from_slice(KV_SNAPSHOT_MAGIC);
    out.push(KV_SNAPSHOT_VERSION);
    out.extend_from_slice(&engine.highest_version().0.to_le_bytes());
    out.extend_from_slice(&key_count.to_le_bytes());
    for (key, versions) in &engine.data {
        let version_count = u32::try_from(versions.len()).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "kv snapshot version count does not fit in record",
            )
        })?;
        write_bytes(key, &mut out)?;
        out.extend_from_slice(&version_count.to_le_bytes());
        for version in versions {
            out.extend_from_slice(&version.version.0.to_le_bytes());
            out.push(u8::from(version.deleted));
            write_bytes(&version.value, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_kv_snapshot_payload(bytes: &[u8]) -> RsfResult<RecoveredKvSnapshot> {
    if !bytes.starts_with(KV_SNAPSHOT_MAGIC) {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "kv snapshot record has invalid magic",
        ));
    }
    let mut offset = KV_SNAPSHOT_MAGIC.len();
    let version_tag = read_u8(bytes, &mut offset)?;
    if version_tag != KV_SNAPSHOT_VERSION {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "kv snapshot record has unsupported version",
        ));
    }
    let highest_version = CommitVersion(read_u64(bytes, &mut offset)?);
    let key_count = read_u32(bytes, &mut offset)? as usize;
    let mut data = BTreeMap::new();
    for _ in 0..key_count {
        let key = read_bytes(bytes, &mut offset)?;
        let version_count = read_u32(bytes, &mut offset)? as usize;
        if version_count == 0 {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "kv snapshot key must carry at least one version",
            ));
        }
        if version_count > bytes.len().saturating_sub(offset) / 13 {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "kv snapshot version count exceeds payload bounds",
            ));
        }
        let mut versions = Vec::with_capacity(version_count);
        let mut previous = CommitVersion(0);
        for _ in 0..version_count {
            let version = CommitVersion(read_u64(bytes, &mut offset)?);
            if version <= previous {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "kv snapshot versions must be strictly increasing",
                ));
            }
            let deleted = match read_u8(bytes, &mut offset)? {
                0 => false,
                1 => true,
                _ => {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "kv snapshot has invalid deleted flag",
                    ));
                }
            };
            let value = read_bytes(bytes, &mut offset)?;
            if deleted && !value.is_empty() {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "kv snapshot tombstone must not carry a value",
                ));
            }
            versions.push(KvVersionedValue {
                version,
                value,
                deleted,
            });
            previous = version;
        }
        if data.insert(key, versions).is_some() {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "kv snapshot contains duplicate key",
            ));
        }
    }
    if offset != bytes.len() {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "kv snapshot record has trailing bytes",
        ));
    }
    Ok(RecoveredKvSnapshot {
        highest_version,
        data,
    })
}

fn write_bytes(bytes: &[u8], out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "kv wal byte field does not fit in record",
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| RsfError::new(ErrorCode::CorruptionDetected, "kv wal record ended early"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let end = (*offset)
        .checked_add(4)
        .ok_or_else(|| RsfError::new(ErrorCode::CorruptionDetected, "kv wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| RsfError::new(ErrorCode::CorruptionDetected, "kv wal record ended early"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let end = (*offset)
        .checked_add(8)
        .ok_or_else(|| RsfError::new(ErrorCode::CorruptionDetected, "kv wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| RsfError::new(ErrorCode::CorruptionDetected, "kv wal record ended early"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> RsfResult<Bytes> {
    let len = read_u32(bytes, offset)? as usize;
    let end = (*offset).checked_add(len).ok_or_else(|| {
        RsfError::new(ErrorCode::CorruptionDetected, "kv wal byte field overflow")
    })?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| RsfError::new(ErrorCode::CorruptionDetected, "kv wal record ended early"))?;
    *offset = end;
    Ok(value.to_vec())
}

fn commit_evidence(certificate: CommitCertificate) -> CommitEvidence {
    CommitEvidence {
        placement_group: certificate.pg,
        log_index: certificate.index,
        version: certificate.version,
        consistency: match certificate.mode {
            ConsistencyMode::Strict => CommitConsistency::Strict,
            ConsistencyMode::AvailabilityFirst => CommitConsistency::AvailabilityFirst,
        },
        operation_id: certificate.operation_id,
    }
}

fn fabric_commit_evidence(commit: FabricCommit) -> CommitEvidence {
    CommitEvidence {
        placement_group: commit.pg,
        log_index: commit.index,
        version: commit.version,
        consistency: match commit.mode {
            ConsistencyMode::Strict => CommitConsistency::Strict,
            ConsistencyMode::AvailabilityFirst => CommitConsistency::AvailabilityFirst,
        },
        operation_id: commit.operation_id,
    }
}

impl KvMutation {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Clear { key } => key,
        }
    }
}

fn mutation_matches_value(mutation: &KvMutation, value: &KvVersionedValue) -> bool {
    match mutation {
        KvMutation::Set {
            value: mutation, ..
        } => !value.deleted && value.value == *mutation,
        KvMutation::Clear { .. } => value.deleted,
    }
}

fn ensure_operation_matches_router_route(
    router: &impl FabricRouterPort,
    pg: PlacementGroupId,
    operation: &FabricOperation,
) -> RsfResult<()> {
    if let Some(routed_pg) = router.route_operation(operation)
        && routed_pg != pg
    {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!(
                "kv batch placement group {} does not match current routed owner {}",
                pg.0, routed_pg.0
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{Epoch, NodeId};
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rejected_committed_batches_preserve_all_values_versions_and_changes() {
        let mut kv = KvEngine::default();
        kv.apply_committed_set(b"existing".to_vec(), b"original".to_vec(), CommitVersion(5));
        for (version, last) in [
            (
                CommitVersion(4),
                KvMutation::Clear {
                    key: b"existing".to_vec(),
                },
            ),
            (
                CommitVersion(5),
                KvMutation::Set {
                    key: b"existing".to_vec(),
                    value: b"changed".to_vec(),
                },
            ),
            (
                CommitVersion(6),
                KvMutation::Clear {
                    key: b"fresh".to_vec(),
                },
            ),
        ] {
            let before_data = kv.data.clone();
            let before_changes = kv.changes.clone();
            assert!(
                kv.apply_committed_batch(
                    version,
                    vec![
                        KvMutation::Set {
                            key: b"fresh".to_vec(),
                            value: b"partial".to_vec()
                        },
                        last,
                    ]
                )
                .is_err()
            );
            assert_eq!(kv.data, before_data, "rejected batch changed MVCC state");
            assert_eq!(
                kv.changes, before_changes,
                "rejected batch published change events"
            );
        }
        let batch = vec![
            KvMutation::Set {
                key: b"fresh".to_vec(),
                value: b"complete".to_vec(),
            },
            KvMutation::Clear {
                key: b"existing".to_vec(),
            },
        ];
        kv.apply_committed_batch(CommitVersion(6), batch.clone())
            .unwrap();
        let before = (kv.data.clone(), kv.changes.clone());
        kv.apply_committed_batch(CommitVersion(6), batch).unwrap();
        assert_eq!(
            (kv.data.clone(), kv.changes.clone()),
            before,
            "exact replay duplicated state"
        );
        assert_eq!(kv.get(b"fresh").unwrap().value, b"complete");
        assert!(kv.get(b"existing").is_none());
    }

    #[test]
    fn committed_batch_preflight_covers_order_tombstones_and_duplicate_replay() {
        for failing_position in 0..=2 {
            for initial_deleted in [false, true] {
                let mut kv = KvEngine::default();
                if initial_deleted {
                    kv.apply_committed_clear(b"conflict".to_vec(), CommitVersion(9));
                } else {
                    kv.apply_committed_set(b"conflict".to_vec(), b"old".to_vec(), CommitVersion(9));
                }
                kv.apply_committed_set(b"cleared".to_vec(), b"keep".to_vec(), CommitVersion(8));
                let before = (kv.data.clone(), kv.changes.clone());
                let mut batch = vec![
                    KvMutation::Clear {
                        key: b"cleared".to_vec(),
                    },
                    KvMutation::Set {
                        key: vec![0, 255],
                        value: b"new".to_vec(),
                    },
                ];
                batch.insert(
                    failing_position,
                    KvMutation::Set {
                        key: b"conflict".to_vec(),
                        value: b"wrong".to_vec(),
                    },
                );
                assert!(kv.apply_committed_batch(CommitVersion(9), batch).is_err());
                assert_eq!((kv.data, kv.changes), before);
            }
        }
        let mut kv = KvEngine::default();
        let set = KvMutation::Set {
            key: vec![0, 255],
            value: b"value".to_vec(),
        };
        let clear = KvMutation::Clear {
            key: b"absent".to_vec(),
        };
        let batch = vec![set.clone(), clear.clone(), set, clear];
        kv.apply_committed_batch(CommitVersion(10), batch.clone())
            .unwrap();
        assert_eq!(kv.version_count(), 2);
        assert_eq!(kv.changes_since(CommitVersion(0)).len(), 2);
        let before = (kv.data.clone(), kv.changes.clone());
        kv.apply_committed_batch(CommitVersion(10), batch).unwrap();
        kv.apply_committed_batch(CommitVersion(11), vec![]).unwrap();
        assert_eq!((kv.data, kv.changes), before);
    }

    #[test]
    fn writes_through_protocol_router() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();

        let version = kv
            .set(
                &mut router,
                PlacementGroupId(1),
                b"user/1".to_vec(),
                b"ada".to_vec(),
            )
            .expect("set");

        assert_eq!(version, CommitVersion(1));
        assert_eq!(kv.get(b"user/1").expect("value").value, b"ada".to_vec());
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn writes_through_vsr_backed_durable_router_and_recovers_certificate() {
        let path = test_path("writes_through_vsr_backed_durable_router");
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");
            let mut kv = KvEngine::default();

            let version = kv
                .set(
                    &mut router,
                    PlacementGroupId(1),
                    b"user/1".to_vec(),
                    b"ada".to_vec(),
                )
                .expect("set");

            assert_eq!(version, CommitVersion(1));
            assert_eq!(router.latest_version(), CommitVersion(1));
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router");

        assert_eq!(recovered.latest_version(), CommitVersion(1));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_kv_recovers_range_snapshot_tombstone_and_change_stream() {
        let router_path = test_path("durable_kv_recovers_router");
        let kv_path = test_path("durable_kv_recovers_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let snapshot_after_first;

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("kv");

            let initial = kv.snapshot(&router);
            let first = kv
                .commit_batch(
                    &mut router,
                    PlacementGroupId(1),
                    KvBatch {
                        client_request: None,
                        snapshot: initial,
                        mutations: vec![KvMutation::Set {
                            key: b"user/1".to_vec(),
                            value: b"ada".to_vec(),
                        }],
                    },
                )
                .expect("first durable batch");
            assert_eq!(first.version, CommitVersion(1));
            assert_eq!(first.evidence.log_index, LogIndex(1));
            assert!(!first.replayed);

            snapshot_after_first = kv.snapshot(&router);
            kv.commit_batch(
                &mut router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: snapshot_after_first,
                    mutations: vec![
                        KvMutation::Set {
                            key: b"user/2".to_vec(),
                            value: b"grace".to_vec(),
                        },
                        KvMutation::Clear {
                            key: b"user/1".to_vec(),
                        },
                    ],
                },
            )
            .expect("atomic set and tombstone batch");

            assert_eq!(kv.visible_key_count(), 1);
            assert!(kv.get(b"user/1").is_none());
            assert_eq!(
                kv.get_at(b"user/1", snapshot_after_first)
                    .expect("snapshot value")
                    .value,
                b"ada".to_vec()
            );
        }

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("recover kv");

            assert_eq!(router.latest_version(), CommitVersion(2));
            let scan = kv.scan_prefix(b"user/");
            assert_eq!(scan.len(), 1);
            assert_eq!(scan[0].0, b"user/2".to_vec());
            assert_eq!(scan[0].1.value, b"grace".to_vec());
            assert_eq!(
                kv.get_at(b"user/1", snapshot_after_first)
                    .expect("recovered snapshot value")
                    .value,
                b"ada".to_vec()
            );
            assert!(kv.get(b"user/1").is_none());
            assert_eq!(kv.visible_key_count(), 1);

            let changes = kv.changes_since(CommitVersion(1));
            assert_eq!(changes.len(), 2);
            assert_eq!(changes[0].key, b"user/2".to_vec());
            assert_eq!(changes[0].value, Some(b"grace".to_vec()));
            assert_eq!(changes[1].key, b"user/1".to_vec());
            assert_eq!(changes[1].value, None);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(kv_path);
    }

    #[test]
    fn durable_kv_idempotent_replay_does_not_append_duplicate_changes() {
        let router_path = test_path("durable_kv_idempotent_router");
        let kv_path = test_path("durable_kv_idempotent_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let request = ClientRequestId::new("kv-client", 44);

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("kv");
            let snapshot = kv.snapshot(&router);
            let batch = KvBatch {
                client_request: Some(request),
                snapshot,
                mutations: vec![KvMutation::Set {
                    key: b"user/44".to_vec(),
                    value: b"hypatia".to_vec(),
                }],
            };

            let first = kv
                .commit_batch(&mut router, PlacementGroupId(1), batch.clone())
                .expect("first durable request");
            let replay = kv
                .commit_batch(&mut router, PlacementGroupId(1), batch)
                .expect("durable request replay");

            assert_eq!(first.version, CommitVersion(1));
            assert_eq!(replay.version, first.version);
            assert!(replay.replayed);
            assert_eq!(kv.changes_since(CommitVersion(0)).len(), 1);
        }

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("recover kv");

            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(kv.changes_since(CommitVersion(0)).len(), 1);
            assert_eq!(
                kv.get(b"user/44").expect("recovered value").value,
                b"hypatia".to_vec()
            );
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(kv_path);
    }

    #[test]
    fn durable_kv_compaction_preserves_snapshots_tombstones_and_later_writes() {
        let router_path = test_path("durable_kv_compaction_router");
        let kv_path = test_path("durable_kv_compaction_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let old_snapshot;

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("kv");

            let initial = kv.snapshot(&router);
            kv.commit_batch(
                &mut router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: initial,
                    mutations: vec![KvMutation::Set {
                        key: b"user/1".to_vec(),
                        value: b"ada".to_vec(),
                    }],
                },
            )
            .expect("first value");
            old_snapshot = kv.snapshot(&router);

            kv.commit_batch(
                &mut router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: old_snapshot,
                    mutations: vec![KvMutation::Set {
                        key: b"user/1".to_vec(),
                        value: b"ada-lovelace".to_vec(),
                    }],
                },
            )
            .expect("obsolete intermediate value");
            let after_update = kv.snapshot(&router);
            kv.commit_batch(
                &mut router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: after_update,
                    mutations: vec![KvMutation::Set {
                        key: b"user/2".to_vec(),
                        value: b"grace".to_vec(),
                    }],
                },
            )
            .expect("second key");
            let before_clear = kv.snapshot(&router);
            kv.commit_batch(
                &mut router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: before_clear,
                    mutations: vec![KvMutation::Clear {
                        key: b"user/1".to_vec(),
                    }],
                },
            )
            .expect("tombstone");

            let output = kv
                .compact(PlacementGroupId(1), CommitVersion(3), &[old_snapshot])
                .expect("compact");
            assert_eq!(output.highest_version, CommitVersion(4));
            assert_eq!(output.retained_versions, 3);
            assert_eq!(output.dropped_versions, 1);
            assert_eq!(output.retained_tombstones, 1);
            assert_eq!(output.snapshot_log_index, LogIndex(9));

            let after_compaction = kv.snapshot(&router);
            kv.commit_batch(
                &mut router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: after_compaction,
                    mutations: vec![KvMutation::Set {
                        key: b"user/3".to_vec(),
                        value: b"katherine".to_vec(),
                    }],
                },
            )
            .expect("post-compaction write");
        }

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("recover kv");

            assert_eq!(router.latest_version(), CommitVersion(5));
            assert_eq!(
                kv.get_at(b"user/1", old_snapshot)
                    .expect("old snapshot survives compaction")
                    .value,
                b"ada".to_vec()
            );
            assert!(kv.get(b"user/1").is_none());
            assert_eq!(
                kv.get(b"user/2").expect("retained live value").value,
                b"grace".to_vec()
            );
            assert_eq!(
                kv.get(b"user/3")
                    .expect("post-compaction write recovered")
                    .value,
                b"katherine".to_vec()
            );
            assert_eq!(kv.scan_prefix(b"user/").len(), 2);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(kv_path);
    }

    #[test]
    fn durable_kv_routed_batch_follows_recovered_placement_policy() {
        let router_path = test_path("durable_kv_routed_router");
        let kv_path = test_path("durable_kv_routed_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let key;
        let routed_pg;

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            router
                .update_placement_map([PlacementGroupId(3), PlacementGroupId(4)])
                .expect("policy placement update");
            let mut kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("kv");
            key = key_for_pg(&router, PlacementGroupId(4));
            routed_pg = router
                .route_operation(&FabricOperation::KvBatch {
                    keys: vec![key.clone()],
                })
                .expect("route");

            let snapshot = kv.snapshot(&router);
            let result = kv
                .commit_batch_routed(
                    &mut router,
                    KvBatch {
                        client_request: None,
                        snapshot,
                        mutations: vec![KvMutation::Set {
                            key: key.clone(),
                            value: b"policy-owned".to_vec(),
                        }],
                    },
                )
                .expect("routed commit");

            assert_eq!(routed_pg, PlacementGroupId(4));
            assert_eq!(result.evidence.placement_group, routed_pg);
            assert_eq!(result.evidence.log_index, LogIndex(1));
        }

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("recover kv");

            assert_eq!(
                router.placement_map().groups,
                vec![PlacementGroupId(3), PlacementGroupId(4)]
            );
            assert_eq!(
                router
                    .recovered_certificates(routed_pg)
                    .expect("routed certificates")
                    .len(),
                1
            );
            assert_eq!(
                kv.get(&key).expect("recovered routed value").value,
                b"policy-owned".to_vec()
            );
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(kv_path);
    }

    #[test]
    fn durable_kv_rejects_explicit_commit_to_stale_owner_pg() {
        let router_path = test_path("durable_kv_rejects_stale_owner_router");
        let kv_path = test_path("durable_kv_rejects_stale_owner_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            router
                .update_placement_map([PlacementGroupId(3), PlacementGroupId(4)])
                .expect("policy placement update");
            let key = key_for_pg(&router, PlacementGroupId(4));
            let mut kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("kv");
            let snapshot = kv.snapshot(&router);

            let err = kv
                .commit_batch(
                    &mut router,
                    PlacementGroupId(3),
                    KvBatch {
                        client_request: None,
                        snapshot,
                        mutations: vec![KvMutation::Set {
                            key: key.clone(),
                            value: b"must-not-apply".to_vec(),
                        }],
                    },
                )
                .expect_err("stale explicit owner must be rejected");

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(err.message.contains("current routed owner 4"));
            assert!(kv.get(&key).is_none());
            assert_eq!(router.latest_version(), CommitVersion(0));
            assert_eq!(
                router
                    .recovered_certificates(PlacementGroupId(3))
                    .expect("pg3 certificates")
                    .len(),
                0
            );
            assert_eq!(
                router
                    .recovered_certificates(PlacementGroupId(4))
                    .expect("pg4 certificates")
                    .len(),
                0
            );
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(kv_path);
    }

    #[test]
    fn idempotent_batch_replay_uses_vsr_request_table_without_new_commit() {
        let path = test_path("idempotent_batch_replay_uses_vsr_request_table");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let mut kv = KvEngine::default();

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let snapshot = kv.snapshot(&router);
            let request = ClientRequestId::new("kv-client", 22);
            let batch = KvBatch {
                client_request: Some(request),
                snapshot,
                mutations: vec![KvMutation::Set {
                    key: b"user/22".to_vec(),
                    value: b"ada".to_vec(),
                }],
            };

            let first = kv
                .commit_batch(&mut router, PlacementGroupId(1), batch.clone())
                .expect("first idempotent batch");
            let replay = kv
                .commit_batch(&mut router, PlacementGroupId(1), batch)
                .expect("replayed idempotent batch");

            assert_eq!(first, CommitVersion(1));
            assert_eq!(replay, first);
            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(kv.changes_since(CommitVersion(0)).len(), 1);
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1), NodeId(0), voters)
                .expect("recover router");

        assert_eq!(recovered.latest_version(), CommitVersion(1));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn snapshot_reads_keep_older_values_visible() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();

        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"ada".to_vec(),
        )
        .expect("first set");
        let snapshot = kv.snapshot(&router);
        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"grace".to_vec(),
        )
        .expect("second set");

        assert_eq!(
            kv.get_at(b"user/1", snapshot)
                .expect("snapshot value")
                .value,
            b"ada".to_vec()
        );
        assert_eq!(
            kv.get(b"user/1").expect("latest value").value,
            b"grace".to_vec()
        );
    }

    #[test]
    fn stale_snapshot_batch_detects_write_conflict() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let stale = kv.snapshot(&router);

        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"ada".to_vec(),
        )
        .expect("first set");
        let err = kv
            .commit_batch(
                &mut router,
                PlacementGroupId(1),
                KvBatch {
                    client_request: None,
                    snapshot: stale,
                    mutations: vec![KvMutation::Set {
                        key: b"user/1".to_vec(),
                        value: b"grace".to_vec(),
                    }],
                },
            )
            .expect_err("stale writer should conflict");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(
            kv.get(b"user/1").expect("latest value").value,
            b"ada".to_vec()
        );
    }

    #[test]
    fn idempotent_batch_replay_returns_original_version_without_reapplying() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let snapshot = kv.snapshot(&router);
        let request = ClientRequestId::new("kv-client", 1);
        let batch = KvBatch {
            client_request: Some(request),
            snapshot,
            mutations: vec![KvMutation::Set {
                key: b"user/1".to_vec(),
                value: b"ada".to_vec(),
            }],
        };

        let first = kv
            .commit_batch(&mut router, PlacementGroupId(1), batch.clone())
            .expect("first idempotent batch");
        let replay = kv
            .commit_batch(&mut router, PlacementGroupId(1), batch)
            .expect("replayed idempotent batch");

        assert_eq!(first, CommitVersion(1));
        assert_eq!(replay, first);
        assert_eq!(router.committed().len(), 1);
        assert_eq!(kv.changes_since(CommitVersion(0)).len(), 1);
        assert_eq!(
            kv.get(b"user/1").expect("latest value").value,
            b"ada".to_vec()
        );
    }

    #[test]
    fn clear_writes_tombstone_visible_to_snapshots() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();

        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"user/1".to_vec(),
            b"ada".to_vec(),
        )
        .expect("set");
        let before_delete = kv.snapshot(&router);
        kv.commit_batch(
            &mut router,
            PlacementGroupId(1),
            KvBatch {
                client_request: None,
                snapshot: before_delete,
                mutations: vec![KvMutation::Clear {
                    key: b"user/1".to_vec(),
                }],
            },
        )
        .expect("clear");

        assert_eq!(
            kv.get_at(b"user/1", before_delete)
                .expect("old snapshot")
                .value,
            b"ada".to_vec()
        );
        assert!(kv.get(b"user/1").is_none());
        assert!(kv.get_at(b"user/1", kv.snapshot(&router)).is_none());
    }

    #[test]
    fn scans_and_change_streams_respect_versions_and_tombstones() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();

        let initial = kv.snapshot(&router);
        kv.commit_batch(
            &mut router,
            PlacementGroupId(1),
            KvBatch {
                client_request: None,
                snapshot: initial,
                mutations: vec![
                    KvMutation::Set {
                        key: b"config/a".to_vec(),
                        value: b"one".to_vec(),
                    },
                    KvMutation::Set {
                        key: b"config/b".to_vec(),
                        value: b"two".to_vec(),
                    },
                ],
            },
        )
        .expect("batch set");
        let after_batch = kv.snapshot(&router);
        kv.set(
            &mut router,
            PlacementGroupId(1),
            b"config/a".to_vec(),
            b"three".to_vec(),
        )
        .expect("update");
        let clear_snapshot = kv.snapshot(&router);
        kv.commit_batch(
            &mut router,
            PlacementGroupId(1),
            KvBatch {
                client_request: None,
                snapshot: clear_snapshot,
                mutations: vec![KvMutation::Clear {
                    key: b"config/b".to_vec(),
                }],
            },
        )
        .expect("clear");

        assert_eq!(kv.scan_prefix_at(b"config/", after_batch).len(), 2);
        assert_eq!(kv.scan_prefix(b"config/").len(), 1);
        assert_eq!(kv.visible_key_count(), 1);

        let changes = kv.changes_since(after_batch.version);
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].value, Some(b"three".to_vec()));
        assert_eq!(changes[1].value, None);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-kv-{name}-{}-{nonce}", std::process::id()))
    }

    fn key_for_pg(router: &impl FabricRouterPort, target: PlacementGroupId) -> Vec<u8> {
        for index in 0..10_000 {
            let key = format!("tenant/routed/{index}").into_bytes();
            let operation = FabricOperation::KvBatch {
                keys: vec![key.clone()],
            };
            if router.route_operation(&operation) == Some(target) {
                return key;
            }
        }
        panic!("no key routed to target placement group {}", target.0);
    }
}
