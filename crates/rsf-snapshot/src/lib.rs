#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const SNAPSHOT_PG: PlacementGroupId = PlacementGroupId(0);
const SNAPSHOT_WAL_MAGIC: &[u8; 8] = b"RSFSNAP1";
const SNAPSHOT_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SnapshotRootKind {
    Catalog,
    TransactionLog,
    Kv,
    Sql,
    Object,
    Blob,
    ChunkIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRoot {
    pub kind: SnapshotRootKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub barrier: SnapshotBarrierKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRequest {
    pub id: String,
    pub namespace: String,
    pub checkpoint: CommitVersion,
    pub roots: Vec<SnapshotRoot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBarrier {
    pub root: SnapshotRootKind,
    pub namespace: String,
    pub name: String,
    pub kind: SnapshotBarrierKind,
    pub version: CommitVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotBarrierKind {
    CopyOnWrite,
    ImmutablePin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseSnapshot {
    pub id: String,
    pub namespace: String,
    pub checkpoint: CommitVersion,
    pub roots: Vec<SnapshotRoot>,
    pub barriers: Vec<SnapshotBarrier>,
}

#[derive(Clone, Debug, Default)]
pub struct SnapshotCoordinator;

#[derive(Debug)]
pub struct DurableSnapshotCatalog {
    coordinator: SnapshotCoordinator,
    snapshots: BTreeMap<String, DatabaseSnapshot>,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl SnapshotCoordinator {
    pub fn create(&self, request: SnapshotRequest) -> RsfResult<DatabaseSnapshot> {
        validate_request(&request)?;
        let barriers: Vec<_> = request
            .roots
            .iter()
            .map(|root| SnapshotBarrier {
                root: root.kind.clone(),
                namespace: root.namespace.clone(),
                name: root.name.clone(),
                kind: root.barrier,
                version: root.version,
            })
            .collect();
        validate_barriers(&request, &barriers)?;
        Ok(DatabaseSnapshot {
            id: request.id,
            namespace: request.namespace,
            checkpoint: request.checkpoint,
            roots: request.roots,
            barriers,
        })
    }
}

impl DurableSnapshotCatalog {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(SNAPSHOT_PG),
        })?;
        let mut snapshots = BTreeMap::new();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Snapshot)
        {
            let snapshot = decode_snapshot_wal_payload(&record.payload)?;
            validate_snapshot_record(&snapshot)?;
            if snapshots
                .insert(snapshot.id.clone(), snapshot.clone())
                .is_some()
            {
                return Err(corrupt(format!(
                    "snapshot catalog replay found duplicate snapshot id {}",
                    snapshot.id
                )));
            }
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            coordinator: SnapshotCoordinator,
            snapshots,
            store,
            epoch,
            next_index,
        })
    }

    pub fn create(&mut self, request: SnapshotRequest) -> RsfResult<DatabaseSnapshot> {
        let snapshot = self.coordinator.create(request)?;
        if let Some(existing) = self.snapshots.get(&snapshot.id) {
            if existing == &snapshot {
                return Ok(existing.clone());
            }
            return Err(invalid(format!(
                "snapshot id {} already exists with different content",
                snapshot.id
            )));
        }

        self.append_snapshot(&snapshot)?;
        self.snapshots.insert(snapshot.id.clone(), snapshot.clone());
        Ok(snapshot)
    }

    pub fn get(&self, id: &str) -> Option<&DatabaseSnapshot> {
        self.snapshots.get(id)
    }

    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    fn append_snapshot(&mut self, snapshot: &DatabaseSnapshot) -> RsfResult<()> {
        let payload = encode_snapshot_wal_payload(snapshot)?;
        let previous_hash = self
            .store
            .recover_current()?
            .recovery
            .pg_tail(SNAPSHOT_PG)
            .map_or([0; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: SNAPSHOT_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::Snapshot,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

impl SnapshotRootKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::TransactionLog => "transaction-log",
            Self::Kv => "kv",
            Self::Sql => "sql",
            Self::Object => "object",
            Self::Blob => "blob",
            Self::ChunkIndex => "chunk-index",
        }
    }

    pub fn required(&self) -> bool {
        matches!(
            self,
            Self::Catalog
                | Self::TransactionLog
                | Self::Kv
                | Self::Sql
                | Self::Object
                | Self::Blob
                | Self::ChunkIndex
        )
    }
}

impl SnapshotBarrierKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CopyOnWrite => "copy-on-write",
            Self::ImmutablePin => "immutable-pin",
        }
    }
}

impl DatabaseSnapshot {
    pub fn root_count(&self) -> usize {
        self.roots.len()
    }

    pub fn barrier_count(&self) -> usize {
        self.barriers.len()
    }

    pub fn root(&self, kind: SnapshotRootKind) -> Option<&SnapshotRoot> {
        self.roots.iter().find(|root| root.kind == kind)
    }
}

fn validate_request(request: &SnapshotRequest) -> RsfResult<()> {
    if request.id.trim().is_empty() {
        return Err(invalid("snapshot id must not be empty"));
    }
    if request.namespace.trim().is_empty() {
        return Err(invalid("snapshot namespace must not be empty"));
    }
    if request.roots.is_empty() {
        return Err(invalid("snapshot must include at least one root"));
    }

    let mut seen = BTreeSet::new();
    for root in &request.roots {
        validate_root(root, request.checkpoint)?;
        let key = (root.kind.clone(), root.namespace.clone(), root.name.clone());
        if !seen.insert(key) {
            return Err(invalid(format!(
                "duplicate snapshot root {}:{}",
                root.kind.as_str(),
                root.name
            )));
        }
    }

    for required in [
        SnapshotRootKind::Catalog,
        SnapshotRootKind::TransactionLog,
        SnapshotRootKind::Kv,
        SnapshotRootKind::Sql,
        SnapshotRootKind::Object,
        SnapshotRootKind::Blob,
        SnapshotRootKind::ChunkIndex,
    ] {
        if !request.roots.iter().any(|root| root.kind == required) {
            return Err(invalid(format!(
                "snapshot missing required {} root",
                required.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_root(root: &SnapshotRoot, checkpoint: CommitVersion) -> RsfResult<()> {
    if root.namespace.trim().is_empty() {
        return Err(invalid("snapshot root namespace must not be empty"));
    }
    if root.name.trim().is_empty() {
        return Err(invalid("snapshot root name must not be empty"));
    }
    if root.version > checkpoint {
        return Err(invalid(format!(
            "snapshot root {} is newer than checkpoint {}",
            root.name, checkpoint.0
        )));
    }
    match (&root.kind, root.barrier) {
        (SnapshotRootKind::Object | SnapshotRootKind::Blob, SnapshotBarrierKind::CopyOnWrite) => {
            return Err(invalid(format!(
                "snapshot root {} must use immutable pin barrier",
                root.name
            )));
        }
        (
            SnapshotRootKind::Catalog
            | SnapshotRootKind::TransactionLog
            | SnapshotRootKind::Kv
            | SnapshotRootKind::Sql
            | SnapshotRootKind::ChunkIndex,
            SnapshotBarrierKind::ImmutablePin,
        ) => {
            return Err(invalid(format!(
                "snapshot root {} must use copy-on-write barrier",
                root.name
            )));
        }
        _ => {}
    }
    Ok(())
}

fn validate_barriers(request: &SnapshotRequest, barriers: &[SnapshotBarrier]) -> RsfResult<()> {
    if barriers.len() != request.roots.len() {
        return Err(invalid("snapshot barriers must cover every root"));
    }

    let mut seen = BTreeSet::new();
    for barrier in barriers {
        if barrier.namespace.trim().is_empty() {
            return Err(invalid("snapshot barrier namespace must not be empty"));
        }
        if barrier.name.trim().is_empty() {
            return Err(invalid("snapshot barrier name must not be empty"));
        }
        if barrier.version > request.checkpoint {
            return Err(invalid(format!(
                "snapshot barrier {} is newer than checkpoint {}",
                barrier.name, request.checkpoint.0
            )));
        }
        let key = (
            barrier.root.clone(),
            barrier.namespace.clone(),
            barrier.name.clone(),
        );
        if !seen.insert(key.clone()) {
            return Err(invalid(format!(
                "duplicate snapshot barrier {}:{}",
                barrier.root.as_str(),
                barrier.name
            )));
        }
        let Some(root) = request.roots.iter().find(|root| {
            root.kind == barrier.root
                && root.namespace == barrier.namespace
                && root.name == barrier.name
        }) else {
            return Err(invalid(format!(
                "snapshot barrier {} has no matching root",
                barrier.name
            )));
        };
        if barrier.version != root.version {
            return Err(invalid(format!(
                "snapshot barrier {} version does not match root",
                barrier.name
            )));
        }
        if barrier.kind != root.barrier {
            return Err(invalid(format!(
                "snapshot barrier {} kind does not match root",
                barrier.name
            )));
        }
    }
    Ok(())
}

fn validate_snapshot_record(snapshot: &DatabaseSnapshot) -> RsfResult<()> {
    validate_request(&SnapshotRequest {
        id: snapshot.id.clone(),
        namespace: snapshot.namespace.clone(),
        checkpoint: snapshot.checkpoint,
        roots: snapshot.roots.clone(),
    })?;
    validate_barriers(
        &SnapshotRequest {
            id: snapshot.id.clone(),
            namespace: snapshot.namespace.clone(),
            checkpoint: snapshot.checkpoint,
            roots: snapshot.roots.clone(),
        },
        &snapshot.barriers,
    )
}

fn encode_snapshot_wal_payload(snapshot: &DatabaseSnapshot) -> RsfResult<Vec<u8>> {
    validate_snapshot_record(snapshot)?;
    let mut out = Vec::new();
    out.extend_from_slice(SNAPSHOT_WAL_MAGIC);
    out.push(SNAPSHOT_WAL_VERSION);
    write_string(&snapshot.id, &mut out)?;
    write_string(&snapshot.namespace, &mut out)?;
    write_u64(snapshot.checkpoint.0, &mut out);
    write_len(snapshot.roots.len(), &mut out)?;
    for root in &snapshot.roots {
        write_u8(root.kind.to_tag(), &mut out);
        write_string(&root.namespace, &mut out)?;
        write_string(&root.name, &mut out)?;
        write_u64(root.version.0, &mut out);
        write_u8(root.barrier.to_tag(), &mut out);
    }
    write_len(snapshot.barriers.len(), &mut out)?;
    for barrier in &snapshot.barriers {
        write_u8(barrier.root.to_tag(), &mut out);
        write_string(&barrier.namespace, &mut out)?;
        write_string(&barrier.name, &mut out)?;
        write_u8(barrier.kind.to_tag(), &mut out);
        write_u64(barrier.version.0, &mut out);
    }
    Ok(out)
}

fn decode_snapshot_wal_payload(bytes: &[u8]) -> RsfResult<DatabaseSnapshot> {
    if !bytes.starts_with(SNAPSHOT_WAL_MAGIC) {
        return Err(corrupt("snapshot wal record has invalid magic"));
    }
    let mut offset = SNAPSHOT_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != SNAPSHOT_WAL_VERSION {
        return Err(corrupt("snapshot wal record has unsupported version"));
    }
    let id = read_string(bytes, &mut offset)?;
    let namespace = read_string(bytes, &mut offset)?;
    let checkpoint = CommitVersion(read_u64(bytes, &mut offset)?);
    let root_count = read_len(bytes, &mut offset)?;
    let mut roots = Vec::with_capacity(root_count);
    for _ in 0..root_count {
        roots.push(SnapshotRoot {
            kind: SnapshotRootKind::from_tag(read_u8(bytes, &mut offset)?)?,
            namespace: read_string(bytes, &mut offset)?,
            name: read_string(bytes, &mut offset)?,
            version: CommitVersion(read_u64(bytes, &mut offset)?),
            barrier: SnapshotBarrierKind::from_tag(read_u8(bytes, &mut offset)?)?,
        });
    }
    let barrier_count = read_len(bytes, &mut offset)?;
    let mut barriers = Vec::with_capacity(barrier_count);
    for _ in 0..barrier_count {
        barriers.push(SnapshotBarrier {
            root: SnapshotRootKind::from_tag(read_u8(bytes, &mut offset)?)?,
            namespace: read_string(bytes, &mut offset)?,
            name: read_string(bytes, &mut offset)?,
            kind: SnapshotBarrierKind::from_tag(read_u8(bytes, &mut offset)?)?,
            version: CommitVersion(read_u64(bytes, &mut offset)?),
        });
    }
    if offset != bytes.len() {
        return Err(corrupt("snapshot wal record has trailing bytes"));
    }

    let snapshot = DatabaseSnapshot {
        id,
        namespace,
        checkpoint,
        roots,
        barriers,
    };
    validate_snapshot_record(&snapshot)?;
    Ok(snapshot)
}

impl SnapshotRootKind {
    fn to_tag(&self) -> u8 {
        match self {
            Self::Catalog => 1,
            Self::TransactionLog => 2,
            Self::Kv => 3,
            Self::Sql => 4,
            Self::Object => 5,
            Self::Blob => 6,
            Self::ChunkIndex => 7,
        }
    }

    fn from_tag(tag: u8) -> RsfResult<Self> {
        match tag {
            1 => Ok(Self::Catalog),
            2 => Ok(Self::TransactionLog),
            3 => Ok(Self::Kv),
            4 => Ok(Self::Sql),
            5 => Ok(Self::Object),
            6 => Ok(Self::Blob),
            7 => Ok(Self::ChunkIndex),
            _ => Err(corrupt("snapshot root kind has unknown tag")),
        }
    }
}

impl SnapshotBarrierKind {
    fn to_tag(self) -> u8 {
        match self {
            Self::CopyOnWrite => 1,
            Self::ImmutablePin => 2,
        }
    }

    fn from_tag(tag: u8) -> RsfResult<Self> {
        match tag {
            1 => Ok(Self::CopyOnWrite),
            2 => Ok(Self::ImmutablePin),
            _ => Err(corrupt("snapshot barrier kind has unknown tag")),
        }
    }
}

fn write_u8(value: u8, out: &mut Vec<u8>) {
    out.push(value);
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_len(value: usize, out: &mut Vec<u8>) -> RsfResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| invalid("snapshot wal collection length does not fit in record"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| invalid("snapshot wal string length does not fit in record"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("snapshot wal record ended early"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let end = (*offset)
        .checked_add(4)
        .ok_or_else(|| corrupt("snapshot wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("snapshot wal record ended early"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let end = (*offset)
        .checked_add(8)
        .ok_or_else(|| corrupt("snapshot wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("snapshot wal record ended early"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    usize::try_from(read_u32(bytes, offset)?)
        .map_err(|_| corrupt("snapshot wal collection length does not fit in memory"))
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_len(bytes, offset)?;
    let end = (*offset)
        .checked_add(len)
        .ok_or_else(|| corrupt("snapshot wal string length overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("snapshot wal record ended early"))?;
    *offset = end;
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("snapshot wal string is not utf-8"))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_consistent_snapshot_with_copy_on_write_barriers() {
        let snapshot = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots: roots(CommitVersion(20)),
            })
            .expect("snapshot");

        assert_eq!(snapshot.root_count(), 7);
        assert_eq!(snapshot.barrier_count(), 7);
        assert_eq!(
            snapshot.root(SnapshotRootKind::Catalog).unwrap().version,
            CommitVersion(18)
        );
        assert!(snapshot.barriers.iter().any(|barrier| {
            barrier.root == SnapshotRootKind::Object
                && barrier.kind == SnapshotBarrierKind::ImmutablePin
        }));
    }

    #[test]
    fn rejects_snapshot_without_each_required_root() {
        for required in required_root_kinds() {
            let mut roots = roots(CommitVersion(20));
            roots.retain(|root| root.kind != required);

            let err = SnapshotCoordinator
                .create(SnapshotRequest {
                    id: "snapshot-app-0001".to_string(),
                    namespace: "app".to_string(),
                    checkpoint: CommitVersion(20),
                    roots,
                })
                .expect_err("missing required root");

            assert!(err.message.contains(required.as_str()));
        }
    }

    #[test]
    fn rejects_wrong_barrier_kind_for_mutable_root() {
        let mut roots = roots(CommitVersion(20));
        roots
            .iter_mut()
            .find(|root| root.kind == SnapshotRootKind::Kv)
            .unwrap()
            .barrier = SnapshotBarrierKind::ImmutablePin;

        let err = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots,
            })
            .expect_err("wrong mutable barrier");

        assert!(err.message.contains("copy-on-write"));
    }

    #[test]
    fn rejects_future_root_versions() {
        let mut roots = roots(CommitVersion(20));
        roots[0].version = CommitVersion(21);

        let err = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots,
            })
            .expect_err("future root");

        assert!(err.message.contains("newer than checkpoint"));
    }

    #[test]
    fn rejects_duplicate_roots() {
        let mut roots = roots(CommitVersion(20));
        roots.push(roots[0].clone());

        let err = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots,
            })
            .expect_err("duplicate root");

        assert!(err.message.contains("duplicate"));
    }

    #[test]
    fn durable_snapshot_catalog_recovers_snapshot_after_restart() {
        let path = test_path("durable_snapshot_catalog_recovers_snapshot_after_restart");
        {
            let mut catalog = DurableSnapshotCatalog::open(&path, Epoch(1)).expect("catalog");
            let snapshot = catalog
                .create(request("snapshot-app-0001", CommitVersion(20)))
                .expect("create snapshot");

            assert_eq!(snapshot.root_count(), 7);
            assert_eq!(catalog.len(), 1);
        }

        {
            let catalog = DurableSnapshotCatalog::open(&path, Epoch(1)).expect("recover catalog");
            let snapshot = catalog
                .get("snapshot-app-0001")
                .expect("snapshot recovered");

            assert_eq!(snapshot.checkpoint, CommitVersion(20));
            assert_eq!(snapshot.barrier_count(), 7);
            assert_eq!(
                snapshot.root(SnapshotRootKind::Blob).unwrap().barrier,
                SnapshotBarrierKind::ImmutablePin
            );
        }

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_snapshot_catalog_duplicate_create_is_idempotent() {
        let path = test_path("durable_snapshot_catalog_duplicate_create_is_idempotent");
        let mut catalog = DurableSnapshotCatalog::open(&path, Epoch(1)).expect("catalog");

        let first = catalog
            .create(request("snapshot-app-0001", CommitVersion(20)))
            .expect("first create");
        let second = catalog
            .create(request("snapshot-app-0001", CommitVersion(20)))
            .expect("idempotent duplicate create");
        assert_eq!(first, second);

        let store = SegmentStore::open(&path).expect("store");
        let scan = store
            .scan(SegmentScanFilter {
                pg: Some(SNAPSHOT_PG),
            })
            .expect("scan");
        assert_eq!(scan.wal_records.len(), 1);
        assert_eq!(scan.wal_records[0].kind, WalRecordKind::Snapshot);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_snapshot_catalog_rejects_conflicting_duplicate_id() {
        let path = test_path("durable_snapshot_catalog_rejects_conflicting_duplicate_id");
        let mut catalog = DurableSnapshotCatalog::open(&path, Epoch(1)).expect("catalog");
        catalog
            .create(request("snapshot-app-0001", CommitVersion(20)))
            .expect("first create");

        let err = catalog
            .create(request("snapshot-app-0001", CommitVersion(21)))
            .expect_err("conflicting duplicate rejected");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("already exists"));

        let store = SegmentStore::open(&path).expect("store");
        let scan = store
            .scan(SegmentScanFilter {
                pg: Some(SNAPSHOT_PG),
            })
            .expect("scan");
        assert_eq!(scan.wal_records.len(), 1);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_snapshot_catalog_recovery_rejects_malformed_payload() {
        let path = test_path("durable_snapshot_catalog_recovery_rejects_malformed_payload");
        let mut store = SegmentStore::open(&path).expect("store");
        store
            .append_wal(WalAppend {
                pg: SNAPSHOT_PG,
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Snapshot,
                previous_hash: [0; 32],
                payload: b"not-snapshot-wal",
            })
            .expect("append malformed snapshot payload");

        let err = DurableSnapshotCatalog::open(&path, Epoch(1))
            .expect_err("malformed snapshot payload must fail recovery");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("invalid magic"));

        let _ = std::fs::remove_dir_all(path);
    }

    fn roots(checkpoint: CommitVersion) -> Vec<SnapshotRoot> {
        vec![
            root(
                SnapshotRootKind::Catalog,
                "catalog-root",
                CommitVersion(18),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::TransactionLog,
                "txn-log",
                checkpoint,
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::Kv,
                "kv-config",
                CommitVersion(19),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::Sql,
                "sql-users",
                CommitVersion(19),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::Object,
                "object-media",
                CommitVersion(17),
                SnapshotBarrierKind::ImmutablePin,
            ),
            root(
                SnapshotRootKind::Blob,
                "blob-chunks",
                CommitVersion(16),
                SnapshotBarrierKind::ImmutablePin,
            ),
            root(
                SnapshotRootKind::ChunkIndex,
                "chunk-index",
                checkpoint,
                SnapshotBarrierKind::CopyOnWrite,
            ),
        ]
    }

    fn request(id: &str, checkpoint: CommitVersion) -> SnapshotRequest {
        SnapshotRequest {
            id: id.to_string(),
            namespace: "app".to_string(),
            checkpoint,
            roots: roots(checkpoint),
        }
    }

    fn required_root_kinds() -> [SnapshotRootKind; 7] {
        [
            SnapshotRootKind::Catalog,
            SnapshotRootKind::TransactionLog,
            SnapshotRootKind::Kv,
            SnapshotRootKind::Sql,
            SnapshotRootKind::Object,
            SnapshotRootKind::Blob,
            SnapshotRootKind::ChunkIndex,
        ]
    }

    fn root(
        kind: SnapshotRootKind,
        name: &str,
        version: CommitVersion,
        barrier: SnapshotBarrierKind,
    ) -> SnapshotRoot {
        SnapshotRoot {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            version,
            barrier,
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-snapshot-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
