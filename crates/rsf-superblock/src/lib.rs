use std::collections::BTreeSet;
use std::path::Path;

use rsf_checkpoint::{CheckpointRoot, CheckpointRootKind};
use rsf_core::{
    ClusterId, CommitVersion, Digest, DigestAlgorithm, Epoch, ErrorCode, LogIndex,
    PlacementGroupId, RsfError, RsfResult, storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const SUPERBLOCK_PG: PlacementGroupId = PlacementGroupId(0);
const SUPERBLOCK_WAL_MAGIC: &[u8; 8] = b"RSFSBLK1";
const SUPERBLOCK_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BootRootKind {
    Catalog,
    TransactionLog,
    Kv,
    Sql,
    Object,
    Blob,
    ChunkIndex,
}

impl BootRootKind {
    pub fn as_str(self) -> &'static str {
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

    pub fn required(self) -> bool {
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

impl From<CheckpointRootKind> for BootRootKind {
    fn from(kind: CheckpointRootKind) -> Self {
        match kind {
            CheckpointRootKind::Catalog => Self::Catalog,
            CheckpointRootKind::TransactionLog => Self::TransactionLog,
            CheckpointRootKind::Kv => Self::Kv,
            CheckpointRootKind::Sql => Self::Sql,
            CheckpointRootKind::Object => Self::Object,
            CheckpointRootKind::Blob => Self::Blob,
            CheckpointRootKind::ChunkIndex => Self::ChunkIndex,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootRoot {
    pub kind: BootRootKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub bytes: u64,
    pub checksum: Digest,
}

impl BootRoot {
    pub fn new(
        kind: BootRootKind,
        namespace: impl Into<String>,
        name: impl Into<String>,
        version: CommitVersion,
        bytes: u64,
        checksum: Digest,
    ) -> Self {
        Self {
            kind,
            namespace: namespace.into(),
            name: name.into(),
            version,
            bytes,
            checksum,
        }
    }

    pub fn from_checkpoint(root: &CheckpointRoot) -> Self {
        Self {
            kind: BootRootKind::from(root.kind),
            namespace: root.namespace.clone(),
            name: root.name.clone(),
            version: root.version,
            bytes: root.bytes,
            checksum: root.checksum,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Superblock {
    pub cluster: ClusterId,
    pub generation: u64,
    pub layout_epoch: Epoch,
    pub checkpoint: CommitVersion,
    pub roots: Vec<BootRoot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuperblockDecision {
    Accepted {
        generation: u64,
        checkpoint: CommitVersion,
        roots: usize,
    },
    RootVerified {
        kind: BootRootKind,
        name: String,
        version: CommitVersion,
    },
    Advanced {
        generation: u64,
        checkpoint: CommitVersion,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct SuperblockStore {
    current: Superblock,
}

#[derive(Debug)]
pub struct DurableSuperblockStore {
    store: SegmentStore,
    current: Superblock,
    epoch: Epoch,
    next_index: u64,
}

impl SuperblockStore {
    pub fn open(superblock: Superblock) -> SuperblockDecision {
        match validate_superblock(&superblock) {
            Ok(()) => SuperblockDecision::Accepted {
                generation: superblock.generation,
                checkpoint: superblock.checkpoint,
                roots: superblock.roots.len(),
            },
            Err(reason) => SuperblockDecision::Rejected { reason },
        }
    }

    pub fn new(superblock: Superblock) -> Result<Self, String> {
        validate_superblock(&superblock)?;
        Ok(Self {
            current: superblock,
        })
    }

    pub fn verify_roots(&self) -> Vec<SuperblockDecision> {
        self.current
            .roots
            .iter()
            .map(|root| SuperblockDecision::RootVerified {
                kind: root.kind,
                name: root.name.clone(),
                version: root.version,
            })
            .collect()
    }

    pub fn advance(&mut self, next: Superblock) -> SuperblockDecision {
        if next.generation <= self.current.generation {
            return SuperblockDecision::Rejected {
                reason: "superblock generation must advance".to_string(),
            };
        }
        if next.layout_epoch.0 < self.current.layout_epoch.0 {
            return SuperblockDecision::Rejected {
                reason: "superblock layout epoch cannot move backward".to_string(),
            };
        }
        if next.checkpoint.0 < self.current.checkpoint.0 {
            return SuperblockDecision::Rejected {
                reason: "superblock checkpoint cannot move backward".to_string(),
            };
        }
        if let Err(reason) = validate_superblock(&next) {
            return SuperblockDecision::Rejected { reason };
        }
        self.current = next;
        SuperblockDecision::Advanced {
            generation: self.current.generation,
            checkpoint: self.current.checkpoint,
        }
    }

    pub fn current(&self) -> &Superblock {
        &self.current
    }
}

impl DurableSuperblockStore {
    pub fn open(path: impl AsRef<Path>, initial: Superblock, epoch: Epoch) -> RsfResult<Self> {
        validate_superblock_result(&initial)?;
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(SUPERBLOCK_PG),
        })?;
        let mut current = initial;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Superblock)
        {
            let next = decode_superblock_wal_payload(&record.payload)?;
            validate_superblock_result(&next)?;
            validate_superblock_advance(&current, &next).map_err(corrupt)?;
            current = next;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            store,
            current,
            epoch,
            next_index,
        })
    }

    pub fn advance(&mut self, next: Superblock) -> SuperblockDecision {
        if let Err(reason) = validate_superblock_advance(&self.current, &next) {
            return SuperblockDecision::Rejected { reason };
        }
        if let Err(reason) = validate_superblock(&next) {
            return SuperblockDecision::Rejected { reason };
        }
        if let Err(err) = self.append_superblock(&next) {
            return SuperblockDecision::Rejected {
                reason: err.message,
            };
        }
        self.current = next;
        SuperblockDecision::Advanced {
            generation: self.current.generation,
            checkpoint: self.current.checkpoint,
        }
    }

    pub fn current(&self) -> &Superblock {
        &self.current
    }

    fn append_superblock(&mut self, superblock: &Superblock) -> RsfResult<()> {
        let payload = encode_superblock_wal_payload(superblock)?;
        let previous_hash = self
            .store
            .recover_current()?
            .recovery
            .pg_tail(SUPERBLOCK_PG)
            .map_or([0; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: SUPERBLOCK_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::Superblock,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn validate_superblock_advance(current: &Superblock, next: &Superblock) -> Result<(), String> {
    if next.cluster != current.cluster {
        return Err("superblock cluster id cannot change".to_string());
    }
    if next.generation <= current.generation {
        return Err("superblock generation must advance".to_string());
    }
    if next.layout_epoch.0 < current.layout_epoch.0 {
        return Err("superblock layout epoch cannot move backward".to_string());
    }
    if next.checkpoint.0 < current.checkpoint.0 {
        return Err("superblock checkpoint cannot move backward".to_string());
    }
    Ok(())
}

fn validate_superblock(superblock: &Superblock) -> Result<(), String> {
    if superblock.cluster.as_str().trim().is_empty() {
        return Err("superblock cluster id must not be empty".to_string());
    }
    if superblock.generation == 0 {
        return Err("superblock generation must be non-zero".to_string());
    }
    if superblock.roots.is_empty() {
        return Err("superblock root inventory must not be empty".to_string());
    }
    let mut seen = BTreeSet::new();
    for root in &superblock.roots {
        if root.namespace.trim().is_empty() {
            return Err("superblock root namespace must not be empty".to_string());
        }
        if root.name.trim().is_empty() {
            return Err("superblock root name must not be empty".to_string());
        }
        if root.bytes == 0 {
            return Err(format!("superblock root {} is empty", root.name));
        }
        if root.version.0 > superblock.checkpoint.0 {
            return Err(format!(
                "superblock root {} is newer than checkpoint",
                root.name
            ));
        }
        if root.checksum.validate().is_err() {
            return Err(format!("superblock root {} has empty checksum", root.name));
        }
        let key = (root.kind, root.namespace.clone(), root.name.clone());
        if !seen.insert(key) {
            return Err(format!(
                "duplicate superblock root {}:{}",
                root.kind.as_str(),
                root.name
            ));
        }
    }
    for kind in [
        BootRootKind::Catalog,
        BootRootKind::TransactionLog,
        BootRootKind::Kv,
        BootRootKind::Sql,
        BootRootKind::Object,
        BootRootKind::Blob,
        BootRootKind::ChunkIndex,
    ] {
        if !superblock.roots.iter().any(|root| root.kind == kind) {
            return Err(format!(
                "superblock missing required {} root",
                kind.as_str()
            ));
        }
    }
    Ok(())
}

fn validate_superblock_result(superblock: &Superblock) -> RsfResult<()> {
    validate_superblock(superblock).map_err(invalid)
}

fn encode_superblock_wal_payload(superblock: &Superblock) -> RsfResult<Vec<u8>> {
    validate_superblock_result(superblock)?;
    let mut out = Vec::new();
    out.extend_from_slice(SUPERBLOCK_WAL_MAGIC);
    out.push(SUPERBLOCK_WAL_VERSION);
    write_string(superblock.cluster.as_str(), &mut out)?;
    write_u64(superblock.generation, &mut out);
    write_u64(superblock.layout_epoch.0, &mut out);
    write_u64(superblock.checkpoint.0, &mut out);
    write_len(superblock.roots.len(), &mut out)?;
    for root in &superblock.roots {
        write_u8(root.kind.to_tag(), &mut out);
        write_string(&root.namespace, &mut out)?;
        write_string(&root.name, &mut out)?;
        write_u64(root.version.0, &mut out);
        write_u64(root.bytes, &mut out);
        write_u8(digest_algorithm_tag(root.checksum.algorithm), &mut out);
        out.extend_from_slice(root.checksum.as_bytes());
    }
    Ok(out)
}

fn decode_superblock_wal_payload(bytes: &[u8]) -> RsfResult<Superblock> {
    if !bytes.starts_with(SUPERBLOCK_WAL_MAGIC) {
        return Err(corrupt("superblock wal record has invalid magic"));
    }
    let mut offset = SUPERBLOCK_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != SUPERBLOCK_WAL_VERSION {
        return Err(corrupt("superblock wal record has unsupported version"));
    }
    let cluster = ClusterId::new(read_string(bytes, &mut offset)?).map_err(|err| {
        corrupt(format!(
            "superblock wal record has invalid cluster id: {}",
            err.message
        ))
    })?;
    let generation = read_u64(bytes, &mut offset)?;
    let layout_epoch = Epoch(read_u64(bytes, &mut offset)?);
    let checkpoint = CommitVersion(read_u64(bytes, &mut offset)?);
    let root_count = read_len(bytes, &mut offset)?;
    let mut roots = Vec::with_capacity(root_count);
    for _ in 0..root_count {
        let kind = BootRootKind::from_tag(read_u8(bytes, &mut offset)?)?;
        let namespace = read_string(bytes, &mut offset)?;
        let name = read_string(bytes, &mut offset)?;
        let version = CommitVersion(read_u64(bytes, &mut offset)?);
        let bytes_len = read_u64(bytes, &mut offset)?;
        let algorithm = digest_algorithm_from_tag(read_u8(bytes, &mut offset)?)?;
        let checksum_bytes = read_digest_bytes(bytes, &mut offset)?;
        let checksum = Digest::new(algorithm, checksum_bytes)
            .map_err(|_| corrupt("superblock root checksum is invalid"))?;
        roots.push(BootRoot::new(
            kind, namespace, name, version, bytes_len, checksum,
        ));
    }
    if offset != bytes.len() {
        return Err(corrupt("superblock wal record has trailing bytes"));
    }
    let superblock = Superblock {
        cluster,
        generation,
        layout_epoch,
        checkpoint,
        roots,
    };
    validate_superblock_result(&superblock)
        .map_err(|err| corrupt(format!("superblock wal record is invalid: {}", err.message)))?;
    Ok(superblock)
}

impl BootRootKind {
    fn to_tag(self) -> u8 {
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
            _ => Err(corrupt("superblock root kind has unknown tag")),
        }
    }
}

fn digest_algorithm_tag(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Blake3 => 1,
        DigestAlgorithm::Sha256 => 2,
    }
}

fn digest_algorithm_from_tag(tag: u8) -> RsfResult<DigestAlgorithm> {
    match tag {
        1 => Ok(DigestAlgorithm::Blake3),
        2 => Ok(DigestAlgorithm::Sha256),
        _ => Err(corrupt("superblock digest algorithm has unknown tag")),
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
        .map_err(|_| invalid("superblock wal collection length does not fit in record"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| invalid("superblock wal string length does not fit in record"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("superblock wal record ended early"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let end = (*offset)
        .checked_add(4)
        .ok_or_else(|| corrupt("superblock wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("superblock wal record ended early"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let end = (*offset)
        .checked_add(8)
        .ok_or_else(|| corrupt("superblock wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("superblock wal record ended early"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    usize::try_from(read_u32(bytes, offset)?)
        .map_err(|_| corrupt("superblock wal collection length does not fit in memory"))
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_len(bytes, offset)?;
    let end = (*offset)
        .checked_add(len)
        .ok_or_else(|| corrupt("superblock wal string length overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("superblock wal record ended early"))?;
    *offset = end;
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("superblock wal string is not utf-8"))
}

fn read_digest_bytes(bytes: &[u8], offset: &mut usize) -> RsfResult<[u8; 32]> {
    let end = (*offset)
        .checked_add(32)
        .ok_or_else(|| corrupt("superblock wal digest offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("superblock wal record ended early"))?;
    *offset = end;
    Ok(raw.try_into().expect("slice length checked"))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

    fn valid_superblock(generation: u64, checkpoint: u64) -> Superblock {
        Superblock {
            cluster: ClusterId::new("rsf-dev").expect("cluster"),
            generation,
            layout_epoch: Epoch(4),
            checkpoint: CommitVersion(checkpoint),
            roots: vec![
                root(BootRootKind::Catalog, "catalog/main", checkpoint, 512, 1),
                root(
                    BootRootKind::TransactionLog,
                    "fabric/wal",
                    checkpoint,
                    2_048,
                    2,
                ),
                root(BootRootKind::Kv, "kv/app/config", checkpoint, 1_024, 3),
                root(BootRootKind::Sql, "sql/app/users", checkpoint, 4_096, 4),
                root(
                    BootRootKind::Object,
                    "object/media/hero.png",
                    checkpoint,
                    3_072,
                    5,
                ),
                root(
                    BootRootKind::Blob,
                    "blob/sha256-content",
                    checkpoint,
                    1_024,
                    6,
                ),
                root(
                    BootRootKind::ChunkIndex,
                    "chunk/index/main",
                    checkpoint,
                    256,
                    7,
                ),
            ],
        }
    }

    #[test]
    fn accepts_valid_boot_record() {
        assert_eq!(
            SuperblockStore::open(valid_superblock(1, 22)),
            SuperblockDecision::Accepted {
                generation: 1,
                checkpoint: CommitVersion(22),
                roots: 7
            }
        );
    }

    #[test]
    fn rejects_missing_each_required_root() {
        for required in required_root_kinds() {
            let mut superblock = valid_superblock(1, 22);
            superblock.roots.retain(|root| root.kind != required);

            assert!(matches!(
                SuperblockStore::open(superblock),
                SuperblockDecision::Rejected { reason } if reason.contains(required.as_str())
            ));
        }
    }

    #[test]
    fn rejects_backward_generation() {
        let mut store = SuperblockStore::new(valid_superblock(2, 22)).unwrap();

        assert!(matches!(
            store.advance(valid_superblock(1, 23)),
            SuperblockDecision::Rejected { .. }
        ));
    }

    #[test]
    fn advances_checkpoint() {
        let mut store = SuperblockStore::new(valid_superblock(1, 22)).unwrap();

        assert_eq!(
            store.advance(valid_superblock(2, 23)),
            SuperblockDecision::Advanced {
                generation: 2,
                checkpoint: CommitVersion(23)
            }
        );
    }

    #[test]
    fn durable_superblock_recovers_latest_accepted_generation_after_restart() {
        let path = test_path("durable_superblock_recovers_latest");
        {
            let mut store = DurableSuperblockStore::open(&path, valid_superblock(1, 22), Epoch(7))
                .expect("durable store");
            assert_eq!(
                store.advance(valid_superblock(2, 23)),
                SuperblockDecision::Advanced {
                    generation: 2,
                    checkpoint: CommitVersion(23)
                }
            );
            assert_eq!(
                store.advance(valid_superblock(3, 25)),
                SuperblockDecision::Advanced {
                    generation: 3,
                    checkpoint: CommitVersion(25)
                }
            );
        }

        {
            let store = DurableSuperblockStore::open(&path, valid_superblock(1, 22), Epoch(7))
                .expect("recover durable store");
            assert_eq!(store.current().generation, 3);
            assert_eq!(store.current().checkpoint, CommitVersion(25));
        }

        let segment = SegmentStore::open(&path).expect("segment store");
        let scan = segment
            .scan(SegmentScanFilter {
                pg: Some(SUPERBLOCK_PG),
            })
            .expect("scan");
        assert_eq!(scan.wal_records.len(), 2);
        assert!(
            scan.wal_records
                .iter()
                .all(|record| record.kind == WalRecordKind::Superblock)
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_superblock_rejected_advance_does_not_persist() {
        let path = test_path("durable_superblock_rejected_advance_does_not_persist");
        let mut store =
            DurableSuperblockStore::open(&path, valid_superblock(2, 22), Epoch(7)).expect("store");

        assert!(matches!(
            store.advance(valid_superblock(1, 23)),
            SuperblockDecision::Rejected { reason } if reason.contains("generation")
        ));
        assert_eq!(store.current().generation, 2);

        let segment = SegmentStore::open(&path).expect("segment store");
        let scan = segment
            .scan(SegmentScanFilter {
                pg: Some(SUPERBLOCK_PG),
            })
            .expect("scan");
        assert!(scan.wal_records.is_empty());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_superblock_recovery_rejects_malformed_payload() {
        let path = test_path("durable_superblock_recovery_rejects_malformed_payload");
        append_raw_superblock_record(&path, b"not-superblock-wal".to_vec(), LogIndex(1), [0; 32]);

        let err = DurableSuperblockStore::open(&path, valid_superblock(1, 22), Epoch(7))
            .expect_err("malformed superblock wal rejects recovery");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("invalid magic"));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_superblock_recovery_rejects_regressing_generation() {
        let path = test_path("durable_superblock_recovery_rejects_regressing_generation");
        let first = encode_superblock_wal_payload(&valid_superblock(2, 23)).expect("encode first");
        let first_hash = append_raw_superblock_record(&path, first, LogIndex(1), [0; 32]);
        let second =
            encode_superblock_wal_payload(&valid_superblock(1, 24)).expect("encode regression");
        append_raw_superblock_record(&path, second, LogIndex(2), first_hash);

        let err = DurableSuperblockStore::open(&path, valid_superblock(1, 22), Epoch(7))
            .expect_err("regressing recovered superblock rejects recovery");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("generation"));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn derives_boot_roots_from_checkpoint_roots() {
        let checkpoint = CheckpointRoot::new(
            CheckpointRootKind::Object,
            "app",
            "object/media/hero.png",
            CommitVersion(22),
            3_072,
            Digest::blake3_bytes(b"object-root"),
        );

        assert_eq!(
            BootRoot::from_checkpoint(&checkpoint),
            BootRoot::new(
                BootRootKind::Object,
                "app",
                "object/media/hero.png",
                CommitVersion(22),
                3_072,
                Digest::blake3_bytes(b"object-root"),
            )
        );
    }

    fn root(kind: BootRootKind, name: &str, version: u64, bytes: u64, checksum: u8) -> BootRoot {
        BootRoot::new(
            kind,
            "app",
            name,
            CommitVersion(version),
            bytes,
            Digest::blake3_bytes(format!("boot:{name}:{checksum}").as_bytes()),
        )
    }

    fn append_raw_superblock_record(
        path: &std::path::Path,
        payload: Vec<u8>,
        index: LogIndex,
        previous_hash: [u8; 32],
    ) -> [u8; 32] {
        let mut store = SegmentStore::open(path).expect("segment store");
        let (_, _, record) = store
            .append_wal(WalAppend {
                pg: SUPERBLOCK_PG,
                epoch: Epoch(7),
                index,
                kind: WalRecordKind::Superblock,
                previous_hash,
                payload: &payload,
            })
            .expect("append raw superblock wal");
        record.record_hash
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-superblock-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn required_root_kinds() -> [BootRootKind; 7] {
        [
            BootRootKind::Catalog,
            BootRootKind::TransactionLog,
            BootRootKind::Kv,
            BootRootKind::Sql,
            BootRootKind::Object,
            BootRootKind::Blob,
            BootRootKind::ChunkIndex,
        ]
    }
}
