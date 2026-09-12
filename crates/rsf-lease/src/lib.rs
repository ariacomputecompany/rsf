use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const LEASE_PG: PlacementGroupId = PlacementGroupId(43);
const LEASE_WAL_MAGIC: &[u8; 7] = b"RSFLES1";
const LEASE_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LeaseKey {
    pub pg: PlacementGroupId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    pub pg: PlacementGroupId,
    pub primary: NodeId,
    pub epoch: Epoch,
    pub fence: FenceToken,
    pub granted_at: CommitVersion,
    pub expires_at: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FenceToken {
    pub pg: PlacementGroupId,
    pub primary: NodeId,
    pub epoch: Epoch,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FenceDecision {
    Accepted,
    Rejected { reason: String },
}

impl FenceDecision {
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }
}

#[derive(Clone, Debug, Default)]
pub struct LeaseTable {
    leases: BTreeMap<LeaseKey, Lease>,
    next_sequence: u64,
}

impl LeaseTable {
    pub fn acquire(
        &mut self,
        pg: PlacementGroupId,
        primary: NodeId,
        epoch: Epoch,
        now: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<Lease> {
        if ttl_versions == 0 {
            return Err(invalid("lease ttl must be greater than zero"));
        }
        let key = LeaseKey { pg };
        if let Some(current) = self.leases.get(&key) {
            if now <= current.expires_at && epoch < current.epoch {
                return Err(stale(format!(
                    "cannot acquire pg {} at stale epoch {} while epoch {} is active",
                    pg.0, epoch.0, current.epoch.0
                )));
            }
            if now <= current.expires_at && epoch == current.epoch && primary != current.primary {
                return Err(stale(format!(
                    "pg {} already has primary {} for epoch {}",
                    pg.0, current.primary.0, epoch.0
                )));
            }
        }

        self.next_sequence += 1;
        let lease = Lease {
            pg,
            primary,
            epoch,
            fence: FenceToken {
                pg,
                primary,
                epoch,
                sequence: self.next_sequence,
            },
            granted_at: now,
            expires_at: CommitVersion(now.0 + ttl_versions),
        };
        self.leases.insert(key, lease.clone());
        Ok(lease)
    }

    pub fn renew(
        &mut self,
        fence: &FenceToken,
        now: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<Lease> {
        if ttl_versions == 0 {
            return Err(invalid("lease ttl must be greater than zero"));
        }
        let key = LeaseKey { pg: fence.pg };
        let Some(current) = self.leases.get_mut(&key) else {
            return Err(stale(format!("pg {} has no active lease", fence.pg.0)));
        };
        if current.fence != *fence {
            return Err(stale(format!(
                "fence token for pg {} does not match active primary",
                fence.pg.0
            )));
        }
        if now > current.expires_at {
            return Err(stale(format!("lease for pg {} has expired", fence.pg.0)));
        }
        current.expires_at = CommitVersion(now.0 + ttl_versions);
        Ok(current.clone())
    }

    pub fn validate_write(&self, fence: &FenceToken, now: CommitVersion) -> FenceDecision {
        let key = LeaseKey { pg: fence.pg };
        let Some(current) = self.leases.get(&key) else {
            return FenceDecision::Rejected {
                reason: "no lease for placement group".to_string(),
            };
        };
        if current.fence != *fence {
            return FenceDecision::Rejected {
                reason: "stale fence token".to_string(),
            };
        }
        if now > current.expires_at {
            return FenceDecision::Rejected {
                reason: "lease expired".to_string(),
            };
        }
        FenceDecision::Accepted
    }

    pub fn active_count(&self) -> usize {
        self.leases.len()
    }

    fn recover_acquired(&mut self, lease: Lease) {
        self.next_sequence = self.next_sequence.max(lease.fence.sequence);
        self.leases.insert(LeaseKey { pg: lease.pg }, lease);
    }

    fn recover_renewed(&mut self, fence: &FenceToken, expires_at: CommitVersion) -> RsfResult<()> {
        let Some(current) = self.leases.get_mut(&LeaseKey { pg: fence.pg }) else {
            return Err(corrupt(format!(
                "lease WAL renewal for pg {} has no acquired lease",
                fence.pg.0
            )));
        };
        if current.fence != *fence {
            return Err(corrupt(format!(
                "lease WAL renewal for pg {} does not match active fence",
                fence.pg.0
            )));
        }
        if expires_at < current.expires_at {
            return Err(corrupt(format!(
                "lease WAL renewal for pg {} moves expiry backwards",
                fence.pg.0
            )));
        }
        current.expires_at = expires_at;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LeaseWalRecord {
    Acquired(Lease),
    Renewed {
        fence: FenceToken,
        expires_at: CommitVersion,
    },
}

#[derive(Debug)]
pub struct DurableLeaseTable {
    table: LeaseTable,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableLeaseTable {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(LEASE_PG) })?;
        let mut table = LeaseTable::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Lease)
        {
            apply_lease_record(&mut table, decode_lease_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            table,
            store,
            epoch,
            next_index,
        })
    }

    pub fn acquire(
        &mut self,
        pg: PlacementGroupId,
        primary: NodeId,
        epoch: Epoch,
        now: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<Lease> {
        let mut table = self.table.clone();
        let lease = table.acquire(pg, primary, epoch, now, ttl_versions)?;
        self.append_record(&LeaseWalRecord::Acquired(lease.clone()))?;
        self.table = table;
        Ok(lease)
    }

    pub fn renew(
        &mut self,
        fence: &FenceToken,
        now: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<Lease> {
        let mut table = self.table.clone();
        let renewed = table.renew(fence, now, ttl_versions)?;
        self.append_record(&LeaseWalRecord::Renewed {
            fence: fence.clone(),
            expires_at: renewed.expires_at,
        })?;
        self.table = table;
        Ok(renewed)
    }

    pub fn validate_write(&self, fence: &FenceToken, now: CommitVersion) -> FenceDecision {
        self.table.validate_write(fence, now)
    }

    pub fn active_count(&self) -> usize {
        self.table.active_count()
    }

    pub fn table(&self) -> &LeaseTable {
        &self.table
    }

    fn append_record(&mut self, record: &LeaseWalRecord) -> RsfResult<()> {
        let payload = encode_lease_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(LEASE_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: LEASE_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Lease,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn apply_lease_record(table: &mut LeaseTable, record: LeaseWalRecord) -> RsfResult<()> {
    match record {
        LeaseWalRecord::Acquired(lease) => {
            validate_lease(&lease)?;
            table.recover_acquired(lease);
            Ok(())
        }
        LeaseWalRecord::Renewed { fence, expires_at } => {
            validate_fence(&fence)?;
            table.recover_renewed(&fence, expires_at)
        }
    }
}

fn encode_lease_wal_payload(record: &LeaseWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(LEASE_WAL_MAGIC);
    out.push(LEASE_WAL_VERSION);
    match record {
        LeaseWalRecord::Acquired(lease) => {
            out.push(1);
            encode_lease(lease, &mut out)?;
        }
        LeaseWalRecord::Renewed { fence, expires_at } => {
            out.push(2);
            encode_fence(fence, &mut out)?;
            write_u64(expires_at.0, &mut out);
        }
    }
    Ok(out)
}

fn decode_lease_wal_payload(bytes: &[u8]) -> RsfResult<LeaseWalRecord> {
    if bytes.len() < LEASE_WAL_MAGIC.len() + 2 {
        return Err(corrupt("lease WAL payload is too short"));
    }
    if &bytes[..LEASE_WAL_MAGIC.len()] != LEASE_WAL_MAGIC {
        return Err(corrupt("lease WAL payload has invalid magic"));
    }
    let mut offset = LEASE_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != LEASE_WAL_VERSION {
        return Err(corrupt(format!(
            "lease WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => LeaseWalRecord::Acquired(decode_lease(bytes, &mut offset)?),
        2 => LeaseWalRecord::Renewed {
            fence: decode_fence(bytes, &mut offset)?,
            expires_at: CommitVersion(read_u64(bytes, &mut offset)?),
        },
        other => {
            return Err(corrupt(format!(
                "lease WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("lease WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn encode_lease(lease: &Lease, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_lease(lease)?;
    write_u64(lease.pg.0, out);
    write_u64(lease.primary.0, out);
    write_u64(lease.epoch.0, out);
    encode_fence(&lease.fence, out)?;
    write_u64(lease.granted_at.0, out);
    write_u64(lease.expires_at.0, out);
    Ok(())
}

fn decode_lease(bytes: &[u8], offset: &mut usize) -> RsfResult<Lease> {
    let lease = Lease {
        pg: PlacementGroupId(read_u64(bytes, offset)?),
        primary: NodeId(read_u64(bytes, offset)?),
        epoch: Epoch(read_u64(bytes, offset)?),
        fence: decode_fence(bytes, offset)?,
        granted_at: CommitVersion(read_u64(bytes, offset)?),
        expires_at: CommitVersion(read_u64(bytes, offset)?),
    };
    validate_lease(&lease)?;
    Ok(lease)
}

fn encode_fence(fence: &FenceToken, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_fence(fence)?;
    write_u64(fence.pg.0, out);
    write_u64(fence.primary.0, out);
    write_u64(fence.epoch.0, out);
    write_u64(fence.sequence, out);
    Ok(())
}

fn decode_fence(bytes: &[u8], offset: &mut usize) -> RsfResult<FenceToken> {
    let fence = FenceToken {
        pg: PlacementGroupId(read_u64(bytes, offset)?),
        primary: NodeId(read_u64(bytes, offset)?),
        epoch: Epoch(read_u64(bytes, offset)?),
        sequence: read_u64(bytes, offset)?,
    };
    validate_fence(&fence)?;
    Ok(fence)
}

fn validate_lease(lease: &Lease) -> RsfResult<()> {
    validate_fence(&lease.fence)?;
    if lease.pg != lease.fence.pg
        || lease.primary != lease.fence.primary
        || lease.epoch != lease.fence.epoch
    {
        return Err(corrupt("lease WAL payload has mismatched fence identity"));
    }
    if lease.expires_at <= lease.granted_at {
        return Err(corrupt("lease WAL payload has non-positive lease duration"));
    }
    Ok(())
}

fn validate_fence(fence: &FenceToken) -> RsfResult<()> {
    if fence.sequence == 0 {
        return Err(corrupt("lease WAL payload has zero fence sequence"));
    }
    Ok(())
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("lease WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = bytes
        .get(*offset..offset.saturating_add(8))
        .ok_or_else(|| corrupt("lease WAL payload ended while reading u64"))?;
    *offset = offset.saturating_add(8);
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

fn stale(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::StaleEpoch, message.into())
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquired_lease_accepts_matching_fence() {
        let mut leases = LeaseTable::default();
        let lease = leases
            .acquire(
                PlacementGroupId(1),
                NodeId(10),
                Epoch(3),
                CommitVersion(5),
                10,
            )
            .unwrap();

        let decision = leases.validate_write(&lease.fence, CommitVersion(6));

        assert!(decision.is_accepted());
        assert_eq!(leases.active_count(), 1);
    }

    #[test]
    fn newer_epoch_rejects_old_fence() {
        let mut leases = LeaseTable::default();
        let old = leases
            .acquire(
                PlacementGroupId(1),
                NodeId(10),
                Epoch(3),
                CommitVersion(5),
                10,
            )
            .unwrap();
        leases
            .acquire(
                PlacementGroupId(1),
                NodeId(11),
                Epoch(4),
                CommitVersion(6),
                10,
            )
            .unwrap();

        let decision = leases.validate_write(&old.fence, CommitVersion(7));

        assert_eq!(
            decision,
            FenceDecision::Rejected {
                reason: "stale fence token".to_string()
            }
        );
    }

    #[test]
    fn stale_epoch_cannot_replace_active_lease() {
        let mut leases = LeaseTable::default();
        leases
            .acquire(
                PlacementGroupId(1),
                NodeId(10),
                Epoch(3),
                CommitVersion(5),
                10,
            )
            .unwrap();

        let err = leases
            .acquire(
                PlacementGroupId(1),
                NodeId(9),
                Epoch(2),
                CommitVersion(6),
                10,
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::StaleEpoch);
    }

    #[test]
    fn expired_lease_rejects_matching_fence() {
        let mut leases = LeaseTable::default();
        let lease = leases
            .acquire(
                PlacementGroupId(1),
                NodeId(10),
                Epoch(3),
                CommitVersion(5),
                2,
            )
            .unwrap();

        let decision = leases.validate_write(&lease.fence, CommitVersion(8));

        assert_eq!(
            decision,
            FenceDecision::Rejected {
                reason: "lease expired".to_string()
            }
        );
    }

    #[test]
    fn renewal_extends_matching_active_lease() {
        let mut leases = LeaseTable::default();
        let lease = leases
            .acquire(
                PlacementGroupId(1),
                NodeId(10),
                Epoch(3),
                CommitVersion(5),
                2,
            )
            .unwrap();

        let renewed = leases.renew(&lease.fence, CommitVersion(6), 10).unwrap();

        assert_eq!(renewed.expires_at, CommitVersion(16));
        assert!(
            leases
                .validate_write(&lease.fence, CommitVersion(15))
                .is_accepted()
        );
    }

    #[test]
    fn durable_lease_recovers_active_fence_and_renewal() {
        let path = test_path("durable_lease_recovers_active_fence_and_renewal");
        let fence;

        {
            let mut leases = DurableLeaseTable::open(&path, Epoch(7)).expect("open leases");
            let lease = leases
                .acquire(
                    PlacementGroupId(11),
                    NodeId(70),
                    Epoch(3),
                    CommitVersion(100),
                    10,
                )
                .expect("acquire");
            fence = lease.fence.clone();
            leases.renew(&fence, CommitVersion(105), 40).expect("renew");
            assert!(
                leases
                    .validate_write(&fence, CommitVersion(144))
                    .is_accepted()
            );
        }

        let recovered = DurableLeaseTable::open(&path, Epoch(7)).expect("recover leases");

        assert_eq!(recovered.active_count(), 1);
        assert!(
            recovered
                .validate_write(&fence, CommitVersion(144))
                .is_accepted()
        );
        assert_eq!(
            recovered.validate_write(&fence, CommitVersion(146)),
            FenceDecision::Rejected {
                reason: "lease expired".to_string()
            }
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_lease_rejects_stale_epoch_after_recovery() {
        let path = test_path("durable_lease_rejects_stale_epoch_after_recovery");

        {
            let mut leases = DurableLeaseTable::open(&path, Epoch(9)).expect("open leases");
            leases
                .acquire(
                    PlacementGroupId(12),
                    NodeId(90),
                    Epoch(5),
                    CommitVersion(20),
                    100,
                )
                .expect("acquire");
        }

        let mut recovered = DurableLeaseTable::open(&path, Epoch(9)).expect("recover leases");
        let err = recovered
            .acquire(
                PlacementGroupId(12),
                NodeId(89),
                Epoch(4),
                CommitVersion(21),
                100,
            )
            .expect_err("stale epoch rejected");

        assert_eq!(err.code, ErrorCode::StaleEpoch);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_lease_continues_fence_sequence_after_recovery() {
        let path = test_path("durable_lease_continues_fence_sequence_after_recovery");

        {
            let mut leases = DurableLeaseTable::open(&path, Epoch(10)).expect("open leases");
            leases
                .acquire(
                    PlacementGroupId(13),
                    NodeId(100),
                    Epoch(1),
                    CommitVersion(10),
                    5,
                )
                .expect("acquire");
        }

        let mut recovered = DurableLeaseTable::open(&path, Epoch(10)).expect("recover leases");
        let lease = recovered
            .acquire(
                PlacementGroupId(13),
                NodeId(101),
                Epoch(2),
                CommitVersion(16),
                5,
            )
            .expect("new epoch acquire");

        assert_eq!(lease.fence.sequence, 2);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn lease_wal_decode_rejects_trailing_bytes() {
        let lease = Lease {
            pg: PlacementGroupId(1),
            primary: NodeId(10),
            epoch: Epoch(3),
            fence: FenceToken {
                pg: PlacementGroupId(1),
                primary: NodeId(10),
                epoch: Epoch(3),
                sequence: 1,
            },
            granted_at: CommitVersion(5),
            expires_at: CommitVersion(10),
        };
        let mut payload =
            encode_lease_wal_payload(&LeaseWalRecord::Acquired(lease)).expect("encode");
        payload.push(99);

        let err = decode_lease_wal_payload(&payload).expect_err("trailing bytes reject");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-lease-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
