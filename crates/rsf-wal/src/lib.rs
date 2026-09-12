use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_page::PageId;
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const RELATIONAL_WAL_PG: PlacementGroupId = PlacementGroupId(62);
const RELATIONAL_WAL_MAGIC: &[u8; 7] = b"RSFRWL1";
const RELATIONAL_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalPolicy {
    pub max_batch_bytes: u32,
    pub require_fsync: bool,
}

impl WalPolicy {
    pub fn new(max_batch_bytes: u32, require_fsync: bool) -> Result<Self, String> {
        if max_batch_bytes == 0 {
            return Err("wal max batch bytes must be non-zero".to_string());
        }
        Ok(Self {
            max_batch_bytes,
            require_fsync,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalEntry {
    pub page: PageId,
    pub lsn: CommitVersion,
    pub bytes: u32,
    pub durable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlushRequest {
    pub page: PageId,
    pub page_lsn: CommitVersion,
    pub bytes: u32,
}

impl FlushRequest {
    pub fn new(page: PageId, page_lsn: CommitVersion, bytes: u32) -> Self {
        Self {
            page,
            page_lsn,
            bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalDecision {
    Appended(WalEntry),
    Synced {
        durable_lsn: CommitVersion,
        entries: usize,
        bytes: u32,
        fsync: bool,
    },
    FlushAllowed {
        page: PageId,
        lsn: CommitVersion,
        bytes: u32,
    },
    Rejected {
        page: Option<PageId>,
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct WalGate {
    policy: WalPolicy,
    entries: BTreeMap<PageId, Vec<WalEntry>>,
    durable_lsn: CommitVersion,
    pending_bytes: u32,
    appended: usize,
}

#[derive(Debug)]
pub struct DurableWalGate {
    gate: WalGate,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl WalGate {
    pub fn new(policy: WalPolicy) -> Self {
        Self {
            policy,
            entries: BTreeMap::new(),
            durable_lsn: CommitVersion(0),
            pending_bytes: 0,
            appended: 0,
        }
    }

    pub fn append(&mut self, page: PageId, lsn: CommitVersion, bytes: u32) -> WalDecision {
        if page.table.is_empty() {
            return WalDecision::Rejected {
                page: Some(page),
                reason: "page table must not be empty".to_string(),
            };
        }
        if bytes == 0 {
            return WalDecision::Rejected {
                page: Some(page),
                reason: "wal entry bytes must be non-zero".to_string(),
            };
        }
        if bytes > self.policy.max_batch_bytes {
            return WalDecision::Rejected {
                page: Some(page),
                reason: "wal entry exceeds batch limit".to_string(),
            };
        }
        let previous_lsn = self
            .entries
            .values()
            .flat_map(|entries| entries.iter().map(|entry| entry.lsn))
            .max()
            .unwrap_or(self.durable_lsn);
        if lsn <= previous_lsn {
            return WalDecision::Rejected {
                page: Some(page),
                reason: "wal lsn must advance".to_string(),
            };
        }
        if self.pending_bytes.saturating_add(bytes) > self.policy.max_batch_bytes {
            return WalDecision::Rejected {
                page: Some(page),
                reason: "wal group would exceed batch limit".to_string(),
            };
        }

        let entry = WalEntry {
            page: page.clone(),
            lsn,
            bytes,
            durable: false,
        };
        self.entries.entry(page).or_default().push(entry.clone());
        self.pending_bytes += bytes;
        self.appended += 1;
        WalDecision::Appended(entry)
    }

    pub fn sync(&mut self) -> WalDecision {
        if self.pending_bytes == 0 {
            return WalDecision::Rejected {
                page: None,
                reason: "no wal records pending sync".to_string(),
            };
        }
        let mut entries = 0;
        for entry in self.entries.values_mut().flatten() {
            if !entry.durable {
                entry.durable = true;
                entries += 1;
                self.durable_lsn = self.durable_lsn.max(entry.lsn);
            }
        }
        let bytes = self.pending_bytes;
        self.pending_bytes = 0;
        WalDecision::Synced {
            durable_lsn: self.durable_lsn,
            entries,
            bytes,
            fsync: self.policy.require_fsync,
        }
    }

    pub fn allow_page_flush(&self, request: FlushRequest) -> WalDecision {
        let Some(entries) = self.entries.get(&request.page) else {
            return WalDecision::Rejected {
                page: Some(request.page),
                reason: "page has no wal history".to_string(),
            };
        };
        let latest = entries
            .iter()
            .max_by_key(|entry| entry.lsn)
            .expect("nonempty");
        if latest.lsn != request.page_lsn {
            return WalDecision::Rejected {
                page: Some(request.page),
                reason: format!(
                    "page flush lsn {} does not match latest wal lsn {}",
                    request.page_lsn.0, latest.lsn.0
                ),
            };
        }
        if !latest.durable {
            return WalDecision::Rejected {
                page: Some(request.page),
                reason: "redo not durable for requested page lsn".to_string(),
            };
        }
        if self.durable_lsn < request.page_lsn {
            return WalDecision::Rejected {
                page: Some(request.page),
                reason: "durable wal lsn is behind page lsn".to_string(),
            };
        }
        WalDecision::FlushAllowed {
            page: request.page,
            lsn: request.page_lsn,
            bytes: request.bytes,
        }
    }

    pub fn durable_lsn(&self) -> CommitVersion {
        self.durable_lsn
    }

    pub fn appended_count(&self) -> usize {
        self.appended
    }
}

impl DurableWalGate {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch, policy: WalPolicy) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(RELATIONAL_WAL_PG),
        })?;
        let mut gate = WalGate::new(policy);
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::RelationalWal)
        {
            apply_relational_wal_record(
                &mut gate,
                decode_relational_wal_payload(&record.payload)?,
            )?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            gate,
            store,
            epoch,
            next_index,
        })
    }

    pub fn append(
        &mut self,
        page: PageId,
        lsn: CommitVersion,
        bytes: u32,
    ) -> RsfResult<WalDecision> {
        let mut gate = self.gate.clone();
        let decision = gate.append(page.clone(), lsn, bytes);
        if matches!(decision, WalDecision::Appended(_)) {
            self.append_record(&RelationalWalRecord::Append { page, lsn, bytes })?;
            self.gate = gate;
        }
        Ok(decision)
    }

    pub fn sync(&mut self) -> RsfResult<WalDecision> {
        let mut gate = self.gate.clone();
        let decision = gate.sync();
        if matches!(decision, WalDecision::Synced { .. }) {
            self.append_record(&RelationalWalRecord::Sync)?;
            self.gate = gate;
        }
        Ok(decision)
    }

    pub fn allow_page_flush(&self, request: FlushRequest) -> WalDecision {
        self.gate.allow_page_flush(request)
    }

    pub fn durable_lsn(&self) -> CommitVersion {
        self.gate.durable_lsn()
    }

    pub fn appended_count(&self) -> usize {
        self.gate.appended_count()
    }

    pub fn gate(&self) -> &WalGate {
        &self.gate
    }

    fn append_record(&mut self, record: &RelationalWalRecord) -> RsfResult<()> {
        let payload = encode_relational_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(RELATIONAL_WAL_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: RELATIONAL_WAL_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::RelationalWal,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RelationalWalRecord {
    Append {
        page: PageId,
        lsn: CommitVersion,
        bytes: u32,
    },
    Sync,
}

fn apply_relational_wal_record(gate: &mut WalGate, record: RelationalWalRecord) -> RsfResult<()> {
    match record {
        RelationalWalRecord::Append { page, lsn, bytes } => match gate.append(page, lsn, bytes) {
            WalDecision::Appended(_) => Ok(()),
            WalDecision::Rejected { reason, .. } => Err(corrupt(format!(
                "relational WAL append replay was rejected: {reason}"
            ))),
            other => Err(corrupt(format!(
                "relational WAL append replay returned unexpected decision: {other:?}"
            ))),
        },
        RelationalWalRecord::Sync => match gate.sync() {
            WalDecision::Synced { .. } => Ok(()),
            WalDecision::Rejected { reason, .. } => Err(corrupt(format!(
                "relational WAL sync replay was rejected: {reason}"
            ))),
            other => Err(corrupt(format!(
                "relational WAL sync replay returned unexpected decision: {other:?}"
            ))),
        },
    }
}

fn encode_relational_wal_payload(record: &RelationalWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(RELATIONAL_WAL_MAGIC);
    out.push(RELATIONAL_WAL_VERSION);
    match record {
        RelationalWalRecord::Append { page, lsn, bytes } => {
            out.push(1);
            write_page(&mut out, page)?;
            write_u64(&mut out, lsn.0);
            write_u32(&mut out, *bytes);
        }
        RelationalWalRecord::Sync => out.push(2),
    }
    Ok(out)
}

fn decode_relational_wal_payload(bytes: &[u8]) -> RsfResult<RelationalWalRecord> {
    let mut offset = 0;
    take_magic(bytes, &mut offset)?;
    let version = read_u8(bytes, &mut offset)?;
    if version != RELATIONAL_WAL_VERSION {
        return Err(corrupt("unsupported relational WAL payload version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => RelationalWalRecord::Append {
            page: read_page(bytes, &mut offset)?,
            lsn: CommitVersion(read_u64(bytes, &mut offset)?),
            bytes: read_u32(bytes, &mut offset)?,
        },
        2 => RelationalWalRecord::Sync,
        _ => return Err(corrupt("unknown relational WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("relational WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn write_page(out: &mut Vec<u8>, page: &PageId) -> RsfResult<()> {
    write_string(out, &page.table)?;
    write_u32(out, page.number);
    Ok(())
}

fn read_page(bytes: &[u8], offset: &mut usize) -> RsfResult<PageId> {
    Ok(PageId::new(
        read_string(bytes, offset)?,
        read_u32(bytes, offset)?,
    ))
}

fn take_magic(bytes: &[u8], offset: &mut usize) -> RsfResult<()> {
    let magic = take(bytes, offset, RELATIONAL_WAL_MAGIC.len())?;
    if magic != RELATIONAL_WAL_MAGIC {
        return Err(corrupt("relational WAL payload magic mismatch"));
    }
    Ok(())
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    if value.is_empty() {
        return Err(invalid("relational WAL string must not be empty"));
    }
    let len =
        u16::try_from(value.len()).map_err(|_| invalid("relational WAL string is too long"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u16(bytes, offset)? as usize;
    let value = take(bytes, offset, len)?;
    if value.is_empty() {
        return Err(corrupt("relational WAL string must not be empty"));
    }
    String::from_utf8(value.to_vec()).map_err(|_| corrupt("relational WAL string is not UTF-8"))
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    Ok(take(bytes, offset, 1)?[0])
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> RsfResult<u16> {
    let raw = take(bytes, offset, 2)?;
    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = take(bytes, offset, 4)?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = take(bytes, offset, 8)?;
    Ok(u64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt("relational WAL payload offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("relational WAL payload is truncated"));
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
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
    use rsf_storage::WalAppend;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rejects_flush_until_redo_is_durable() {
        let page = PageId::new("users", 1);
        let mut gate = WalGate::new(WalPolicy::new(1_024, true).unwrap());

        assert!(matches!(
            gate.append(page.clone(), CommitVersion(31), 128),
            WalDecision::Appended(_)
        ));
        assert!(matches!(
            gate.allow_page_flush(FlushRequest::new(page.clone(), CommitVersion(31), 4_224)),
            WalDecision::Rejected { .. }
        ));
        assert!(matches!(gate.sync(), WalDecision::Synced { .. }));
        assert!(matches!(
            gate.allow_page_flush(FlushRequest::new(page, CommitVersion(31), 4_224)),
            WalDecision::FlushAllowed { .. }
        ));
    }

    #[test]
    fn enforces_monotonic_lsn() {
        let page = PageId::new("users", 1);
        let mut gate = WalGate::new(WalPolicy::new(1_024, true).unwrap());

        gate.append(page.clone(), CommitVersion(31), 128);
        assert!(matches!(
            gate.append(page, CommitVersion(31), 128),
            WalDecision::Rejected { .. }
        ));
    }

    #[test]
    fn enforces_group_limit() {
        let page = PageId::new("users", 1);
        let mut gate = WalGate::new(WalPolicy::new(128, true).unwrap());

        assert!(matches!(
            gate.append(page.clone(), CommitVersion(31), 96),
            WalDecision::Appended(_)
        ));
        assert!(matches!(
            gate.append(page, CommitVersion(32), 96),
            WalDecision::Rejected { .. }
        ));
    }

    #[test]
    fn rejects_flush_when_page_lsn_is_not_latest_wal_lsn() {
        let page = PageId::new("users", 1);
        let mut gate = WalGate::new(WalPolicy::new(1_024, true).unwrap());

        assert!(matches!(
            gate.append(page.clone(), CommitVersion(31), 128),
            WalDecision::Appended(_)
        ));
        assert!(matches!(gate.sync(), WalDecision::Synced { .. }));
        assert!(matches!(
            gate.append(page.clone(), CommitVersion(32), 128),
            WalDecision::Appended(_)
        ));
        assert!(matches!(gate.sync(), WalDecision::Synced { .. }));

        let stale =
            gate.allow_page_flush(FlushRequest::new(page.clone(), CommitVersion(31), 4_096));
        let WalDecision::Rejected { reason, .. } = stale else {
            panic!("expected stale page-lsn flush rejection");
        };
        assert!(reason.contains("does not match latest wal lsn"));

        let future =
            gate.allow_page_flush(FlushRequest::new(page.clone(), CommitVersion(33), 4_096));
        let WalDecision::Rejected { reason, .. } = future else {
            panic!("expected future page-lsn flush rejection");
        };
        assert!(reason.contains("does not match latest wal lsn"));

        assert!(matches!(
            gate.allow_page_flush(FlushRequest::new(page, CommitVersion(32), 4_096)),
            WalDecision::FlushAllowed { .. }
        ));
    }

    #[test]
    fn durable_wal_recovers_synced_flush_gate_after_restart() {
        let root = test_root("durable_wal_recovers_synced_flush_gate_after_restart");
        let page = PageId::new("users", 1);
        let policy = WalPolicy::new(1_024, true).unwrap();

        {
            let mut wal = DurableWalGate::open(&root, Epoch(7), policy).expect("open wal");
            assert!(matches!(
                wal.append(page.clone(), CommitVersion(31), 128)
                    .expect("append"),
                WalDecision::Appended(_)
            ));
            assert!(matches!(
                wal.sync().expect("sync"),
                WalDecision::Synced {
                    durable_lsn: CommitVersion(31),
                    entries: 1,
                    ..
                }
            ));
        }

        let wal = DurableWalGate::open(&root, Epoch(7), policy).expect("recover wal");
        assert_eq!(wal.appended_count(), 1);
        assert_eq!(wal.durable_lsn(), CommitVersion(31));
        assert!(matches!(
            wal.allow_page_flush(FlushRequest::new(page, CommitVersion(31), 4_096)),
            WalDecision::FlushAllowed { .. }
        ));
    }

    #[test]
    fn durable_wal_recovers_unsynced_append_as_not_flushable() {
        let root = test_root("durable_wal_recovers_unsynced_append_as_not_flushable");
        let page = PageId::new("users", 1);
        let policy = WalPolicy::new(1_024, true).unwrap();

        {
            let mut wal = DurableWalGate::open(&root, Epoch(7), policy).expect("open wal");
            assert!(matches!(
                wal.append(page.clone(), CommitVersion(31), 128)
                    .expect("append"),
                WalDecision::Appended(_)
            ));
        }

        let wal = DurableWalGate::open(&root, Epoch(7), policy).expect("recover wal");
        let decision = wal.allow_page_flush(FlushRequest::new(page, CommitVersion(31), 4_096));
        let WalDecision::Rejected { reason, .. } = decision else {
            panic!("expected recovered unsynced redo to reject flush");
        };
        assert!(reason.contains("redo not durable"));
    }

    #[test]
    fn durable_wal_does_not_persist_rejected_append() {
        let root = test_root("durable_wal_does_not_persist_rejected_append");
        let page = PageId::new("users", 1);
        let policy = WalPolicy::new(128, true).unwrap();

        {
            let mut wal = DurableWalGate::open(&root, Epoch(7), policy).expect("open wal");
            assert!(matches!(
                wal.append(page.clone(), CommitVersion(31), 512)
                    .expect("append decision"),
                WalDecision::Rejected { .. }
            ));
        }

        let wal = DurableWalGate::open(&root, Epoch(7), policy).expect("recover wal");
        assert_eq!(wal.appended_count(), 0);
        assert!(matches!(
            wal.allow_page_flush(FlushRequest::new(page, CommitVersion(31), 4_096)),
            WalDecision::Rejected { .. }
        ));
    }

    #[test]
    fn durable_wal_recovery_rejects_malformed_payload() {
        let root = test_root("durable_wal_recovery_rejects_malformed_payload");
        let mut store = SegmentStore::open(&root).expect("store");
        store
            .append_wal(WalAppend {
                pg: RELATIONAL_WAL_PG,
                epoch: Epoch(7),
                index: LogIndex(1),
                kind: WalRecordKind::RelationalWal,
                previous_hash: [0; 32],
                payload: b"bad relational wal payload",
            })
            .expect("append corrupt logical payload");

        let err = DurableWalGate::open(&root, Epoch(7), WalPolicy::new(1_024, true).unwrap())
            .expect_err("malformed logical payload must fail closed");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn test_root(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!("rsf-wal-{name}-{nanos}"));
        let _ = fs::remove_dir_all(&path);
        path
    }
}
