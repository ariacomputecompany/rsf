//! Bounded records for restartable transfer into an explicitly non-voting learner.
use super::*;
use rsf_core::Digest;
mod batch;
pub use batch::{HISTORY_BATCH_TARGET_BYTES, MAX_HISTORY_BATCH_RECORDS, VsrHistoryBatch};

pub const MAX_HISTORY_RECORD_BYTES: usize = MAX_REPLICATED_COMMAND_BYTES + 128 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrHistoryRecord {
    pub epoch: Epoch,
    pub kind: WalRecordKind,
    pub payload: Vec<u8>,
}
impl VsrHistoryRecord {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let kind = match self.kind {
            WalRecordKind::Prepare => 1,
            WalRecordKind::Commit => 2,
            WalRecordKind::Membership => 3,
            WalRecordKind::ViewPromise => 4,
            WalRecordKind::ViewInstallation => 5,
            _ => return Err(invalid_input("unsupported VSR history record kind")),
        };
        if self.payload.len() > MAX_HISTORY_RECORD_BYTES {
            return Err(invalid_input("VSR history record exceeds limit"));
        }
        let mut out = b"RSFH1".to_vec();
        out.push(kind);
        out.extend_from_slice(&self.epoch.0.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 18
            || &bytes[..5] != b"RSFH1"
            || bytes.len() > 18 + MAX_HISTORY_RECORD_BYTES
        {
            return Err(corrupt("invalid VSR history record"));
        }
        let length = u32::from_le_bytes(bytes[14..18].try_into().unwrap()) as usize;
        if bytes.len() != 18 + length {
            return Err(corrupt("VSR history record length mismatch"));
        }
        Ok(Self {
            epoch: Epoch(read_u64(bytes, 6)?),
            kind: match bytes[5] {
                1 => WalRecordKind::Prepare,
                2 => WalRecordKind::Commit,
                3 => WalRecordKind::Membership,
                4 => WalRecordKind::ViewPromise,
                5 => WalRecordKind::ViewInstallation,
                _ => return Err(corrupt("unknown VSR history kind")),
            },
            payload: bytes[18..].to_vec(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrHistoryDescriptor {
    pub probe: VsrPrimaryProbe,
    pub records: u64,
    pub digest: Digest,
}
impl VsrHistoryDescriptor {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if self.records == 0 || self.digest.algorithm != rsf_core::DigestAlgorithm::Blake3 {
            return Err(invalid_input("invalid history descriptor"));
        }
        self.digest.validate()?;
        let mut out = b"RSFD1".to_vec();
        out.extend_from_slice(&self.records.to_le_bytes());
        out.extend_from_slice(self.digest.as_bytes());
        out.extend_from_slice(&self.probe.encode()?);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 45 || &bytes[..5] != b"RSFD1" {
            return Err(corrupt("invalid history descriptor"));
        }
        let value = Self {
            records: read_u64(bytes, 5)?,
            digest: Digest::blake3(bytes[13..45].try_into().unwrap())?,
            probe: VsrPrimaryProbe::decode(&bytes[45..])?,
        };
        value.encode()?;
        Ok(value)
    }
    pub fn id(&self) -> RsfResult<[u8; 32]> {
        Ok(Digest::blake3_bytes(&self.encode()?).bytes)
    }
}

#[derive(Debug)]
pub struct VsrHistorySnapshot {
    pub descriptor: VsrHistoryDescriptor,
    pub records: Vec<VsrHistoryRecord>,
}
pub(super) fn history_digest(records: &[VsrHistoryRecord]) -> RsfResult<Digest> {
    let mut digest = Digest::blake3_bytes(b"RSF history chain v1");
    for record in records {
        digest = extend_digest(digest, record)?;
    }
    Ok(digest)
}
pub(super) fn extend_digest(previous: Digest, record: &VsrHistoryRecord) -> RsfResult<Digest> {
    let record = Digest::blake3_bytes(&record.encode()?);
    let mut pair = [0; 64];
    pair[..32].copy_from_slice(previous.as_bytes());
    pair[32..].copy_from_slice(record.as_bytes());
    Ok(Digest::blake3_bytes(&pair))
}
impl DurableVsrCell {
    /// The caller retains this cell's writer ownership during transfer. This
    /// preserves prepare/commit/configuration/client-table ordering, including
    /// uncommitted prepares without treating them as committed application data.
    pub fn history_snapshot(&mut self) -> RsfResult<VsrHistorySnapshot> {
        self.ensure_active_view()?;
        self.history_snapshot_for_recovery()
    }

    pub(super) fn history_snapshot_for_recovery(&mut self) -> RsfResult<VsrHistorySnapshot> {
        self.ensure_healthy()?;
        if !self.bootstrap_persisted {
            return Err(invalid_input(
                "migrate legacy bootstrap before exporting history",
            ));
        }
        let original = decode_bootstrap(&self.initial_bootstrap)?;
        let mut records = vec![VsrHistoryRecord {
            epoch: original.view,
            kind: WalRecordKind::Membership,
            payload: self.initial_bootstrap.clone(),
        }];
        for record in self.store.recover_current()?.recovery.wal_records {
            if record.pg != self.cell.pg
                || record.payload.starts_with(VSR_BOOTSTRAP_MAGIC)
                || migration::is_record(&record.payload)
            {
                continue;
            }
            let record = VsrHistoryRecord {
                epoch: record.epoch,
                kind: record.kind,
                payload: record.payload.to_vec(),
            };
            record.encode()?;
            records.push(record);
        }
        let descriptor = VsrHistoryDescriptor {
            probe: self.recovery_probe()?,
            records: records.len() as u64,
            digest: history_digest(&records)?,
        };
        Ok(VsrHistorySnapshot {
            descriptor,
            records,
        })
    }
}
