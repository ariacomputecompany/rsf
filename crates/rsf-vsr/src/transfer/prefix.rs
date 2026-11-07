//! Frozen historical data from the actual owner, without current-view authority.
use super::*;
use rsf_storage::{SegmentReadError, SegmentReadLimit, SegmentReadLimits, SegmentScanControl};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VsrPrefixLimits {
    pub max_records: u64,
    /// Sum of canonical VsrHistoryRecord encodings (18-byte header + payload).
    pub max_encoded_bytes: u64,
}
impl VsrPrefixLimits {
    /// Check finite platform and physical framing bounds without opening an owner.
    pub fn validate(self) -> RsfResult<()> {
        if self.max_records == 0 || self.max_encoded_bytes < 18 {
            return Err(invalid_input("prefix limits must be finite and nonzero"));
        }
        usize::try_from(self.max_records)
            .map_err(|_| invalid_input("prefix record limit exceeds platform"))?;
        usize::try_from(self.max_encoded_bytes)
            .map_err(|_| invalid_input("prefix byte limit exceeds platform"))?;
        let overhead = rsf_storage::wal_record_disk_bytes(0)?
            .checked_sub(18)
            .ok_or_else(|| corrupt("storage WAL framing is smaller than history framing"))?;
        self.max_records
            .checked_mul(overhead)
            .and_then(|extra| self.max_encoded_bytes.checked_add(extra))
            .ok_or_else(|| invalid_input("prefix physical byte budget overflow"))?;
        rsf_storage::append_only_manifest_byte_bound(self.max_records)?;
        Ok(())
    }
    pub(crate) fn storage(self) -> RsfResult<SegmentReadLimits> {
        self.validate()?;
        let overhead = rsf_storage::wal_record_disk_bytes(0)?
            .checked_sub(18)
            .ok_or_else(|| corrupt("storage WAL framing is smaller than history framing"))?;
        let encoded = self
            .max_records
            .checked_mul(overhead)
            .and_then(|extra| self.max_encoded_bytes.checked_add(extra))
            .ok_or_else(|| invalid_input("prefix physical byte budget overflow"))?;
        Ok(SegmentReadLimits {
            max_manifest_bytes: rsf_storage::append_only_manifest_byte_bound(self.max_records)?,
            max_records: self.max_records,
            max_encoded_bytes: encoded,
            max_record_bytes: rsf_storage::wal_record_disk_bytes(MAX_HISTORY_RECORD_BYTES as u64)?,
        })
    }
    pub(crate) fn admission(self) -> RsfResult<impl FnMut(u64) -> Result<(), SegmentReadLimit>> {
        self.validate()?;
        let framing = rsf_storage::wal_record_disk_bytes(0)?;
        let mut bytes = 0u64;
        Ok(move |disk_bytes: u64| {
            let normalized = disk_bytes
                .saturating_sub(framing)
                .checked_add(18)
                .ok_or(SegmentReadLimit::CallerBytes)?;
            let next = bytes
                .checked_add(normalized)
                .ok_or(SegmentReadLimit::CallerBytes)?;
            if next > self.max_encoded_bytes {
                return Err(SegmentReadLimit::CallerBytes);
            }
            bytes = next;
            Ok(())
        })
    }
    pub(crate) fn check(self, records: u64, bytes: u64) -> RsfResult<()> {
        self.validate()?;
        if records > self.max_records || bytes > self.max_encoded_bytes {
            return Err(invalid_input(
                "learner prefix exceeds configured record/byte budget",
            ));
        }
        Ok(())
    }
}
#[derive(Debug)]
pub enum VsrPrefixExportError {
    LimitExceeded,
    RequiresInstallationBoundary,
    Failure(RsfError),
}
impl From<RsfError> for VsrPrefixExportError {
    fn from(error: RsfError) -> Self {
        Self::Failure(error)
    }
}
impl From<SegmentReadError> for VsrPrefixExportError {
    fn from(error: SegmentReadError) -> Self {
        match error {
            SegmentReadError::LimitExceeded { .. } => Self::LimitExceeded,
            SegmentReadError::CompactedPrefixUnsupported => Self::RequiresInstallationBoundary,
            SegmentReadError::Failure(error) => Self::Failure(error),
        }
    }
}
impl std::fmt::Display for VsrPrefixExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LimitExceeded => f.write_str("historical prefix exceeds configured limits"),
            Self::RequiresInstallationBoundary => {
                f.write_str("historical prefix needs a supported certified installation boundary")
            }
            Self::Failure(error) => write!(f, "{}", error.message),
        }
    }
}
impl std::error::Error for VsrPrefixExportError {}

#[derive(Debug)]
pub(crate) struct FrozenPrefix {
    descriptor: VsrHistoryDescriptor,
    records: Vec<VsrHistoryRecord>,
    bytes: u64,
}
#[derive(Debug)]
pub struct VsrVoterPrefixExport {
    prefix: FrozenPrefix,
    token: VsrStateToken,
}
#[derive(Debug)]
pub struct VsrLearnerPrefixExport {
    pub(crate) prefix: FrozenPrefix,
    pub(crate) token: VsrLearnerStateToken,
}
macro_rules! prefix_metadata {
    ($owner:ty) => {
        impl $owner {
            pub fn descriptor(&self) -> &VsrHistoryDescriptor {
                &self.prefix.descriptor
            }
            pub fn through(&self) -> LogIndex {
                self.prefix.descriptor.probe.committed_tail
            }
            pub fn encoded_bytes(&self) -> u64 {
                self.prefix.bytes
            }
        }
    };
}
prefix_metadata!(VsrVoterPrefixExport);
prefix_metadata!(VsrLearnerPrefixExport);

impl DurableVsrCell {
    /// Export a genuine earlier normal prefix. The current owner may be fenced;
    /// this method never clears that fence or returns primary/voting authority.
    pub fn export_committed_prefix(
        &self,
        through: LogIndex,
        limits: VsrPrefixLimits,
    ) -> Result<VsrVoterPrefixExport, VsrPrefixExportError> {
        let token = self.recovery_state_token()?;
        let prefix = freeze(self, through, limits)?;
        if token != self.recovery_state_token()? {
            return Err(corrupt("prefix source changed during export").into());
        }
        Ok(VsrVoterPrefixExport { prefix, token })
    }
    pub fn read_committed_prefix_batch(
        &self,
        export: &VsrVoterPrefixExport,
        ordinal: u64,
    ) -> RsfResult<VsrHistoryBatch> {
        if export.token != self.recovery_state_token()? {
            return Err(invalid_input(
                "prefix export belongs to a different or changed voter owner",
            ));
        }
        let batch = export.prefix.batch(ordinal)?;
        if export.token != self.recovery_state_token()? {
            return Err(corrupt("prefix source changed during batch read"));
        }
        Ok(batch)
    }
}
impl FrozenPrefix {
    pub(crate) fn batch(&self, ordinal: u64) -> RsfResult<VsrHistoryBatch> {
        let ordinal_index = usize::try_from(ordinal)
            .map_err(|_| invalid_input("prefix ordinal exceeds platform"))?;
        let records = self
            .records
            .get(ordinal_index..)
            .filter(|records| !records.is_empty())
            .ok_or_else(|| invalid_input("prefix ordinal is outside frozen history"))?;
        VsrHistoryBatch::pack(ordinal, records)
    }
}
pub(crate) fn freeze(
    owner: &DurableVsrCell,
    through: LogIndex,
    limits: VsrPrefixLimits,
) -> Result<FrozenPrefix, VsrPrefixExportError> {
    limits.validate()?;
    owner.ensure_healthy()?;
    if !owner.bootstrap_persisted || through.0 == 0 {
        return Err(invalid_input(
            "prefix export requires persisted bootstrap and a committed boundary",
        )
        .into());
    }
    let prepared = owner
        .cell
        .pending
        .get(&through)
        .filter(|p| p.committed)
        .ok_or_else(|| invalid_input("prefix boundary is not committed by this owner"))?;
    let mut bytes = 18u64
        .checked_add(owner.initial_bootstrap.len() as u64)
        .ok_or_else(|| invalid_input("prefix bootstrap size overflow"))?;
    if bytes > limits.max_encoded_bytes {
        return Err(VsrPrefixExportError::LimitExceeded);
    }
    let original = decode_bootstrap(&owner.initial_bootstrap)?;
    let first = VsrHistoryRecord {
        epoch: original.view,
        kind: WalRecordKind::Membership,
        payload: owner.initial_bootstrap.clone(),
    };
    let mut records = vec![first];
    let mut found = false;
    let mut limited = false;
    let scan = owner.store.scan_owned_prefix(
        &owner.store_custody,
        limits.storage()?,
        limits.admission()?,
        |raw| {
            if raw.pg != owner.cell.pg
                || raw.payload.starts_with(VSR_BOOTSTRAP_MAGIC)
                || migration::is_record(&raw.payload)
            {
                return Ok(SegmentScanControl::Continue);
            }
            let length = 18u64
                .checked_add(raw.payload.len() as u64)
                .ok_or_else(|| invalid_input("prefix record size overflow"))?;
            let next_bytes = bytes
                .checked_add(length)
                .ok_or_else(|| invalid_input("prefix byte count overflow"))?;
            if records.len() as u64 >= limits.max_records || next_bytes > limits.max_encoded_bytes {
                limited = true;
                return Ok(SegmentScanControl::Stop);
            }
            let record = VsrHistoryRecord {
                epoch: raw.epoch,
                kind: raw.kind,
                payload: raw.payload.clone(),
            };
            record.encode()?;
            found = record.kind == WalRecordKind::Commit
                && is_vsr_commit_payload(&record.payload)
                && decode_commit(&record.payload)?.index == through;
            records.push(record);
            bytes = next_bytes;
            Ok(if found {
                SegmentScanControl::Stop
            } else {
                SegmentScanControl::Continue
            })
        },
    );
    if let Err(error) = scan {
        if let SegmentReadError::Failure(failure) = &error {
            // A scan that witnesses physical/certified-record failure cannot
            // later regain authority by restoring a file behind this owner.
            fence(owner, failure)?;
        }
        return Err(error.into());
    }
    if limited {
        return Err(VsrPrefixExportError::LimitExceeded);
    }
    if !found {
        return Err(VsrPrefixExportError::RequiresInstallationBoundary);
    }
    let recovered = installation::recover_records(&records)?;
    let boundary = recovered
        .0
        .pending
        .get(&through)
        .filter(|p| p.committed)
        .ok_or_else(|| corrupt("export cut lacks its committed boundary"))?;
    if recovered.0.committed_tail() != through
        || recovered.2.is_some()
        || recovered.3.is_some()
        || !installation::same_operation(&boundary.prepare, &prepared.prepare)
        || recovered.1.membership_lineage_at(boundary.prepare.view)?
            != owner.history.membership_lineage_at(prepared.prepare.view)?
    {
        return Err(corrupt("export cut differs from the source's certified normal prefix").into());
    }
    let descriptor = VsrHistoryDescriptor {
        probe: installation::probe_for_cell(&recovered.0)?,
        records: records.len() as u64,
        digest: history_digest(&records)?,
    };
    descriptor.encode()?;
    Ok(FrozenPrefix {
        descriptor,
        records,
        bytes,
    })
}

pub(crate) fn fence(owner: &DurableVsrCell, error: &RsfError) -> RsfResult<()> {
    *owner
        .durability_failure
        .lock()
        .map_err(|_| corrupt("VSR durability fence is poisoned"))? = Some(error.message.clone());
    Ok(())
}
