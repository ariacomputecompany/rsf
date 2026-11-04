use super::*;

/// Trusted finite physical-read policy. Counts include filtered records; bytes
/// include segment framing, not only the returned logical WAL payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentReadLimits {
    pub max_manifest_bytes: usize,
    pub max_records: u64,
    pub max_encoded_bytes: u64,
    pub max_record_bytes: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentReadLimit {
    ManifestBytes,
    Records,
    EncodedBytes,
    RecordBytes,
    CallerBytes,
}
#[derive(Debug)]
pub enum SegmentReadError {
    LimitExceeded {
        kind: SegmentReadLimit,
        cursor: SegmentReadCursor,
    },
    CompactedPrefixUnsupported,
    Failure(RsfError),
}
impl From<RsfError> for SegmentReadError {
    fn from(error: RsfError) -> Self {
        Self::Failure(error)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentScanControl {
    Continue,
    Stop,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentScanOutcome {
    Complete(SegmentReadCursor),
    Stopped(SegmentReadCursor),
}

/// Position after complete checked records. This is accounting, never authority
/// or caller-provided input to resume a scan against a different owner/state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentReadCursor {
    records: u64,
    bytes: u64,
    segment: Option<u64>,
    offset: u64,
}
impl SegmentReadCursor {
    pub fn physical_records(&self) -> u64 {
        self.records
    }
    pub fn encoded_bytes(&self) -> u64 {
        self.bytes
    }
    pub fn segment_id(&self) -> Option<u64> {
        self.segment
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub(super) fn limit(&self, kind: SegmentReadLimit) -> SegmentReadError {
        SegmentReadError::LimitExceeded {
            kind,
            cursor: *self,
        }
    }
    pub(super) fn before_record(
        &self,
        limits: SegmentReadLimits,
        bytes: u64,
    ) -> Result<(), SegmentReadLimit> {
        if self.records >= limits.max_records {
            return Err(SegmentReadLimit::Records);
        }
        if bytes > limits.max_record_bytes {
            return Err(SegmentReadLimit::RecordBytes);
        }
        if self
            .bytes
            .checked_add(bytes)
            .is_none_or(|total| total > limits.max_encoded_bytes)
        {
            return Err(SegmentReadLimit::EncodedBytes);
        }
        Ok(())
    }
    pub(super) fn completed(&mut self, segment: u64, offset: u64, bytes: u64) -> RsfResult<()> {
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| custody_error("physical record count overflow"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| custody_error("physical read byte count overflow"))?;
        self.segment = Some(segment);
        self.offset = offset
            .checked_add(bytes)
            .ok_or_else(|| custody_error("physical segment offset overflow"))?;
        Ok(())
    }
}
impl SegmentReadLimits {
    pub(super) fn validate(self) -> RsfResult<()> {
        if self.max_manifest_bytes == 0
            || self.max_records == 0
            || self.max_encoded_bytes == 0
            || self.max_record_bytes == 0
        {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "bounded storage reads require nonzero finite limits",
            ));
        }
        Ok(())
    }
}

/// Encoded disk size of one WAL payload under the existing physical formats.
pub fn wal_record_disk_bytes(payload_len: u64) -> RsfResult<u64> {
    payload_len
        .checked_add(crate::WAL_HEADER_LEN as u64)
        .and_then(|n| n.checked_add(crate::HEADER_LEN as u64))
        .ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "WAL physical record length overflow",
            )
        })
}

/// Conservative canonical manifest bound for append-only factories. At most
/// one final active segment can be empty. Compacted prefixes are not covered.
pub fn append_only_manifest_byte_bound(max_physical_records: u64) -> RsfResult<usize> {
    let segments = max_physical_records
        .checked_add(1)
        .ok_or_else(|| custody_error("append-only segment bound overflow"))?;
    let header = format!(
        "rsf-segment-manifest {MANIFEST_VERSION}\nactive {}\n",
        u64::MAX
    )
    .len() as u64;
    let entry = format!("segment {} {}\n", u64::MAX, segment_file_name(u64::MAX)).len() as u64;
    segments
        .checked_mul(entry)
        .and_then(|n| n.checked_add(header))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "append-only manifest bound exceeds platform range",
            )
        })
}
