//! Shared physical framing and WAL-chain interpretation for recovery and bounded scans.
//! The caller owns custody and decides whether incomplete tail recovery may repair.
use super::*;

#[derive(Debug)]
pub(crate) enum FrameError<E> {
    Policy(E),
    Failure(RsfError),
}
impl<E> From<RsfError> for FrameError<E> {
    fn from(error: RsfError) -> Self {
        Self::Failure(error)
    }
}

pub(crate) fn next_frame<E>(
    file: &mut File,
    file_len: u64,
    offset: u64,
    before_allocate: impl FnOnce(u64) -> Result<(), E>,
) -> Result<Option<SegmentRecord>, FrameError<E>> {
    let mut header = [0_u8; HEADER_LEN];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => {
            return Err(FrameError::Failure(storage_error(
                StorageIoOperation::Recover,
                error,
            )));
        }
    }
    if header[0..4] != MAGIC {
        return Err(FrameError::Failure(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("invalid segment magic at offset {offset}"),
        )));
    }

    let version = u16::from_le_bytes(header[4..6].try_into().expect("slice length"));
    if version != SEGMENT_RECORD_VERSION {
        return Err(FrameError::Failure(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "unsupported segment record version {version} at offset {offset}; expected {SEGMENT_RECORD_VERSION}"
            ),
        )));
    }
    let checksum_algorithm_id = u16::from_le_bytes(header[6..8].try_into().expect("slice length"));
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
    let expected_checksum = u64::from_le_bytes(header[16..24].try_into().expect("slice length"));
    // A corrupt or torn length header must not allocate untrusted
    // quantities of memory before discovering the missing payload.
    if len
        > file_len
            .saturating_sub(offset)
            .saturating_sub(HEADER_LEN as u64)
    {
        return Ok(None);
    }
    let disk_bytes = (HEADER_LEN as u64).checked_add(len).ok_or_else(|| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "physical record length overflow",
        )
    })?;
    before_allocate(disk_bytes).map_err(FrameError::Policy)?;
    let payload_len = usize::try_from(len).map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("record length too large at offset {offset}"),
        )
    })?;
    #[cfg(test)]
    PAYLOAD_READS.with(|observed| observed.set((observed.get().0 + 1, observed.get().1 + len)));
    let mut payload = vec![0_u8; payload_len];

    match file.read_exact(&mut payload) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(None);
        }
        Err(err) => {
            return Err(FrameError::Failure(storage_error(
                StorageIoOperation::Recover,
                err,
            )));
        }
    }

    let actual_checksum = checksum_algorithm.checksum(&payload);
    if actual_checksum != expected_checksum {
        return Err(FrameError::Failure(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "checksum mismatch at offset {offset} using {:?}",
                checksum_algorithm
            ),
        )));
    }

    Ok(Some(SegmentRecord {
        offset,
        header: SegmentRecordHeader {
            version,
            checksum_algorithm,
            payload_len: len,
            checksum: expected_checksum,
        },
        payload,
    }))
}

pub(crate) struct Decoded {
    pub wal: Option<WalRecord>,
    pub maintenance: Option<MaintenanceRecord<'static>>,
}
pub(crate) fn decode_and_chain(
    record: &SegmentRecord,
    pg_tails: &mut BTreeMap<PlacementGroupId, WalTail>,
) -> RsfResult<Decoded> {
    let payload = &record.payload;
    let offset = record.offset;
    if let Some(record) = decode_maintenance_record(payload)? {
        return Ok(Decoded {
            wal: None,
            maintenance: Some(record),
        });
    } else if let Some(wal_record) = decode_wal_record(payload)? {
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
        return Ok(Decoded {
            wal: Some(wal_record),
            maintenance: None,
        });
    }

    Ok(Decoded {
        wal: None,
        maintenance: None,
    })
}

// Observation at the actual shared parser allocation boundary. No allocation
// replacement, fake I/O or production policy is involved.
#[cfg(test)]
thread_local! {
    static PAYLOAD_READS: std::cell::Cell<(u64,u64)> = const { std::cell::Cell::new((0,0)) };
}
#[cfg(test)]
pub(crate) fn payload_reads() -> (u64, u64) {
    PAYLOAD_READS.with(std::cell::Cell::get)
}
