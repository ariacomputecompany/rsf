use std::path::Path;

use rsf_core::{
    Bytes, CommitVersion, Digest, DigestAlgorithm, Epoch, ErrorCode, LogIndex, PlacementGroupId,
    RsfError, RsfResult, storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{
    KeyRange, RangeDescriptor, RangeId, RangeLayout, RangeOwnershipTransfer, SstableManifest,
};

const RANGE_PG: PlacementGroupId = PlacementGroupId(62);
const RANGE_WAL_MAGIC: &[u8; 7] = b"RSFRNG1";
const RANGE_WAL_VERSION: u8 = 1;

#[derive(Debug)]
pub struct DurableRangeLayout {
    layout: RangeLayout,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableRangeLayout {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(RANGE_PG) })?;
        let mut layout = RangeLayout::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Range)
        {
            apply_range_record(&mut layout, decode_range_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            layout,
            store,
            epoch,
            next_index,
        })
    }

    pub fn create_range(
        &mut self,
        span: KeyRange,
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<RangeDescriptor> {
        let mut candidate = self.layout.clone();
        let descriptor = candidate.create_range(span, pg, version)?;
        self.append_record(&RangeWalRecord::Created {
            descriptor: descriptor.clone(),
        })?;
        self.layout = candidate;
        Ok(descriptor)
    }

    pub fn split(
        &mut self,
        range: &RangeId,
        split_key: impl Into<Bytes>,
        left_pg: PlacementGroupId,
        right_pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<(RangeDescriptor, RangeDescriptor)> {
        let split_key = split_key.into();
        let mut candidate = self.layout.clone();
        let (left, right) =
            candidate.split(range, split_key.clone(), left_pg, right_pg, version)?;
        self.append_record(&RangeWalRecord::Split {
            range: range.clone(),
            split_key,
            left_pg,
            right_pg,
            version,
            left: left.clone(),
            right: right.clone(),
        })?;
        self.layout = candidate;
        Ok((left, right))
    }

    pub fn transfer_ownership(
        &mut self,
        range: &RangeId,
        expected_version: CommitVersion,
        expected_owner_epoch: Epoch,
        new_pg: PlacementGroupId,
        new_owner_epoch: Epoch,
        version: CommitVersion,
    ) -> RsfResult<RangeOwnershipTransfer> {
        let mut candidate = self.layout.clone();
        let transfer = candidate.transfer_ownership(
            range,
            expected_version,
            expected_owner_epoch,
            new_pg,
            new_owner_epoch,
            version,
        )?;
        self.append_record(&RangeWalRecord::Transferred {
            transfer: transfer.clone(),
        })?;
        self.layout = candidate;
        Ok(transfer)
    }

    pub fn add_table(&mut self, table: SstableManifest) -> RsfResult<()> {
        let mut candidate = self.layout.clone();
        candidate.add_table(table.clone())?;
        self.append_record(&RangeWalRecord::TableAdded { table })?;
        self.layout = candidate;
        Ok(())
    }

    pub fn layout(&self) -> &RangeLayout {
        &self.layout
    }

    pub fn descriptor(&self, range: &RangeId) -> Option<&RangeDescriptor> {
        self.layout.descriptor(range)
    }

    pub fn range_for_key(&self, key: &[u8]) -> Option<&RangeDescriptor> {
        self.layout.range_for_key(key)
    }

    pub fn range_count(&self) -> usize {
        self.layout.range_count()
    }

    pub fn table_count(&self) -> usize {
        self.layout.table_count()
    }

    pub fn non_overlapping(&self) -> bool {
        self.layout.non_overlapping()
    }

    fn append_record(&mut self, record: &RangeWalRecord) -> RsfResult<()> {
        let payload = encode_range_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let previous_hash = recovery
            .pg_tail(RANGE_PG)
            .map_or([0_u8; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: RANGE_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::Range,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RangeWalRecord {
    Created {
        descriptor: RangeDescriptor,
    },
    Split {
        range: RangeId,
        split_key: Bytes,
        left_pg: PlacementGroupId,
        right_pg: PlacementGroupId,
        version: CommitVersion,
        left: RangeDescriptor,
        right: RangeDescriptor,
    },
    Transferred {
        transfer: RangeOwnershipTransfer,
    },
    TableAdded {
        table: SstableManifest,
    },
}

fn apply_range_record(layout: &mut RangeLayout, record: RangeWalRecord) -> RsfResult<()> {
    match record {
        RangeWalRecord::Created { descriptor } => {
            let created =
                layout.create_range(descriptor.span.clone(), descriptor.pg, descriptor.version)?;
            if created != descriptor {
                return Err(corrupt("range replay created unexpected descriptor"));
            }
        }
        RangeWalRecord::Split {
            range,
            split_key,
            left_pg,
            right_pg,
            version,
            left,
            right,
        } => {
            let produced = layout.split(&range, split_key, left_pg, right_pg, version)?;
            if produced != (left, right) {
                return Err(corrupt(
                    "range replay split produced unexpected descriptors",
                ));
            }
        }
        RangeWalRecord::Transferred { transfer } => {
            let produced = layout.transfer_ownership(
                &transfer.range,
                transfer.previous_version,
                transfer.previous_owner_epoch,
                transfer.new_pg,
                transfer.new_owner_epoch,
                transfer.version,
            )?;
            if produced != transfer {
                return Err(corrupt(
                    "range replay produced unexpected ownership transfer",
                ));
            }
        }
        RangeWalRecord::TableAdded { table } => layout.add_table(table)?,
    }
    Ok(())
}

fn encode_range_wal_payload(record: &RangeWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(RANGE_WAL_MAGIC);
    out.push(RANGE_WAL_VERSION);
    match record {
        RangeWalRecord::Created { descriptor } => {
            out.push(1);
            encode_descriptor(&mut out, descriptor)?;
        }
        RangeWalRecord::Split {
            range,
            split_key,
            left_pg,
            right_pg,
            version,
            left,
            right,
        } => {
            out.push(2);
            write_u64(&mut out, range.0);
            write_bytes(&mut out, split_key)?;
            write_u64(&mut out, left_pg.0);
            write_u64(&mut out, right_pg.0);
            write_u64(&mut out, version.0);
            encode_descriptor(&mut out, left)?;
            encode_descriptor(&mut out, right)?;
        }
        RangeWalRecord::Transferred { transfer } => {
            out.push(3);
            encode_transfer(&mut out, transfer);
        }
        RangeWalRecord::TableAdded { table } => {
            out.push(4);
            encode_table(&mut out, table)?;
        }
    }
    Ok(out)
}

fn decode_range_wal_payload(bytes: &[u8]) -> RsfResult<RangeWalRecord> {
    if bytes.len() < RANGE_WAL_MAGIC.len() + 2 {
        return Err(corrupt("range WAL payload is too short"));
    }
    if &bytes[..RANGE_WAL_MAGIC.len()] != RANGE_WAL_MAGIC {
        return Err(corrupt("range WAL magic mismatch"));
    }
    let mut offset = RANGE_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != RANGE_WAL_VERSION {
        return Err(corrupt("unsupported range WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => RangeWalRecord::Created {
            descriptor: decode_descriptor(bytes, &mut offset)?,
        },
        2 => RangeWalRecord::Split {
            range: RangeId(read_u64(bytes, &mut offset)?),
            split_key: read_bytes(bytes, &mut offset)?,
            left_pg: PlacementGroupId(read_u64(bytes, &mut offset)?),
            right_pg: PlacementGroupId(read_u64(bytes, &mut offset)?),
            version: CommitVersion(read_u64(bytes, &mut offset)?),
            left: decode_descriptor(bytes, &mut offset)?,
            right: decode_descriptor(bytes, &mut offset)?,
        },
        3 => RangeWalRecord::Transferred {
            transfer: decode_transfer(bytes, &mut offset)?,
        },
        4 => RangeWalRecord::TableAdded {
            table: decode_table(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("unknown range WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in range WAL payload"));
    }
    Ok(record)
}

fn encode_descriptor(out: &mut Vec<u8>, descriptor: &RangeDescriptor) -> RsfResult<()> {
    write_u64(out, descriptor.id.0);
    write_bytes(out, &descriptor.span.start)?;
    write_bytes(out, &descriptor.span.end)?;
    write_u64(out, descriptor.pg.0);
    write_u64(out, descriptor.owner_epoch.0);
    write_u64(out, descriptor.version.0);
    Ok(())
}

fn decode_descriptor(bytes: &[u8], offset: &mut usize) -> RsfResult<RangeDescriptor> {
    Ok(RangeDescriptor {
        id: RangeId(read_u64(bytes, offset)?),
        span: KeyRange::new(read_bytes(bytes, offset)?, read_bytes(bytes, offset)?)?,
        pg: PlacementGroupId(read_u64(bytes, offset)?),
        owner_epoch: Epoch(read_u64(bytes, offset)?),
        version: CommitVersion(read_u64(bytes, offset)?),
    })
}

fn encode_transfer(out: &mut Vec<u8>, transfer: &RangeOwnershipTransfer) {
    write_u64(out, transfer.range.0);
    write_u64(out, transfer.previous_pg.0);
    write_u64(out, transfer.new_pg.0);
    write_u64(out, transfer.previous_owner_epoch.0);
    write_u64(out, transfer.new_owner_epoch.0);
    write_u64(out, transfer.previous_version.0);
    write_u64(out, transfer.version.0);
}

fn decode_transfer(bytes: &[u8], offset: &mut usize) -> RsfResult<RangeOwnershipTransfer> {
    Ok(RangeOwnershipTransfer {
        range: RangeId(read_u64(bytes, offset)?),
        previous_pg: PlacementGroupId(read_u64(bytes, offset)?),
        new_pg: PlacementGroupId(read_u64(bytes, offset)?),
        previous_owner_epoch: Epoch(read_u64(bytes, offset)?),
        new_owner_epoch: Epoch(read_u64(bytes, offset)?),
        previous_version: CommitVersion(read_u64(bytes, offset)?),
        version: CommitVersion(read_u64(bytes, offset)?),
    })
}

fn encode_table(out: &mut Vec<u8>, table: &SstableManifest) -> RsfResult<()> {
    write_u64(out, table.range.0);
    out.push(table.level);
    write_bytes(out, &table.first_key)?;
    write_bytes(out, &table.last_key)?;
    write_u64(out, table.version.0);
    write_u64(out, table.bytes);
    encode_digest(out, table.checksum);
    Ok(())
}

fn decode_table(bytes: &[u8], offset: &mut usize) -> RsfResult<SstableManifest> {
    let table = SstableManifest {
        range: RangeId(read_u64(bytes, offset)?),
        level: read_u8(bytes, offset)?,
        first_key: read_bytes(bytes, offset)?,
        last_key: read_bytes(bytes, offset)?,
        version: CommitVersion(read_u64(bytes, offset)?),
        bytes: read_u64(bytes, offset)?,
        checksum: decode_digest(bytes, offset)?,
    };
    table.checksum.validate()?;
    Ok(table)
}

fn encode_digest(out: &mut Vec<u8>, digest: Digest) {
    out.push(match digest.algorithm {
        DigestAlgorithm::Blake3 => 1,
        DigestAlgorithm::Sha256 => 2,
    });
    out.extend_from_slice(&digest.bytes);
}

fn decode_digest(bytes: &[u8], offset: &mut usize) -> RsfResult<Digest> {
    let algorithm = match read_u8(bytes, offset)? {
        1 => DigestAlgorithm::Blake3,
        2 => DigestAlgorithm::Sha256,
        _ => return Err(corrupt("unknown range digest algorithm tag")),
    };
    let end = offset.saturating_add(32);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of range digest"))?;
    *offset = end;
    Digest::new(
        algorithm,
        slice.try_into().expect("slice length already checked"),
    )
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("unexpected end of range WAL payload"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let end = offset.saturating_add(4);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of range WAL payload"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("slice length already checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let end = offset.saturating_add(8);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of range WAL payload"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("slice length already checked"),
    ))
}

fn write_len(out: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| invalid("range WAL byte length exceeds encoding limit"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    usize::try_from(read_u32(bytes, offset)?)
        .map_err(|_| corrupt("range WAL byte length is unsupported"))
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> RsfResult<()> {
    write_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> RsfResult<Bytes> {
    let len = read_len(bytes, offset)?;
    let end = offset.saturating_add(len);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of range WAL bytes"))?;
    *offset = end;
    Ok(slice.to_vec())
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
    use rsf_storage::SegmentStore;

    #[test]
    fn durable_range_layout_recovers_range_split_transfer_and_tables() {
        let path = test_path("recovers_range_split_transfer_and_tables");
        let right_id;
        {
            let mut layout = DurableRangeLayout::open(&path, Epoch(9)).expect("layout");
            let root = layout
                .create_range(
                    KeyRange::new(b"a".to_vec(), b"z".to_vec()).unwrap(),
                    PlacementGroupId(1),
                    CommitVersion(1),
                )
                .expect("create");
            let (left, right) = layout
                .split(
                    &root.id,
                    b"m".to_vec(),
                    PlacementGroupId(1),
                    PlacementGroupId(2),
                    CommitVersion(2),
                )
                .expect("split");
            right_id = right.id.clone();
            layout
                .transfer_ownership(
                    &right.id,
                    CommitVersion(2),
                    Epoch(2),
                    PlacementGroupId(3),
                    Epoch(3),
                    CommitVersion(3),
                )
                .expect("transfer");
            layout
                .add_table(table(left.id, b"a", b"l", 2, 100))
                .expect("left table");
            layout
                .add_table(table(right.id, b"m", b"y", 3, 140))
                .expect("right table");
        }

        let reopened = DurableRangeLayout::open(&path, Epoch(9)).expect("reopen");

        assert_eq!(reopened.range_count(), 2);
        assert_eq!(reopened.table_count(), 2);
        assert!(reopened.non_overlapping());
        let moved = reopened.descriptor(&right_id).expect("right descriptor");
        assert_eq!(moved.pg, PlacementGroupId(3));
        assert_eq!(moved.owner_epoch, Epoch(3));
        assert_eq!(moved.version, CommitVersion(3));
        assert_eq!(reopened.range_for_key(b"m").unwrap().id, right_id);
    }

    #[test]
    fn durable_range_layout_rejected_split_does_not_persist() {
        let path = test_path("rejected_split_does_not_persist");
        let root_id;
        {
            let mut layout = DurableRangeLayout::open(&path, Epoch(9)).expect("layout");
            let root = layout
                .create_range(
                    KeyRange::new(b"a".to_vec(), b"z".to_vec()).unwrap(),
                    PlacementGroupId(1),
                    CommitVersion(1),
                )
                .expect("create");
            root_id = root.id.clone();
            let err = layout
                .split(
                    &root.id,
                    b"a".to_vec(),
                    PlacementGroupId(1),
                    PlacementGroupId(2),
                    CommitVersion(2),
                )
                .expect_err("boundary split rejected");
            assert_eq!(err.code, ErrorCode::InvalidInput);
        }

        let reopened = DurableRangeLayout::open(&path, Epoch(9)).expect("reopen");

        assert_eq!(reopened.range_count(), 1);
        assert_eq!(
            reopened.descriptor(&root_id).unwrap().pg,
            PlacementGroupId(1)
        );
    }

    #[test]
    fn durable_range_layout_replay_rejects_malformed_payload() {
        let path = test_path("replay_rejects_malformed_payload");
        append_raw_range_record(&path, b"not-a-range-payload".to_vec(), 1, [0; 32]);

        let err = DurableRangeLayout::open(&path, Epoch(9))
            .expect_err("malformed range payload rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("magic"));
    }

    #[test]
    fn durable_range_layout_replay_rejects_split_without_source_range() {
        let path = test_path("replay_rejects_split_without_source_range");
        let left = descriptor(
            RangeId(2),
            b"a",
            b"m",
            PlacementGroupId(1),
            CommitVersion(2),
        );
        let right = descriptor(
            RangeId(3),
            b"m",
            b"z",
            PlacementGroupId(2),
            CommitVersion(2),
        );
        let payload = encode_range_wal_payload(&RangeWalRecord::Split {
            range: RangeId(1),
            split_key: b"m".to_vec(),
            left_pg: PlacementGroupId(1),
            right_pg: PlacementGroupId(2),
            version: CommitVersion(2),
            left,
            right,
        })
        .expect("encode");
        append_raw_range_record(&path, payload, 1, [0; 32]);

        let err = DurableRangeLayout::open(&path, Epoch(9))
            .expect_err("orphan split rejected during replay");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("unknown range"));
    }

    fn table(
        range: RangeId,
        first: &[u8],
        last: &[u8],
        version: u64,
        bytes: u64,
    ) -> SstableManifest {
        SstableManifest {
            range,
            level: 0,
            first_key: first.to_vec(),
            last_key: last.to_vec(),
            version: CommitVersion(version),
            bytes,
            checksum: Digest::blake3_bytes(
                format!(
                    "sstable:{}:{}:{}",
                    String::from_utf8_lossy(first),
                    String::from_utf8_lossy(last),
                    version
                )
                .as_bytes(),
            ),
        }
    }

    fn descriptor(
        id: RangeId,
        start: &[u8],
        end: &[u8],
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RangeDescriptor {
        RangeDescriptor {
            id,
            span: KeyRange::new(start.to_vec(), end.to_vec()).expect("range"),
            pg,
            owner_epoch: Epoch(version.0),
            version,
        }
    }

    fn append_raw_range_record(path: &Path, payload: Vec<u8>, index: u64, previous_hash: [u8; 32]) {
        let mut store = SegmentStore::open(path).expect("segment store");
        store
            .append_wal(WalAppend {
                pg: RANGE_PG,
                epoch: Epoch(9),
                index: LogIndex(index),
                kind: WalRecordKind::Range,
                previous_hash,
                payload: &payload,
            })
            .expect("append raw range record");
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-range-{name}-{}-{nonce}", std::process::id()))
    }
}
