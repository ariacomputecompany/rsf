//! A bounded ordered slice of frozen history. One large record is permitted;
//! multiple small records share a frame and one durable staging acknowledgement.
use super::*;

pub const MAX_HISTORY_BATCH_RECORDS: usize = 32;
pub const HISTORY_BATCH_TARGET_BYTES: usize = 64 * 1024;
const HEADER: usize = 17;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrHistoryBatch {
    pub ordinal: u64,
    pub records: Vec<VsrHistoryRecord>,
}
impl VsrHistoryBatch {
    /// Pack a nonempty bounded prefix, preserving exact order and record bytes.
    pub fn pack(ordinal: u64, records: &[VsrHistoryRecord]) -> RsfResult<Self> {
        let mut bytes = HEADER;
        let mut batch = Vec::new();
        for record in records.iter().take(MAX_HISTORY_BATCH_RECORDS) {
            let length = record.encode()?.len() + 4;
            if !batch.is_empty() && bytes + length > HISTORY_BATCH_TARGET_BYTES {
                break;
            }
            bytes += length;
            batch.push(record.clone());
        }
        let value = Self {
            ordinal,
            records: batch,
        };
        value.encode()?;
        Ok(value)
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if self.records.is_empty()
            || self.records.len() > MAX_HISTORY_BATCH_RECORDS
            || self
                .ordinal
                .checked_add(self.records.len() as u64)
                .is_none()
        {
            return Err(invalid_input("invalid history batch range"));
        }
        let mut bytes = b"RSFB1".to_vec();
        bytes.extend_from_slice(&self.ordinal.to_le_bytes());
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        for record in &self.records {
            let encoded = record.encode()?;
            bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&encoded);
            if self.records.len() > 1 && bytes.len() > HISTORY_BATCH_TARGET_BYTES {
                return Err(invalid_input(
                    "multi-record history batch exceeds byte budget",
                ));
            }
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < HEADER
            || !bytes.starts_with(b"RSFB1")
            || bytes.len() > HEADER + 4 + 18 + MAX_HISTORY_RECORD_BYTES
        {
            return Err(corrupt("invalid history batch envelope"));
        }
        let ordinal = read_u64(bytes, 5)?;
        let count = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
        if count == 0 || count > MAX_HISTORY_BATCH_RECORDS {
            return Err(corrupt("invalid history batch count"));
        }
        let mut cursor = HEADER;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let length = bytes
                .get(cursor..cursor + 4)
                .ok_or_else(|| corrupt("truncated history batch length"))?;
            let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
            cursor += 4;
            let end = cursor
                .checked_add(length)
                .ok_or_else(|| corrupt("history batch length overflow"))?;
            let record = bytes
                .get(cursor..end)
                .ok_or_else(|| corrupt("truncated history batch record"))?;
            records.push(VsrHistoryRecord::decode(record)?);
            cursor = end;
        }
        let value = Self { ordinal, records };
        if cursor != bytes.len() || value.encode()? != bytes {
            return Err(corrupt("noncanonical history batch"));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_history_batches_reject_truncation_bad_counts_and_trailing_data() {
        let record = VsrHistoryRecord {
            epoch: Epoch(1),
            kind: WalRecordKind::Commit,
            payload: vec![7; 100],
        };
        let batch = VsrHistoryBatch::pack(9, &vec![record; 100]).unwrap();
        assert_eq!(batch.records.len(), MAX_HISTORY_BATCH_RECORDS);
        let bytes = batch.encode().unwrap();
        assert_eq!(VsrHistoryBatch::decode(&bytes).unwrap(), batch);
        for end in 0..bytes.len() {
            assert!(VsrHistoryBatch::decode(&bytes[..end]).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(VsrHistoryBatch::decode(&extra).is_err());
        for count in [0u32, 33, u32::MAX] {
            let mut bad = bytes.clone();
            bad[13..17].copy_from_slice(&count.to_le_bytes());
            assert!(VsrHistoryBatch::decode(&bad).is_err());
        }
        assert!(VsrHistoryBatch::pack(u64::MAX, &batch.records).is_err());
        let mut large = batch.records[0].clone();
        large.payload = vec![8; HISTORY_BATCH_TARGET_BYTES];
        assert_eq!(
            VsrHistoryBatch::pack(0, &[large.clone(), large])
                .unwrap()
                .records
                .len(),
            1
        );
    }
}
