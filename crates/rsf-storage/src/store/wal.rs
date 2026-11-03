//! Shared validation and byte encoding for single and grouped WAL appends.
use super::*;

pub(super) struct Frame {
    pub encoded: Vec<u8>,
    pub payload_hash: [u8; 32],
    pub record_hash: [u8; 32],
}

pub(super) fn encode_at_tail(input: WalAppend<'_>, tail: Option<WalTail>) -> RsfResult<Frame> {
    let expected_index = tail
        .map_or(Some(1), |tail| tail.index.0.checked_add(1))
        .ok_or_else(|| RsfError::new(ErrorCode::InvalidInput, "WAL index exhausted"))?;
    if input.index.0 != expected_index {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!(
                "wal index {} does not match next placement-group index {expected_index} for pg {}",
                input.index.0, input.pg.0
            ),
        ));
    }

    let expected_previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    if input.previous_hash != expected_previous_hash {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "wal previous hash does not match recovered placement-group tail for pg {}",
                input.pg.0
            ),
        ));
    }
    if tail.is_some_and(|tail| input.epoch < tail.epoch) {
        return Err(RsfError::new(
            ErrorCode::StaleEpoch,
            "WAL epoch would move backward",
        ));
    }

    let payload_hash = digest(input.payload);
    let record_hash = wal_record_hash(
        input.pg,
        input.epoch,
        input.index,
        input.kind,
        &input.previous_hash,
        &payload_hash,
        input.payload,
    );
    let encoded = encode_wal_record(input, payload_hash, record_hash)?;
    Ok(Frame {
        encoded,
        payload_hash,
        record_hash,
    })
}
