use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult, TraceId,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{RetryClass, RpcDecision, RpcEnvelope, RpcPolicy, RpcRouter, ServiceRole};

const RPC_PG: PlacementGroupId = PlacementGroupId(57);
const RPC_WAL_MAGIC: &[u8; 7] = b"RSFRPC1";
const RPC_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RpcCallId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcCallState {
    Accepted,
    Retryable,
    Rejected,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RpcCall {
    pub id: RpcCallId,
    pub envelope: RpcEnvelope,
    pub state: RpcCallState,
    pub rejection: Option<String>,
    pub completion: Option<String>,
    pub failure: Option<String>,
}

#[derive(Debug)]
pub struct DurableRpcLedger {
    router: RpcRouter,
    calls: BTreeMap<RpcCallId, RpcCall>,
    store: SegmentStore,
    epoch: Epoch,
    next_id: u64,
    next_index: u64,
}

impl DurableRpcLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch, policy: RpcPolicy) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(RPC_PG) })?;
        let mut calls = BTreeMap::new();
        let mut next_id = 1;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Rpc)
        {
            apply_rpc_record(&mut calls, decode_rpc_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        if let Some(last) = calls.keys().next_back() {
            next_id = last.0.saturating_add(1);
        }

        Ok(Self {
            router: RpcRouter::new(policy),
            calls,
            store,
            epoch,
            next_id,
            next_index,
        })
    }

    pub fn submit(&mut self, envelope: RpcEnvelope) -> RsfResult<(RpcCallId, RpcDecision)> {
        crate::validate_envelope(&envelope)?;
        let decision = self.router.route(envelope.clone());
        let id = RpcCallId(self.next_id);
        let (state, rejection) = state_from_decision(&decision)?;
        let record = RpcWalRecord::Submitted {
            id,
            envelope,
            state,
            rejection,
        };
        self.append_record(&record)?;
        self.calls.insert(id, call_from_submission(&record)?);
        self.next_id = self.next_id.saturating_add(1);
        Ok((id, decision))
    }

    pub fn mark_completed(&mut self, id: RpcCallId, detail: impl Into<String>) -> RsfResult<()> {
        let detail = detail.into();
        validate_terminal_detail(&detail, "rpc completion detail")?;
        let mut calls = self.calls.clone();
        let call = call_mut(&mut calls, id)?;
        if !matches!(call.state, RpcCallState::Accepted | RpcCallState::Retryable) {
            return Err(invalid(format!(
                "rpc call {} cannot complete from {:?}",
                id.0, call.state
            )));
        }
        call.state = RpcCallState::Completed;
        call.completion = Some(detail.clone());
        call.failure = None;
        self.append_record(&RpcWalRecord::Completed { id, detail })?;
        self.calls = calls;
        Ok(())
    }

    pub fn mark_failed(&mut self, id: RpcCallId, reason: impl Into<String>) -> RsfResult<()> {
        let reason = reason.into();
        validate_terminal_detail(&reason, "rpc failure reason")?;
        let mut calls = self.calls.clone();
        let call = call_mut(&mut calls, id)?;
        if !matches!(call.state, RpcCallState::Accepted | RpcCallState::Retryable) {
            return Err(invalid(format!(
                "rpc call {} cannot fail from {:?}",
                id.0, call.state
            )));
        }
        call.state = RpcCallState::Failed;
        call.failure = Some(reason.clone());
        call.completion = None;
        self.append_record(&RpcWalRecord::Failed { id, reason })?;
        self.calls = calls;
        Ok(())
    }

    pub fn call(&self, id: RpcCallId) -> Option<&RpcCall> {
        self.calls.get(&id)
    }

    pub fn calls(&self) -> Vec<RpcCall> {
        self.calls.values().cloned().collect()
    }

    pub fn in_flight(&self) -> Vec<RpcCall> {
        self.calls
            .values()
            .filter(|call| matches!(call.state, RpcCallState::Accepted | RpcCallState::Retryable))
            .cloned()
            .collect()
    }

    fn append_record(&mut self, record: &RpcWalRecord) -> RsfResult<()> {
        let payload = encode_rpc_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(RPC_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: RPC_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Rpc,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RpcWalRecord {
    Submitted {
        id: RpcCallId,
        envelope: RpcEnvelope,
        state: RpcCallState,
        rejection: Option<String>,
    },
    Completed {
        id: RpcCallId,
        detail: String,
    },
    Failed {
        id: RpcCallId,
        reason: String,
    },
}

fn apply_rpc_record(
    calls: &mut BTreeMap<RpcCallId, RpcCall>,
    record: RpcWalRecord,
) -> RsfResult<()> {
    match record {
        RpcWalRecord::Submitted { id, .. } => {
            if calls.contains_key(&id) {
                return Err(corrupt(format!("duplicate rpc call id {} in WAL", id.0)));
            }
            calls.insert(id, call_from_submission(&record)?);
        }
        RpcWalRecord::Completed { id, detail } => {
            validate_terminal_detail(&detail, "rpc completion detail")?;
            let call = call_mut(calls, id)?;
            if !matches!(call.state, RpcCallState::Accepted | RpcCallState::Retryable) {
                return Err(corrupt(format!(
                    "rpc call {} cannot replay completion from {:?}",
                    id.0, call.state
                )));
            }
            call.state = RpcCallState::Completed;
            call.completion = Some(detail);
            call.failure = None;
        }
        RpcWalRecord::Failed { id, reason } => {
            validate_terminal_detail(&reason, "rpc failure reason")?;
            let call = call_mut(calls, id)?;
            if !matches!(call.state, RpcCallState::Accepted | RpcCallState::Retryable) {
                return Err(corrupt(format!(
                    "rpc call {} cannot replay failure from {:?}",
                    id.0, call.state
                )));
            }
            call.state = RpcCallState::Failed;
            call.failure = Some(reason);
            call.completion = None;
        }
    }
    Ok(())
}

fn call_from_submission(record: &RpcWalRecord) -> RsfResult<RpcCall> {
    let RpcWalRecord::Submitted {
        id,
        envelope,
        state,
        rejection,
    } = record
    else {
        return Err(corrupt("rpc WAL record is not a submission"));
    };
    crate::validate_envelope(envelope)?;
    match state {
        RpcCallState::Accepted | RpcCallState::Retryable => {
            if rejection.is_some() {
                return Err(corrupt("accepted rpc WAL submission carried rejection"));
            }
        }
        RpcCallState::Rejected => {
            validate_terminal_detail(
                rejection
                    .as_deref()
                    .ok_or_else(|| corrupt("rejected rpc WAL submission missing reason"))?,
                "rpc rejection reason",
            )?;
        }
        RpcCallState::Completed | RpcCallState::Failed => {
            return Err(corrupt("rpc WAL submission cannot start terminal"));
        }
    }
    Ok(RpcCall {
        id: *id,
        envelope: envelope.clone(),
        state: *state,
        rejection: rejection.clone(),
        completion: None,
        failure: None,
    })
}

fn state_from_decision(decision: &RpcDecision) -> RsfResult<(RpcCallState, Option<String>)> {
    match decision {
        RpcDecision::Accepted(_) => Ok((RpcCallState::Accepted, None)),
        RpcDecision::Retryable(_) => Ok((RpcCallState::Retryable, None)),
        RpcDecision::Rejected { reason, .. } => {
            validate_terminal_detail(reason, "rpc rejection reason")?;
            Ok((RpcCallState::Rejected, Some(reason.clone())))
        }
    }
}

fn call_mut(calls: &mut BTreeMap<RpcCallId, RpcCall>, id: RpcCallId) -> RsfResult<&mut RpcCall> {
    calls
        .get_mut(&id)
        .ok_or_else(|| corrupt(format!("unknown rpc call id {}", id.0)))
}

fn validate_terminal_detail(value: &str, label: &str) -> RsfResult<()> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{label} must not be empty")));
    }
    Ok(())
}

fn encode_rpc_wal_payload(record: &RpcWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(RPC_WAL_MAGIC);
    out.push(RPC_WAL_VERSION);
    match record {
        RpcWalRecord::Submitted {
            id,
            envelope,
            state,
            rejection,
        } => {
            out.push(1);
            write_u64(&mut out, id.0);
            write_envelope(&mut out, envelope)?;
            out.push(state_to_u8(*state)?);
            match rejection {
                Some(reason) => {
                    out.push(1);
                    write_string(&mut out, reason)?;
                }
                None => out.push(0),
            }
        }
        RpcWalRecord::Completed { id, detail } => {
            out.push(2);
            write_u64(&mut out, id.0);
            write_string(&mut out, detail)?;
        }
        RpcWalRecord::Failed { id, reason } => {
            out.push(3);
            write_u64(&mut out, id.0);
            write_string(&mut out, reason)?;
        }
    }
    Ok(out)
}

fn decode_rpc_wal_payload(bytes: &[u8]) -> RsfResult<RpcWalRecord> {
    let mut offset = 0;
    take_magic(bytes, &mut offset)?;
    let version = read_u8(bytes, &mut offset)?;
    if version != RPC_WAL_VERSION {
        return Err(corrupt(format!("unsupported rpc WAL version {version}")));
    }
    let record = match read_u8(bytes, &mut offset)? {
        1 => RpcWalRecord::Submitted {
            id: RpcCallId(read_u64(bytes, &mut offset)?),
            envelope: read_envelope(bytes, &mut offset)?,
            state: state_from_u8(read_u8(bytes, &mut offset)?)?,
            rejection: match read_u8(bytes, &mut offset)? {
                0 => None,
                1 => Some(read_string(bytes, &mut offset)?),
                other => return Err(corrupt(format!("invalid rpc rejection flag {other}"))),
            },
        },
        2 => RpcWalRecord::Completed {
            id: RpcCallId(read_u64(bytes, &mut offset)?),
            detail: read_string(bytes, &mut offset)?,
        },
        3 => RpcWalRecord::Failed {
            id: RpcCallId(read_u64(bytes, &mut offset)?),
            reason: read_string(bytes, &mut offset)?,
        },
        other => return Err(corrupt(format!("unknown rpc WAL record kind {other}"))),
    };
    if offset != bytes.len() {
        return Err(corrupt("rpc WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn write_envelope(out: &mut Vec<u8>, envelope: &RpcEnvelope) -> RsfResult<()> {
    write_string(out, envelope.trace_id.as_str())?;
    out.push(role_to_u8(envelope.from));
    out.push(role_to_u8(envelope.to));
    write_string(out, &envelope.operation)?;
    write_u32(out, envelope.deadline_ms);
    out.push(envelope.attempt);
    out.push(retry_to_u8(envelope.retry_class));
    write_u64(out, envelope.body_bytes);
    Ok(())
}

fn state_to_u8(state: RpcCallState) -> RsfResult<u8> {
    match state {
        RpcCallState::Accepted => Ok(1),
        RpcCallState::Retryable => Ok(2),
        RpcCallState::Rejected => Ok(3),
        RpcCallState::Completed | RpcCallState::Failed => Err(corrupt(
            "terminal rpc state cannot be encoded as submission",
        )),
    }
}

fn state_from_u8(value: u8) -> RsfResult<RpcCallState> {
    match value {
        1 => Ok(RpcCallState::Accepted),
        2 => Ok(RpcCallState::Retryable),
        3 => Ok(RpcCallState::Rejected),
        other => Err(corrupt(format!("unknown rpc call state {other}"))),
    }
}

fn read_envelope(bytes: &[u8], offset: &mut usize) -> RsfResult<RpcEnvelope> {
    Ok(RpcEnvelope {
        trace_id: TraceId::new(read_string(bytes, offset)?)?,
        from: role_from_u8(read_u8(bytes, offset)?)?,
        to: role_from_u8(read_u8(bytes, offset)?)?,
        operation: read_string(bytes, offset)?,
        deadline_ms: read_u32(bytes, offset)?,
        attempt: read_u8(bytes, offset)?,
        retry_class: retry_from_u8(read_u8(bytes, offset)?)?,
        body_bytes: read_u64(bytes, offset)?,
    })
}

fn role_to_u8(role: ServiceRole) -> u8 {
    match role {
        ServiceRole::Gateway => 1,
        ServiceRole::Metadata => 2,
        ServiceRole::Storage => 3,
        ServiceRole::Sql => 4,
        ServiceRole::Object => 5,
        ServiceRole::Blob => 6,
        ServiceRole::Background => 7,
    }
}

fn role_from_u8(value: u8) -> RsfResult<ServiceRole> {
    match value {
        1 => Ok(ServiceRole::Gateway),
        2 => Ok(ServiceRole::Metadata),
        3 => Ok(ServiceRole::Storage),
        4 => Ok(ServiceRole::Sql),
        5 => Ok(ServiceRole::Object),
        6 => Ok(ServiceRole::Blob),
        7 => Ok(ServiceRole::Background),
        other => Err(corrupt(format!("unknown rpc role {other}"))),
    }
}

fn retry_to_u8(class: RetryClass) -> u8 {
    match class {
        RetryClass::Never => 1,
        RetryClass::Idempotent => 2,
        RetryClass::HedgeableRead => 3,
    }
}

fn retry_from_u8(value: u8) -> RsfResult<RetryClass> {
    match value {
        1 => Ok(RetryClass::Never),
        2 => Ok(RetryClass::Idempotent),
        3 => Ok(RetryClass::HedgeableRead),
        other => Err(corrupt(format!("unknown rpc retry class {other}"))),
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let len = u32::try_from(value.len()).map_err(|_| invalid("rpc WAL string exceeds u32"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)? as usize;
    let raw = take(bytes, offset, len)?;
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("rpc WAL string is not utf8"))
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = take(bytes, offset, 8)?;
    let mut buf = [0; 8];
    buf.copy_from_slice(raw);
    Ok(u64::from_le_bytes(buf))
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = take(bytes, offset, 4)?;
    let mut buf = [0; 4];
    buf.copy_from_slice(raw);
    Ok(u32::from_le_bytes(buf))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    Ok(*take(bytes, offset, 1)?
        .first()
        .expect("one requested byte must exist"))
}

fn take_magic(bytes: &[u8], offset: &mut usize) -> RsfResult<()> {
    let magic = take(bytes, offset, RPC_WAL_MAGIC.len())?;
    if magic != RPC_WAL_MAGIC {
        return Err(corrupt("rpc WAL payload has invalid magic"));
    }
    Ok(())
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt("rpc WAL offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("rpc WAL payload is truncated"));
    }
    let out = &bytes[*offset..end];
    *offset = end;
    Ok(out)
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests;
