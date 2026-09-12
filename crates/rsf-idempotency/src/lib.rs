#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const IDEMPOTENCY_PG: PlacementGroupId = PlacementGroupId(47);
const IDEMPOTENCY_WAL_MAGIC: &[u8; 7] = b"RSFIDEM";
const IDEMPOTENCY_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey(pub String);

impl IdempotencyKey {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(invalid("idempotency key must not be empty"));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestEnvelope {
    pub key: IdempotencyKey,
    pub fingerprint: String,
    pub now: CommitVersion,
    pub ttl_versions: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredResponse {
    pub status: ResponseStatus,
    pub commit: Option<CommitVersion>,
    pub body_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseStatus {
    Committed,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdempotencyDecision {
    FirstSeen { expires_at: CommitVersion },
    InFlight { expires_at: CommitVersion },
    Replay { response: StoredResponse },
    Conflict { original_fingerprint: String },
    Expired { expired_at: CommitVersion },
}

#[derive(Clone, Debug, Default)]
pub struct IdempotencyLedger {
    entries: BTreeMap<IdempotencyKey, IdempotencyEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IdempotencyEntry {
    fingerprint: String,
    expires_at: CommitVersion,
    response: Option<StoredResponse>,
}

impl IdempotencyLedger {
    pub fn claim(&mut self, envelope: RequestEnvelope) -> RsfResult<IdempotencyDecision> {
        if envelope.fingerprint.trim().is_empty() {
            return Err(invalid("request fingerprint must not be empty"));
        }
        if envelope.ttl_versions == 0 {
            return Err(invalid("idempotency ttl must be greater than zero"));
        }

        if let Some(entry) = self.entries.get(&envelope.key) {
            if envelope.now > entry.expires_at {
                return Ok(IdempotencyDecision::Expired {
                    expired_at: entry.expires_at,
                });
            }
            if entry.fingerprint != envelope.fingerprint {
                return Ok(IdempotencyDecision::Conflict {
                    original_fingerprint: entry.fingerprint.clone(),
                });
            }
            if let Some(response) = &entry.response {
                return Ok(IdempotencyDecision::Replay {
                    response: response.clone(),
                });
            }
            return Ok(IdempotencyDecision::InFlight {
                expires_at: entry.expires_at,
            });
        }

        let expires_at = CommitVersion(envelope.now.0 + envelope.ttl_versions);
        self.entries.insert(
            envelope.key,
            IdempotencyEntry {
                fingerprint: envelope.fingerprint,
                expires_at,
                response: None,
            },
        );
        Ok(IdempotencyDecision::FirstSeen { expires_at })
    }

    pub fn finish(&mut self, key: &IdempotencyKey, response: StoredResponse) -> RsfResult<()> {
        validate_response(&response)?;
        let Some(entry) = self.entries.get_mut(key) else {
            return Err(invalid("cannot finish an unknown idempotency key"));
        };
        entry.response = Some(response);
        Ok(())
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Debug)]
pub struct DurableIdempotencyLedger {
    ledger: IdempotencyLedger,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableIdempotencyLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(IDEMPOTENCY_PG),
        })?;
        let mut ledger = IdempotencyLedger::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Idempotency)
        {
            apply_idempotency_record(
                &mut ledger,
                decode_idempotency_wal_payload(&record.payload)?,
            )?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            ledger,
            store,
            epoch,
            next_index,
        })
    }

    pub fn claim(&mut self, envelope: RequestEnvelope) -> RsfResult<IdempotencyDecision> {
        let mut ledger = self.ledger.clone();
        let decision = ledger.claim(envelope.clone())?;
        if matches!(decision, IdempotencyDecision::FirstSeen { .. }) {
            self.append_record(&IdempotencyWalRecord::Claimed { envelope })?;
            self.ledger = ledger;
        }
        Ok(decision)
    }

    pub fn finish(&mut self, key: &IdempotencyKey, response: StoredResponse) -> RsfResult<()> {
        let mut ledger = self.ledger.clone();
        ledger.finish(key, response.clone())?;
        self.append_record(&IdempotencyWalRecord::Finished {
            key: key.clone(),
            response,
        })?;
        self.ledger = ledger;
        Ok(())
    }

    pub fn ledger(&self) -> &IdempotencyLedger {
        &self.ledger
    }

    pub fn entry_count(&self) -> usize {
        self.ledger.entry_count()
    }

    fn append_record(&mut self, record: &IdempotencyWalRecord) -> RsfResult<()> {
        let payload = encode_idempotency_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(IDEMPOTENCY_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: IDEMPOTENCY_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Idempotency,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum IdempotencyWalRecord {
    Claimed {
        envelope: RequestEnvelope,
    },
    Finished {
        key: IdempotencyKey,
        response: StoredResponse,
    },
}

fn apply_idempotency_record(
    ledger: &mut IdempotencyLedger,
    record: IdempotencyWalRecord,
) -> RsfResult<()> {
    match record {
        IdempotencyWalRecord::Claimed { envelope } => match ledger.claim(envelope)? {
            IdempotencyDecision::FirstSeen { .. } => Ok(()),
            other => Err(corrupt(format!(
                "idempotency WAL claim replay produced unexpected decision {other:?}"
            ))),
        },
        IdempotencyWalRecord::Finished { key, response } => ledger.finish(&key, response),
    }
}

fn encode_idempotency_wal_payload(record: &IdempotencyWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(IDEMPOTENCY_WAL_MAGIC);
    out.push(IDEMPOTENCY_WAL_VERSION);
    match record {
        IdempotencyWalRecord::Claimed { envelope } => {
            out.push(1);
            encode_envelope(envelope, &mut out)?;
        }
        IdempotencyWalRecord::Finished { key, response } => {
            out.push(2);
            write_string(&key.0, &mut out)?;
            encode_response(response, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_idempotency_wal_payload(bytes: &[u8]) -> RsfResult<IdempotencyWalRecord> {
    if bytes.len() < IDEMPOTENCY_WAL_MAGIC.len() + 2 {
        return Err(corrupt("idempotency WAL payload is too short"));
    }
    if &bytes[..IDEMPOTENCY_WAL_MAGIC.len()] != IDEMPOTENCY_WAL_MAGIC {
        return Err(corrupt("idempotency WAL payload has invalid magic"));
    }
    let mut offset = IDEMPOTENCY_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != IDEMPOTENCY_WAL_VERSION {
        return Err(corrupt(format!(
            "idempotency WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => IdempotencyWalRecord::Claimed {
            envelope: decode_envelope(bytes, &mut offset)?,
        },
        2 => IdempotencyWalRecord::Finished {
            key: IdempotencyKey::new(read_string(bytes, &mut offset)?)?,
            response: decode_response(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "idempotency WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("idempotency WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn encode_envelope(envelope: &RequestEnvelope, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_envelope(envelope)?;
    write_string(&envelope.key.0, out)?;
    write_string(&envelope.fingerprint, out)?;
    write_u64(envelope.now.0, out);
    write_u64(envelope.ttl_versions, out);
    Ok(())
}

fn decode_envelope(bytes: &[u8], offset: &mut usize) -> RsfResult<RequestEnvelope> {
    let envelope = RequestEnvelope {
        key: IdempotencyKey::new(read_string(bytes, offset)?)?,
        fingerprint: read_string(bytes, offset)?,
        now: CommitVersion(read_u64(bytes, offset)?),
        ttl_versions: read_u64(bytes, offset)?,
    };
    validate_envelope(&envelope)?;
    Ok(envelope)
}

fn encode_response(response: &StoredResponse, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_response(response)?;
    out.push(match response.status {
        ResponseStatus::Committed => 1,
        ResponseStatus::Rejected => 2,
    });
    match response.commit {
        Some(commit) => {
            out.push(1);
            write_u64(commit.0, out);
        }
        None => out.push(0),
    }
    write_string(&response.body_digest, out)?;
    Ok(())
}

fn decode_response(bytes: &[u8], offset: &mut usize) -> RsfResult<StoredResponse> {
    let status = match read_u8(bytes, offset)? {
        1 => ResponseStatus::Committed,
        2 => ResponseStatus::Rejected,
        other => {
            return Err(corrupt(format!(
                "idempotency response status {other} is invalid"
            )));
        }
    };
    let commit = match read_u8(bytes, offset)? {
        0 => None,
        1 => Some(CommitVersion(read_u64(bytes, offset)?)),
        other => {
            return Err(corrupt(format!(
                "idempotency commit marker {other} is invalid"
            )));
        }
    };
    let response = StoredResponse {
        status,
        commit,
        body_digest: read_string(bytes, offset)?,
    };
    validate_response(&response)?;
    Ok(response)
}

fn validate_envelope(envelope: &RequestEnvelope) -> RsfResult<()> {
    if envelope.fingerprint.trim().is_empty() {
        return Err(invalid("request fingerprint must not be empty"));
    }
    if envelope.ttl_versions == 0 {
        return Err(invalid("idempotency ttl must be greater than zero"));
    }
    Ok(())
}

fn validate_response(response: &StoredResponse) -> RsfResult<()> {
    if response.body_digest.trim().is_empty() {
        return Err(invalid(
            "idempotency response body digest must not be empty",
        ));
    }
    Ok(())
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| invalid("idempotency string is too large for WAL"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len =
        usize::try_from(len).map_err(|_| corrupt("idempotency string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("idempotency WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec())
        .map_err(|_| corrupt("idempotency WAL payload has invalid UTF-8"))
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("idempotency WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("idempotency WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = bytes
        .get(*offset..offset.saturating_add(8))
        .ok_or_else(|| corrupt("idempotency WAL payload ended while reading u64"))?;
    *offset = offset.saturating_add(8);
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Retryable,
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u8,
    pub base_delay_ms: u16,
}

impl RetryPolicy {
    pub fn next_delay_ms(&self, class: RetryClass, attempt: u8) -> Option<u16> {
        if class == RetryClass::Terminal || attempt >= self.max_attempts {
            return None;
        }
        let shift = u32::from(attempt.saturating_sub(1));
        Some(
            self.base_delay_ms
                .saturating_mul(2u16.saturating_pow(shift)),
        )
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 25,
        }
    }
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

    #[test]
    fn first_claim_can_finish_and_replay_response() {
        let mut ledger = IdempotencyLedger::default();
        let key = IdempotencyKey::new("app/req-1").unwrap();
        let envelope = envelope(key.clone(), "sql:insert", CommitVersion(10));

        assert_eq!(
            ledger.claim(envelope.clone()).unwrap(),
            IdempotencyDecision::FirstSeen {
                expires_at: CommitVersion(15)
            }
        );
        ledger
            .finish(
                &key,
                StoredResponse {
                    status: ResponseStatus::Committed,
                    commit: Some(CommitVersion(11)),
                    body_digest: "row:4".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            ledger.claim(envelope).unwrap(),
            IdempotencyDecision::Replay {
                response: StoredResponse {
                    status: ResponseStatus::Committed,
                    commit: Some(CommitVersion(11)),
                    body_digest: "row:4".to_string(),
                }
            }
        );
    }

    #[test]
    fn reused_key_with_different_fingerprint_is_conflict() {
        let mut ledger = IdempotencyLedger::default();
        let key = IdempotencyKey::new("app/req-2").unwrap();
        ledger
            .claim(envelope(key.clone(), "sql:insert:1", CommitVersion(4)))
            .unwrap();

        assert_eq!(
            ledger
                .claim(envelope(key, "sql:insert:2", CommitVersion(5)))
                .unwrap(),
            IdempotencyDecision::Conflict {
                original_fingerprint: "sql:insert:1".to_string()
            }
        );
    }

    #[test]
    fn duplicate_in_flight_claim_does_not_reopen_execution() {
        let mut ledger = IdempotencyLedger::default();
        let key = IdempotencyKey::new("app/req-inflight").unwrap();
        let envelope = envelope(key.clone(), "sql:update", CommitVersion(20));

        assert_eq!(
            ledger.claim(envelope.clone()).unwrap(),
            IdempotencyDecision::FirstSeen {
                expires_at: CommitVersion(25)
            }
        );
        assert_eq!(
            ledger.claim(envelope).unwrap(),
            IdempotencyDecision::InFlight {
                expires_at: CommitVersion(25)
            }
        );
    }

    #[test]
    fn expired_key_requires_new_request_identity() {
        let mut ledger = IdempotencyLedger::default();
        let key = IdempotencyKey::new("app/req-3").unwrap();
        ledger
            .claim(envelope(key.clone(), "blob:put", CommitVersion(1)))
            .unwrap();

        assert_eq!(
            ledger
                .claim(envelope(key, "blob:put", CommitVersion(7)))
                .unwrap(),
            IdempotencyDecision::Expired {
                expired_at: CommitVersion(6)
            }
        );
    }

    #[test]
    fn retry_policy_only_retries_retryable_failures_inside_budget() {
        let policy = RetryPolicy::default();

        assert_eq!(policy.next_delay_ms(RetryClass::Retryable, 1), Some(25));
        assert_eq!(policy.next_delay_ms(RetryClass::Retryable, 2), Some(50));
        assert_eq!(policy.next_delay_ms(RetryClass::Retryable, 3), None);
        assert_eq!(policy.next_delay_ms(RetryClass::Terminal, 1), None);
    }

    #[test]
    fn durable_idempotency_recovers_finished_response_for_replay() {
        let path = test_path("durable_idempotency_recovers_finished_response_for_replay");
        let key = IdempotencyKey::new("app/req-4").unwrap();
        let request = envelope(key.clone(), "object:put", CommitVersion(30));
        let response = StoredResponse {
            status: ResponseStatus::Committed,
            commit: Some(CommitVersion(31)),
            body_digest: "object:sha256:abc".to_string(),
        };

        {
            let mut ledger =
                DurableIdempotencyLedger::open(&path, Epoch(2)).expect("open durable ledger");
            assert_eq!(
                ledger.claim(request.clone()).expect("claim"),
                IdempotencyDecision::FirstSeen {
                    expires_at: CommitVersion(35)
                }
            );
            ledger.finish(&key, response.clone()).expect("finish");
        }

        let mut recovered =
            DurableIdempotencyLedger::open(&path, Epoch(2)).expect("recover durable ledger");
        assert_eq!(recovered.entry_count(), 1);
        assert_eq!(
            recovered.claim(request).expect("replay"),
            IdempotencyDecision::Replay { response }
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_idempotency_recovers_unfinished_claim_as_in_flight() {
        let path = test_path("durable_idempotency_recovers_unfinished_claim_as_in_flight");
        let key = IdempotencyKey::new("app/req-5").unwrap();
        let request = envelope(key, "sql:delete", CommitVersion(40));

        {
            let mut ledger =
                DurableIdempotencyLedger::open(&path, Epoch(3)).expect("open durable ledger");
            ledger.claim(request.clone()).expect("claim");
        }

        let mut recovered =
            DurableIdempotencyLedger::open(&path, Epoch(3)).expect("recover durable ledger");
        assert_eq!(
            recovered.claim(request).expect("in flight"),
            IdempotencyDecision::InFlight {
                expires_at: CommitVersion(45)
            }
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn idempotency_wal_decode_rejects_trailing_bytes() {
        let payload = encode_idempotency_wal_payload(&IdempotencyWalRecord::Claimed {
            envelope: envelope(
                IdempotencyKey::new("app/req-6").unwrap(),
                "kv:set",
                CommitVersion(50),
            ),
        })
        .expect("encode");
        let mut corrupt_payload = payload;
        corrupt_payload.push(99);

        let err = decode_idempotency_wal_payload(&corrupt_payload).expect_err("reject trailing");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn finish_rejects_empty_response_digest() {
        let mut ledger = IdempotencyLedger::default();
        let key = IdempotencyKey::new("app/req-7").unwrap();
        ledger
            .claim(envelope(key.clone(), "sql:insert", CommitVersion(60)))
            .expect("claim");

        let err = ledger
            .finish(
                &key,
                StoredResponse {
                    status: ResponseStatus::Rejected,
                    commit: None,
                    body_digest: " ".to_string(),
                },
            )
            .expect_err("empty digest rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    fn envelope(
        key: IdempotencyKey,
        fingerprint: impl Into<String>,
        now: CommitVersion,
    ) -> RequestEnvelope {
        RequestEnvelope {
            key,
            fingerprint: fingerprint.into(),
            now,
            ttl_versions: 5,
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-idempotency-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
