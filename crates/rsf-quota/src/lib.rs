#![forbid(unsafe_code)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_api::ApiSurface;
use rsf_core::{
    Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult, storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const QUOTA_PG: PlacementGroupId = PlacementGroupId(42);
const QUOTA_WAL_MAGIC: &[u8; 7] = b"RSFQTA1";
const QUOTA_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuotaKey {
    pub namespace: String,
    pub surface: ApiSurface,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuotaPolicy {
    pub key: QuotaKey,
    pub max_requests: u64,
    pub max_rows: Option<u64>,
    pub max_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuotaUsage {
    pub requests: u64,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuotaReservation {
    pub key: QuotaKey,
    pub usage: QuotaUsage,
    pub remaining_requests: u64,
    pub remaining_rows: Option<u64>,
    pub remaining_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuotaDecision {
    Allowed(QuotaReservation),
    Rejected { key: QuotaKey, reason: String },
}

#[derive(Clone, Debug, Default)]
pub struct QuotaEngine {
    policies: BTreeMap<QuotaKey, QuotaPolicy>,
    usage: BTreeMap<QuotaKey, QuotaUsage>,
}

impl QuotaEngine {
    pub fn set_policy(&mut self, policy: QuotaPolicy) -> RsfResult<()> {
        validate_policy(&policy)?;
        self.policies.insert(policy.key.clone(), policy);
        Ok(())
    }

    pub fn reserve(&mut self, key: QuotaKey, rows: u64, bytes: u64) -> RsfResult<QuotaDecision> {
        let Some(policy) = self.policies.get(&key) else {
            return Ok(QuotaDecision::Rejected {
                key,
                reason: "no quota policy".to_string(),
            });
        };
        let current = self.usage.get(&key).cloned().unwrap_or_default();
        let requested = QuotaUsage {
            requests: 1,
            rows,
            bytes,
        };
        let next = QuotaUsage {
            requests: current.requests + requested.requests,
            rows: current.rows + requested.rows,
            bytes: current.bytes + requested.bytes,
        };

        if next.requests > policy.max_requests {
            return Ok(QuotaDecision::Rejected {
                key,
                reason: "request quota exceeded".to_string(),
            });
        }
        if policy.max_rows.is_some_and(|limit| next.rows > limit) {
            return Ok(QuotaDecision::Rejected {
                key,
                reason: "row quota exceeded".to_string(),
            });
        }
        if policy.max_bytes.is_some_and(|limit| next.bytes > limit) {
            return Ok(QuotaDecision::Rejected {
                key,
                reason: "byte quota exceeded".to_string(),
            });
        }

        self.usage.insert(key.clone(), next.clone());
        Ok(QuotaDecision::Allowed(QuotaReservation {
            key,
            usage: next.clone(),
            remaining_requests: policy.max_requests - next.requests,
            remaining_rows: policy.max_rows.map(|limit| limit - next.rows),
            remaining_bytes: policy.max_bytes.map(|limit| limit - next.bytes),
        }))
    }

    pub fn policy_count(&self) -> usize {
        self.policies.len()
    }

    pub fn namespace_count(&self) -> usize {
        self.policies
            .keys()
            .map(|key| key.namespace.clone())
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn usage(&self, key: &QuotaKey) -> Option<&QuotaUsage> {
        self.usage.get(key)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QuotaWalRecord {
    Policy(QuotaPolicy),
    Reservation {
        key: QuotaKey,
        rows: u64,
        bytes: u64,
    },
}

#[derive(Debug)]
pub struct DurableQuotaEngine {
    engine: QuotaEngine,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableQuotaEngine {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(QUOTA_PG) })?;
        let mut engine = QuotaEngine::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Quota)
        {
            apply_quota_record(&mut engine, decode_quota_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            engine,
            store,
            epoch,
            next_index,
        })
    }

    pub fn set_policy(&mut self, policy: QuotaPolicy) -> RsfResult<()> {
        let mut engine = self.engine.clone();
        engine.set_policy(policy.clone())?;
        self.append_record(&QuotaWalRecord::Policy(policy))?;
        self.engine = engine;
        Ok(())
    }

    pub fn reserve(&mut self, key: QuotaKey, rows: u64, bytes: u64) -> RsfResult<QuotaDecision> {
        let mut engine = self.engine.clone();
        let decision = engine.reserve(key.clone(), rows, bytes)?;
        if decision.is_allowed() {
            self.append_record(&QuotaWalRecord::Reservation { key, rows, bytes })?;
            self.engine = engine;
        }
        Ok(decision)
    }

    pub fn engine(&self) -> &QuotaEngine {
        &self.engine
    }

    pub fn policy_count(&self) -> usize {
        self.engine.policy_count()
    }

    pub fn namespace_count(&self) -> usize {
        self.engine.namespace_count()
    }

    pub fn usage(&self, key: &QuotaKey) -> Option<&QuotaUsage> {
        self.engine.usage(key)
    }

    fn append_record(&mut self, record: &QuotaWalRecord) -> RsfResult<()> {
        let payload = encode_quota_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(QUOTA_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: QUOTA_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Quota,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

impl QuotaDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed(_))
    }
}

impl QuotaKey {
    pub fn new(namespace: impl Into<String>, surface: ApiSurface) -> Self {
        Self {
            namespace: namespace.into(),
            surface,
        }
    }
}

impl Ord for QuotaKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.namespace
            .cmp(&other.namespace)
            .then_with(|| surface_rank(&self.surface).cmp(&surface_rank(&other.surface)))
    }
}

impl PartialOrd for QuotaKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn surface_rank(surface: &ApiSurface) -> u8 {
    match surface {
        ApiSurface::Sql => 0,
        ApiSurface::Kv => 1,
        ApiSurface::Object => 2,
        ApiSurface::Blob => 3,
        ApiSurface::Admin => 4,
    }
}

fn surface_to_u8(surface: &ApiSurface) -> u8 {
    surface_rank(surface) + 1
}

fn surface_from_u8(value: u8) -> RsfResult<ApiSurface> {
    match value {
        1 => Ok(ApiSurface::Sql),
        2 => Ok(ApiSurface::Kv),
        3 => Ok(ApiSurface::Object),
        4 => Ok(ApiSurface::Blob),
        5 => Ok(ApiSurface::Admin),
        _ => Err(corrupt(format!("unknown quota API surface id {value}"))),
    }
}

fn validate_policy(policy: &QuotaPolicy) -> RsfResult<()> {
    if policy.key.namespace.trim().is_empty() {
        return Err(invalid("quota namespace must not be empty"));
    }
    if policy.max_requests == 0 {
        return Err(invalid("request quota must be greater than zero"));
    }
    if policy.max_rows == Some(0) {
        return Err(invalid("row quota must be greater than zero"));
    }
    if policy.max_bytes == Some(0) {
        return Err(invalid("byte quota must be greater than zero"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

fn apply_quota_record(engine: &mut QuotaEngine, record: QuotaWalRecord) -> RsfResult<()> {
    match record {
        QuotaWalRecord::Policy(policy) => engine.set_policy(policy),
        QuotaWalRecord::Reservation { key, rows, bytes } => {
            match engine.reserve(key, rows, bytes)? {
                QuotaDecision::Allowed(_) => Ok(()),
                QuotaDecision::Rejected { reason, .. } => Err(corrupt(format!(
                    "quota WAL reservation replay was rejected: {reason}"
                ))),
            }
        }
    }
}

fn encode_quota_wal_payload(record: &QuotaWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(QUOTA_WAL_MAGIC);
    out.push(QUOTA_WAL_VERSION);
    match record {
        QuotaWalRecord::Policy(policy) => {
            out.push(1);
            encode_policy(policy, &mut out)?;
        }
        QuotaWalRecord::Reservation { key, rows, bytes } => {
            out.push(2);
            encode_key(key, &mut out)?;
            write_u64(*rows, &mut out);
            write_u64(*bytes, &mut out);
        }
    }
    Ok(out)
}

fn decode_quota_wal_payload(bytes: &[u8]) -> RsfResult<QuotaWalRecord> {
    if bytes.len() < QUOTA_WAL_MAGIC.len() + 2 {
        return Err(corrupt("quota WAL payload is too short"));
    }
    if &bytes[..QUOTA_WAL_MAGIC.len()] != QUOTA_WAL_MAGIC {
        return Err(corrupt("quota WAL payload has invalid magic"));
    }
    let mut offset = QUOTA_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != QUOTA_WAL_VERSION {
        return Err(corrupt(format!(
            "quota WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => QuotaWalRecord::Policy(decode_policy(bytes, &mut offset)?),
        2 => QuotaWalRecord::Reservation {
            key: decode_key(bytes, &mut offset)?,
            rows: read_u64(bytes, &mut offset)?,
            bytes: read_u64(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "quota WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("quota WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn encode_policy(policy: &QuotaPolicy, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_policy(policy)?;
    encode_key(&policy.key, out)?;
    write_u64(policy.max_requests, out);
    encode_optional_u64(policy.max_rows, out);
    encode_optional_u64(policy.max_bytes, out);
    Ok(())
}

fn decode_policy(bytes: &[u8], offset: &mut usize) -> RsfResult<QuotaPolicy> {
    let policy = QuotaPolicy {
        key: decode_key(bytes, offset)?,
        max_requests: read_u64(bytes, offset)?,
        max_rows: decode_optional_u64(bytes, offset)?,
        max_bytes: decode_optional_u64(bytes, offset)?,
    };
    validate_policy(&policy)?;
    Ok(policy)
}

fn encode_key(key: &QuotaKey, out: &mut Vec<u8>) -> RsfResult<()> {
    write_string(&key.namespace, out)?;
    out.push(surface_to_u8(&key.surface));
    Ok(())
}

fn decode_key(bytes: &[u8], offset: &mut usize) -> RsfResult<QuotaKey> {
    Ok(QuotaKey {
        namespace: read_string(bytes, offset)?,
        surface: surface_from_u8(read_u8(bytes, offset)?)?,
    })
}

fn encode_optional_u64(value: Option<u64>, out: &mut Vec<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            write_u64(value, out);
        }
        None => out.push(0),
    }
}

fn decode_optional_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<Option<u64>> {
    match read_u8(bytes, offset)? {
        0 => Ok(None),
        1 => Ok(Some(read_u64(bytes, offset)?)),
        other => Err(corrupt(format!("quota optional marker {other} is invalid"))),
    }
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| invalid("quota string is too large for WAL"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len = usize::try_from(len).map_err(|_| corrupt("quota string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("quota WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("quota WAL payload has invalid UTF-8"))
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("quota WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("quota WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = bytes
        .get(*offset..offset.saturating_add(8))
        .ok_or_else(|| corrupt("quota WAL payload ended while reading u64"))?;
    *offset = offset.saturating_add(8);
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_consume_request_row_and_byte_budgets() {
        let mut quota = QuotaEngine::default();
        let key = QuotaKey::new("app", ApiSurface::Sql);
        quota
            .set_policy(policy(key.clone(), 2, Some(10), Some(1_000)))
            .expect("policy");

        let first = quota.reserve(key.clone(), 4, 256).expect("reserve");
        let QuotaDecision::Allowed(first) = first else {
            panic!("expected allowed reservation");
        };
        assert_eq!(first.remaining_requests, 1);
        assert_eq!(first.remaining_rows, Some(6));
        assert_eq!(first.remaining_bytes, Some(744));

        let second = quota.reserve(key.clone(), 6, 744).expect("reserve");
        assert!(second.is_allowed());
        let third = quota.reserve(key, 1, 1).expect("reserve");
        assert_eq!(
            third,
            QuotaDecision::Rejected {
                key: QuotaKey::new("app", ApiSurface::Sql),
                reason: "request quota exceeded".to_string()
            }
        );
    }

    #[test]
    fn byte_limit_rejects_without_charging_usage() {
        let mut quota = QuotaEngine::default();
        let key = QuotaKey::new("app", ApiSurface::Blob);
        quota
            .set_policy(policy(key.clone(), 3, None, Some(100)))
            .expect("policy");

        let rejected = quota.reserve(key.clone(), 0, 101).expect("reserve");
        assert_eq!(
            rejected,
            QuotaDecision::Rejected {
                key: key.clone(),
                reason: "byte quota exceeded".to_string()
            }
        );
        let allowed = quota.reserve(key, 0, 100).expect("reserve");
        assert!(allowed.is_allowed());
    }

    #[test]
    fn missing_policy_rejects_request() {
        let mut quota = QuotaEngine::default();
        let key = QuotaKey::new("app", ApiSurface::Kv);

        let decision = quota.reserve(key.clone(), 0, 1).expect("reserve");
        assert_eq!(
            decision,
            QuotaDecision::Rejected {
                key,
                reason: "no quota policy".to_string()
            }
        );
    }

    #[test]
    fn policy_registration_counts_namespaces_and_rejects_empty_limits() {
        let mut quota = QuotaEngine::default();
        quota
            .set_policy(policy(QuotaKey::new("app", ApiSurface::Sql), 1, None, None))
            .expect("policy");
        quota
            .set_policy(policy(
                QuotaKey::new("audit", ApiSurface::Kv),
                1,
                None,
                None,
            ))
            .expect("policy");

        assert_eq!(quota.policy_count(), 2);
        assert_eq!(quota.namespace_count(), 2);
        assert!(
            quota
                .set_policy(policy(QuotaKey::new("", ApiSurface::Sql), 1, None, None))
                .is_err()
        );
        assert!(
            quota
                .set_policy(policy(QuotaKey::new("app", ApiSurface::Sql), 0, None, None))
                .is_err()
        );
    }

    #[test]
    fn durable_quota_recovers_policy_and_charged_usage() {
        let path = test_path("durable_quota_recovers_policy_and_charged_usage");
        let key = QuotaKey::new("app", ApiSurface::Sql);

        {
            let mut quota = DurableQuotaEngine::open(&path, Epoch(9)).expect("open quota");
            quota
                .set_policy(policy(key.clone(), 2, Some(10), Some(1_000)))
                .expect("policy");
            assert!(
                quota
                    .reserve(key.clone(), 4, 256)
                    .expect("reserve")
                    .is_allowed()
            );
        }

        let mut recovered = DurableQuotaEngine::open(&path, Epoch(9)).expect("recover quota");
        assert_eq!(recovered.policy_count(), 1);
        assert_eq!(recovered.namespace_count(), 1);
        assert_eq!(
            recovered.usage(&key),
            Some(&QuotaUsage {
                requests: 1,
                rows: 4,
                bytes: 256,
            })
        );

        let second = recovered.reserve(key.clone(), 6, 744).expect("reserve");
        assert!(second.is_allowed());
        let rejected = recovered.reserve(key.clone(), 1, 1).expect("reserve");
        assert_eq!(
            rejected,
            QuotaDecision::Rejected {
                key,
                reason: "request quota exceeded".to_string()
            }
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_quota_does_not_charge_rejected_reservations() {
        let path = test_path("durable_quota_does_not_charge_rejected_reservations");
        let key = QuotaKey::new("app", ApiSurface::Blob);

        {
            let mut quota = DurableQuotaEngine::open(&path, Epoch(9)).expect("open quota");
            quota
                .set_policy(policy(key.clone(), 2, None, Some(100)))
                .expect("policy");
            let rejected = quota.reserve(key.clone(), 0, 101).expect("reserve");
            assert_eq!(
                rejected,
                QuotaDecision::Rejected {
                    key: key.clone(),
                    reason: "byte quota exceeded".to_string()
                }
            );
        }

        let mut recovered = DurableQuotaEngine::open(&path, Epoch(9)).expect("recover quota");
        assert_eq!(recovered.usage(&key), None);
        assert!(
            recovered
                .reserve(key, 0, 100)
                .expect("reserve")
                .is_allowed()
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn quota_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_quota_wal_payload(&QuotaWalRecord::Policy(policy(
            QuotaKey::new("app", ApiSurface::Object),
            1,
            None,
            Some(1024),
        )))
        .expect("encode");
        payload.push(99);

        let err = decode_quota_wal_payload(&payload).expect_err("trailing bytes reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn policy(
        key: QuotaKey,
        max_requests: u64,
        max_rows: Option<u64>,
        max_bytes: Option<u64>,
    ) -> QuotaPolicy {
        QuotaPolicy {
            key,
            max_requests,
            max_rows,
            max_bytes,
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-quota-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
