#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    blob::ContentId, storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const RETENTION_PG: PlacementGroupId = PlacementGroupId(52);
const RETENTION_WAL_MAGIC: &[u8; 7] = b"RSFRET1";
const RETENTION_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionPolicy {
    pub namespace: String,
    pub retain_until: CommitVersion,
    pub expire_after_versions: Option<u64>,
    pub compact_noncurrent_after_versions: Option<u64>,
    pub legal_hold: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedObject {
    pub bucket: String,
    pub key: String,
    pub version: CommitVersion,
    pub current: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedBlob {
    pub id: ContentId,
    pub pin_version: CommitVersion,
    pub referenced: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetentionDecision {
    Keep {
        target: RetentionTarget,
        reason: KeepReason,
    },
    ExpireObject {
        bucket: String,
        key: String,
        version: CommitVersion,
    },
    CompactObjectVersion {
        bucket: String,
        key: String,
        version: CommitVersion,
    },
    ReleaseBlobPin {
        id: ContentId,
        pin_version: CommitVersion,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetentionTarget {
    Object {
        bucket: String,
        key: String,
        version: CommitVersion,
    },
    Blob {
        id: ContentId,
        pin_version: CommitVersion,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeepReason {
    LegalHold,
    RetainUntil,
    CurrentVersion,
    StillReferenced,
    NotExpired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetentionHook {
    ObjectDeleteAllowed {
        bucket: String,
        key: String,
        version: CommitVersion,
    },
    BlobReleaseAllowed {
        id: ContentId,
        pin_version: CommitVersion,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionReport {
    pub decisions: Vec<RetentionDecision>,
}

#[derive(Clone, Debug, Default)]
pub struct RetentionEngine;

impl RetentionEngine {
    pub fn evaluate(
        &self,
        policy: &RetentionPolicy,
        now: CommitVersion,
        objects: &[RetainedObject],
        blobs: &[RetainedBlob],
    ) -> RsfResult<RetentionReport> {
        validate_policy(policy)?;
        let mut report = RetentionReport::default();

        for object in objects {
            report
                .decisions
                .push(self.evaluate_object(policy, now, object));
        }
        for blob in blobs {
            report.decisions.push(self.evaluate_blob(policy, now, blob));
        }

        Ok(report)
    }

    pub fn authorize_object_delete(
        &self,
        policy: &RetentionPolicy,
        object: &RetainedObject,
    ) -> RsfResult<RetentionHook> {
        validate_policy(policy)?;
        if policy.legal_hold {
            return Err(denied(format!(
                "object {}/{} version {} is under legal hold",
                object.bucket, object.key, object.version.0
            )));
        }
        if object.version <= policy.retain_until {
            return Err(denied(format!(
                "object {}/{} version {} is retained until version {}",
                object.bucket, object.key, object.version.0, policy.retain_until.0
            )));
        }
        Ok(RetentionHook::ObjectDeleteAllowed {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version: object.version,
        })
    }

    pub fn authorize_blob_pin_release(
        &self,
        policy: &RetentionPolicy,
        now: CommitVersion,
        blob: &RetainedBlob,
    ) -> RsfResult<RetentionHook> {
        validate_policy(policy)?;
        match self.evaluate_blob(policy, now, blob) {
            RetentionDecision::ReleaseBlobPin { id, pin_version } => {
                Ok(RetentionHook::BlobReleaseAllowed { id, pin_version })
            }
            RetentionDecision::Keep { reason, .. } => Err(denied(format!(
                "blob pin {} cannot be released because retention reason is {:?}",
                blob.pin_version.0, reason
            ))),
            RetentionDecision::ExpireObject { .. }
            | RetentionDecision::CompactObjectVersion { .. } => {
                unreachable!("blob evaluation cannot produce object retention decisions")
            }
        }
    }

    fn evaluate_object(
        &self,
        policy: &RetentionPolicy,
        now: CommitVersion,
        object: &RetainedObject,
    ) -> RetentionDecision {
        if policy.legal_hold {
            return keep_object(object, KeepReason::LegalHold);
        }
        if object.version <= policy.retain_until {
            return keep_object(object, KeepReason::RetainUntil);
        }
        if object.current {
            return keep_object(object, KeepReason::CurrentVersion);
        }
        if let Some(window) = policy.compact_noncurrent_after_versions
            && age(now, object.version) >= window
        {
            return RetentionDecision::CompactObjectVersion {
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version: object.version,
            };
        }
        if let Some(window) = policy.expire_after_versions
            && age(now, object.version) >= window
        {
            return RetentionDecision::ExpireObject {
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version: object.version,
            };
        }
        keep_object(object, KeepReason::NotExpired)
    }

    fn evaluate_blob(
        &self,
        policy: &RetentionPolicy,
        now: CommitVersion,
        blob: &RetainedBlob,
    ) -> RetentionDecision {
        if policy.legal_hold {
            return keep_blob(blob, KeepReason::LegalHold);
        }
        if blob.pin_version <= policy.retain_until {
            return keep_blob(blob, KeepReason::RetainUntil);
        }
        if blob.referenced {
            return keep_blob(blob, KeepReason::StillReferenced);
        }
        if policy
            .expire_after_versions
            .is_some_and(|window| age(now, blob.pin_version) >= window)
        {
            return RetentionDecision::ReleaseBlobPin {
                id: blob.id.clone(),
                pin_version: blob.pin_version,
            };
        }
        keep_blob(blob, KeepReason::NotExpired)
    }
}

#[derive(Clone, Debug, Default)]
pub struct RetentionCatalog {
    policies: BTreeMap<String, RetentionPolicy>,
}

impl RetentionCatalog {
    pub fn set_policy(&mut self, policy: RetentionPolicy) -> RsfResult<()> {
        validate_policy(&policy)?;
        self.policies.insert(policy.namespace.clone(), policy);
        Ok(())
    }

    pub fn policy(&self, namespace: &str) -> Option<&RetentionPolicy> {
        self.policies.get(namespace)
    }

    pub fn evaluate(
        &self,
        namespace: &str,
        now: CommitVersion,
        objects: &[RetainedObject],
        blobs: &[RetainedBlob],
    ) -> RsfResult<RetentionReport> {
        let policy = self.policy(namespace).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("missing retention policy for namespace {namespace}"),
            )
        })?;
        RetentionEngine.evaluate(policy, now, objects, blobs)
    }

    pub fn policy_count(&self) -> usize {
        self.policies.len()
    }
}

#[derive(Debug)]
pub struct DurableRetentionCatalog {
    catalog: RetentionCatalog,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableRetentionCatalog {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(RETENTION_PG),
        })?;
        let mut catalog = RetentionCatalog::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Retention)
        {
            apply_retention_record(&mut catalog, decode_retention_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            catalog,
            store,
            epoch,
            next_index,
        })
    }

    pub fn set_policy(&mut self, policy: RetentionPolicy) -> RsfResult<()> {
        let mut catalog = self.catalog.clone();
        catalog.set_policy(policy.clone())?;
        self.append_record(&RetentionWalRecord::Policy(policy))?;
        self.catalog = catalog;
        Ok(())
    }

    pub fn policy(&self, namespace: &str) -> Option<&RetentionPolicy> {
        self.catalog.policy(namespace)
    }

    pub fn evaluate(
        &self,
        namespace: &str,
        now: CommitVersion,
        objects: &[RetainedObject],
        blobs: &[RetainedBlob],
    ) -> RsfResult<RetentionReport> {
        self.catalog.evaluate(namespace, now, objects, blobs)
    }

    pub fn catalog(&self) -> &RetentionCatalog {
        &self.catalog
    }

    fn append_record(&mut self, record: &RetentionWalRecord) -> RsfResult<()> {
        let payload = encode_retention_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(RETENTION_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: RETENTION_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Retention,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RetentionWalRecord {
    Policy(RetentionPolicy),
}

fn apply_retention_record(
    catalog: &mut RetentionCatalog,
    record: RetentionWalRecord,
) -> RsfResult<()> {
    match record {
        RetentionWalRecord::Policy(policy) => catalog.set_policy(policy),
    }
}

impl RetentionReport {
    pub fn action_count(&self) -> usize {
        self.decisions
            .iter()
            .filter(|decision| !matches!(decision, RetentionDecision::Keep { .. }))
            .count()
    }

    pub fn keep_count(&self) -> usize {
        self.decisions.len() - self.action_count()
    }
}

fn validate_policy(policy: &RetentionPolicy) -> RsfResult<()> {
    if policy.namespace.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "retention policy namespace must not be empty",
        ));
    }
    if policy.expire_after_versions == Some(0)
        || policy.compact_noncurrent_after_versions == Some(0)
    {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "retention windows must be greater than zero",
        ));
    }
    Ok(())
}

fn encode_retention_wal_payload(record: &RetentionWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(RETENTION_WAL_MAGIC);
    out.push(RETENTION_WAL_VERSION);
    match record {
        RetentionWalRecord::Policy(policy) => {
            validate_policy(policy)?;
            out.push(1);
            write_string(&mut out, &policy.namespace)?;
            write_u64(&mut out, policy.retain_until.0);
            write_optional_u64(&mut out, policy.expire_after_versions);
            write_optional_u64(&mut out, policy.compact_noncurrent_after_versions);
            out.push(u8::from(policy.legal_hold));
        }
    }
    Ok(out)
}

fn decode_retention_wal_payload(bytes: &[u8]) -> RsfResult<RetentionWalRecord> {
    if bytes.len() < RETENTION_WAL_MAGIC.len() + 2 {
        return Err(corrupt("retention WAL payload is too short"));
    }
    if &bytes[..RETENTION_WAL_MAGIC.len()] != RETENTION_WAL_MAGIC {
        return Err(corrupt("retention WAL magic mismatch"));
    }
    let mut offset = RETENTION_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != RETENTION_WAL_VERSION {
        return Err(corrupt("unsupported retention WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => RetentionWalRecord::Policy(RetentionPolicy {
            namespace: read_string(bytes, &mut offset)?,
            retain_until: CommitVersion(read_u64(bytes, &mut offset)?),
            expire_after_versions: read_optional_u64(bytes, &mut offset)?,
            compact_noncurrent_after_versions: read_optional_u64(bytes, &mut offset)?,
            legal_hold: match read_u8(bytes, &mut offset)? {
                0 => false,
                1 => true,
                _ => return Err(corrupt("invalid retention legal-hold flag")),
            },
        }),
        _ => return Err(corrupt("unknown retention WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in retention WAL payload"));
    }
    Ok(record)
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let bytes = value.as_bytes();
    write_len(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_len(out: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| RsfError::new(ErrorCode::InvalidInput, "retention WAL length exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            write_u64(out, value);
        }
        None => out.push(0),
    }
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    if *offset >= bytes.len() {
        return Err(corrupt("unexpected end of retention WAL payload"));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(corrupt("unexpected end of retention WAL payload"));
    }
    let value = u64::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
        bytes[*offset + 4],
        bytes[*offset + 5],
        bytes[*offset + 6],
        bytes[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}

fn read_optional_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<Option<u64>> {
    match read_u8(bytes, offset)? {
        0 => Ok(None),
        1 => Ok(Some(read_u64(bytes, offset)?)),
        _ => Err(corrupt("invalid retention optional integer tag")),
    }
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(corrupt("unexpected end of retention WAL payload"));
    }
    let value = u32::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    Ok(value as usize)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_len(bytes, offset)?;
    if bytes.len().saturating_sub(*offset) < len {
        return Err(corrupt("unexpected end of retention WAL string"));
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + len])
        .map_err(|_| corrupt("retention WAL string is not valid UTF-8"))?
        .to_string();
    *offset += len;
    Ok(value)
}

fn age(now: CommitVersion, version: CommitVersion) -> u64 {
    now.0.saturating_sub(version.0)
}

fn keep_object(object: &RetainedObject, reason: KeepReason) -> RetentionDecision {
    RetentionDecision::Keep {
        target: RetentionTarget::Object {
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version: object.version,
        },
        reason,
    }
}

fn keep_blob(blob: &RetainedBlob, reason: KeepReason) -> RetentionDecision {
    RetentionDecision::Keep {
        target: RetentionTarget::Blob {
            id: blob.id.clone(),
            pin_version: blob.pin_version,
        },
        reason,
    }
}

fn denied(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetentionPolicy {
        RetentionPolicy {
            namespace: "app".to_string(),
            retain_until: CommitVersion(5),
            expire_after_versions: Some(10),
            compact_noncurrent_after_versions: Some(6),
            legal_hold: false,
        }
    }

    fn cid(byte: u8) -> ContentId {
        ContentId([byte; 32])
    }

    #[test]
    fn current_object_is_kept_even_when_old() {
        let report = RetentionEngine
            .evaluate(
                &policy(),
                CommitVersion(30),
                &[RetainedObject {
                    bucket: "media".to_string(),
                    key: "hero.png".to_string(),
                    version: CommitVersion(7),
                    current: true,
                }],
                &[],
            )
            .unwrap();

        assert_eq!(report.action_count(), 0);
        assert_eq!(report.keep_count(), 1);
    }

    #[test]
    fn noncurrent_object_can_be_compacted() {
        let report = RetentionEngine
            .evaluate(
                &policy(),
                CommitVersion(20),
                &[RetainedObject {
                    bucket: "media".to_string(),
                    key: "old.png".to_string(),
                    version: CommitVersion(8),
                    current: false,
                }],
                &[],
            )
            .unwrap();

        assert!(matches!(
            report.decisions[0],
            RetentionDecision::CompactObjectVersion { .. }
        ));
    }

    #[test]
    fn unreferenced_expired_blob_releases_pin() {
        let report = RetentionEngine
            .evaluate(
                &policy(),
                CommitVersion(20),
                &[],
                &[RetainedBlob {
                    id: cid(7),
                    pin_version: CommitVersion(8),
                    referenced: false,
                }],
            )
            .unwrap();

        assert!(matches!(
            report.decisions[0],
            RetentionDecision::ReleaseBlobPin { .. }
        ));
    }

    #[test]
    fn legal_hold_keeps_everything() {
        let mut policy = policy();
        policy.legal_hold = true;
        let report = RetentionEngine
            .evaluate(
                &policy,
                CommitVersion(100),
                &[RetainedObject {
                    bucket: "media".to_string(),
                    key: "old.png".to_string(),
                    version: CommitVersion(6),
                    current: false,
                }],
                &[RetainedBlob {
                    id: cid(9),
                    pin_version: CommitVersion(6),
                    referenced: false,
                }],
            )
            .unwrap();

        assert_eq!(report.keep_count(), 2);
        assert_eq!(report.action_count(), 0);
    }

    #[test]
    fn legal_hold_blocks_object_delete_hook() {
        let mut policy = policy();
        policy.legal_hold = true;

        let err = RetentionEngine
            .authorize_object_delete(
                &policy,
                &RetainedObject {
                    bucket: "media".to_string(),
                    key: "hero.png".to_string(),
                    version: CommitVersion(30),
                    current: true,
                },
            )
            .expect_err("legal hold blocks delete");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn retain_until_blocks_object_delete_hook() {
        let err = RetentionEngine
            .authorize_object_delete(
                &policy(),
                &RetainedObject {
                    bucket: "media".to_string(),
                    key: "hero.png".to_string(),
                    version: CommitVersion(5),
                    current: true,
                },
            )
            .expect_err("retention window blocks delete");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn object_delete_hook_allows_version_after_retention_window() {
        let hook = RetentionEngine
            .authorize_object_delete(
                &policy(),
                &RetainedObject {
                    bucket: "media".to_string(),
                    key: "hero.png".to_string(),
                    version: CommitVersion(30),
                    current: true,
                },
            )
            .expect("delete allowed");

        assert!(matches!(hook, RetentionHook::ObjectDeleteAllowed { .. }));
    }

    #[test]
    fn blob_pin_release_hook_requires_unreferenced_expired_blob() {
        let blocked = RetentionEngine
            .authorize_blob_pin_release(
                &policy(),
                CommitVersion(20),
                &RetainedBlob {
                    id: cid(7),
                    pin_version: CommitVersion(8),
                    referenced: true,
                },
            )
            .expect_err("referenced blob is kept");
        assert_eq!(blocked.code, ErrorCode::InvalidInput);

        let allowed = RetentionEngine
            .authorize_blob_pin_release(
                &policy(),
                CommitVersion(20),
                &RetainedBlob {
                    id: cid(7),
                    pin_version: CommitVersion(8),
                    referenced: false,
                },
            )
            .expect("release allowed");
        assert!(matches!(allowed, RetentionHook::BlobReleaseAllowed { .. }));
    }

    #[test]
    fn durable_retention_recovers_policy_and_blocks_delete_after_restart() {
        let path = test_path("durable_retention_recovers_policy_and_blocks_delete_after_restart");
        let mut policy = policy();
        policy.legal_hold = true;

        {
            let mut catalog =
                DurableRetentionCatalog::open(&path, Epoch(6)).expect("open retention catalog");
            catalog.set_policy(policy.clone()).expect("persist policy");
            assert_eq!(catalog.catalog().policy_count(), 1);
        }

        let catalog =
            DurableRetentionCatalog::open(&path, Epoch(6)).expect("recover retention catalog");
        assert_eq!(catalog.policy("app"), Some(&policy));
        let report = catalog
            .evaluate(
                "app",
                CommitVersion(100),
                &[RetainedObject {
                    bucket: "media".to_string(),
                    key: "old.png".to_string(),
                    version: CommitVersion(30),
                    current: false,
                }],
                &[],
            )
            .expect("evaluate recovered policy");
        assert_eq!(report.keep_count(), 1);
        assert_eq!(report.action_count(), 0);

        let err = RetentionEngine
            .authorize_object_delete(
                catalog.policy("app").expect("policy"),
                &RetainedObject {
                    bucket: "media".to_string(),
                    key: "old.png".to_string(),
                    version: CommitVersion(30),
                    current: false,
                },
            )
            .expect_err("legal hold still blocks delete after restart");
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn durable_retention_rejects_invalid_recovered_policy() {
        let path = test_path("durable_retention_rejects_invalid_recovered_policy");
        let mut invalid = policy();
        invalid.namespace.clear();
        let payload =
            encode_retention_wal_payload_unchecked(&RetentionWalRecord::Policy(invalid.clone()));
        let mut store = SegmentStore::open(&path).expect("segment store");
        let recovery = store.recover_current().expect("recover").recovery;
        let tail = recovery.pg_tail(RETENTION_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        store
            .append_wal(WalAppend {
                pg: RETENTION_PG,
                epoch: Epoch(7),
                index: LogIndex(1),
                kind: WalRecordKind::Retention,
                previous_hash,
                payload: &payload,
            })
            .expect("append invalid policy");

        let err =
            DurableRetentionCatalog::open(&path, Epoch(7)).expect_err("replay rejects policy");
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn retention_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_retention_wal_payload(&RetentionWalRecord::Policy(policy()))
            .expect("encode policy");
        payload.push(1);

        let err = decode_retention_wal_payload(&payload).expect_err("trailing bytes reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn encode_retention_wal_payload_unchecked(record: &RetentionWalRecord) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(RETENTION_WAL_MAGIC);
        out.push(RETENTION_WAL_VERSION);
        match record {
            RetentionWalRecord::Policy(policy) => {
                out.push(1);
                write_string(&mut out, &policy.namespace).expect("namespace");
                write_u64(&mut out, policy.retain_until.0);
                write_optional_u64(&mut out, policy.expire_after_versions);
                write_optional_u64(&mut out, policy.compact_noncurrent_after_versions);
                out.push(u8::from(policy.legal_hold));
            }
        }
        out
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-retention-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        path
    }
}
