mod journal;

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use rsf_blob::DurableBlobEngine;
use rsf_core::{
    CommitConsistency, CommitEvidence, CommitVersion, Digest, ErrorCode, LogIndex, OperationId,
    PlacementGroupId, RsfError, RsfResult, blob::ContentId,
};
use rsf_protocol::{ClientRequestId, CommitCertificate, FabricOperation, FabricRouterPort};

const INDEX_FILE: &str = "objects.index";
const FORMAT: &str = "RSF_OBJECT_INDEX_V1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableObjectBucket {
    pub name: String,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableObjectHead {
    pub bucket: String,
    pub key: String,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
    pub content: ContentId,
    pub bytes: u64,
    pub manifest_digest: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableObjectPut {
    pub head: DurableObjectHead,
    pub pin_version: CommitVersion,
    pub pin_evidence: CommitEvidence,
    pub deduped: bool,
    pub pins: u32,
}

#[derive(Debug)]
pub struct DurableObjectStore {
    root: PathBuf,
    blobs: DurableBlobEngine,
    buckets: BTreeMap<String, DurableObjectBucket>,
    heads: BTreeMap<(String, String), DurableObjectHead>,
    versions: BTreeMap<(String, String), Vec<DurableObjectHead>>,
    journal: journal::Journal,
    _writer_lock: rsf_storage::ExclusiveFileLock,
}

impl DurableObjectStore {
    pub fn open(root: impl AsRef<Path>) -> RsfResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "failed to create durable object root {}: {err}",
                    root.display()
                ),
            )
        })?;
        let writer_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join(".object-writer.lock"))
            .map_err(|err| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("cannot open object writer lock: {err}"),
                )
            })?;
        let writer_lock = rsf_storage::ExclusiveFileLock::acquire(writer_lock).map_err(|err| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("object store already has a writer: {err}"),
            )
        })?;
        let journal = journal::Journal::open(&root)?;
        let blobs = DurableBlobEngine::open(root.join("blob"))?;
        let mut store = Self {
            root,
            blobs,
            buckets: BTreeMap::new(),
            heads: BTreeMap::new(),
            versions: BTreeMap::new(),
            journal,
            _writer_lock: writer_lock,
        };
        store.recover_journal()?;
        Ok(store)
    }

    pub fn create_bucket(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
    ) -> RsfResult<DurableObjectBucket> {
        let bucket = bucket.into();
        let request = ClientRequestId::new(
            format!("object:{}:bucket:{bucket}", self.journal.scope()),
            0,
        );
        self.create_bucket_request(router, metadata_pg, request, bucket)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Keep routing, durable request identity, object identity and payload explicit in the public API"
    )]
    pub fn put(
        &mut self,
        router: &mut impl FabricRouterPort,
        blob_pg: PlacementGroupId,
        metadata_pg: PlacementGroupId,
        request: ClientRequestId,
        bucket: impl Into<String>,
        key: impl Into<String>,
        bytes: Vec<u8>,
    ) -> RsfResult<DurableObjectPut> {
        self.put_durable(
            router,
            blob_pg,
            metadata_pg,
            request,
            bucket.into(),
            key.into(),
            bytes,
        )
    }

    pub fn read_verified(&mut self, bucket: &str, key: &str) -> RsfResult<Vec<u8>> {
        self.journal.ensure_healthy()?;
        validate_bucket(bucket)?;
        validate_key(key)?;
        let head = self
            .heads
            .get(&(bucket.to_string(), key.to_string()))
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("object {bucket}/{key} is not present"),
                )
            })?
            .clone();
        let bytes = self.blobs.read_verified(&head.content)?;
        if bytes.len() as u64 != head.bytes {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "object {bucket}/{key} expected {} bytes but verified {}",
                    head.bytes,
                    bytes.len()
                ),
            ));
        }
        if ContentId::for_bytes(&bytes) != head.content {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("object {bucket}/{key} content id does not match durable payload"),
            ));
        }
        if manifest_digest(bucket, key, &head.content, head.bytes) != head.manifest_digest {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("object {bucket}/{key} manifest digest does not match head record"),
            ));
        }
        Ok(bytes)
    }

    pub fn bucket(&self, bucket: &str) -> Option<&DurableObjectBucket> {
        self.buckets.get(bucket)
    }

    pub fn head(&self, bucket: &str, key: &str) -> Option<&DurableObjectHead> {
        self.heads.get(&(bucket.to_string(), key.to_string()))
    }

    pub fn versions(&self, bucket: &str, key: &str) -> Option<&[DurableObjectHead]> {
        self.versions
            .get(&(bucket.to_string(), key.to_string()))
            .map(Vec::as_slice)
    }

    fn require_bucket(&self, bucket: &str) -> RsfResult<()> {
        if self.buckets.contains_key(bucket) {
            Ok(())
        } else {
            Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown object bucket {bucket}"),
            ))
        }
    }

    fn install_head(&mut self, head: DurableObjectHead) {
        let object_key = (head.bucket.clone(), head.key.clone());
        self.heads.insert(object_key.clone(), head.clone());
        self.versions.entry(object_key).or_default().push(head);
    }

    fn load_index(&mut self) -> RsfResult<()> {
        let path = self.root.join(INDEX_FILE);
        if !path.exists() {
            return Ok(());
        }
        let mut file = File::open(&path).map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "failed to open durable object index {}: {err}",
                    path.display()
                ),
            )
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "failed to read durable object index {}: {err}",
                    path.display()
                ),
            )
        })?;
        let text = String::from_utf8(bytes).map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "durable object index {} is not utf-8: {err}",
                    path.display()
                ),
            )
        })?;
        decode_index(
            &text,
            &mut self.buckets,
            &mut self.heads,
            &mut self.versions,
        )
    }
}

fn encode_index(
    buckets: &BTreeMap<String, DurableObjectBucket>,
    versions: &BTreeMap<(String, String), Vec<DurableObjectHead>>,
) -> String {
    let mut out = String::new();
    out.push_str(FORMAT);
    out.push('\n');
    for bucket in buckets.values() {
        out.push_str(&format!(
            "bucket\t{}\t{}\n",
            hex_bytes(bucket.name.as_bytes()),
            encode_evidence(&bucket.evidence)
        ));
    }
    for history in versions.values() {
        for head in history {
            out.push_str(&format!(
                "head\t{}\t{}\t{}\t{}\t{}\t{}\n",
                hex_bytes(head.bucket.as_bytes()),
                hex_bytes(head.key.as_bytes()),
                hex_bytes(head.content.as_bytes()),
                head.bytes,
                hex_bytes(head.manifest_digest.as_bytes()),
                encode_evidence(&head.evidence)
            ));
        }
    }
    out
}

fn decode_index(
    text: &str,
    buckets: &mut BTreeMap<String, DurableObjectBucket>,
    heads: &mut BTreeMap<(String, String), DurableObjectHead>,
    versions: &mut BTreeMap<(String, String), Vec<DurableObjectHead>>,
) -> RsfResult<()> {
    let mut lines = text.lines();
    match lines.next() {
        Some(FORMAT) => {}
        _ => {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "durable object index has an unsupported format",
            ));
        }
    }

    for (line_number, line) in lines.enumerate() {
        if line.is_empty() {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["bucket", name, evidence] => {
                let name = decode_string(name, "object bucket name")?;
                validate_bucket(&name)?;
                if buckets.contains_key(&name) {
                    return Err(corrupt_line(line_number, "duplicate object bucket"));
                }
                let evidence = decode_evidence(evidence)?;
                journal::validate_evidence(
                    &evidence,
                    &FabricOperation::ObjectBucket {
                        bucket: name.clone(),
                    },
                    CommitConsistency::Strict,
                )?;
                buckets.insert(
                    name.clone(),
                    DurableObjectBucket {
                        name,
                        version: evidence.version,
                        evidence,
                    },
                );
            }
            ["head", bucket, key, content, bytes, manifest, evidence] => {
                let bucket = decode_string(bucket, "object head bucket")?;
                let key = decode_string(key, "object head key")?;
                validate_bucket(&bucket)?;
                validate_key(&key)?;
                if !buckets.contains_key(&bucket) {
                    return Err(corrupt_line(
                        line_number,
                        format!("object head references unknown bucket {bucket}"),
                    ));
                }
                let content = ContentId::from_blake3(hex_decode_32(content)?)
                    .map_err(|err| corrupt_line(line_number, err.message))?;
                let bytes = bytes.parse::<u64>().map_err(|_| {
                    corrupt_line(line_number, "object head byte length is not a u64")
                })?;
                let encoded_manifest_digest = Digest::blake3(hex_decode_32(manifest)?)
                    .map_err(|err| corrupt_line(line_number, err.message))?;
                let evidence = decode_evidence(evidence)?;
                journal::validate_evidence(
                    &evidence,
                    &FabricOperation::ObjectManifest {
                        bucket: bucket.clone(),
                        key: key.clone(),
                    },
                    CommitConsistency::Strict,
                )?;
                if bytes == 0 {
                    return Err(corrupt_line(line_number, "object payload is empty"));
                }
                let expected = manifest_digest(&bucket, &key, &content, bytes);
                if expected != encoded_manifest_digest {
                    return Err(corrupt_line(
                        line_number,
                        "object head manifest digest does not match encoded content",
                    ));
                }
                let head = DurableObjectHead {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version: evidence.version,
                    evidence,
                    content,
                    bytes,
                    manifest_digest: encoded_manifest_digest,
                };
                let object_key = (bucket, key);
                heads.insert(object_key.clone(), head.clone());
                versions.entry(object_key).or_default().push(head);
            }
            _ => {
                return Err(corrupt_line(
                    line_number,
                    "malformed durable object index record",
                ));
            }
        }
    }

    for ((bucket, key), history) in versions {
        let mut previous = CommitVersion::GENESIS;
        for head in history {
            if head.version <= previous {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("object {bucket}/{key} head versions are not strictly increasing"),
                ));
            }
            previous = head.version;
        }
    }
    Ok(())
}

fn encode_evidence(evidence: &CommitEvidence) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        evidence.placement_group.0,
        evidence.log_index.0,
        evidence.version.0,
        match evidence.consistency {
            CommitConsistency::Strict => "strict",
            CommitConsistency::AvailabilityFirst => "ap",
        },
        evidence.operation_id.0
    )
}

fn decode_evidence(value: &str) -> RsfResult<CommitEvidence> {
    let fields = value.split(':').collect::<Vec<_>>();
    let [pg, log, version, consistency, operation] = fields.as_slice() else {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "commit evidence has malformed field count",
        ));
    };
    let placement_group = PlacementGroupId(parse_u64(pg, "placement group")?);
    let log_index = LogIndex(parse_u64(log, "log index")?);
    let version = CommitVersion(parse_u64(version, "commit version")?);
    version.validate_committed()?;
    let consistency = match *consistency {
        "strict" => CommitConsistency::Strict,
        "ap" => CommitConsistency::AvailabilityFirst,
        _ => {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "commit evidence has unknown consistency mode",
            ));
        }
    };
    let operation_id = OperationId::new(parse_u64(operation, "operation id")?)?;
    Ok(CommitEvidence {
        placement_group,
        log_index,
        version,
        consistency,
        operation_id,
    })
}

fn parse_u64(value: &str, label: &str) -> RsfResult<u64> {
    value.parse::<u64>().map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("commit evidence {label} is not a u64"),
        )
    })
}

fn manifest_digest(bucket: &str, key: &str, content: &ContentId, bytes: u64) -> Digest {
    let mut material = Vec::new();
    material.extend_from_slice(bucket.as_bytes());
    material.push(0);
    material.extend_from_slice(key.as_bytes());
    material.push(0);
    material.extend_from_slice(content.as_bytes());
    material.extend_from_slice(&bytes.to_le_bytes());
    Digest::blake3_bytes(&material)
}

fn validate_bucket(bucket: &str) -> RsfResult<()> {
    validate_name(bucket, "object bucket")?;
    if bucket.contains('/') {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "object bucket must not contain '/'",
        ));
    }
    Ok(())
}

fn validate_key(key: &str) -> RsfResult<()> {
    validate_name(key, "object key")
}

fn validate_name(value: &str, label: &str) -> RsfResult<()> {
    if value.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("{label} must not be empty"),
        ));
    }
    if value.len() > 4096 {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("{label} exceeds 4096 bytes"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("{label} contains control characters"),
        ));
    }
    Ok(())
}

fn commit_evidence(certificate: CommitCertificate) -> CommitEvidence {
    CommitEvidence {
        placement_group: certificate.pg,
        log_index: certificate.index,
        version: certificate.version,
        consistency: match certificate.mode {
            rsf_protocol::ConsistencyMode::Strict => CommitConsistency::Strict,
            rsf_protocol::ConsistencyMode::AvailabilityFirst => {
                CommitConsistency::AvailabilityFirst
            }
        },
        operation_id: certificate.operation_id,
    }
}

fn corrupt_line(line_number: usize, reason: impl Into<String>) -> RsfError {
    RsfError::new(
        ErrorCode::CorruptionDetected,
        format!(
            "durable object index line {} is corrupt: {}",
            line_number + 2,
            reason.into()
        ),
    )
}

fn decode_string(value: &str, label: &str) -> RsfResult<String> {
    let bytes = hex_decode(value)?;
    String::from_utf8(bytes).map_err(|err| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("{label} is not utf-8: {err}"),
        )
    })
}

fn hex_decode(value: &str) -> RsfResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "hex field has an odd number of digits",
        ));
    }
    let raw = value.as_bytes();
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.chunks_exact(2) {
        out.push((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?);
    }
    Ok(out)
}

fn hex_decode_32(value: &str) -> RsfResult<[u8; 32]> {
    if value.len() != 64 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "hex digest must contain 64 characters",
        ));
    }
    let bytes = hex_decode(value)?;
    let mut out = [0_u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn hex_nibble(byte: u8) -> RsfResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "hex field contains a non-hex character",
        )),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_protocol::FabricRouter;

    #[test]
    fn durable_object_store_recovers_payload_and_head_history() {
        let path = test_path("durable_object_store_recovers_payload_and_head_history");
        let _ = fs::remove_dir_all(&path);

        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut objects = DurableObjectStore::open(&path).expect("open object store");
        let bucket = objects
            .create_bucket(&mut router, PlacementGroupId(1), "media")
            .expect("create bucket");
        assert_eq!(bucket.version, CommitVersion(1));

        let first = objects
            .put(
                &mut router,
                PlacementGroupId(1),
                PlacementGroupId(1),
                ClientRequestId::new("object-test-put-1", 0),
                "media",
                "hero.txt",
                b"first body".to_vec(),
            )
            .expect("first put");
        assert_eq!(first.head.version, CommitVersion(3));

        let second = objects
            .put(
                &mut router,
                PlacementGroupId(1),
                PlacementGroupId(1),
                ClientRequestId::new("object-test-put-2", 0),
                "media",
                "hero.txt",
                b"second body".to_vec(),
            )
            .expect("replacement put");
        assert!(second.head.version > first.head.version);
        assert_eq!(
            objects.read_verified("media", "hero.txt").unwrap(),
            b"second body"
        );

        drop(objects);
        let mut recovered = DurableObjectStore::open(&path).expect("reopen object store");
        assert_eq!(
            recovered.read_verified("media", "hero.txt").unwrap(),
            b"second body"
        );
        assert_eq!(
            recovered
                .versions("media", "hero.txt")
                .expect("head history")
                .len(),
            2
        );

        let _ = fs::remove_dir_all(&path);
    }

    fn test_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-object-durable-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
