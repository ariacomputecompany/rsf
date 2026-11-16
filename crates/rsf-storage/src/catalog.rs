use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Digest, DigestAlgorithm, Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId,
    RegionId, RsfError, RsfResult, storage::WalRecordKind,
};
use rsf_manifest::{
    ByteRange, ExtentManifest, ManifestChecksumProof, ManifestChecksumReport, ManifestEntry,
    ManifestKind, ManifestObject, ProviderMetadata, ProviderReplica, ProviderStorageClass,
};

use crate::{SegmentScanFilter, SegmentStore, WalAppend};

const MANIFEST_PG: PlacementGroupId = PlacementGroupId(63);
const MANIFEST_WAL_MAGIC: &[u8; 7] = b"RSFMFT1";
const MANIFEST_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedManifest {
    pub id: String,
    pub manifest: ExtentManifest,
    pub checksum_report: ManifestChecksumReport,
}

#[derive(Debug)]
pub struct DurableManifestCatalog {
    manifests: BTreeMap<String, PublishedManifest>,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableManifestCatalog {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(MANIFEST_PG),
        })?;
        let mut manifests = BTreeMap::new();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Manifest)
        {
            apply_manifest_record(
                &mut manifests,
                decode_manifest_wal_payload(&record.payload)?,
            )?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            manifests,
            store,
            epoch,
            next_index,
        })
    }

    pub fn publish(
        &mut self,
        id: impl Into<String>,
        entries: Vec<ManifestEntry>,
        proofs: Vec<ManifestChecksumProof>,
    ) -> RsfResult<PublishedManifest> {
        let published = build_published_manifest(id.into(), entries, proofs)?;
        if let Some(existing) = self.manifests.get(&published.id) {
            if existing == &published {
                return Ok(existing.clone());
            }
            return Err(invalid(format!(
                "manifest id {} already exists with different content",
                published.id
            )));
        }

        self.append_record(&ManifestWalRecord::Published {
            manifest: published.clone(),
        })?;
        self.manifests
            .insert(published.id.clone(), published.clone());
        Ok(published)
    }

    pub fn get(&self, id: &str) -> Option<&PublishedManifest> {
        self.manifests.get(id)
    }

    pub fn manifests(&self) -> Vec<PublishedManifest> {
        self.manifests.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.manifests.len()
    }

    pub fn is_empty(&self) -> bool {
        self.manifests.is_empty()
    }

    fn append_record(&mut self, record: &ManifestWalRecord) -> RsfResult<()> {
        let payload = encode_manifest_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let previous_hash = recovery
            .pg_tail(MANIFEST_PG)
            .map_or([0_u8; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: MANIFEST_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::Manifest,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ManifestWalRecord {
    Published { manifest: PublishedManifest },
}

fn apply_manifest_record(
    manifests: &mut BTreeMap<String, PublishedManifest>,
    record: ManifestWalRecord,
) -> RsfResult<()> {
    match record {
        ManifestWalRecord::Published { manifest } => {
            validate_published_manifest(&manifest)?;
            if manifests
                .insert(manifest.id.clone(), manifest.clone())
                .is_some()
            {
                return Err(corrupt(format!(
                    "manifest catalog replay found duplicate manifest id {}",
                    manifest.id
                )));
            }
        }
    }
    Ok(())
}

fn build_published_manifest(
    id: String,
    entries: Vec<ManifestEntry>,
    proofs: Vec<ManifestChecksumProof>,
) -> RsfResult<PublishedManifest> {
    validate_manifest_id(&id)?;
    let manifest = ExtentManifest::new(entries)?;
    let checksum_report = manifest.verify_checksums(&proofs)?;
    Ok(PublishedManifest {
        id,
        manifest,
        checksum_report,
    })
}

fn validate_published_manifest(published: &PublishedManifest) -> RsfResult<()> {
    validate_manifest_id(&published.id)?;
    let report = verify_embedded_manifest_checksums(published.manifest.entries())?;
    if report != published.checksum_report {
        return Err(corrupt(format!(
            "manifest {} checksum report does not match entries",
            published.id
        )));
    }
    Ok(())
}

fn verify_embedded_manifest_checksums(
    entries: &[ManifestEntry],
) -> RsfResult<ManifestChecksumReport> {
    let proofs = entries
        .iter()
        .map(|entry| ManifestChecksumProof::new(entry.extent_id, entry.checksum, entry.range.len))
        .collect::<Vec<_>>();
    ExtentManifest::new(entries.to_vec())?.verify_checksums(&proofs)
}

fn validate_manifest_id(id: &str) -> RsfResult<()> {
    if id.trim().is_empty() {
        return Err(invalid("manifest id must not be empty"));
    }
    Ok(())
}

fn encode_manifest_wal_payload(record: &ManifestWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(MANIFEST_WAL_MAGIC);
    out.push(MANIFEST_WAL_VERSION);
    match record {
        ManifestWalRecord::Published { manifest } => {
            validate_published_manifest(manifest)?;
            out.push(1);
            encode_published_manifest(&mut out, manifest)?;
        }
    }
    Ok(out)
}

fn decode_manifest_wal_payload(bytes: &[u8]) -> RsfResult<ManifestWalRecord> {
    if bytes.len() < MANIFEST_WAL_MAGIC.len() + 2 {
        return Err(corrupt("manifest WAL payload is too short"));
    }
    if &bytes[..MANIFEST_WAL_MAGIC.len()] != MANIFEST_WAL_MAGIC {
        return Err(corrupt("manifest WAL magic mismatch"));
    }
    let mut offset = MANIFEST_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != MANIFEST_WAL_VERSION {
        return Err(corrupt("unsupported manifest WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => ManifestWalRecord::Published {
            manifest: decode_published_manifest(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("unknown manifest WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in manifest WAL payload"));
    }
    Ok(record)
}

fn encode_published_manifest(out: &mut Vec<u8>, published: &PublishedManifest) -> RsfResult<()> {
    write_string(out, &published.id)?;
    write_len(out, published.checksum_report.checked)?;
    write_u64(out, published.checksum_report.total_bytes);
    write_len(out, published.manifest.entries().len())?;
    for entry in published.manifest.entries() {
        encode_entry(out, entry)?;
    }
    Ok(())
}

fn decode_published_manifest(bytes: &[u8], offset: &mut usize) -> RsfResult<PublishedManifest> {
    let id = read_string(bytes, offset)?;
    let checked = read_len(bytes, offset)?;
    let total_bytes = read_u64(bytes, offset)?;
    let entry_len = read_len(bytes, offset)?;
    let mut entries = Vec::with_capacity(entry_len);
    for _ in 0..entry_len {
        entries.push(decode_entry(bytes, offset)?);
    }
    let published = PublishedManifest {
        id,
        manifest: ExtentManifest::new(entries)?,
        checksum_report: ManifestChecksumReport {
            checked,
            total_bytes,
        },
    };
    validate_published_manifest(&published)?;
    Ok(published)
}

fn encode_entry(out: &mut Vec<u8>, entry: &ManifestEntry) -> RsfResult<()> {
    out.push(manifest_kind_tag(entry.kind));
    write_string(out, &entry.namespace)?;
    write_string(out, &entry.object)?;
    write_u64(out, entry.version.0);
    encode_digest(out, entry.extent_id);
    write_u64(out, entry.placement_group.0);
    write_u64(out, entry.range.offset);
    write_u64(out, entry.range.len);
    encode_digest(out, entry.checksum);
    encode_provider(out, &entry.provider)?;
    Ok(())
}

fn decode_entry(bytes: &[u8], offset: &mut usize) -> RsfResult<ManifestEntry> {
    let kind = manifest_kind_from_tag(read_u8(bytes, offset)?)?;
    let namespace = read_string(bytes, offset)?;
    let object = read_string(bytes, offset)?;
    let version = CommitVersion(read_u64(bytes, offset)?);
    let extent_id = decode_digest(bytes, offset)?;
    let placement_group = PlacementGroupId(read_u64(bytes, offset)?);
    let range = ByteRange::new(read_u64(bytes, offset)?, read_u64(bytes, offset)?);
    let checksum = decode_digest(bytes, offset)?;
    let provider = decode_provider(bytes, offset)?;
    Ok(ManifestEntry::new(
        kind,
        ManifestObject::new(namespace, object),
        version,
        extent_id,
        placement_group,
        range,
        checksum,
    )
    .with_provider_metadata(provider))
}

fn encode_provider(out: &mut Vec<u8>, provider: &ProviderMetadata) -> RsfResult<()> {
    write_string(out, &provider.provider_id)?;
    write_len(out, provider.min_replicas)?;
    write_len(out, provider.replicas.len())?;
    for replica in &provider.replicas {
        write_u64(out, replica.node.0);
        write_string(out, replica.region.as_str())?;
        out.push(storage_class_tag(replica.storage_class));
    }
    Ok(())
}

fn decode_provider(bytes: &[u8], offset: &mut usize) -> RsfResult<ProviderMetadata> {
    let provider_id = read_string(bytes, offset)?;
    let min_replicas = read_len(bytes, offset)?;
    let replica_len = read_len(bytes, offset)?;
    let mut replicas = Vec::with_capacity(replica_len);
    for _ in 0..replica_len {
        replicas.push(ProviderReplica::new(
            NodeId(read_u64(bytes, offset)?),
            RegionId::new(read_string(bytes, offset)?)?,
            storage_class_from_tag(read_u8(bytes, offset)?)?,
        ));
    }
    Ok(ProviderMetadata::new(provider_id, min_replicas, replicas))
}

fn manifest_kind_tag(kind: ManifestKind) -> u8 {
    match kind {
        ManifestKind::RelationalPage => 1,
        ManifestKind::RelationalWal => 2,
        ManifestKind::OrderedKvSstable => 3,
        ManifestKind::ObjectPart => 4,
        ManifestKind::BlobChunk => 5,
        ManifestKind::SnapshotRoot => 6,
        ManifestKind::BackupBlock => 7,
    }
}

fn manifest_kind_from_tag(tag: u8) -> RsfResult<ManifestKind> {
    match tag {
        1 => Ok(ManifestKind::RelationalPage),
        2 => Ok(ManifestKind::RelationalWal),
        3 => Ok(ManifestKind::OrderedKvSstable),
        4 => Ok(ManifestKind::ObjectPart),
        5 => Ok(ManifestKind::BlobChunk),
        6 => Ok(ManifestKind::SnapshotRoot),
        7 => Ok(ManifestKind::BackupBlock),
        _ => Err(corrupt("unknown manifest kind tag")),
    }
}

fn storage_class_tag(storage_class: ProviderStorageClass) -> u8 {
    match storage_class {
        ProviderStorageClass::Hot => 1,
        ProviderStorageClass::Warm => 2,
        ProviderStorageClass::Cold => 3,
        ProviderStorageClass::Archive => 4,
    }
}

fn storage_class_from_tag(tag: u8) -> RsfResult<ProviderStorageClass> {
    match tag {
        1 => Ok(ProviderStorageClass::Hot),
        2 => Ok(ProviderStorageClass::Warm),
        3 => Ok(ProviderStorageClass::Cold),
        4 => Ok(ProviderStorageClass::Archive),
        _ => Err(corrupt("unknown manifest storage class tag")),
    }
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
        _ => return Err(corrupt("unknown manifest digest algorithm tag")),
    };
    let end = offset.saturating_add(32);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of manifest digest"))?;
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
        .ok_or_else(|| corrupt("unexpected end of manifest WAL payload"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let end = offset.saturating_add(4);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of manifest WAL payload"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("slice length already checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let end = offset.saturating_add(8);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of manifest WAL payload"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("slice length already checked"),
    ))
}

fn write_len(out: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| invalid("manifest WAL collection length exceeds encoding limit"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    usize::try_from(read_u32(bytes, offset)?)
        .map_err(|_| corrupt("manifest WAL collection length is unsupported"))
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    write_len(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_len(bytes, offset)?;
    let end = offset.saturating_add(len);
    let slice = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("unexpected end of manifest WAL string"))?;
    *offset = end;
    String::from_utf8(slice.to_vec()).map_err(|_| corrupt("manifest WAL string is not valid UTF-8"))
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
    fn durable_manifest_catalog_recovers_published_manifest_after_reopen() {
        let path = test_path("recovers_published_manifest_after_reopen");
        let published;
        {
            let mut catalog = DurableManifestCatalog::open(&path, Epoch(11)).expect("catalog");
            let entries = entries();
            published = catalog
                .publish(
                    "manifest/app/checkpoint-22",
                    entries.clone(),
                    checksum_proofs(&entries),
                )
                .expect("publish");
        }

        let reopened = DurableManifestCatalog::open(&path, Epoch(11)).expect("reopen");
        let recovered = reopened
            .get("manifest/app/checkpoint-22")
            .expect("published manifest");

        assert_eq!(reopened.len(), 1);
        assert_eq!(recovered, &published);
        assert_eq!(recovered.manifest.entries().len(), 3);
        assert_eq!(recovered.checksum_report.checked, 3);
    }

    #[test]
    fn durable_manifest_catalog_rejected_publish_does_not_persist() {
        let path = test_path("rejected_publish_does_not_persist");
        {
            let mut catalog = DurableManifestCatalog::open(&path, Epoch(11)).expect("catalog");
            let entries = entries();
            let mut proofs = checksum_proofs(&entries);
            proofs[0].checksum = Digest::blake3_bytes(b"wrong checksum");
            let err = catalog
                .publish("manifest/app/checkpoint-22", entries, proofs)
                .expect_err("bad checksum rejected");
            assert_eq!(err.code, ErrorCode::InvalidInput);
        }

        let reopened = DurableManifestCatalog::open(&path, Epoch(11)).expect("reopen");

        assert!(reopened.is_empty());
    }

    #[test]
    fn durable_manifest_catalog_duplicate_id_must_match_existing_content() {
        let path = test_path("duplicate_id_must_match_existing_content");
        let mut catalog = DurableManifestCatalog::open(&path, Epoch(11)).expect("catalog");
        let entries = entries();
        let first = catalog
            .publish(
                "manifest/app/checkpoint-22",
                entries.clone(),
                checksum_proofs(&entries),
            )
            .expect("first");
        let duplicate = catalog
            .publish(
                "manifest/app/checkpoint-22",
                entries.clone(),
                checksum_proofs(&entries),
            )
            .expect("idempotent duplicate");

        let mut changed = entries;
        changed[0].range = ByteRange::new(0, 512);
        let err = catalog
            .publish(
                "manifest/app/checkpoint-22",
                changed.clone(),
                checksum_proofs(&changed),
            )
            .expect_err("conflicting duplicate rejected");

        assert_eq!(first, duplicate);
        assert_eq!(catalog.len(), 1);
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("different content"));
    }

    #[test]
    fn durable_manifest_catalog_replay_rejects_malformed_payload() {
        let path = test_path("replay_rejects_malformed_payload");
        append_raw_manifest_record(&path, b"not-a-manifest-payload".to_vec(), 1, [0; 32]);

        let err = DurableManifestCatalog::open(&path, Epoch(11))
            .expect_err("malformed payload rejected during replay");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("magic"));
    }

    #[test]
    fn durable_manifest_catalog_replay_rejects_duplicate_manifest_id() {
        let path = test_path("replay_rejects_duplicate_manifest_id");
        let entries = entries();
        let published = build_published_manifest(
            "manifest/app/checkpoint-22".to_string(),
            entries.clone(),
            checksum_proofs(&entries),
        )
        .expect("published");
        let payload = encode_manifest_wal_payload(&ManifestWalRecord::Published {
            manifest: published,
        })
        .expect("payload");
        let (_, first_hash) = append_raw_manifest_record(&path, payload.clone(), 1, [0; 32]);
        append_raw_manifest_record(&path, payload, 2, first_hash);

        let err = DurableManifestCatalog::open(&path, Epoch(11))
            .expect_err("duplicate manifest id rejected during replay");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("duplicate manifest id"));
    }

    fn entries() -> Vec<ManifestEntry> {
        vec![
            entry(
                ManifestKind::RelationalPage,
                "sql/app/users/page-0",
                1,
                0,
                4096,
            ),
            entry(
                ManifestKind::OrderedKvSstable,
                "kv/app/config/run-7",
                2,
                0,
                8192,
            ),
            entry(ManifestKind::BlobChunk, "blob/cid-hero/chunk-0", 3, 0, 1024),
        ]
    }

    fn entry(kind: ManifestKind, object: &str, seed: u8, offset: u64, len: u64) -> ManifestEntry {
        ManifestEntry::new(
            kind,
            ManifestObject::new("app", object),
            CommitVersion(22),
            Digest::blake3_bytes(format!("extent:{object}:{seed}").as_bytes()),
            PlacementGroupId(u64::from(seed % 3) + 1),
            ByteRange::new(offset, len),
            Digest::blake3_bytes(format!("checksum:{object}:{seed}:{len}").as_bytes()),
        )
        .with_provider_metadata(ProviderMetadata::new(
            "local-replicated",
            2,
            vec![
                ProviderReplica::new(
                    NodeId(1),
                    RegionId::new("iad").expect("region"),
                    ProviderStorageClass::Hot,
                ),
                ProviderReplica::new(
                    NodeId(2),
                    RegionId::new("sfo").expect("region"),
                    ProviderStorageClass::Warm,
                ),
            ],
        ))
    }

    fn checksum_proofs(entries: &[ManifestEntry]) -> Vec<ManifestChecksumProof> {
        entries
            .iter()
            .map(|entry| {
                ManifestChecksumProof::new(entry.extent_id, entry.checksum, entry.range.len)
            })
            .collect()
    }

    fn append_raw_manifest_record(
        path: &Path,
        payload: Vec<u8>,
        index: u64,
        previous_hash: [u8; 32],
    ) -> ([u8; 32], [u8; 32]) {
        let mut store = SegmentStore::open(path).expect("segment store");
        let (_, _, record) = store
            .append_wal(WalAppend {
                pg: MANIFEST_PG,
                epoch: Epoch(11),
                index: LogIndex(index),
                kind: WalRecordKind::Manifest,
                previous_hash,
                payload: &payload,
            })
            .expect("append raw manifest record");
        (record.payload_hash, record.record_hash)
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-manifest-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
