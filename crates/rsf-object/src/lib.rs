#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_blob::BlobEngine;
use rsf_chunkstore::{ChunkClass, ChunkDecision, ChunkStore, ChunkWrite, read_bytes_verified};
use rsf_core::{
    CommitConsistency, CommitEvidence, CommitVersion, ErrorCode, NodeId, PlacementGroupId,
    RegionId, RsfError, RsfResult,
    blob::ContentId,
    object::{BucketName, ObjectKey, ObjectManifest, ObjectPart, ObjectVersion, ObjectVersionKind},
};
use rsf_erasure::{ErasurePlanner, ErasurePolicy, ObjectStripe, ShardKind, ShardManifest};
use rsf_protocol::{
    ClientRequestId, CommitCertificate, ConsistencyMode, FabricOperation, FabricRouterPort,
};
use rsf_provider::{
    ChunkSurface, ProviderDecision, ProviderDirectory, ProviderQuery, ProviderRecord,
};
use rsf_retention::{RetainedObject, RetentionEngine, RetentionPolicy};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bucket {
    pub name: String,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultipartUploadState {
    Open,
    Completed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartUpload {
    id: u64,
    bucket: String,
    key: String,
    parts: Vec<StoredObjectPart>,
    state: MultipartUploadState,
    completed_version: Option<CommitVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredObjectPart {
    pub number: u16,
    pub offset: u64,
    pub len: u64,
    pub content: ContentId,
    pub chunk_pins: u32,
    pub providers: Vec<ProviderRecord>,
    pub pin_version: CommitVersion,
    pub pin_evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColdObjectLayout {
    pub content: ContentId,
    pub policy: ErasurePolicy,
    pub original_len: usize,
    pub shards: Vec<ColdObjectShard>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColdObjectShard {
    pub ordinal: u8,
    pub kind: ShardKind,
    pub node: NodeId,
    pub content: ContentId,
    pub bytes: u64,
    pub checksum: rsf_core::Digest,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectEngine {
    buckets: BTreeMap<String, Bucket>,
    objects: BTreeMap<(String, String), ObjectVersion>,
    versions: BTreeMap<(String, String), Vec<ObjectVersion>>,
    blobs: BlobEngine,
    chunks: ChunkStore,
    providers: ProviderDirectory,
    uploads: BTreeMap<u64, MultipartUpload>,
    cold_layouts: BTreeMap<ContentId, ColdObjectLayout>,
    next_upload_id: u64,
}

impl ObjectEngine {
    pub fn create_bucket(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
    ) -> RsfResult<Bucket> {
        let bucket = bucket.into();
        if let Some(existing) = self.buckets.get(&bucket) {
            return Ok(existing.clone());
        }

        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(format!("object:bucket:{bucket}"), 0),
            FabricOperation::ObjectBucket {
                bucket: bucket.clone(),
            },
        )?;
        let bucket_record = Bucket {
            name: bucket.clone(),
            version: commit.certificate.version,
            evidence: commit_evidence(commit.certificate),
        };
        self.buckets.insert(bucket, bucket_record.clone());
        Ok(bucket_record)
    }

    pub fn put(
        &mut self,
        router: &mut impl FabricRouterPort,
        blob_pg: PlacementGroupId,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
        key: impl Into<String>,
        bytes: Vec<u8>,
    ) -> RsfResult<ObjectVersion> {
        let bucket = bucket.into();
        let key = key.into();
        self.require_bucket(&bucket)?;
        let upload = self.begin_multipart(bucket, key)?;
        let _ = self.upload_part(router, blob_pg, upload.id, bytes)?;
        self.complete_multipart(router, metadata_pg, upload.id)
    }

    pub fn put_cold_erasure(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
        key: impl Into<String>,
        bytes: Vec<u8>,
        policy: ErasurePolicy,
        nodes: &[NodeId],
    ) -> RsfResult<ObjectVersion> {
        let bucket = bucket.into();
        let key = key.into();
        self.require_bucket(&bucket)?;
        if bytes.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "cold object payload must not be empty",
            ));
        }
        self.validate_expected_object_head(&bucket, &key, None)?;

        let content = ContentId::for_bytes(&bytes);
        let planner = ErasurePlanner::new(policy);
        let stripe = planner.stripe(&bytes, nodes)?;
        let layout = self.store_cold_stripe(&bucket, &key, content.clone(), stripe)?;

        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(format!("object:cold-manifest:{bucket}:{key}"), 0),
            FabricOperation::ObjectManifest {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        )?;
        let version_number = commit.certificate.version;
        let version = ObjectVersion {
            bucket: BucketName(bucket.clone()),
            key: ObjectKey(key.clone()),
            kind: ObjectVersionKind::Live,
            version: version_number,
            evidence: commit_evidence(commit.certificate),
            manifest: ObjectManifest {
                parts: vec![ObjectPart {
                    offset: 0,
                    len: layout.original_len as u64,
                    content,
                }],
                metadata: vec![
                    ("storage-class".to_string(), b"cold-erasure".to_vec()),
                    (
                        "erasure-policy".to_string(),
                        format!("{}+{}", policy.data, policy.parity).into_bytes(),
                    ),
                ],
            },
        };
        self.install_version(bucket, key, version.clone());
        Ok(version)
    }

    pub fn begin_multipart(
        &mut self,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> RsfResult<MultipartUpload> {
        let bucket = bucket.into();
        self.require_bucket(&bucket)?;

        let upload = MultipartUpload {
            id: self.next_upload_id,
            bucket,
            key: key.into(),
            parts: Vec::new(),
            state: MultipartUploadState::Open,
            completed_version: None,
        };
        self.next_upload_id += 1;
        self.uploads.insert(upload.id, upload.clone());
        Ok(upload)
    }

    pub fn upload_part(
        &mut self,
        router: &mut impl FabricRouterPort,
        blob_pg: PlacementGroupId,
        upload_id: u64,
        bytes: Vec<u8>,
    ) -> RsfResult<StoredObjectPart> {
        let Some(upload) = self.uploads.get(&upload_id) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown multipart upload {upload_id}"),
            ));
        };
        if upload.state != MultipartUploadState::Open {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("multipart upload {upload_id} is not open"),
            ));
        }
        if bytes.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "object part payload must not be empty",
            ));
        }
        let bucket = upload.bucket.clone();
        let key = upload.key.clone();
        let number = u16::try_from(upload.parts.len() + 1).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "multipart upload has too many object parts",
            )
        })?;
        let offset = upload
            .parts
            .last()
            .map(|part| part.offset + part.len)
            .unwrap_or(0);
        let blob = self.blobs.put(router, blob_pg, bytes.clone())?;
        blob.verify()?;
        let chunk = match self.chunks.put(ChunkWrite::new(
            ChunkClass::ObjectBody,
            format!("{bucket}/{key}"),
            bytes,
            true,
        )) {
            ChunkDecision::Stored { id, pins, .. } => {
                if id != blob.id {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "object body chunk id does not match blob content id",
                    ));
                }
                (id, pins)
            }
            ChunkDecision::Quarantined { reason, .. } | ChunkDecision::Rejected { reason } => {
                return Err(RsfError::new(ErrorCode::CorruptionDetected, reason));
            }
            ChunkDecision::ReadVerified { .. }
            | ChunkDecision::Pinned { .. }
            | ChunkDecision::Released { .. } => {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    "unexpected chunkstore decision while storing object body",
                ));
            }
        };
        self.advertise_local_object_body_provider(&chunk.0)?;
        let providers = self.select_object_body_providers(&chunk.0)?;
        let part = StoredObjectPart {
            number,
            offset,
            len: blob.bytes.len() as u64,
            content: blob.id,
            chunk_pins: chunk.1,
            providers,
            pin_version: blob.pin_version,
            pin_evidence: blob.pin_evidence,
        };
        let upload = self
            .uploads
            .get_mut(&upload_id)
            .expect("upload existence was checked before object body placement");
        upload.parts.push(part.clone());
        Ok(part)
    }

    pub fn complete_multipart(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        upload_id: u64,
    ) -> RsfResult<ObjectVersion> {
        self.complete_multipart_with_expected(router, metadata_pg, upload_id, None)
    }

    pub fn complete_multipart_with_expected(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        upload_id: u64,
        expected_current: Option<CommitVersion>,
    ) -> RsfResult<ObjectVersion> {
        let Some(upload) = self.uploads.get(&upload_id) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown multipart upload {upload_id}"),
            ));
        };
        if upload.state != MultipartUploadState::Open {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("multipart upload {upload_id} is not open"),
            ));
        }
        if upload.parts.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "multipart upload has no parts",
            ));
        }

        let mut offset = 0;
        let bucket = upload.bucket.clone();
        let key = upload.key.clone();
        self.validate_expected_object_head(&bucket, &key, expected_current)?;
        let parts = upload
            .parts
            .iter()
            .map(|stored| {
                let part = ObjectPart {
                    offset,
                    len: stored.len,
                    content: stored.content.clone(),
                };
                offset += part.len;
                part
            })
            .collect::<Vec<_>>();
        let manifest = ObjectManifest {
            parts,
            metadata: Vec::new(),
        };
        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(format!("object:manifest:{bucket}:{key}:{upload_id}"), 0),
            FabricOperation::ObjectManifest {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        )?;
        let version_number = commit.certificate.version;
        let version = ObjectVersion {
            bucket: BucketName(bucket.clone()),
            key: ObjectKey(key.clone()),
            kind: ObjectVersionKind::Live,
            version: version_number,
            evidence: commit_evidence(commit.certificate),
            manifest,
        };
        self.install_version(bucket, key, version.clone());
        let upload = self
            .uploads
            .get_mut(&upload_id)
            .expect("upload existence was checked before manifest commit");
        upload.state = MultipartUploadState::Completed;
        upload.completed_version = Some(version_number);
        Ok(version)
    }

    pub fn delete_object_with_expected(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
        key: impl Into<String>,
        expected_current: CommitVersion,
    ) -> RsfResult<ObjectVersion> {
        let bucket = bucket.into();
        let key = key.into();
        self.require_bucket(&bucket)?;
        self.validate_expected_object_head(&bucket, &key, Some(expected_current))?;

        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(
                format!("object:delete-marker:{bucket}:{key}:{}", expected_current.0),
                0,
            ),
            FabricOperation::ObjectHead {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        )?;
        let version_number = commit.certificate.version;
        let version = ObjectVersion {
            bucket: BucketName(bucket.clone()),
            key: ObjectKey(key.clone()),
            kind: ObjectVersionKind::DeleteMarker,
            version: version_number,
            evidence: commit_evidence(commit.certificate),
            manifest: ObjectManifest {
                parts: Vec::new(),
                metadata: vec![(
                    "delete-marker".to_string(),
                    version_number.0.to_le_bytes().to_vec(),
                )],
            },
        };
        self.install_version(bucket, key, version.clone());
        Ok(version)
    }

    pub fn delete_object_with_retention(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        policy: &RetentionPolicy,
        bucket: impl Into<String>,
        key: impl Into<String>,
        expected_current: CommitVersion,
    ) -> RsfResult<ObjectVersion> {
        let bucket = bucket.into();
        let key = key.into();
        let current = self.get(&bucket, &key).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("object {bucket}/{key} is not present"),
            )
        })?;
        RetentionEngine.authorize_object_delete(
            policy,
            &RetainedObject {
                bucket: bucket.clone(),
                key: key.clone(),
                version: current.version,
                current: true,
            },
        )?;
        self.delete_object_with_expected(router, metadata_pg, bucket, key, expected_current)
    }

    fn install_version(&mut self, bucket: String, key: String, version: ObjectVersion) {
        let object_key = (bucket, key);
        self.objects.insert(object_key.clone(), version.clone());
        self.versions.entry(object_key).or_default().push(version);
    }

    fn validate_expected_object_head(
        &self,
        bucket: &str,
        key: &str,
        expected_current: Option<CommitVersion>,
    ) -> RsfResult<()> {
        let current = self.objects.get(&(bucket.to_string(), key.to_string()));
        match (current, expected_current) {
            (None, None) => Ok(()),
            (None, Some(version)) => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "object {bucket}/{key} expected version {} but no current head exists",
                    version.0
                ),
            )),
            (Some(version), None) => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "object {bucket}/{key} already exists at version {}; expected version required",
                    version.version.0
                ),
            )),
            (Some(version), Some(expected)) if version.version == expected => Ok(()),
            (Some(version), Some(expected)) => Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "stale object head update for {bucket}/{key}: expected version {} but current is {}",
                    expected.0, version.version.0
                ),
            )),
        }
    }

    pub fn bucket(&self, bucket: &str) -> Option<&Bucket> {
        self.buckets.get(bucket)
    }

    pub fn get(&self, bucket: &str, key: &str) -> Option<&ObjectVersion> {
        self.objects.get(&(bucket.to_string(), key.to_string()))
    }

    pub fn versions(&self, bucket: &str, key: &str) -> Option<&[ObjectVersion]> {
        self.versions
            .get(&(bucket.to_string(), key.to_string()))
            .map(Vec::as_slice)
    }

    pub fn multipart_upload(&self, upload_id: u64) -> Option<&MultipartUpload> {
        self.uploads.get(&upload_id)
    }

    pub fn object_parts(&self, upload_id: u64) -> Option<&[StoredObjectPart]> {
        Some(&self.uploads.get(&upload_id)?.parts)
    }

    pub fn blob_pin_version(&self, bucket: &str, key: &str) -> Option<CommitVersion> {
        let version = self.get(bucket, key)?;
        if version.kind != ObjectVersionKind::Live {
            return None;
        }
        let part = version.manifest.parts.first()?;
        self.blobs.get(&part.content).map(|blob| blob.pin_version)
    }

    pub fn read_object(&self, bucket: &str, key: &str) -> RsfResult<Vec<u8>> {
        let Some(version) = self.get(bucket, key) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("object {bucket}/{key} is not present"),
            ));
        };
        if version.kind == ObjectVersionKind::DeleteMarker {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "object {bucket}/{key} is deleted at version {}",
                    version.version.0
                ),
            ));
        }

        let mut bytes = Vec::new();
        for part in &version.manifest.parts {
            if let Some(layout) = self.cold_layouts.get(&part.content) {
                bytes.extend(self.read_cold_part(layout)?);
            } else {
                bytes.extend(read_bytes_verified(&self.chunks, &part.content)?);
            }
        }
        Ok(bytes)
    }

    pub fn body_chunk(&self, id: &ContentId) -> Option<&rsf_chunkstore::StoredChunk> {
        self.chunks.get(id)
    }

    pub fn cold_layout(&self, id: &ContentId) -> Option<&ColdObjectLayout> {
        self.cold_layouts.get(id)
    }

    pub fn quarantine_cold_shard(
        &mut self,
        content: &ContentId,
        ordinal: u8,
        reason: impl Into<String>,
    ) -> RsfResult<()> {
        let layout = self.cold_layouts.get(content).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "cold erasure layout is not present for content id",
            )
        })?;
        let shard = layout
            .shards
            .iter()
            .find(|shard| shard.ordinal == ordinal)
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("cold erasure shard ordinal {ordinal} is not present"),
                )
            })?;
        match self.chunks.quarantine(&shard.content, reason) {
            ChunkDecision::Quarantined { .. } => Ok(()),
            ChunkDecision::Rejected { reason } => {
                Err(RsfError::new(ErrorCode::InvalidInput, reason))
            }
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected chunkstore decision while quarantining cold shard {other:?}"),
            )),
        }
    }

    fn store_cold_stripe(
        &mut self,
        bucket: &str,
        key: &str,
        content: ContentId,
        stripe: ObjectStripe,
    ) -> RsfResult<ColdObjectLayout> {
        let mut shards = Vec::with_capacity(stripe.shards.len());
        for shard in stripe.shards {
            shard.verify()?;
            let stored = match self.chunks.put(ChunkWrite::new(
                ChunkClass::ObjectBody,
                format!("{bucket}/{key}/cold-shard-{}", shard.ordinal),
                shard.bytes,
                true,
            )) {
                ChunkDecision::Stored {
                    id, bytes, pins, ..
                } => {
                    if pins == 0 {
                        return Err(RsfError::new(
                            ErrorCode::CorruptionDetected,
                            "cold erasure shard was stored without a pin",
                        ));
                    }
                    let checksum = id.digest()?;
                    if checksum != shard.checksum {
                        return Err(RsfError::new(
                            ErrorCode::CorruptionDetected,
                            "cold erasure shard checksum does not match stored content id",
                        ));
                    }
                    (id, bytes, checksum)
                }
                ChunkDecision::Quarantined { reason, .. } | ChunkDecision::Rejected { reason } => {
                    return Err(RsfError::new(ErrorCode::CorruptionDetected, reason));
                }
                ChunkDecision::ReadVerified { .. }
                | ChunkDecision::Pinned { .. }
                | ChunkDecision::Released { .. } => {
                    return Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        "unexpected chunkstore decision while storing cold erasure shard",
                    ));
                }
            };
            shards.push(ColdObjectShard {
                ordinal: shard.ordinal,
                kind: shard.kind,
                node: shard.node,
                content: stored.0,
                bytes: stored.1,
                checksum: stored.2,
            });
        }

        let layout = ColdObjectLayout {
            content: content.clone(),
            policy: stripe.policy,
            original_len: stripe.original_len,
            shards,
        };
        self.cold_layouts.insert(content, layout.clone());
        Ok(layout)
    }

    fn read_cold_part(&self, layout: &ColdObjectLayout) -> RsfResult<Vec<u8>> {
        let mut shards = Vec::with_capacity(layout.shards.len());
        let mut missing = Vec::new();
        for shard in &layout.shards {
            match read_bytes_verified(&self.chunks, &shard.content) {
                Ok(bytes) => {
                    let manifest = ShardManifest {
                        ordinal: shard.ordinal,
                        kind: shard.kind,
                        node: shard.node,
                        bytes,
                        checksum: shard.checksum,
                    };
                    if manifest.verify().is_ok() {
                        shards.push(manifest);
                    } else {
                        missing.push(shard.ordinal);
                    }
                }
                Err(err)
                    if err.code == ErrorCode::InvalidInput
                        || err.code == ErrorCode::CorruptionDetected =>
                {
                    missing.push(shard.ordinal);
                }
                Err(err) => return Err(err),
            }
        }
        missing.sort_unstable();
        missing.dedup();
        let planner = ErasurePlanner::new(layout.policy);
        let payload = planner.reconstruct(
            &ObjectStripe {
                policy: layout.policy,
                original_len: layout.original_len,
                shards,
            },
            &missing,
        )?;
        if ContentId::for_bytes(&payload) != layout.content {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "cold erasure reconstruction did not match object body content id",
            ));
        }
        Ok(payload)
    }

    fn require_bucket(&self, bucket: &str) -> RsfResult<()> {
        if !self.buckets.contains_key(bucket) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("unknown bucket {bucket}"),
            ));
        }
        Ok(())
    }

    fn advertise_local_object_body_provider(&mut self, id: &ContentId) -> RsfResult<()> {
        let record = ProviderRecord::new(
            provider_content_id(id),
            ChunkSurface::Object,
            NodeId(0),
            RegionId::new("local")?,
            1,
        );
        match self.providers.advertise(record) {
            Ok(()) => Ok(()),
            Err(reason) if reason.starts_with("duplicate provider") => Ok(()),
            Err(reason) => Err(RsfError::new(ErrorCode::InvalidInput, reason)),
        }
    }

    fn select_object_body_providers(&self, id: &ContentId) -> RsfResult<Vec<ProviderRecord>> {
        match self.providers.select(&ProviderQuery::new(
            provider_content_id(id),
            RegionId::new("local")?,
            1,
            3,
        )) {
            ProviderDecision::Selected(selection) | ProviderDecision::Degraded(selection) => {
                Ok(selection.providers)
            }
            ProviderDecision::Rejected { reason, .. } => {
                Err(RsfError::new(ErrorCode::InvalidInput, reason))
            }
        }
    }
}

fn commit_evidence(certificate: CommitCertificate) -> CommitEvidence {
    CommitEvidence {
        placement_group: certificate.pg,
        log_index: certificate.index,
        version: certificate.version,
        consistency: match certificate.mode {
            ConsistencyMode::Strict => CommitConsistency::Strict,
            ConsistencyMode::AvailabilityFirst => CommitConsistency::AvailabilityFirst,
        },
        operation_id: certificate.operation_id,
    }
}

impl MultipartUpload {
    pub fn id(&self) -> u64 {
        self.id
    }
}

fn provider_content_id(id: &ContentId) -> String {
    format!("object-body:{}", hex_digest(id.as_bytes()))
}

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{Epoch, NodeId};
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn object_put_materializes_blob_and_manifest() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");

        let object = objects
            .put(
                &mut router,
                PlacementGroupId(3),
                PlacementGroupId(2),
                "media",
                "hero.png",
                b"image-bytes".to_vec(),
            )
            .expect("put");

        assert_eq!(object.version, CommitVersion(3));
        assert_eq!(object.kind, ObjectVersionKind::Live);
        assert_eq!(object.manifest.parts[0].len, 11);
        let part = objects
            .object_parts(0)
            .expect("uploaded parts")
            .first()
            .expect("first part");
        assert_eq!(part.providers.len(), 1);
        assert_eq!(part.providers[0].surface, ChunkSurface::Object);
        assert_eq!(
            objects
                .body_chunk(&part.content)
                .expect("object body chunk")
                .pin_count,
            1
        );
        assert_eq!(router.committed().len(), 3);
        assert_eq!(
            objects.blob_pin_version("media", "hero.png"),
            Some(CommitVersion(2))
        );
    }

    #[test]
    fn object_facade_commits_through_vsr_backed_router_and_recovers_certificates() {
        let path = test_path("object_facade_commits_through_vsr_backed_router");
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(2), PlacementGroupId(3)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");
            let mut objects = ObjectEngine::default();

            objects
                .create_bucket(&mut router, PlacementGroupId(2), "media")
                .expect("bucket");
            let object = objects
                .put(
                    &mut router,
                    PlacementGroupId(3),
                    PlacementGroupId(2),
                    "media",
                    "hero.png",
                    b"image-bytes".to_vec(),
                )
                .expect("put");

            assert_eq!(object.version, CommitVersion(3));
            assert_eq!(
                objects.blob_pin_version("media", "hero.png"),
                Some(CommitVersion(2))
            );
            assert_eq!(router.latest_version(), CommitVersion(3));
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(2), PlacementGroupId(3)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router");

        assert_eq!(recovered.latest_version(), CommitVersion(3));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(2))
                .expect("metadata certificates")
                .len(),
            2
        );
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(3))
                .expect("blob certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn object_facade_retries_replay_vsr_request_certificates_without_new_commits() {
        let path = test_path("object_facade_retries_replay_vsr_request_certificates");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(2), PlacementGroupId(3)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut objects = ObjectEngine::default();

            objects
                .create_bucket(&mut router, PlacementGroupId(2), "media")
                .expect("bucket");
            let object = objects
                .put(
                    &mut router,
                    PlacementGroupId(3),
                    PlacementGroupId(2),
                    "media",
                    "hero.png",
                    b"image-bytes".to_vec(),
                )
                .expect("put");

            assert_eq!(object.version, CommitVersion(3));
            assert_eq!(router.latest_version(), CommitVersion(3));
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(2), PlacementGroupId(3)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut retried = ObjectEngine::default();

            let bucket = retried
                .create_bucket(&mut router, PlacementGroupId(2), "media")
                .expect("bucket replay");
            let object = retried
                .put(
                    &mut router,
                    PlacementGroupId(3),
                    PlacementGroupId(2),
                    "media",
                    "hero.png",
                    b"image-bytes".to_vec(),
                )
                .expect("put replay");

            assert_eq!(bucket.version, CommitVersion(1));
            assert_eq!(
                retried.blob_pin_version("media", "hero.png"),
                Some(CommitVersion(2))
            );
            assert_eq!(object.version, CommitVersion(3));
            assert_eq!(router.latest_version(), CommitVersion(3));
            assert_eq!(
                retried
                    .read_object("media", "hero.png")
                    .expect("read replayed object"),
                b"image-bytes".to_vec()
            );
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(2), PlacementGroupId(3)],
            Epoch(1),
            NodeId(0),
            voters,
        )
        .expect("recover router again");

        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(2))
                .expect("metadata certificates")
                .len(),
            2
        );
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(3))
                .expect("blob certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn multipart_object_is_invisible_until_manifest_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");

        let upload = objects
            .begin_multipart("media", "hero.png")
            .expect("begin multipart");
        objects
            .upload_part(
                &mut router,
                PlacementGroupId(3),
                upload.id,
                b"image-".to_vec(),
            )
            .expect("part one");
        objects
            .upload_part(
                &mut router,
                PlacementGroupId(3),
                upload.id,
                b"bytes".to_vec(),
            )
            .expect("part two");

        assert!(objects.get("media", "hero.png").is_none());
        let stored_parts = objects
            .object_parts(upload.id)
            .expect("stored parts")
            .to_vec();
        assert_eq!(stored_parts.len(), 2);
        assert_eq!(stored_parts[0].number, 1);
        assert_eq!(stored_parts[0].offset, 0);
        assert_eq!(stored_parts[0].len, 6);
        assert_eq!(stored_parts[0].providers.len(), 1);
        assert!(objects.body_chunk(&stored_parts[0].content).is_some());
        assert_eq!(stored_parts[1].number, 2);
        assert_eq!(stored_parts[1].offset, 6);
        assert_eq!(stored_parts[1].len, 5);
        assert_eq!(stored_parts[1].providers.len(), 1);
        assert!(objects.body_chunk(&stored_parts[1].content).is_some());
        assert!(objects.blobs.is_pinned(&stored_parts[0].content));
        assert!(objects.blobs.is_pinned(&stored_parts[1].content));

        let object = objects
            .complete_multipart(&mut router, PlacementGroupId(2), upload.id)
            .expect("complete");

        assert_eq!(object.manifest.parts.len(), 2);
        assert_eq!(object.manifest.parts[0].offset, stored_parts[0].offset);
        assert_eq!(object.manifest.parts[1].offset, stored_parts[1].offset);
        assert_eq!(object.version, CommitVersion(4));
        assert_eq!(
            objects
                .read_object("media", "hero.png")
                .expect("read object"),
            b"image-bytes".to_vec()
        );
    }

    #[test]
    fn cold_object_body_is_erasure_coded_and_reconstructed_on_read() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "archive")
            .expect("bucket");
        let payload = b"cold-media-object-body".to_vec();
        let content = ContentId::for_bytes(&payload);

        let object = objects
            .put_cold_erasure(
                &mut router,
                PlacementGroupId(2),
                "archive",
                "hero.raw",
                payload.clone(),
                rsf_erasure::ErasurePolicy::new(3, 2).unwrap(),
                &nodes(5),
            )
            .expect("cold put");

        assert_eq!(object.version, CommitVersion(2));
        assert_eq!(object.manifest.parts.len(), 1);
        assert_eq!(object.manifest.parts[0].content, content);
        assert_eq!(object.manifest.parts[0].len, payload.len() as u64);
        assert_eq!(
            object
                .manifest
                .metadata
                .iter()
                .find(|(key, _)| key == "storage-class")
                .map(|(_, value)| value.as_slice()),
            Some(b"cold-erasure".as_slice())
        );

        let layout = objects.cold_layout(&content).expect("cold layout");
        assert_eq!(layout.policy.data, 3);
        assert_eq!(layout.policy.parity, 2);
        assert_eq!(layout.shards.len(), 5);
        assert_eq!(
            layout
                .shards
                .iter()
                .filter(|shard| shard.kind == ShardKind::Data)
                .count(),
            3
        );
        assert_eq!(
            layout
                .shards
                .iter()
                .filter(|shard| shard.kind == ShardKind::Parity)
                .count(),
            2
        );
        assert!(
            objects.body_chunk(&content).is_none(),
            "cold body must not depend on storing the full replicated object chunk"
        );
        for shard in &layout.shards {
            let chunk = objects
                .body_chunk(&shard.content)
                .expect("cold shard chunk");
            assert!(chunk.pin_count >= 1);
            assert_eq!(chunk.checksum, shard.checksum);
        }

        assert_eq!(
            objects
                .read_object("archive", "hero.raw")
                .expect("read cold object"),
            payload
        );
    }

    #[test]
    fn cold_object_read_reconstructs_with_degraded_shards() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "archive")
            .expect("bucket");
        let payload = b"cold-media-object-body".to_vec();
        let content = ContentId::for_bytes(&payload);

        objects
            .put_cold_erasure(
                &mut router,
                PlacementGroupId(2),
                "archive",
                "hero.raw",
                payload.clone(),
                rsf_erasure::ErasurePolicy::new(3, 2).unwrap(),
                &nodes(5),
            )
            .expect("cold put");

        objects
            .quarantine_cold_shard(&content, 1, "injected data shard loss")
            .expect("quarantine data shard");
        objects
            .quarantine_cold_shard(&content, 2, "injected second data shard loss")
            .expect("quarantine second data shard");
        assert_eq!(
            objects
                .read_object("archive", "hero.raw")
                .expect("degraded read reconstructs object"),
            payload
        );
    }

    #[test]
    fn cold_object_read_rejects_unrecoverable_degradation() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "archive")
            .expect("bucket");
        let payload = b"cold-media-object-body".to_vec();
        let content = ContentId::for_bytes(&payload);

        objects
            .put_cold_erasure(
                &mut router,
                PlacementGroupId(2),
                "archive",
                "hero.raw",
                payload,
                rsf_erasure::ErasurePolicy::new(3, 2).unwrap(),
                &nodes(5),
            )
            .expect("cold put");

        objects
            .quarantine_cold_shard(&content, 0, "injected data shard loss")
            .expect("quarantine first data shard");
        objects
            .quarantine_cold_shard(&content, 1, "injected data shard loss")
            .expect("quarantine second data shard");
        objects
            .quarantine_cold_shard(&content, 2, "injected data shard loss")
            .expect("quarantine third data shard");

        let err = objects
            .read_object("archive", "hero.raw")
            .expect_err("three missing shards must fail closed");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("too many missing shards"));
    }

    #[test]
    fn cold_object_rejects_insufficient_erasure_nodes() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "archive")
            .expect("bucket");

        let err = objects
            .put_cold_erasure(
                &mut router,
                PlacementGroupId(2),
                "archive",
                "hero.raw",
                b"cold-body".to_vec(),
                rsf_erasure::ErasurePolicy::new(3, 2).unwrap(),
                &nodes(4),
            )
            .expect_err("insufficient erasure nodes must reject");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("requires 5 nodes"));
    }

    #[test]
    fn multipart_upload_state_survives_failed_completion() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let upload = objects
            .begin_multipart("media", "empty.png")
            .expect("begin multipart");

        assert!(
            objects
                .complete_multipart(&mut router, PlacementGroupId(2), upload.id)
                .is_err()
        );
        assert_eq!(
            objects.multipart_upload(upload.id).expect("upload").state,
            MultipartUploadState::Open
        );

        objects
            .upload_part(
                &mut router,
                PlacementGroupId(3),
                upload.id,
                b"bytes".to_vec(),
            )
            .expect("part");
        let object = objects
            .complete_multipart(&mut router, PlacementGroupId(2), upload.id)
            .expect("complete");

        assert_eq!(object.version, CommitVersion(3));
        let completed = objects.multipart_upload(upload.id).expect("upload");
        assert_eq!(completed.state, MultipartUploadState::Completed);
        assert_eq!(completed.completed_version, Some(CommitVersion(3)));
        assert!(
            objects
                .upload_part(
                    &mut router,
                    PlacementGroupId(3),
                    upload.id,
                    b"again".to_vec(),
                )
                .is_err()
        );
    }

    #[test]
    fn object_facade_rejects_stale_head_updates() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");

        let first = objects.begin_multipart("media", "hero.png").expect("begin");
        objects
            .upload_part(
                &mut router,
                PlacementGroupId(3),
                first.id,
                b"first".to_vec(),
            )
            .expect("first part");
        let first_version = objects
            .complete_multipart(&mut router, PlacementGroupId(2), first.id)
            .expect("first commit");

        let replacement = objects
            .begin_multipart("media", "hero.png")
            .expect("begin replacement");
        objects
            .upload_part(
                &mut router,
                PlacementGroupId(3),
                replacement.id,
                b"second".to_vec(),
            )
            .expect("second part");
        let committed_before_reject = router.committed().len();
        assert!(
            objects
                .complete_multipart(&mut router, PlacementGroupId(2), replacement.id)
                .is_err()
        );
        assert_eq!(router.committed().len(), committed_before_reject);
        assert!(
            objects
                .complete_multipart_with_expected(
                    &mut router,
                    PlacementGroupId(2),
                    replacement.id,
                    Some(CommitVersion(first_version.version.0 - 1)),
                )
                .is_err()
        );
        assert_eq!(router.committed().len(), committed_before_reject);

        let replacement_version = objects
            .complete_multipart_with_expected(
                &mut router,
                PlacementGroupId(2),
                replacement.id,
                Some(first_version.version),
            )
            .expect("replacement commit");

        assert!(replacement_version.version.0 > first_version.version.0);
        assert_eq!(
            objects
                .read_object("media", "hero.png")
                .expect("replacement read"),
            b"second".to_vec()
        );
    }

    #[test]
    fn object_versions_record_replacements_and_delete_markers() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");

        let first = objects
            .put(
                &mut router,
                PlacementGroupId(3),
                PlacementGroupId(2),
                "media",
                "hero.png",
                b"first".to_vec(),
            )
            .expect("first put");

        let replacement = objects
            .begin_multipart("media", "hero.png")
            .expect("begin replacement");
        objects
            .upload_part(
                &mut router,
                PlacementGroupId(3),
                replacement.id,
                b"second".to_vec(),
            )
            .expect("replacement part");
        let second = objects
            .complete_multipart_with_expected(
                &mut router,
                PlacementGroupId(2),
                replacement.id,
                Some(first.version),
            )
            .expect("replacement commit");

        let committed_before_reject = router.committed().len();
        assert!(
            objects
                .delete_object_with_expected(
                    &mut router,
                    PlacementGroupId(2),
                    "media",
                    "hero.png",
                    first.version,
                )
                .is_err()
        );
        assert_eq!(router.committed().len(), committed_before_reject);

        let deleted = objects
            .delete_object_with_expected(
                &mut router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                second.version,
            )
            .expect("delete marker");

        assert_eq!(deleted.kind, ObjectVersionKind::DeleteMarker);
        assert_eq!(
            objects
                .get("media", "hero.png")
                .expect("current object")
                .kind,
            ObjectVersionKind::DeleteMarker
        );
        assert!(objects.read_object("media", "hero.png").is_err());
        assert_eq!(objects.blob_pin_version("media", "hero.png"), None);

        let versions = objects.versions("media", "hero.png").expect("history");
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].kind, ObjectVersionKind::Live);
        assert_eq!(versions[0].version, first.version);
        assert_eq!(versions[1].kind, ObjectVersionKind::Live);
        assert_eq!(versions[1].version, second.version);
        assert_eq!(versions[2].kind, ObjectVersionKind::DeleteMarker);
        assert_eq!(versions[2].version, deleted.version);
    }

    #[test]
    fn retention_policy_blocks_object_delete_before_protocol_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let first = objects
            .put(
                &mut router,
                PlacementGroupId(3),
                PlacementGroupId(2),
                "media",
                "hero.png",
                b"image-bytes".to_vec(),
            )
            .expect("put");
        let committed_before_reject = router.committed().len();
        let mut policy = retention_policy();
        policy.legal_hold = true;

        let err = objects
            .delete_object_with_retention(
                &mut router,
                PlacementGroupId(2),
                &policy,
                "media",
                "hero.png",
                first.version,
            )
            .expect_err("legal hold blocks object delete");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(router.committed().len(), committed_before_reject);
        assert_eq!(
            objects.get("media", "hero.png").expect("object").kind,
            ObjectVersionKind::Live
        );
    }

    #[test]
    fn retention_policy_allows_object_delete_after_retain_until() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        let mut objects = ObjectEngine::default();
        objects
            .create_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let first = objects
            .put(
                &mut router,
                PlacementGroupId(3),
                PlacementGroupId(2),
                "media",
                "hero.png",
                b"image-bytes".to_vec(),
            )
            .expect("put");
        let deleted = objects
            .delete_object_with_retention(
                &mut router,
                PlacementGroupId(2),
                &retention_policy(),
                "media",
                "hero.png",
                first.version,
            )
            .expect("delete allowed");

        assert_eq!(deleted.kind, ObjectVersionKind::DeleteMarker);
        assert_eq!(router.committed().len(), 4);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-object-{name}-{}-{nonce}", std::process::id()))
    }

    fn retention_policy() -> RetentionPolicy {
        RetentionPolicy {
            namespace: "app".to_string(),
            retain_until: CommitVersion(1),
            expire_after_versions: Some(10),
            compact_noncurrent_after_versions: Some(6),
            legal_hold: false,
        }
    }

    fn nodes(count: u64) -> Vec<NodeId> {
        (0..count).map(NodeId).collect()
    }
}
