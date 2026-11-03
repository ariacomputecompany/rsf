#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{
    CommitConsistency, CommitEvidence, CommitVersion, Digest, Epoch, ErrorCode, PlacementGroupId,
    RsfError, RsfResult,
};
use rsf_protocol::{
    ClientRequestId, CommitCertificate, ConsistencyMode, FabricOperation, FabricRouterPort,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPartRef {
    pub number: u16,
    pub digest: Digest,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectBucketRecord {
    pub name: String,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultipartUploadState {
    Open,
    Committed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartSession {
    pub upload_id: u64,
    pub bucket: String,
    pub key: String,
    pub initiated_at: CommitVersion,
    pub initiated_evidence: CommitEvidence,
    pub parts: Vec<ObjectPartRef>,
    pub state: MultipartUploadState,
    pub completed_at: Option<CommitVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectHead {
    pub bucket: String,
    pub key: String,
    pub kind: ObjectHeadKind,
    pub owner_pg: PlacementGroupId,
    pub owner_epoch: Epoch,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
    pub manifest_digest: Digest,
    pub part_count: u16,
    pub committed_at: CommitVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectHeadKind {
    Live,
    DeleteMarker,
}

/// Compare-and-swap preconditions and destination for an object head ownership transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectHeadOwnershipChange {
    pub expected_current: CommitVersion,
    pub expected_owner_epoch: Epoch,
    pub new_owner_pg: PlacementGroupId,
    pub new_owner_epoch: Epoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectHeadOwnershipTransfer {
    pub bucket: String,
    pub key: String,
    pub previous_owner_pg: PlacementGroupId,
    pub new_owner_pg: PlacementGroupId,
    pub previous_owner_epoch: Epoch,
    pub new_owner_epoch: Epoch,
    pub previous_version: CommitVersion,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectMetadataDecision {
    BucketRegistered {
        bucket: String,
        evidence: CommitEvidence,
    },
    MultipartStarted {
        upload_id: u64,
        bucket: String,
        key: String,
        evidence: CommitEvidence,
    },
    PartAccepted {
        upload_id: u64,
        number: u16,
        bytes: u64,
        evidence: CommitEvidence,
    },
    HeadCommitted {
        bucket: String,
        key: String,
        version: CommitVersion,
        evidence: CommitEvidence,
    },
    HeadDeleted {
        bucket: String,
        key: String,
        version: CommitVersion,
        evidence: CommitEvidence,
    },
    HeadRead {
        bucket: String,
        key: String,
        version: CommitVersion,
    },
    HeadOwnershipTransferred {
        bucket: String,
        key: String,
        previous_owner_pg: PlacementGroupId,
        new_owner_pg: PlacementGroupId,
        previous_owner_epoch: Epoch,
        new_owner_epoch: Epoch,
        previous_version: CommitVersion,
        version: CommitVersion,
        evidence: CommitEvidence,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectMetadataReport {
    pub buckets: usize,
    pub active_uploads: usize,
    pub completed_uploads: usize,
    pub uploads_started: usize,
    pub parts_accepted: usize,
    pub visible_heads: usize,
    pub rejections: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectMetadataCheckpoint {
    pub buckets: Vec<ObjectBucketRecord>,
    pub uploads: Vec<MultipartSession>,
    pub heads: Vec<ObjectHead>,
    pub head_versions: Vec<ObjectHead>,
    pub next_upload_id: u64,
    pub uploads_started: usize,
    pub parts_accepted: usize,
    pub rejections: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectMetadataRecoveryReport {
    pub buckets: usize,
    pub active_uploads: usize,
    pub completed_uploads: usize,
    pub visible_heads: usize,
    pub historical_heads: usize,
    pub next_upload_id: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectMetadataEngine {
    buckets: BTreeMap<String, ObjectBucketRecord>,
    uploads: BTreeMap<u64, MultipartSession>,
    heads: BTreeMap<(String, String), ObjectHead>,
    head_versions: BTreeMap<(String, String), Vec<ObjectHead>>,
    next_upload_id: u64,
    uploads_started: usize,
    parts_accepted: usize,
    rejections: usize,
}

impl ObjectMetadataEngine {
    pub fn checkpoint(&self) -> ObjectMetadataCheckpoint {
        ObjectMetadataCheckpoint {
            buckets: self.buckets.values().cloned().collect(),
            uploads: self.uploads.values().cloned().collect(),
            heads: self.heads.values().cloned().collect(),
            head_versions: self
                .head_versions
                .values()
                .flat_map(|versions| versions.iter().cloned())
                .collect(),
            next_upload_id: self.next_upload_id,
            uploads_started: self.uploads_started,
            parts_accepted: self.parts_accepted,
            rejections: self.rejections,
        }
    }

    pub fn recover(
        checkpoint: ObjectMetadataCheckpoint,
    ) -> RsfResult<(Self, ObjectMetadataRecoveryReport)> {
        let mut engine = Self::default();

        for bucket in checkpoint.buckets {
            validate_bucket_name(&bucket.name)?;
            bucket.version.validate_committed()?;
            if bucket.evidence.version != bucket.version {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "bucket {} evidence version {} does not match record version {}",
                        bucket.name, bucket.evidence.version.0, bucket.version.0
                    ),
                ));
            }
            if engine.buckets.insert(bucket.name.clone(), bucket).is_some() {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "object metadata checkpoint contains duplicate bucket",
                ));
            }
        }

        let mut highest_upload_id = None;
        let mut parts_accepted = 0usize;
        let mut uploads_started = 0usize;
        for upload in checkpoint.uploads {
            validate_recovered_upload(&engine, &upload)?;
            highest_upload_id = Some(highest_upload_id.map_or(upload.upload_id, |highest: u64| {
                highest.max(upload.upload_id)
            }));
            parts_accepted += upload.parts.len();
            uploads_started += 1;
            if engine.uploads.insert(upload.upload_id, upload).is_some() {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "object metadata checkpoint contains duplicate multipart upload",
                ));
            }
        }

        for head in checkpoint.head_versions {
            validate_recovered_head(&engine, &head)?;
            engine
                .head_versions
                .entry((head.bucket.clone(), head.key.clone()))
                .or_default()
                .push(head);
        }
        for (object_key, versions) in &mut engine.head_versions {
            versions.sort_by_key(|head| head.version);
            for pair in versions.windows(2) {
                if pair[0].version == pair[1].version {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        format!(
                            "object metadata checkpoint contains duplicate head version for {}/{}",
                            object_key.0, object_key.1
                        ),
                    ));
                }
            }
        }

        for head in checkpoint.heads {
            validate_recovered_head(&engine, &head)?;
            let object_key = (head.bucket.clone(), head.key.clone());
            let Some(history) = engine.head_versions.get(&object_key) else {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "current object head {}/{} is missing from recovered history",
                        object_key.0, object_key.1
                    ),
                ));
            };
            let Some(latest) = history.last() else {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "current object head {}/{} has empty recovered history",
                        object_key.0, object_key.1
                    ),
                ));
            };
            if latest != &head {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "current object head {}/{} is not the latest recovered version",
                        object_key.0, object_key.1
                    ),
                ));
            }
            if engine.heads.insert(object_key, head).is_some() {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "object metadata checkpoint contains duplicate current head",
                ));
            }
        }

        let minimum_next_upload_id = highest_upload_id.map_or(0, |id| id.saturating_add(1));
        if checkpoint.next_upload_id < minimum_next_upload_id {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "object metadata checkpoint next upload id {} is behind recovered upload id {}",
                    checkpoint.next_upload_id,
                    minimum_next_upload_id.saturating_sub(1)
                ),
            ));
        }
        if checkpoint.uploads_started < uploads_started {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "object metadata checkpoint upload counter is behind recovered uploads",
            ));
        }
        if checkpoint.parts_accepted < parts_accepted {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "object metadata checkpoint part counter is behind recovered parts",
            ));
        }

        engine.next_upload_id = checkpoint.next_upload_id;
        engine.uploads_started = checkpoint.uploads_started;
        engine.parts_accepted = checkpoint.parts_accepted;
        engine.rejections = checkpoint.rejections;

        let report = ObjectMetadataRecoveryReport {
            buckets: engine.buckets.len(),
            active_uploads: engine
                .uploads
                .values()
                .filter(|upload| upload.state == MultipartUploadState::Open)
                .count(),
            completed_uploads: engine
                .uploads
                .values()
                .filter(|upload| upload.state == MultipartUploadState::Committed)
                .count(),
            visible_heads: engine.heads.len(),
            historical_heads: engine
                .head_versions
                .values()
                .map(|versions| versions.len())
                .sum(),
            next_upload_id: engine.next_upload_id,
        };
        Ok((engine, report))
    }

    pub fn register_bucket(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
    ) -> RsfResult<ObjectMetadataDecision> {
        let bucket = bucket.into();
        validate_bucket_name(&bucket)?;
        if let Some(existing) = self.buckets.get(&bucket) {
            return Ok(ObjectMetadataDecision::BucketRegistered {
                bucket,
                evidence: existing.evidence.clone(),
            });
        }
        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(format!("objectmeta:bucket:{bucket}"), 0),
            FabricOperation::ObjectBucket {
                bucket: bucket.clone(),
            },
        )?;
        let evidence = commit_evidence(commit.certificate);
        let record = ObjectBucketRecord {
            name: bucket.clone(),
            version: evidence.version,
            evidence: evidence.clone(),
        };
        self.buckets.insert(bucket.clone(), record);
        Ok(ObjectMetadataDecision::BucketRegistered { bucket, evidence })
    }

    pub fn start_multipart(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> RsfResult<ObjectMetadataDecision> {
        let bucket = bucket.into();
        let key = key.into();
        self.require_bucket(&bucket)?;

        let upload_id = self.next_upload_id;
        self.next_upload_id += 1;
        self.uploads_started += 1;
        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(format!("objectmeta:upload:{bucket}:{key}:{upload_id}"), 0),
            FabricOperation::ObjectMultipartStart {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id,
            },
        )?;
        self.uploads.insert(
            upload_id,
            MultipartSession {
                upload_id,
                bucket: bucket.clone(),
                key: key.clone(),
                initiated_at: commit.certificate.version,
                initiated_evidence: commit_evidence(commit.certificate),
                parts: Vec::new(),
                state: MultipartUploadState::Open,
                completed_at: None,
            },
        );
        Ok(ObjectMetadataDecision::MultipartStarted {
            upload_id,
            bucket,
            key,
            evidence: self
                .uploads
                .get(&upload_id)
                .expect("upload was inserted before decision")
                .initiated_evidence
                .clone(),
        })
    }

    pub fn accept_part(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        upload_id: u64,
        part: ObjectPartRef,
    ) -> RsfResult<ObjectMetadataDecision> {
        if part.number == 0 {
            return self.reject("multipart part numbers are one-based");
        }
        if part.digest.validate().is_err() {
            return self.reject("multipart part digest must be non-empty");
        }
        if part.bytes == 0 {
            return self.reject("multipart part must contain at least one byte");
        }
        let Some(upload) = self.uploads.get(&upload_id) else {
            return self.reject(format!("unknown multipart upload {upload_id}"));
        };
        if upload.state != MultipartUploadState::Open {
            return self.reject(format!("multipart upload {upload_id} is not open"));
        }
        if upload
            .parts
            .iter()
            .any(|existing| existing.number == part.number)
        {
            return self.reject(format!(
                "duplicate multipart part {} for upload {upload_id}",
                part.number
            ));
        }
        let upload = self
            .uploads
            .get_mut(&upload_id)
            .expect("upload existence was checked before mutation");
        let number = part.number;
        let bytes = part.bytes;
        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(format!("objectmeta:part:{upload_id}:{number}"), 0),
            FabricOperation::ObjectMultipartPart {
                bucket: upload.bucket.clone(),
                key: upload.key.clone(),
                upload_id,
                part_number: number,
            },
        )?;
        upload.parts.push(part);
        upload.parts.sort_by_key(|part| part.number);
        self.parts_accepted += 1;
        Ok(ObjectMetadataDecision::PartAccepted {
            upload_id,
            number,
            bytes,
            evidence: commit_evidence(commit.certificate),
        })
    }

    pub fn commit_head(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        upload_id: u64,
        manifest_digest: Digest,
    ) -> RsfResult<ObjectMetadataDecision> {
        self.commit_head_with_expected(router, metadata_pg, upload_id, manifest_digest, None)
    }

    pub fn commit_head_with_expected(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        upload_id: u64,
        manifest_digest: Digest,
        expected_current: Option<CommitVersion>,
    ) -> RsfResult<ObjectMetadataDecision> {
        if manifest_digest.validate().is_err() {
            return self.reject("manifest digest must be non-empty before object head commit");
        }
        let Some(upload) = self.uploads.get(&upload_id) else {
            return self.reject(format!("unknown multipart upload {upload_id}"));
        };
        if upload.state != MultipartUploadState::Open {
            return self.reject(format!("multipart upload {upload_id} is not open"));
        }
        if upload.parts.is_empty() {
            return self.reject("multipart upload must contain at least one part before commit");
        }
        let bucket = upload.bucket.clone();
        let key = upload.key.clone();
        let part_count = upload.parts.len() as u16;
        self.validate_expected_head(&bucket, &key, expected_current)?;

        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(format!("objectmeta:head:{upload_id}"), 0),
            FabricOperation::ObjectHead {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        )?;
        let version = commit.certificate.version;
        let evidence = commit_evidence(commit.certificate);
        let head = ObjectHead {
            bucket,
            key,
            kind: ObjectHeadKind::Live,
            owner_pg: metadata_pg,
            owner_epoch: Epoch(version.0),
            version,
            evidence: evidence.clone(),
            manifest_digest,
            part_count,
            committed_at: version,
        };
        let bucket = head.bucket.clone();
        let key = head.key.clone();
        self.install_head(bucket.clone(), key.clone(), head);
        let upload = self
            .uploads
            .get_mut(&upload_id)
            .expect("upload existence was checked before head commit");
        upload.state = MultipartUploadState::Committed;
        upload.completed_at = Some(version);
        Ok(ObjectMetadataDecision::HeadCommitted {
            bucket,
            key,
            version,
            evidence,
        })
    }

    pub fn delete_head_with_expected(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
        key: impl Into<String>,
        expected_current: CommitVersion,
    ) -> RsfResult<ObjectMetadataDecision> {
        let bucket = bucket.into();
        let key = key.into();
        self.require_bucket(&bucket)?;
        self.validate_expected_head(&bucket, &key, Some(expected_current))?;

        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(
                format!(
                    "objectmeta:delete-marker:{bucket}:{key}:{}",
                    expected_current.0
                ),
                0,
            ),
            FabricOperation::ObjectHead {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        )?;
        let version = commit.certificate.version;
        let evidence = commit_evidence(commit.certificate);
        let marker_digest = Digest::blake3_bytes(
            format!("objectmeta:delete-marker:{bucket}:{key}:{}", version.0).as_bytes(),
        );
        let head = ObjectHead {
            bucket: bucket.clone(),
            key: key.clone(),
            kind: ObjectHeadKind::DeleteMarker,
            owner_pg: metadata_pg,
            owner_epoch: Epoch(version.0),
            version,
            evidence: evidence.clone(),
            manifest_digest: marker_digest,
            part_count: 0,
            committed_at: version,
        };
        self.install_head(bucket.clone(), key.clone(), head);

        Ok(ObjectMetadataDecision::HeadDeleted {
            bucket,
            key,
            version,
            evidence,
        })
    }

    pub fn transfer_head_ownership(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        bucket: impl Into<String>,
        key: impl Into<String>,
        change: ObjectHeadOwnershipChange,
    ) -> RsfResult<ObjectMetadataDecision> {
        let ObjectHeadOwnershipChange {
            expected_current,
            expected_owner_epoch,
            new_owner_pg,
            new_owner_epoch,
        } = change;
        let bucket = bucket.into();
        let key = key.into();
        self.require_bucket(&bucket)?;
        let current = self
            .heads
            .get(&(bucket.clone(), key.clone()))
            .cloned()
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("object head {bucket}/{key} is not visible"),
                )
            })?;
        if current.version != expected_current {
            return self.reject(format!(
                "stale object head ownership transfer for {bucket}/{key}: expected version {} but current is {}",
                expected_current.0, current.version.0
            ));
        }
        if current.owner_epoch != expected_owner_epoch {
            return self.reject(format!(
                "stale object head ownership epoch for {bucket}/{key}: expected owner epoch {} but current is {}",
                expected_owner_epoch.0, current.owner_epoch.0
            ));
        }
        if new_owner_epoch <= current.owner_epoch {
            return self.reject(format!(
                "object head {bucket}/{key} ownership transfer must advance owner epoch"
            ));
        }
        if current.owner_pg == new_owner_pg {
            return self.reject(format!(
                "object head {bucket}/{key} is already owned by placement group {}",
                new_owner_pg.0
            ));
        }

        let commit = router.commit_request(
            metadata_pg,
            ClientRequestId::new(
                format!(
                    "objectmeta:head-owner:{bucket}:{key}:{}:{}",
                    expected_current.0, new_owner_pg.0
                ),
                0,
            ),
            FabricOperation::ObjectHead {
                bucket: bucket.clone(),
                key: key.clone(),
            },
        )?;
        let version = commit.certificate.version;
        let evidence = commit_evidence(commit.certificate);
        let mut moved = current.clone();
        moved.owner_pg = new_owner_pg;
        moved.owner_epoch = new_owner_epoch;
        moved.version = version;
        moved.evidence = evidence.clone();
        self.install_head(bucket.clone(), key.clone(), moved);

        Ok(ObjectMetadataDecision::HeadOwnershipTransferred {
            bucket,
            key,
            previous_owner_pg: current.owner_pg,
            new_owner_pg,
            previous_owner_epoch: current.owner_epoch,
            new_owner_epoch,
            previous_version: current.version,
            version,
            evidence,
        })
    }

    fn install_head(&mut self, bucket: String, key: String, head: ObjectHead) {
        let object_key = (bucket, key);
        self.heads.insert(object_key.clone(), head.clone());
        self.head_versions.entry(object_key).or_default().push(head);
    }

    fn validate_expected_head(
        &mut self,
        bucket: &str,
        key: &str,
        expected_current: Option<CommitVersion>,
    ) -> RsfResult<()> {
        let current = self.heads.get(&(bucket.to_string(), key.to_string()));
        match (current, expected_current) {
            (None, None) => Ok(()),
            (None, Some(version)) => self.reject(format!(
                "object head {bucket}/{key} expected version {} but no current head exists",
                version.0
            )),
            (Some(head), None) => self.reject(format!(
                "object head {bucket}/{key} already exists at version {}; expected version required",
                head.version.0
            )),
            (Some(head), Some(expected)) if head.version == expected => Ok(()),
            (Some(head), Some(expected)) => self.reject(format!(
                "stale object head update for {bucket}/{key}: expected version {} but current is {}",
                expected.0, head.version.0
            )),
        }
    }

    pub fn read_head(
        &self,
        bucket: &str,
        key: &str,
    ) -> RsfResult<(ObjectHead, ObjectMetadataDecision)> {
        let Some(head) = self.heads.get(&(bucket.to_string(), key.to_string())) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("object head {bucket}/{key} is not visible"),
            ));
        };
        Ok((
            head.clone(),
            ObjectMetadataDecision::HeadRead {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version: head.version,
            },
        ))
    }

    pub fn report(&self) -> ObjectMetadataReport {
        ObjectMetadataReport {
            buckets: self.buckets.len(),
            active_uploads: self
                .uploads
                .values()
                .filter(|upload| upload.state == MultipartUploadState::Open)
                .count(),
            completed_uploads: self
                .uploads
                .values()
                .filter(|upload| upload.state == MultipartUploadState::Committed)
                .count(),
            uploads_started: self.uploads_started,
            parts_accepted: self.parts_accepted,
            visible_heads: self.heads.len(),
            rejections: self.rejections,
        }
    }

    pub fn bucket(&self, bucket: &str) -> Option<&ObjectBucketRecord> {
        self.buckets.get(bucket)
    }

    pub fn bucket_catalog(&self) -> Vec<&ObjectBucketRecord> {
        self.buckets.values().collect()
    }

    pub fn multipart_session(&self, upload_id: u64) -> Option<&MultipartSession> {
        self.uploads.get(&upload_id)
    }

    pub fn head_versions(&self, bucket: &str, key: &str) -> Option<&[ObjectHead]> {
        self.head_versions
            .get(&(bucket.to_string(), key.to_string()))
            .map(Vec::as_slice)
    }

    fn require_bucket(&mut self, bucket: &str) -> RsfResult<()> {
        if !self.buckets.contains_key(bucket) {
            return self.reject(format!("unknown object bucket {bucket}"));
        }
        Ok(())
    }

    fn reject<T>(&mut self, reason: impl Into<String>) -> RsfResult<T> {
        self.rejections += 1;
        Err(RsfError::new(ErrorCode::InvalidInput, reason.into()))
    }
}

fn validate_bucket_name(bucket: &str) -> RsfResult<()> {
    if bucket.trim().is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "object bucket name must not be empty",
        ));
    }
    if bucket.contains('/') || bucket.contains(char::is_whitespace) {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("object bucket name {bucket} contains invalid characters"),
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
            ConsistencyMode::Strict => CommitConsistency::Strict,
            ConsistencyMode::AvailabilityFirst => CommitConsistency::AvailabilityFirst,
        },
        operation_id: certificate.operation_id,
    }
}

fn validate_recovered_upload(
    engine: &ObjectMetadataEngine,
    upload: &MultipartSession,
) -> RsfResult<()> {
    if !engine.buckets.contains_key(&upload.bucket) {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "multipart upload {} references unknown bucket {}",
                upload.upload_id, upload.bucket
            ),
        ));
    }
    if upload.initiated_at != upload.initiated_evidence.version {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "multipart upload {} initiated version does not match evidence",
                upload.upload_id
            ),
        ));
    }
    upload.initiated_at.validate_committed()?;
    let mut previous_part = None;
    for part in &upload.parts {
        if part.number == 0 {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "multipart upload {} has zero-numbered part",
                    upload.upload_id
                ),
            ));
        }
        if Some(part.number) <= previous_part {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "multipart upload {} has unsorted or duplicate parts",
                    upload.upload_id
                ),
            ));
        }
        part.digest.validate()?;
        if part.bytes == 0 {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "multipart upload {} contains an empty part",
                    upload.upload_id
                ),
            ));
        }
        previous_part = Some(part.number);
    }
    match upload.state {
        MultipartUploadState::Open if upload.completed_at.is_some() => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "open multipart upload {} has a completion version",
                upload.upload_id
            ),
        )),
        MultipartUploadState::Committed if upload.completed_at.is_none() => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "committed multipart upload {} is missing completion version",
                upload.upload_id
            ),
        )),
        MultipartUploadState::Committed if upload.parts.is_empty() => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "committed multipart upload {} has no committed parts",
                upload.upload_id
            ),
        )),
        _ => Ok(()),
    }
}

fn validate_recovered_head(engine: &ObjectMetadataEngine, head: &ObjectHead) -> RsfResult<()> {
    if !engine.buckets.contains_key(&head.bucket) {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "object head {}/{} references unknown bucket",
                head.bucket, head.key
            ),
        ));
    }
    head.version.validate_committed()?;
    if head.version != head.evidence.version {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "object head {}/{} version does not match commit evidence",
                head.bucket, head.key
            ),
        ));
    }
    if head.owner_epoch.0 == 0 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "object head {}/{} has an invalid owner epoch",
                head.bucket, head.key
            ),
        ));
    }
    if head.committed_at != head.version {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "object head {}/{} committed_at does not match version",
                head.bucket, head.key
            ),
        ));
    }
    head.manifest_digest.validate()?;
    if head.kind == ObjectHeadKind::DeleteMarker && head.part_count != 0 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "delete marker {}/{} cannot reference object parts",
                head.bucket, head.key
            ),
        ));
    }
    if head.kind == ObjectHeadKind::Live && head.part_count == 0 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "live object head {}/{} must reference at least one part",
                head.bucket, head.key
            ),
        ));
    }
    Ok(())
}

impl ObjectMetadataDecision {
    pub fn upload_id(&self) -> Option<u64> {
        match self {
            Self::MultipartStarted { upload_id, .. } | Self::PartAccepted { upload_id, .. } => {
                Some(*upload_id)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{Epoch, NodeId};
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn multipart_commit_makes_latest_head_visible() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .unwrap();
        let upload_id = metadata
            .start_multipart(&mut router, PlacementGroupId(2), "media", "hero.png")
            .unwrap()
            .upload_id()
            .unwrap();

        metadata
            .accept_part(
                &mut router,
                PlacementGroupId(2),
                upload_id,
                ObjectPartRef {
                    number: 1,
                    digest: part_digest("hero.png", 1),
                    bytes: 10,
                },
            )
            .unwrap();
        let committed = metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                upload_id,
                manifest_digest("hero.png"),
            )
            .unwrap();
        let (head, read) = metadata.read_head("media", "hero.png").unwrap();

        assert!(matches!(
            committed,
            ObjectMetadataDecision::HeadCommitted {
                version: CommitVersion(4),
                ..
            }
        ));
        assert!(matches!(
            read,
            ObjectMetadataDecision::HeadRead {
                version: CommitVersion(4),
                ..
            }
        ));
        assert_eq!(head.kind, ObjectHeadKind::Live);
        assert_eq!(head.part_count, 1);
        assert_eq!(head.version, CommitVersion(4));
        assert_eq!(
            metadata
                .head_versions("media", "hero.png")
                .expect("head history")
                .len(),
            1
        );
        assert_eq!(metadata.report().visible_heads, 1);
        assert_eq!(router.committed().len(), 4);
    }

    #[test]
    fn bucket_catalog_persists_committed_bucket_records() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);

        let first = metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let second = metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket replay");

        assert_eq!(router.committed().len(), 1);
        assert_eq!(metadata.report().buckets, 1);
        assert_eq!(metadata.bucket_catalog().len(), 1);
        let bucket = metadata.bucket("media").expect("bucket record");
        assert_eq!(bucket.name, "media");
        assert_eq!(bucket.version, CommitVersion(1));
        assert_eq!(bucket.evidence.placement_group, PlacementGroupId(2));
        assert_eq!(bucket.evidence.log_index.0, 1);
        assert_eq!(first, second);
    }

    #[test]
    fn bucket_catalog_rejects_invalid_bucket_names() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);

        assert!(
            metadata
                .register_bucket(&mut router, PlacementGroupId(2), "")
                .is_err()
        );
        assert!(
            metadata
                .register_bucket(&mut router, PlacementGroupId(2), "bad/name")
                .is_err()
        );
        assert_eq!(router.committed().len(), 0);
        assert_eq!(metadata.report().buckets, 0);
    }

    #[test]
    fn multipart_upload_state_survives_failed_completion_and_records_commit() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .unwrap();
        let upload_id = metadata
            .start_multipart(&mut router, PlacementGroupId(2), "media", "hero.png")
            .unwrap()
            .upload_id()
            .unwrap();

        assert!(
            metadata
                .commit_head(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    manifest_digest("hero.png"),
                )
                .is_err()
        );
        assert_eq!(
            metadata
                .multipart_session(upload_id)
                .expect("session")
                .state,
            MultipartUploadState::Open
        );
        assert_eq!(metadata.report().active_uploads, 1);

        metadata
            .accept_part(
                &mut router,
                PlacementGroupId(2),
                upload_id,
                ObjectPartRef {
                    number: 1,
                    digest: part_digest("hero.png", 1),
                    bytes: 10,
                },
            )
            .unwrap();
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                upload_id,
                manifest_digest("hero.png"),
            )
            .unwrap();

        let session = metadata.multipart_session(upload_id).expect("session");
        assert_eq!(session.state, MultipartUploadState::Committed);
        assert_eq!(session.completed_at, Some(CommitVersion(4)));
        assert_eq!(metadata.report().active_uploads, 0);
        assert_eq!(metadata.report().completed_uploads, 1);
        assert!(
            metadata
                .accept_part(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    ObjectPartRef {
                        number: 2,
                        digest: part_digest("hero.png", 2),
                        bytes: 10,
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn strict_head_commit_rejects_stale_updates() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .unwrap();
        let first = start_upload_with_part(&mut metadata, &mut router, "hero.png", 1);
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                first,
                manifest_digest("hero.png:first"),
            )
            .unwrap();

        let replacement = start_upload_with_part(&mut metadata, &mut router, "hero.png", 2);
        let committed_before_reject = router.committed().len();
        assert!(
            metadata
                .commit_head(
                    &mut router,
                    PlacementGroupId(2),
                    replacement,
                    manifest_digest("hero.png:replacement"),
                )
                .is_err()
        );
        assert_eq!(router.committed().len(), committed_before_reject);
        assert!(
            metadata
                .commit_head_with_expected(
                    &mut router,
                    PlacementGroupId(2),
                    replacement,
                    manifest_digest("hero.png:replacement"),
                    Some(CommitVersion(3)),
                )
                .is_err()
        );
        assert_eq!(router.committed().len(), committed_before_reject);
        let committed = metadata
            .commit_head_with_expected(
                &mut router,
                PlacementGroupId(2),
                replacement,
                manifest_digest("hero.png:replacement"),
                Some(CommitVersion(4)),
            )
            .unwrap();

        assert!(matches!(
            committed,
            ObjectMetadataDecision::HeadCommitted {
                version: CommitVersion(7),
                ..
            }
        ));
        let (head, _) = metadata.read_head("media", "hero.png").unwrap();
        assert_eq!(head.version, CommitVersion(7));
        assert_eq!(head.part_count, 1);
    }

    #[test]
    fn object_head_ownership_transfer_advances_head_version() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let upload = start_upload_with_part(&mut metadata, &mut router, "hero.png", 1);
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                upload,
                Digest::blake3([42; 32]).expect("digest"),
            )
            .expect("head");

        let transferred = metadata
            .transfer_head_ownership(
                &mut router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                ObjectHeadOwnershipChange {
                    expected_current: CommitVersion(4),
                    expected_owner_epoch: Epoch(4),
                    new_owner_pg: PlacementGroupId(3),
                    new_owner_epoch: Epoch(5),
                },
            )
            .expect("ownership transfer");

        let ObjectMetadataDecision::HeadOwnershipTransferred {
            previous_owner_pg,
            new_owner_pg,
            previous_owner_epoch,
            new_owner_epoch,
            previous_version,
            version,
            ..
        } = transferred
        else {
            panic!("expected ownership transfer decision");
        };
        assert_eq!(previous_owner_pg, PlacementGroupId(2));
        assert_eq!(new_owner_pg, PlacementGroupId(3));
        assert_eq!(previous_owner_epoch, Epoch(4));
        assert_eq!(new_owner_epoch, Epoch(5));
        assert_eq!(previous_version, CommitVersion(4));
        assert_eq!(version, CommitVersion(5));
        let (head, _) = metadata.read_head("media", "hero.png").unwrap();
        assert_eq!(head.owner_pg, PlacementGroupId(3));
        assert_eq!(head.owner_epoch, Epoch(5));
        assert_eq!(head.version, CommitVersion(5));
        let versions = metadata.head_versions("media", "hero.png").unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].owner_pg, PlacementGroupId(2));
        assert_eq!(versions[0].owner_epoch, Epoch(4));
        assert_eq!(versions[1].owner_pg, PlacementGroupId(3));
        assert_eq!(versions[1].owner_epoch, Epoch(5));
    }

    #[test]
    fn object_head_ownership_transfer_rejects_stale_expected_version() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let upload = start_upload_with_part(&mut metadata, &mut router, "hero.png", 1);
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                upload,
                Digest::blake3([42; 32]).expect("digest"),
            )
            .expect("head");
        metadata
            .transfer_head_ownership(
                &mut router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                ObjectHeadOwnershipChange {
                    expected_current: CommitVersion(4),
                    expected_owner_epoch: Epoch(4),
                    new_owner_pg: PlacementGroupId(3),
                    new_owner_epoch: Epoch(5),
                },
            )
            .expect("ownership transfer");

        let err = metadata
            .transfer_head_ownership(
                &mut router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                ObjectHeadOwnershipChange {
                    expected_current: CommitVersion(4),
                    expected_owner_epoch: Epoch(4),
                    new_owner_pg: PlacementGroupId(2),
                    new_owner_epoch: Epoch(6),
                },
            )
            .expect_err("stale transfer rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        let (head, _) = metadata.read_head("media", "hero.png").unwrap();
        assert_eq!(head.owner_pg, PlacementGroupId(3));
        assert_eq!(head.owner_epoch, Epoch(5));
        assert_eq!(head.version, CommitVersion(5));
    }

    #[test]
    fn object_head_ownership_transfer_rejects_stale_or_non_advancing_owner_epoch() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2), PlacementGroupId(3)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let upload = start_upload_with_part(&mut metadata, &mut router, "hero.png", 1);
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                upload,
                Digest::blake3([42; 32]).expect("digest"),
            )
            .expect("head");

        let stale_epoch = metadata
            .transfer_head_ownership(
                &mut router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                ObjectHeadOwnershipChange {
                    expected_current: CommitVersion(4),
                    expected_owner_epoch: Epoch(3),
                    new_owner_pg: PlacementGroupId(3),
                    new_owner_epoch: Epoch(5),
                },
            )
            .expect_err("stale owner epoch rejected");
        assert!(stale_epoch.message.contains("expected owner epoch"));

        let non_advancing_epoch = metadata
            .transfer_head_ownership(
                &mut router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                ObjectHeadOwnershipChange {
                    expected_current: CommitVersion(4),
                    expected_owner_epoch: Epoch(4),
                    new_owner_pg: PlacementGroupId(3),
                    new_owner_epoch: Epoch(4),
                },
            )
            .expect_err("non-advancing owner epoch rejected");
        assert!(
            non_advancing_epoch
                .message
                .contains("must advance owner epoch")
        );

        let (head, _) = metadata.read_head("media", "hero.png").unwrap();
        assert_eq!(head.owner_pg, PlacementGroupId(2));
        assert_eq!(head.owner_epoch, Epoch(4));
        assert_eq!(head.version, CommitVersion(4));
    }

    #[test]
    fn object_head_history_records_replacements_and_delete_markers() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .unwrap();

        let first = start_upload_with_part(&mut metadata, &mut router, "hero.png", 1);
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                first,
                manifest_digest("hero.png:first"),
            )
            .unwrap();

        let replacement = start_upload_with_part(&mut metadata, &mut router, "hero.png", 2);
        metadata
            .commit_head_with_expected(
                &mut router,
                PlacementGroupId(2),
                replacement,
                manifest_digest("hero.png:replacement"),
                Some(CommitVersion(4)),
            )
            .unwrap();

        let committed_before_reject = router.committed().len();
        assert!(
            metadata
                .delete_head_with_expected(
                    &mut router,
                    PlacementGroupId(2),
                    "media",
                    "hero.png",
                    CommitVersion(4),
                )
                .is_err()
        );
        assert_eq!(router.committed().len(), committed_before_reject);

        let deleted = metadata
            .delete_head_with_expected(
                &mut router,
                PlacementGroupId(2),
                "media",
                "hero.png",
                CommitVersion(7),
            )
            .expect("delete marker");
        assert!(matches!(
            deleted,
            ObjectMetadataDecision::HeadDeleted {
                version: CommitVersion(8),
                ..
            }
        ));

        let (current, _) = metadata.read_head("media", "hero.png").unwrap();
        assert_eq!(current.kind, ObjectHeadKind::DeleteMarker);
        assert_eq!(current.part_count, 0);
        assert_eq!(current.version, CommitVersion(8));
        let versions = metadata
            .head_versions("media", "hero.png")
            .expect("head history");
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].kind, ObjectHeadKind::Live);
        assert_eq!(versions[1].kind, ObjectHeadKind::Live);
        assert_eq!(versions[2].kind, ObjectHeadKind::DeleteMarker);
    }

    #[test]
    fn object_metadata_checkpoint_recovers_heads_uploads_and_stale_write_guards() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");

        let first = start_upload_with_part(&mut metadata, &mut router, "hero.png", 1);
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                first,
                manifest_digest("hero.png:first"),
            )
            .expect("first head");
        let replacement = start_upload_with_part(&mut metadata, &mut router, "hero.png", 2);
        metadata
            .commit_head_with_expected(
                &mut router,
                PlacementGroupId(2),
                replacement,
                manifest_digest("hero.png:replacement"),
                Some(CommitVersion(4)),
            )
            .expect("replacement head");
        let open_upload = metadata
            .start_multipart(&mut router, PlacementGroupId(2), "media", "draft.png")
            .expect("open upload")
            .upload_id()
            .expect("upload id");

        let checkpoint = metadata.checkpoint();
        drop(metadata);

        let (mut recovered, recovery) =
            ObjectMetadataEngine::recover(checkpoint).expect("recover metadata checkpoint");
        assert_eq!(
            recovery,
            ObjectMetadataRecoveryReport {
                buckets: 1,
                active_uploads: 1,
                completed_uploads: 2,
                visible_heads: 1,
                historical_heads: 2,
                next_upload_id: 3,
            }
        );
        assert_eq!(recovered.report().active_uploads, 1);
        assert_eq!(
            recovered
                .multipart_session(open_upload)
                .expect("open upload survives")
                .state,
            MultipartUploadState::Open
        );
        let (head, _) = recovered.read_head("media", "hero.png").expect("head");
        assert_eq!(head.version, CommitVersion(7));
        assert_eq!(head.part_count, 1);
        assert_eq!(
            recovered
                .head_versions("media", "hero.png")
                .expect("history")
                .len(),
            2
        );

        let stale = start_upload_with_part(&mut recovered, &mut router, "hero.png", 3);
        assert!(
            recovered
                .commit_head_with_expected(
                    &mut router,
                    PlacementGroupId(2),
                    stale,
                    manifest_digest("hero.png:stale"),
                    Some(CommitVersion(4)),
                )
                .is_err()
        );
        let committed_after_recovery = recovered
            .commit_head_with_expected(
                &mut router,
                PlacementGroupId(2),
                stale,
                manifest_digest("hero.png:after-recovery"),
                Some(CommitVersion(7)),
            )
            .expect("fresh expected version");
        assert!(matches!(
            committed_after_recovery,
            ObjectMetadataDecision::HeadCommitted {
                version: CommitVersion(11),
                ..
            }
        ));
    }

    #[test]
    fn object_metadata_recovery_rejects_incomplete_head_history() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .expect("bucket");
        let upload = start_upload_with_part(&mut metadata, &mut router, "hero.png", 1);
        metadata
            .commit_head(
                &mut router,
                PlacementGroupId(2),
                upload,
                manifest_digest("hero.png"),
            )
            .expect("head");

        let mut checkpoint = metadata.checkpoint();
        checkpoint.head_versions.clear();
        let err = ObjectMetadataEngine::recover(checkpoint).expect_err("missing history rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn object_metadata_commits_through_vsr_backed_router_and_recovers_certificates() {
        let path = test_path("object_metadata_commits_through_vsr_backed_router");
        {
            let mut metadata = ObjectMetadataEngine::default();
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");

            metadata
                .register_bucket(&mut router, PlacementGroupId(2), "media")
                .expect("bucket");
            let upload_id = metadata
                .start_multipart(&mut router, PlacementGroupId(2), "media", "hero.png")
                .expect("start")
                .upload_id()
                .expect("upload id");
            metadata
                .accept_part(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    ObjectPartRef {
                        number: 1,
                        digest: part_digest("hero.png", 1),
                        bytes: 10,
                    },
                )
                .expect("part");
            metadata
                .commit_head(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    manifest_digest("hero.png"),
                )
                .expect("head");

            assert_eq!(router.latest_version(), CommitVersion(4));
            assert_eq!(metadata.report().visible_heads, 1);
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router");

        assert_eq!(recovered.latest_version(), CommitVersion(4));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(2))
                .expect("certificates")
                .len(),
            4
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn object_metadata_retries_replay_vsr_request_certificates_without_new_commits() {
        let path = test_path("object_metadata_retries_replay_vsr_request_certificates");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut metadata = ObjectMetadataEngine::default();
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");

            metadata
                .register_bucket(&mut router, PlacementGroupId(2), "media")
                .expect("bucket");
            let upload_id = metadata
                .start_multipart(&mut router, PlacementGroupId(2), "media", "hero.png")
                .expect("start")
                .upload_id()
                .expect("upload id");
            metadata
                .accept_part(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    ObjectPartRef {
                        number: 1,
                        digest: part_digest("hero.png", 1),
                        bytes: 10,
                    },
                )
                .expect("part");
            metadata
                .commit_head(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    manifest_digest("hero.png"),
                )
                .expect("head");

            assert_eq!(router.latest_version(), CommitVersion(4));
        }

        {
            let mut metadata = ObjectMetadataEngine::default();
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");

            metadata
                .register_bucket(&mut router, PlacementGroupId(2), "media")
                .expect("bucket replay");
            let upload_id = metadata
                .start_multipart(&mut router, PlacementGroupId(2), "media", "hero.png")
                .expect("start replay")
                .upload_id()
                .expect("upload id");
            metadata
                .accept_part(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    ObjectPartRef {
                        number: 1,
                        digest: part_digest("hero.png", 1),
                        bytes: 10,
                    },
                )
                .expect("part replay");
            let committed = metadata
                .commit_head(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    manifest_digest("hero.png"),
                )
                .expect("head replay");

            assert!(matches!(
                committed,
                ObjectMetadataDecision::HeadCommitted {
                    version: CommitVersion(4),
                    ..
                }
            ));
            assert_eq!(router.latest_version(), CommitVersion(4));
            assert_eq!(metadata.report().visible_heads, 1);
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(2)], Epoch(1), NodeId(0), voters)
                .expect("recover router again");

        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(2))
                .expect("certificates")
                .len(),
            4
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn rejects_empty_commits_and_duplicate_parts() {
        let mut metadata = ObjectMetadataEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        metadata
            .register_bucket(&mut router, PlacementGroupId(2), "media")
            .unwrap();
        let upload_id = metadata
            .start_multipart(&mut router, PlacementGroupId(2), "media", "hero.png")
            .unwrap()
            .upload_id()
            .unwrap();
        let part = ObjectPartRef {
            number: 1,
            digest: part_digest("hero.png", 1),
            bytes: 10,
        };

        metadata
            .accept_part(&mut router, PlacementGroupId(2), upload_id, part.clone())
            .unwrap();
        assert!(
            metadata
                .accept_part(&mut router, PlacementGroupId(2), upload_id, part)
                .is_err()
        );
        assert!(
            metadata
                .commit_head(
                    &mut router,
                    PlacementGroupId(2),
                    upload_id,
                    Digest {
                        algorithm: rsf_core::DigestAlgorithm::Blake3,
                        bytes: [0; 32],
                    },
                )
                .is_err()
        );
        assert_eq!(metadata.report().rejections, 2);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-objectmeta-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn part_digest(key: &str, part: u32) -> Digest {
        Digest::blake3_bytes(format!("object-part:{key}:{part}").as_bytes())
    }

    fn start_upload_with_part(
        metadata: &mut ObjectMetadataEngine,
        router: &mut FabricRouter,
        key: &str,
        part_number: u16,
    ) -> u64 {
        let upload_id = metadata
            .start_multipart(router, PlacementGroupId(2), "media", key)
            .expect("start")
            .upload_id()
            .expect("upload id");
        metadata
            .accept_part(
                router,
                PlacementGroupId(2),
                upload_id,
                ObjectPartRef {
                    number: part_number,
                    digest: part_digest(key, u32::from(part_number)),
                    bytes: 10,
                },
            )
            .expect("part");
        upload_id
    }

    fn manifest_digest(key: &str) -> Digest {
        Digest::blake3_bytes(format!("object-manifest:{key}").as_bytes())
    }
}
