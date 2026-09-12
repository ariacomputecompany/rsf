#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{
    Bytes, CommitConsistency, CommitEvidence, CommitVersion, Digest, DigestAlgorithm, ErrorCode,
    PlacementGroupId, RsfError, RsfResult, blob::ContentId,
};
use rsf_protocol::{
    ClientRequestId, CommitCertificate, ConsistencyMode, FabricOperation, FabricRouterPort,
};
use rsf_retention::{RetainedBlob, RetentionEngine, RetentionHook, RetentionPolicy};
use rsf_stream::{ReadStream, StreamChunkData, StreamPlanner, StreamTarget, WriteStream};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredBlob {
    pub id: ContentId,
    pub bytes: Bytes,
    pub pin_version: CommitVersion,
    pub pin_evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredBlobStream {
    pub write: WriteStream,
    pub chunks: Vec<StoredBlob>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBlobStream {
    pub read: ReadStream,
    pub bytes: Bytes,
}

#[derive(Clone, Debug, Default)]
pub struct BlobEngine {
    chunks: BTreeMap<ContentId, StoredBlob>,
    pins: BTreeSet<ContentId>,
}

impl BlobEngine {
    pub fn put(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        bytes: impl Into<Bytes>,
    ) -> RsfResult<StoredBlob> {
        let bytes = bytes.into();
        let id = ContentId::for_bytes(&bytes);
        self.put_by_digest(router, pg, id.digest()?, bytes)
    }

    pub fn put_by_digest(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        digest: Digest,
        bytes: impl Into<Bytes>,
    ) -> RsfResult<StoredBlob> {
        let bytes = bytes.into();
        let id = content_id_from_digest(digest)?;
        if ContentId::for_bytes(&bytes) != id {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "blob payload does not match supplied digest",
            ));
        }
        if let Some(existing) = self.chunks.get(&id) {
            self.pins.insert(id);
            return Ok(existing.clone());
        }

        let commit = router.commit_request_with_mode(
            pg,
            ClientRequestId::new(format!("blob:{}", hex_digest(&id.0)), 0),
            ConsistencyMode::AvailabilityFirst,
            FabricOperation::BlobPin { digest: id.0 },
        )?;
        let blob = StoredBlob {
            id: id.clone(),
            bytes,
            pin_version: commit.certificate.version,
            pin_evidence: commit_evidence(commit.certificate),
        };
        self.chunks.insert(id.clone(), blob.clone());
        self.pins.insert(id);
        Ok(blob)
    }

    pub fn get(&self, id: &ContentId) -> Option<&StoredBlob> {
        self.chunks.get(id)
    }

    pub fn get_by_digest(&self, digest: Digest) -> RsfResult<&StoredBlob> {
        let id = content_id_from_digest(digest)?;
        self.chunks.get(&id).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("blob {} is not present", digest),
            )
        })
    }

    pub fn is_pinned(&self, id: &ContentId) -> bool {
        self.pins.contains(id)
    }

    pub fn release_pin_with_retention(
        &mut self,
        policy: &RetentionPolicy,
        now: CommitVersion,
        id: &ContentId,
        referenced: bool,
    ) -> RsfResult<RetentionHook> {
        let Some(blob) = self.chunks.get(id) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "blob chunk is not present",
            ));
        };
        let hook = RetentionEngine.authorize_blob_pin_release(
            policy,
            now,
            &RetainedBlob {
                id: id.clone(),
                pin_version: blob.pin_version,
                referenced,
            },
        )?;
        self.pins.remove(id);
        Ok(hook)
    }

    pub fn read_verified(&self, id: &ContentId) -> RsfResult<Bytes> {
        let Some(blob) = self.chunks.get(id) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "blob chunk is not present",
            ));
        };
        blob.verify()?;
        Ok(blob.bytes.clone())
    }

    pub fn read_digest_verified(&self, digest: Digest) -> RsfResult<Bytes> {
        let blob = self.get_by_digest(digest)?;
        blob.verify()?;
        Ok(blob.bytes.clone())
    }

    pub fn write_stream(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        planner: &StreamPlanner,
        target: StreamTarget,
        bytes: impl Into<Bytes>,
    ) -> RsfResult<StoredBlobStream> {
        let bytes = bytes.into();
        let write = planner.plan_write_bytes(target, &bytes)?;
        let chunks = planner.chunk_bytes(&write, &bytes)?;
        let mut stored = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let digest = Digest::blake3(chunk.chunk.expected_hash)?;
            stored.push(self.put_by_digest(router, pg, digest, chunk.bytes)?);
        }
        Ok(StoredBlobStream {
            write,
            chunks: stored,
        })
    }

    pub fn read_stream(
        &self,
        planner: &StreamPlanner,
        read: ReadStream,
    ) -> RsfResult<ReadBlobStream> {
        let mut bytes = Vec::new();
        let mut chunks = Vec::with_capacity(read.chunks.len());
        for chunk in &read.chunks {
            let digest = Digest::blake3(chunk.expected_hash)?;
            let part = self.read_digest_verified(digest)?;
            bytes.extend_from_slice(&part);
            chunks.push(StreamChunkData {
                chunk: chunk.clone(),
                bytes: part,
            });
        }
        planner.verify_bytes(&read, &chunks)?;
        Ok(ReadBlobStream { read, bytes })
    }

    #[cfg(test)]
    fn corrupt_for_test(&mut self, id: &ContentId) {
        if let Some(blob) = self.chunks.get_mut(id)
            && let Some(first) = blob.bytes.first_mut()
        {
            *first = first.wrapping_add(1);
        }
    }
}

fn content_id_from_digest(digest: Digest) -> RsfResult<ContentId> {
    digest.validate()?;
    if digest.algorithm != DigestAlgorithm::Blake3 {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!(
                "blob content ids require blake3 digests, got {}",
                digest.algorithm
            ),
        ));
    }
    ContentId::from_blake3(digest.bytes)
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

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

impl StoredBlob {
    pub fn verify(&self) -> RsfResult<()> {
        if ContentId::for_bytes(&self.bytes) != self.id {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "blob digest verification failed",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{Epoch, NodeId};
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use rsf_stream::{StreamPolicy, StreamSurface};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn blob_put_pins_content_through_protocol() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();

        let blob = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("put");

        assert!(blobs.is_pinned(&blob.id));
        assert_eq!(
            blobs.get(&blob.id).expect("blob").bytes,
            b"payload".to_vec()
        );
        assert_eq!(router.committed().len(), 1);
        assert_eq!(
            router.committed()[0].mode,
            ConsistencyMode::AvailabilityFirst
        );
    }

    #[test]
    fn duplicate_blob_dedupes_without_new_protocol_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();

        let first = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("first put");
        let second = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("second put");

        assert_eq!(first.id, second.id);
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn blob_put_and_get_by_digest_verify_supplied_identity() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let digest = Digest::blake3_bytes(b"payload");

        let blob = blobs
            .put_by_digest(
                &mut router,
                PlacementGroupId(3),
                digest,
                b"payload".to_vec(),
            )
            .expect("put by digest");

        assert_eq!(blob.id, ContentId::for_bytes(b"payload"));
        assert_eq!(
            blobs
                .get_by_digest(digest)
                .expect("get by digest")
                .pin_version,
            CommitVersion(1)
        );
        assert_eq!(
            blobs.read_digest_verified(digest).expect("read by digest"),
            b"payload".to_vec()
        );
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn blob_put_by_digest_rejects_mismatched_payload_without_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let digest = Digest::blake3_bytes(b"payload");

        let err = blobs
            .put_by_digest(
                &mut router,
                PlacementGroupId(3),
                digest,
                b"different".to_vec(),
            )
            .expect_err("mismatched payload should be rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert_eq!(router.committed().len(), 0);
    }

    #[test]
    fn blob_get_by_digest_rejects_unsupported_digest_algorithm() {
        let blobs = BlobEngine::default();
        let digest = Digest::sha256([7_u8; 32]).expect("sha256 digest");

        let err = blobs
            .get_by_digest(digest)
            .expect_err("sha256 is not a content id");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn retention_policy_blocks_blob_pin_release_when_referenced() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let blob = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("put");

        let err = blobs
            .release_pin_with_retention(&retention_policy(), CommitVersion(20), &blob.id, true)
            .expect_err("referenced blob pin is retained");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(blobs.is_pinned(&blob.id));
    }

    #[test]
    fn retention_policy_releases_expired_unreferenced_blob_pin() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let blob = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("put");

        let hook = blobs
            .release_pin_with_retention(&retention_policy(), CommitVersion(20), &blob.id, false)
            .expect("release");

        assert!(matches!(hook, RetentionHook::BlobReleaseAllowed { .. }));
        assert!(!blobs.is_pinned(&blob.id));
    }

    #[test]
    fn legal_hold_blocks_blob_pin_release() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let blob = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("put");
        let mut policy = retention_policy();
        policy.legal_hold = true;

        let err = blobs
            .release_pin_with_retention(&policy, CommitVersion(20), &blob.id, false)
            .expect_err("legal hold retains pin");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(blobs.is_pinned(&blob.id));
    }

    #[test]
    fn streaming_write_pins_chunks_and_read_reassembles_verified_bytes() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let planner = stream_planner();
        let target = StreamTarget::new(StreamSurface::Blob, "app", "backup.bin");

        let written = blobs
            .write_stream(
                &mut router,
                PlacementGroupId(3),
                &planner,
                target.clone(),
                b"streaming-payload".to_vec(),
            )
            .expect("stream write");
        let read = planner
            .plan_read(target, CommitVersion(1), written.write.chunks.clone())
            .expect("read plan");
        let readback = blobs.read_stream(&planner, read).expect("stream read");

        assert_eq!(written.write.chunks.len(), 3);
        assert_eq!(written.chunks.len(), 3);
        assert_eq!(router.committed().len(), 3);
        assert_eq!(readback.bytes, b"streaming-payload".to_vec());
    }

    #[test]
    fn repeated_streaming_write_dedupes_existing_chunk_blobs() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let planner = stream_planner();
        let target = StreamTarget::new(StreamSurface::Blob, "app", "backup.bin");

        let first = blobs
            .write_stream(
                &mut router,
                PlacementGroupId(3),
                &planner,
                target.clone(),
                b"streaming-payload".to_vec(),
            )
            .expect("first write");
        let second = blobs
            .write_stream(
                &mut router,
                PlacementGroupId(3),
                &planner,
                target,
                b"streaming-payload".to_vec(),
            )
            .expect("second write");

        assert_eq!(first.write.chunks, second.write.chunks);
        assert_eq!(router.committed().len(), first.write.chunks.len());
    }

    #[test]
    fn streaming_read_detects_corrupt_chunk_payloads() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let planner = stream_planner();
        let target = StreamTarget::new(StreamSurface::Blob, "app", "backup.bin");
        let written = blobs
            .write_stream(
                &mut router,
                PlacementGroupId(3),
                &planner,
                target.clone(),
                b"streaming-payload".to_vec(),
            )
            .expect("stream write");
        let first = written.chunks[0].id.clone();
        blobs.corrupt_for_test(&first);
        let read = planner
            .plan_read(target, CommitVersion(1), written.write.chunks)
            .expect("read plan");

        let err = blobs
            .read_stream(&planner, read)
            .expect_err("corrupt stream chunk rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn blob_pin_commits_through_vsr_backed_router_and_recovers_certificate() {
        let path = test_path("blob_pin_commits_through_vsr_backed_router");
        let blob_id = {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(3)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");
            let mut blobs = BlobEngine::default();

            let blob = blobs
                .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
                .expect("put");

            assert_eq!(blob.pin_version, CommitVersion(1));
            assert_eq!(router.latest_version(), CommitVersion(1));
            blob.id
        };

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(3)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router");

        assert_eq!(blob_id, ContentId::for_bytes(b"payload"));
        assert_eq!(recovered.latest_version(), CommitVersion(1));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(3))
                .expect("certificates")
                .len(),
            1
        );
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(3))
                .expect("certificates")[0]
                .mode,
            ConsistencyMode::AvailabilityFirst
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn blob_pin_retry_replays_vsr_request_certificate_without_new_commit() {
        let path = test_path("blob_pin_retry_replays_vsr_request_certificate");
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(3)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");
            let mut blobs = BlobEngine::default();

            let blob = blobs
                .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
                .expect("put");

            assert_eq!(blob.pin_version, CommitVersion(1));
            assert_eq!(router.latest_version(), CommitVersion(1));
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(3)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("recover router");
            let mut retried = BlobEngine::default();

            let blob = retried
                .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
                .expect("retry put");

            assert_eq!(blob.pin_version, CommitVersion(1));
            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(
                retried.read_verified(&blob.id).unwrap(),
                b"payload".to_vec()
            );
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(3)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router again");

        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(3))
                .expect("certificates")
                .len(),
            1
        );
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(3))
                .expect("certificates")[0]
                .mode,
            ConsistencyMode::AvailabilityFirst
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn verified_reads_detect_corruption() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();
        let blob = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("put");

        assert_eq!(
            blobs.read_verified(&blob.id).expect("verified read"),
            b"payload".to_vec()
        );

        blobs.corrupt_for_test(&blob.id);
        let err = blobs
            .read_verified(&blob.id)
            .expect_err("corruption should be detected");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn blob_content_id_matches_core_blake3_contract() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(3)]);
        let mut blobs = BlobEngine::default();

        let blob = blobs
            .put(&mut router, PlacementGroupId(3), b"payload".to_vec())
            .expect("put");

        assert_eq!(blob.id, ContentId::for_bytes(b"payload"));
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-blob-{name}-{}-{nonce}", std::process::id()))
    }

    fn stream_planner() -> StreamPlanner {
        StreamPlanner::new(StreamPolicy {
            max_chunk_bytes: 6,
            max_inflight_bytes: 12,
            read_lanes: 2,
        })
        .expect("planner")
    }

    fn retention_policy() -> RetentionPolicy {
        RetentionPolicy {
            namespace: "app".to_string(),
            retain_until: CommitVersion(0),
            expire_after_versions: Some(10),
            compact_noncurrent_after_versions: Some(6),
            legal_hold: false,
        }
    }
}
