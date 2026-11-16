#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use rsf_core::{CommitVersion, Digest, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CheckpointRootKind {
    Catalog,
    TransactionLog,
    Kv,
    Sql,
    Object,
    Blob,
    ChunkIndex,
}

impl CheckpointRootKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::TransactionLog => "transaction-log",
            Self::Kv => "kv",
            Self::Sql => "sql",
            Self::Object => "object",
            Self::Blob => "blob",
            Self::ChunkIndex => "chunk-index",
        }
    }

    pub fn required(self) -> bool {
        matches!(
            self,
            Self::Catalog
                | Self::TransactionLog
                | Self::Kv
                | Self::Sql
                | Self::Object
                | Self::Blob
                | Self::ChunkIndex
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRoot {
    pub kind: CheckpointRootKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub bytes: u64,
    pub checksum: Digest,
}

impl CheckpointRoot {
    pub fn new(
        kind: CheckpointRootKind,
        namespace: impl Into<String>,
        name: impl Into<String>,
        version: CommitVersion,
        bytes: u64,
        checksum: Digest,
    ) -> Self {
        Self {
            kind,
            namespace: namespace.into(),
            name: name.into(),
            version,
            bytes,
            checksum,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentInventory {
    pub id: String,
    pub max_version: CommitVersion,
    pub bytes: u64,
    pub live_bytes: u64,
    pub contains_required_root: bool,
}

impl SegmentInventory {
    pub fn new(
        id: impl Into<String>,
        max_version: CommitVersion,
        bytes: u64,
        live_bytes: u64,
        contains_required_root: bool,
    ) -> Self {
        Self {
            id: id.into(),
            max_version,
            bytes,
            live_bytes,
            contains_required_root,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointDecision {
    RetainRoot {
        kind: CheckpointRootKind,
        name: String,
        version: CommitVersion,
        bytes: u64,
    },
    KeepSegment {
        id: String,
        reason: String,
    },
    CompactSegment {
        id: String,
        reclaimable_bytes: u64,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointPlan {
    pub checkpoint: CommitVersion,
    pub roots: Vec<CheckpointRoot>,
    pub segments: Vec<SegmentInventory>,
    pub decisions: Vec<CheckpointDecision>,
    pub retained_roots: usize,
    pub compacted_segments: usize,
    pub reclaimable_bytes: u64,
    pub high_watermark: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointCertificate {
    pub checkpoint: CommitVersion,
    pub high_watermark: CommitVersion,
    pub retained_roots: usize,
    pub compacted_segments: usize,
    pub reclaimable_bytes: u64,
    pub root_digest: Digest,
    pub segment_digest: Digest,
    pub decision_digest: Digest,
    pub certificate_digest: Digest,
}

#[derive(Clone, Debug, Default)]
pub struct CheckpointPlanner;

impl CheckpointPlanner {
    pub fn plan(
        &self,
        checkpoint: CommitVersion,
        roots: Vec<CheckpointRoot>,
        segments: Vec<SegmentInventory>,
    ) -> RsfResult<CheckpointPlan> {
        validate_roots(checkpoint, &roots)?;
        validate_segments(&segments)?;

        let high_watermark = roots
            .iter()
            .map(|root| root.version)
            .max()
            .unwrap_or(CommitVersion(0));
        let mut retained_roots = 0;
        let mut compacted_segments = 0;
        let mut reclaimable_bytes = 0;
        let mut decisions = Vec::new();

        for root in &roots {
            retained_roots += 1;
            decisions.push(CheckpointDecision::RetainRoot {
                kind: root.kind,
                name: root.name.clone(),
                version: root.version,
                bytes: root.bytes,
            });
        }

        for segment in &segments {
            if segment.contains_required_root {
                decisions.push(CheckpointDecision::KeepSegment {
                    id: segment.id.clone(),
                    reason: "contains required checkpoint root".to_string(),
                });
                continue;
            }
            if segment.max_version > checkpoint {
                decisions.push(CheckpointDecision::KeepSegment {
                    id: segment.id.clone(),
                    reason: "segment contains records newer than checkpoint".to_string(),
                });
                continue;
            }
            let reclaimable = segment.bytes.saturating_sub(segment.live_bytes);
            if reclaimable == 0 {
                decisions.push(CheckpointDecision::KeepSegment {
                    id: segment.id.clone(),
                    reason: "segment has no reclaimable bytes".to_string(),
                });
                continue;
            }
            compacted_segments += 1;
            reclaimable_bytes += reclaimable;
            decisions.push(CheckpointDecision::CompactSegment {
                id: segment.id.clone(),
                reclaimable_bytes: reclaimable,
            });
        }

        Ok(CheckpointPlan {
            checkpoint,
            roots,
            segments,
            decisions,
            retained_roots,
            compacted_segments,
            reclaimable_bytes,
            high_watermark,
        })
    }

    pub fn certify(&self, plan: &CheckpointPlan) -> RsfResult<CheckpointCertificate> {
        validate_roots(plan.checkpoint, &plan.roots)?;
        validate_segments(&plan.segments)?;
        validate_plan_counters(plan)?;
        let root_digest = digest_roots(&plan.roots);
        let segment_digest = digest_segments(&plan.segments);
        let decision_digest = digest_decisions(&plan.decisions);
        let certificate_digest =
            digest_certificate_fields(plan, root_digest, segment_digest, decision_digest);

        Ok(CheckpointCertificate {
            checkpoint: plan.checkpoint,
            high_watermark: plan.high_watermark,
            retained_roots: plan.retained_roots,
            compacted_segments: plan.compacted_segments,
            reclaimable_bytes: plan.reclaimable_bytes,
            root_digest,
            segment_digest,
            decision_digest,
            certificate_digest,
        })
    }

    pub fn verify_certificate(
        &self,
        plan: &CheckpointPlan,
        certificate: &CheckpointCertificate,
    ) -> RsfResult<()> {
        let expected = self.certify(plan)?;
        if &expected == certificate {
            return Ok(());
        }
        Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "checkpoint certificate does not match checkpoint plan",
        ))
    }
}

pub fn reject_if_checkpoint_regresses(
    previous: CommitVersion,
    next: CommitVersion,
) -> CheckpointDecision {
    if next < previous {
        return CheckpointDecision::Rejected {
            reason: format!(
                "checkpoint {} cannot regress behind previous {}",
                next.0, previous.0
            ),
        };
    }
    CheckpointDecision::KeepSegment {
        id: "checkpoint".to_string(),
        reason: format!("checkpoint advanced from {} to {}", previous.0, next.0),
    }
}

fn validate_roots(checkpoint: CommitVersion, roots: &[CheckpointRoot]) -> RsfResult<()> {
    if roots.is_empty() {
        return Err(invalid("checkpoint requires root inventory"));
    }

    let mut seen = BTreeSet::new();
    for root in roots {
        if root.namespace.trim().is_empty() {
            return Err(invalid("checkpoint root namespace must not be empty"));
        }
        if root.name.trim().is_empty() {
            return Err(invalid("checkpoint root name must not be empty"));
        }
        if root.bytes == 0 {
            return Err(invalid(format!("checkpoint root {} is empty", root.name)));
        }
        if root.checksum.validate().is_err() {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("checkpoint root {} has empty checksum", root.name),
            ));
        }
        if root.version > checkpoint {
            return Err(invalid(format!(
                "checkpoint root {} version {} is newer than checkpoint {}",
                root.name, root.version.0, checkpoint.0
            )));
        }
        let key = (root.kind, root.namespace.clone(), root.name.clone());
        if !seen.insert(key) {
            return Err(invalid(format!(
                "duplicate checkpoint root {}:{}",
                root.kind.as_str(),
                root.name
            )));
        }
    }

    for required in [
        CheckpointRootKind::Catalog,
        CheckpointRootKind::TransactionLog,
        CheckpointRootKind::Kv,
        CheckpointRootKind::Sql,
        CheckpointRootKind::Object,
        CheckpointRootKind::Blob,
        CheckpointRootKind::ChunkIndex,
    ] {
        if !roots.iter().any(|root| root.kind == required) {
            return Err(invalid(format!(
                "checkpoint missing required {} root",
                required.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_segments(segments: &[SegmentInventory]) -> RsfResult<()> {
    let mut seen = BTreeSet::new();
    for segment in segments {
        if segment.id.trim().is_empty() {
            return Err(invalid("segment id must not be empty"));
        }
        if segment.bytes == 0 {
            return Err(invalid(format!("segment {} is empty", segment.id)));
        }
        if segment.live_bytes > segment.bytes {
            return Err(invalid(format!(
                "segment {} live bytes exceed total bytes",
                segment.id
            )));
        }
        if !seen.insert(segment.id.clone()) {
            return Err(invalid(format!("duplicate segment {}", segment.id)));
        }
    }
    Ok(())
}

fn validate_plan_counters(plan: &CheckpointPlan) -> RsfResult<()> {
    let retained_roots = plan
        .decisions
        .iter()
        .filter(|decision| matches!(decision, CheckpointDecision::RetainRoot { .. }))
        .count();
    if retained_roots != plan.retained_roots || retained_roots != plan.roots.len() {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "checkpoint retained-root counter does not match decisions",
        ));
    }

    let mut compacted_segments = 0;
    let mut reclaimable_bytes = 0_u64;
    for decision in &plan.decisions {
        if let CheckpointDecision::CompactSegment {
            reclaimable_bytes: bytes,
            ..
        } = decision
        {
            compacted_segments += 1;
            reclaimable_bytes = reclaimable_bytes.saturating_add(*bytes);
        }
    }
    if compacted_segments != plan.compacted_segments || reclaimable_bytes != plan.reclaimable_bytes
    {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "checkpoint compaction counters do not match decisions",
        ));
    }

    let high_watermark = plan
        .roots
        .iter()
        .map(|root| root.version)
        .max()
        .unwrap_or(CommitVersion(0));
    if high_watermark != plan.high_watermark {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "checkpoint high watermark does not match root inventory",
        ));
    }

    Ok(())
}

fn digest_roots(roots: &[CheckpointRoot]) -> Digest {
    let mut bytes = Vec::new();
    for root in roots {
        write_str(&mut bytes, root.kind.as_str());
        write_str(&mut bytes, &root.namespace);
        write_str(&mut bytes, &root.name);
        write_u64(&mut bytes, root.version.0);
        write_u64(&mut bytes, root.bytes);
        write_str(&mut bytes, root.checksum.algorithm.as_str());
        bytes.extend_from_slice(&root.checksum.bytes);
    }
    Digest::blake3_bytes(&bytes)
}

fn digest_segments(segments: &[SegmentInventory]) -> Digest {
    let mut bytes = Vec::new();
    for segment in segments {
        write_str(&mut bytes, &segment.id);
        write_u64(&mut bytes, segment.max_version.0);
        write_u64(&mut bytes, segment.bytes);
        write_u64(&mut bytes, segment.live_bytes);
        bytes.push(u8::from(segment.contains_required_root));
    }
    Digest::blake3_bytes(&bytes)
}

fn digest_decisions(decisions: &[CheckpointDecision]) -> Digest {
    let mut bytes = Vec::new();
    for decision in decisions {
        match decision {
            CheckpointDecision::RetainRoot {
                kind,
                name,
                version,
                bytes: root_bytes,
            } => {
                bytes.push(1);
                write_str(&mut bytes, kind.as_str());
                write_str(&mut bytes, name);
                write_u64(&mut bytes, version.0);
                write_u64(&mut bytes, *root_bytes);
            }
            CheckpointDecision::KeepSegment { id, reason } => {
                bytes.push(2);
                write_str(&mut bytes, id);
                write_str(&mut bytes, reason);
            }
            CheckpointDecision::CompactSegment {
                id,
                reclaimable_bytes,
            } => {
                bytes.push(3);
                write_str(&mut bytes, id);
                write_u64(&mut bytes, *reclaimable_bytes);
            }
            CheckpointDecision::Rejected { reason } => {
                bytes.push(4);
                write_str(&mut bytes, reason);
            }
        }
    }
    Digest::blake3_bytes(&bytes)
}

fn digest_certificate_fields(
    plan: &CheckpointPlan,
    root_digest: Digest,
    segment_digest: Digest,
    decision_digest: Digest,
) -> Digest {
    let mut bytes = Vec::new();
    write_u64(&mut bytes, plan.checkpoint.0);
    write_u64(&mut bytes, plan.high_watermark.0);
    write_u64(&mut bytes, plan.retained_roots as u64);
    write_u64(&mut bytes, plan.compacted_segments as u64);
    write_u64(&mut bytes, plan.reclaimable_bytes);
    bytes.extend_from_slice(&root_digest.bytes);
    bytes.extend_from_slice(&segment_digest.bytes);
    bytes.extend_from_slice(&decision_digest.bytes);
    Digest::blake3_bytes(&bytes)
}

fn write_str(out: &mut Vec<u8>, value: &str) {
    write_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_safe_checkpoint_compaction() {
        let plan = CheckpointPlanner
            .plan(CommitVersion(22), roots(), segments())
            .expect("plan");

        assert_eq!(plan.retained_roots, 7);
        assert_eq!(plan.compacted_segments, 2);
        assert_eq!(plan.reclaimable_bytes, 3_584);
        assert_eq!(plan.high_watermark, CommitVersion(22));
    }

    #[test]
    fn keeps_required_and_future_segments() {
        let plan = CheckpointPlanner
            .plan(CommitVersion(22), roots(), segments())
            .expect("plan");

        assert!(plan.decisions.iter().any(|decision| {
            matches!(
                decision,
                CheckpointDecision::KeepSegment { id, reason }
                    if id == "segment-0001" && reason.contains("required checkpoint root")
            )
        }));
        assert!(plan.decisions.iter().any(|decision| {
            matches!(
                decision,
                CheckpointDecision::KeepSegment { id, reason }
                    if id == "segment-0004" && reason.contains("newer than checkpoint")
            )
        }));
    }

    #[test]
    fn rejects_missing_each_required_root() {
        for required in required_root_kinds() {
            let mut roots = roots();
            roots.retain(|root| root.kind != required);

            let err = CheckpointPlanner
                .plan(CommitVersion(22), roots, segments())
                .expect_err("missing required root must reject checkpoint");

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(err.message.contains(required.as_str()));
        }
    }

    #[test]
    fn rejects_empty_root_checksum_as_corruption() {
        let mut roots = roots();
        roots[0].checksum = Digest {
            algorithm: rsf_core::DigestAlgorithm::Blake3,
            bytes: [0_u8; 32],
        };

        let err = CheckpointPlanner
            .plan(CommitVersion(22), roots, segments())
            .expect_err("empty checksum must reject checkpoint");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn rejects_segment_with_impossible_live_bytes() {
        let mut segments = segments();
        segments[1].live_bytes = segments[1].bytes + 1;

        let err = CheckpointPlanner
            .plan(CommitVersion(22), roots(), segments)
            .expect_err("impossible live bytes must reject checkpoint");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("live bytes"));
    }

    #[test]
    fn detects_checkpoint_regression() {
        assert!(matches!(
            reject_if_checkpoint_regresses(CommitVersion(22), CommitVersion(21)),
            CheckpointDecision::Rejected { .. }
        ));
    }

    #[test]
    fn certifies_checkpoint_plan_with_deterministic_digests() {
        let plan = CheckpointPlanner
            .plan(CommitVersion(22), roots(), segments())
            .expect("plan");
        let first = CheckpointPlanner.certify(&plan).expect("certificate");
        let second = CheckpointPlanner.certify(&plan).expect("certificate");

        assert_eq!(first, second);
        assert_eq!(first.checkpoint, CommitVersion(22));
        assert_eq!(first.retained_roots, 7);
        assert_eq!(first.compacted_segments, 2);
        assert_eq!(first.reclaimable_bytes, 3_584);
        first.certificate_digest.validate().unwrap();
        CheckpointPlanner
            .verify_certificate(&plan, &first)
            .expect("certificate verifies");
    }

    #[test]
    fn certificate_rejects_tampered_plan_decisions() {
        let mut plan = CheckpointPlanner
            .plan(CommitVersion(22), roots(), segments())
            .expect("plan");
        let certificate = CheckpointPlanner.certify(&plan).expect("certificate");
        plan.decisions.push(CheckpointDecision::Rejected {
            reason: "tampered".to_string(),
        });

        let err = CheckpointPlanner
            .verify_certificate(&plan, &certificate)
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("does not match"));
    }

    #[test]
    fn certificate_rejects_counter_mismatch() {
        let mut plan = CheckpointPlanner
            .plan(CommitVersion(22), roots(), segments())
            .expect("plan");
        plan.compacted_segments += 1;

        let err = CheckpointPlanner.certify(&plan).unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("compaction counters"));
    }

    #[test]
    fn certificate_rejects_high_watermark_mismatch() {
        let mut plan = CheckpointPlanner
            .plan(CommitVersion(22), roots(), segments())
            .expect("plan");
        plan.high_watermark = CommitVersion(21);

        let err = CheckpointPlanner.certify(&plan).unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("high watermark"));
    }

    fn roots() -> Vec<CheckpointRoot> {
        vec![
            root(CheckpointRootKind::Catalog, "catalog/main", 22, 512, 1),
            root(
                CheckpointRootKind::TransactionLog,
                "fabric/wal",
                22,
                2_048,
                2,
            ),
            root(CheckpointRootKind::Kv, "kv/app/config", 21, 1_024, 3),
            root(CheckpointRootKind::Sql, "sql/app/users", 21, 4_096, 4),
            root(
                CheckpointRootKind::Object,
                "object/media/hero.png",
                20,
                3_072,
                5,
            ),
            root(
                CheckpointRootKind::Blob,
                "blob/sha256-content",
                22,
                1_024,
                6,
            ),
            root(
                CheckpointRootKind::ChunkIndex,
                "chunk/index/main",
                22,
                256,
                7,
            ),
        ]
    }

    fn required_root_kinds() -> [CheckpointRootKind; 7] {
        [
            CheckpointRootKind::Catalog,
            CheckpointRootKind::TransactionLog,
            CheckpointRootKind::Kv,
            CheckpointRootKind::Sql,
            CheckpointRootKind::Object,
            CheckpointRootKind::Blob,
            CheckpointRootKind::ChunkIndex,
        ]
    }

    fn root(
        kind: CheckpointRootKind,
        name: &str,
        version: u64,
        bytes: u64,
        checksum: u8,
    ) -> CheckpointRoot {
        CheckpointRoot::new(
            kind,
            "app",
            name,
            CommitVersion(version),
            bytes,
            Digest::blake3_bytes(format!("checkpoint-root:{name}:{checksum}").as_bytes()),
        )
    }

    fn segments() -> Vec<SegmentInventory> {
        vec![
            SegmentInventory::new("segment-0001", CommitVersion(22), 4_096, 4_096, true),
            SegmentInventory::new("segment-0002", CommitVersion(20), 4_096, 1_024, false),
            SegmentInventory::new("segment-0003", CommitVersion(22), 2_048, 1_536, false),
            SegmentInventory::new("segment-0004", CommitVersion(23), 1_024, 512, false),
        ]
    }
}
