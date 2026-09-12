use std::collections::BTreeSet;

mod ledger;

pub use ledger::{
    DurableRestoreLedger, RestoreOperation, RestoreOperationId, RestoreOperationState,
};

use rsf_core::{CommitVersion, Digest, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackupItemKind {
    CatalogRoot,
    TransactionLog,
    KvRange,
    SqlTable,
    ObjectManifest,
    BlobChunk,
    ChunkIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupItem {
    pub kind: BackupItemKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub checksum: Digest,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest {
    pub id: String,
    pub checkpoint: CommitVersion,
    pub created_at_epoch: u64,
    pub items: Vec<BackupItem>,
    pub checksum: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestorePlan {
    pub checkpoint: CommitVersion,
    pub manifest_id: String,
    pub manifest_checksum: Digest,
    pub item_count: usize,
    pub total_bytes: u64,
    pub checks: Vec<RestoreCheck>,
    pub steps: Vec<RestoreStep>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreExecution {
    pub checkpoint: CommitVersion,
    pub manifest_id: String,
    pub manifest_checksum: Digest,
    pub restored_items: usize,
    pub restored_bytes: u64,
    pub restored_kinds: Vec<BackupItemKind>,
    pub applied: Vec<RestoreApplied>,
    pub execution_checksum: Digest,
}

impl RestoreExecution {
    pub fn covers_manifest(&self, manifest: &SnapshotManifest) -> bool {
        self.checkpoint == manifest.checkpoint
            && self.manifest_id == manifest.id
            && self.manifest_checksum == manifest.checksum
            && self.restored_items == manifest.items.len()
            && self.restored_bytes == manifest.items.iter().map(|item| item.bytes).sum()
            && self.restored_kinds.contains(&BackupItemKind::CatalogRoot)
            && self
                .restored_kinds
                .contains(&BackupItemKind::TransactionLog)
            && self.restored_kinds.contains(&BackupItemKind::KvRange)
            && self.restored_kinds.contains(&BackupItemKind::SqlTable)
            && self
                .restored_kinds
                .contains(&BackupItemKind::ObjectManifest)
            && self.restored_kinds.contains(&BackupItemKind::BlobChunk)
            && self.restored_kinds.contains(&BackupItemKind::ChunkIndex)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreApplied {
    pub kind: BackupItemKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub bytes: u64,
    pub checksum: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreCheck {
    pub name: String,
    pub passed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreStep {
    pub kind: BackupItemKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub bytes: u64,
    pub checksum: Digest,
}

#[derive(Clone, Debug, Default)]
pub struct BackupPlanner;

impl BackupPlanner {
    pub fn create_manifest(
        &self,
        id: impl Into<String>,
        checkpoint: CommitVersion,
        created_at_epoch: u64,
        items: Vec<BackupItem>,
    ) -> RsfResult<SnapshotManifest> {
        let id = id.into();
        if id.is_empty() {
            return Err(invalid("snapshot id must not be empty"));
        }
        if items.is_empty() {
            return Err(invalid("snapshot manifest must contain at least one item"));
        }
        validate_manifest_coverage(&items)?;
        validate_manifest_uniqueness(&items)?;
        for item in &items {
            validate_item(item, checkpoint)?;
        }
        let checksum = manifest_checksum(&id, checkpoint, created_at_epoch, &items);
        Ok(SnapshotManifest {
            id,
            checkpoint,
            created_at_epoch,
            items,
            checksum,
        })
    }

    pub fn plan_restore(&self, manifest: &SnapshotManifest) -> RsfResult<RestorePlan> {
        if manifest.items.is_empty() {
            return Err(invalid("restore manifest must contain at least one item"));
        }
        validate_manifest_coverage(&manifest.items)?;
        validate_manifest_uniqueness(&manifest.items)?;
        validate_manifest_checksum(manifest)?;
        for item in &manifest.items {
            validate_item(item, manifest.checkpoint)?;
        }

        let total_bytes = manifest.items.iter().map(|item| item.bytes).sum();
        let steps = restore_steps(&manifest.items);
        Ok(RestorePlan {
            checkpoint: manifest.checkpoint,
            manifest_id: manifest.id.clone(),
            manifest_checksum: manifest.checksum,
            item_count: manifest.items.len(),
            total_bytes,
            checks: vec![
                RestoreCheck {
                    name: "catalog-root".to_string(),
                    passed: true,
                },
                RestoreCheck {
                    name: "transaction-log".to_string(),
                    passed: true,
                },
                RestoreCheck {
                    name: "item-checksums".to_string(),
                    passed: true,
                },
                RestoreCheck {
                    name: "manifest-checksum".to_string(),
                    passed: true,
                },
                RestoreCheck {
                    name: "surface-coverage".to_string(),
                    passed: true,
                },
            ],
            steps,
        })
    }

    pub fn execute_restore(
        &self,
        manifest: &SnapshotManifest,
        plan: &RestorePlan,
    ) -> RsfResult<RestoreExecution> {
        if plan.checkpoint != manifest.checkpoint {
            return Err(invalid("restore plan checkpoint does not match manifest"));
        }
        if plan.manifest_id != manifest.id {
            return Err(invalid("restore plan manifest id does not match manifest"));
        }
        if plan.manifest_checksum != manifest.checksum {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "restore plan manifest checksum does not match manifest",
            ));
        }
        if plan.item_count != manifest.items.len() {
            return Err(invalid("restore plan item count does not match manifest"));
        }
        if plan.steps.len() != manifest.items.len() {
            return Err(invalid("restore plan step count does not match manifest"));
        }
        if !plan.checks.iter().all(|check| check.passed) {
            return Err(invalid("restore plan has failed checks"));
        }
        validate_manifest_checksum(manifest)?;
        validate_restore_steps(manifest, plan)?;
        for item in &manifest.items {
            validate_item(item, manifest.checkpoint)?;
        }

        let mut restored_kinds = manifest
            .items
            .iter()
            .map(|item| item.kind.clone())
            .collect::<Vec<_>>();
        restored_kinds.sort_by_key(kind_order);
        restored_kinds.dedup();

        let applied = applied_steps(&plan.steps);
        let execution_checksum = restore_execution_checksum(plan, &applied);

        Ok(RestoreExecution {
            checkpoint: manifest.checkpoint,
            manifest_id: manifest.id.clone(),
            manifest_checksum: manifest.checksum,
            restored_items: manifest.items.len(),
            restored_bytes: manifest.items.iter().map(|item| item.bytes).sum(),
            restored_kinds,
            applied,
            execution_checksum,
        })
    }
}

impl RestoreExecution {
    pub fn covers_plan(&self, plan: &RestorePlan) -> bool {
        self.checkpoint == plan.checkpoint
            && self.manifest_id == plan.manifest_id
            && self.manifest_checksum == plan.manifest_checksum
            && self.restored_items == plan.steps.len()
            && self.restored_bytes == plan.total_bytes
            && self.applied == applied_steps(&plan.steps)
            && self.execution_checksum == restore_execution_checksum(plan, &self.applied)
    }
}

fn applied_steps(steps: &[RestoreStep]) -> Vec<RestoreApplied> {
    steps
        .iter()
        .map(|step| RestoreApplied {
            kind: step.kind.clone(),
            namespace: step.namespace.clone(),
            name: step.name.clone(),
            version: step.version,
            bytes: step.bytes,
            checksum: step.checksum,
        })
        .collect()
}

fn restore_execution_checksum(plan: &RestorePlan, applied: &[RestoreApplied]) -> Digest {
    let entries = applied
        .iter()
        .map(|item| {
            format!(
                "{}\0{}\0{}\0{}\0{}\0{}\0{}",
                item.kind.as_str(),
                item.namespace,
                item.name,
                item.version.0,
                item.bytes,
                item.checksum.algorithm.as_str(),
                hex_digest(&item.checksum.bytes)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let payload = format!(
        "rsf.restore.execution.v1\0{}\0{}\0{}\0{}",
        plan.manifest_id,
        plan.checkpoint.0,
        hex_digest(&plan.manifest_checksum.bytes),
        entries
    );
    Digest::blake3_bytes(payload.as_bytes())
}

fn restore_steps(items: &[BackupItem]) -> Vec<RestoreStep> {
    let mut steps = items
        .iter()
        .map(|item| RestoreStep {
            kind: item.kind.clone(),
            namespace: item.namespace.clone(),
            name: item.name.clone(),
            version: item.version,
            bytes: item.bytes,
            checksum: item.checksum,
        })
        .collect::<Vec<_>>();
    steps.sort_by(|left, right| {
        kind_order(&left.kind)
            .cmp(&kind_order(&right.kind))
            .then(left.namespace.cmp(&right.namespace))
            .then(left.name.cmp(&right.name))
            .then(left.version.cmp(&right.version))
    });
    steps
}

fn validate_restore_steps(manifest: &SnapshotManifest, plan: &RestorePlan) -> RsfResult<()> {
    let expected = restore_steps(&manifest.items);
    if plan.steps != expected {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "restore plan steps do not match manifest items",
        ));
    }
    Ok(())
}

fn kind_order(kind: &BackupItemKind) -> u8 {
    match kind {
        BackupItemKind::CatalogRoot => 0,
        BackupItemKind::TransactionLog => 1,
        BackupItemKind::KvRange => 2,
        BackupItemKind::SqlTable => 3,
        BackupItemKind::ObjectManifest => 4,
        BackupItemKind::BlobChunk => 5,
        BackupItemKind::ChunkIndex => 6,
    }
}

fn validate_manifest_coverage(items: &[BackupItem]) -> RsfResult<()> {
    for required in [
        BackupItemKind::CatalogRoot,
        BackupItemKind::TransactionLog,
        BackupItemKind::KvRange,
        BackupItemKind::SqlTable,
        BackupItemKind::ObjectManifest,
        BackupItemKind::BlobChunk,
        BackupItemKind::ChunkIndex,
    ] {
        if !items.iter().any(|item| item.kind == required) {
            return Err(invalid(format!(
                "snapshot manifest is missing {}",
                required.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_manifest_uniqueness(items: &[BackupItem]) -> RsfResult<()> {
    let mut seen = BTreeSet::new();
    for item in items {
        let key = (item.kind.clone(), item.namespace.clone(), item.name.clone());
        if !seen.insert(key) {
            return Err(invalid(format!(
                "duplicate backup item {}:{}",
                item.kind.as_str(),
                item.name
            )));
        }
    }
    Ok(())
}

fn validate_manifest_checksum(manifest: &SnapshotManifest) -> RsfResult<()> {
    let expected = manifest_checksum(
        &manifest.id,
        manifest.checkpoint,
        manifest.created_at_epoch,
        &manifest.items,
    );
    if manifest.checksum != expected {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "snapshot manifest checksum does not match items",
        ));
    }
    Ok(())
}

fn manifest_checksum(
    id: &str,
    checkpoint: CommitVersion,
    created_at_epoch: u64,
    items: &[BackupItem],
) -> Digest {
    let mut entries = items
        .iter()
        .map(|item| {
            format!(
                "{}\0{}\0{}\0{}\0{}\0{}\0{}",
                item.kind.as_str(),
                item.namespace,
                item.name,
                item.version.0,
                item.bytes,
                item.checksum.algorithm.as_str(),
                hex_digest(&item.checksum.bytes)
            )
        })
        .collect::<Vec<_>>();
    entries.sort();
    let payload = format!(
        "rsf.backup.manifest.v1\0{id}\0{}\0{}\0{}",
        checkpoint.0,
        created_at_epoch,
        entries.join("\n")
    );
    Digest::blake3_bytes(payload.as_bytes())
}

impl BackupItemKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CatalogRoot => "catalog-root",
            Self::TransactionLog => "transaction-log",
            Self::KvRange => "kv-range",
            Self::SqlTable => "sql-table",
            Self::ObjectManifest => "object-manifest",
            Self::BlobChunk => "blob-chunk",
            Self::ChunkIndex => "chunk-index",
        }
    }
}

fn hex_digest(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn validate_item(item: &BackupItem, checkpoint: CommitVersion) -> RsfResult<()> {
    if item.namespace.is_empty() {
        return Err(invalid("backup item namespace must not be empty"));
    }
    if item.name.is_empty() {
        return Err(invalid("backup item name must not be empty"));
    }
    if item.version > checkpoint {
        return Err(invalid(format!(
            "backup item {} is newer than checkpoint {}",
            item.name, checkpoint.0
        )));
    }
    if item.bytes == 0 {
        return Err(invalid(format!("backup item {} has no bytes", item.name)));
    }
    if item.checksum.validate().is_err() {
        return Err(invalid(format!(
            "backup item {} has an empty checksum",
            item.name
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_plan_requires_each_required_item() {
        let planner = BackupPlanner;
        for required in required_item_kinds() {
            let required_label = required.as_str();
            let mut items = full_items();
            items.retain(|item| item.kind.as_str() != required_label);
            let err = planner
                .create_manifest("snapshot-1", CommitVersion(10), 1, items)
                .unwrap_err();

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(err.message.contains(required_label));
        }
    }

    #[test]
    fn restore_plan_counts_items_checks_and_bytes() {
        let planner = BackupPlanner;
        let manifest = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, full_items())
            .unwrap();

        let plan = planner.plan_restore(&manifest).unwrap();

        assert_eq!(plan.checkpoint, CommitVersion(10));
        assert_eq!(plan.manifest_id, "snapshot-1");
        assert_eq!(plan.manifest_checksum, manifest.checksum);
        assert_eq!(plan.item_count, 7);
        assert_eq!(plan.total_bytes, 1_376);
        assert_eq!(plan.checks.len(), 5);
        assert_eq!(plan.steps.len(), 7);
        assert!(plan.checks.iter().all(|check| check.passed));
        assert_eq!(plan.steps[0].kind, BackupItemKind::CatalogRoot);
        assert_eq!(plan.steps[6].kind, BackupItemKind::ChunkIndex);
    }

    #[test]
    fn restore_execution_covers_manifest_items_and_surfaces() {
        let planner = BackupPlanner;
        let manifest = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, full_items())
            .unwrap();
        let plan = planner.plan_restore(&manifest).unwrap();

        let execution = planner.execute_restore(&manifest, &plan).unwrap();

        assert_eq!(execution.manifest_id, manifest.id);
        assert_eq!(execution.manifest_checksum, manifest.checksum);
        assert_eq!(execution.restored_items, 7);
        assert_eq!(execution.restored_bytes, 1_376);
        assert_eq!(execution.applied.len(), 7);
        assert_eq!(execution.applied[0].kind, BackupItemKind::CatalogRoot);
        assert_eq!(execution.applied[6].kind, BackupItemKind::ChunkIndex);
        assert!(execution.execution_checksum.validate().is_ok());
        assert!(execution.covers_manifest(&manifest));
        assert!(execution.covers_plan(&plan));
    }

    #[test]
    fn restore_execution_checksum_detects_modified_applied_record() {
        let planner = BackupPlanner;
        let manifest = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, full_items())
            .unwrap();
        let plan = planner.plan_restore(&manifest).unwrap();
        let mut execution = planner.execute_restore(&manifest, &plan).unwrap();
        execution.applied[1].bytes += 1;

        assert!(!execution.covers_plan(&plan));
    }

    #[test]
    fn restore_execution_rejects_stale_plan_manifest_checksum() {
        let planner = BackupPlanner;
        let manifest = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, full_items())
            .unwrap();
        let mut plan = planner.plan_restore(&manifest).unwrap();
        plan.manifest_checksum = Digest::blake3_bytes(b"stale-plan");

        let err = planner.execute_restore(&manifest, &plan).unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("manifest checksum"));
    }

    #[test]
    fn restore_execution_rejects_plan_steps_that_do_not_match_manifest() {
        let planner = BackupPlanner;
        let manifest = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, full_items())
            .unwrap();
        let mut plan = planner.plan_restore(&manifest).unwrap();
        plan.steps[2].bytes += 1;

        let err = planner.execute_restore(&manifest, &plan).unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("steps"));
    }

    #[test]
    fn manifest_rejects_future_item() {
        let planner = BackupPlanner;
        let mut items = full_items();
        items[0].version = CommitVersion(11);
        let err = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, items)
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("newer than checkpoint"));
    }

    #[test]
    fn manifest_rejects_empty_checksum() {
        let planner = BackupPlanner;
        let mut items = full_items();
        items[0].checksum = Digest {
            algorithm: rsf_core::DigestAlgorithm::Blake3,
            bytes: [0; 32],
        };

        let err = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, items)
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("empty checksum"));
    }

    #[test]
    fn manifest_checksum_detects_tampered_items() {
        let planner = BackupPlanner;
        let mut manifest = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, full_items())
            .unwrap();
        manifest.items[2].bytes += 1;

        let err = planner.plan_restore(&manifest).unwrap_err();

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("checksum"));
    }

    #[test]
    fn manifest_rejects_duplicate_items() {
        let planner = BackupPlanner;
        let mut items = full_items();
        items.push(items[0].clone());

        let err = planner
            .create_manifest("snapshot-1", CommitVersion(10), 1, items)
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicate"));
    }

    fn full_items() -> Vec<BackupItem> {
        vec![
            item(BackupItemKind::CatalogRoot, "catalog", 10, 128),
            item(BackupItemKind::TransactionLog, "wal", 10, 256),
            item(BackupItemKind::KvRange, "kv/app", 9, 64),
            item(BackupItemKind::SqlTable, "sql/users", 9, 512),
            item(BackupItemKind::ObjectManifest, "object/hero", 9, 96),
            item(BackupItemKind::BlobChunk, "blob/chunk", 9, 256),
            item(BackupItemKind::ChunkIndex, "chunk/index", 10, 64),
        ]
    }

    fn required_item_kinds() -> [BackupItemKind; 7] {
        [
            BackupItemKind::CatalogRoot,
            BackupItemKind::TransactionLog,
            BackupItemKind::KvRange,
            BackupItemKind::SqlTable,
            BackupItemKind::ObjectManifest,
            BackupItemKind::BlobChunk,
            BackupItemKind::ChunkIndex,
        ]
    }

    fn item(kind: BackupItemKind, name: &str, version: u64, bytes: u64) -> BackupItem {
        BackupItem {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            version: CommitVersion(version),
            checksum: Digest::blake3_bytes(format!("backup:{name}:{version}:{bytes}").as_bytes()),
            bytes,
        }
    }
}
