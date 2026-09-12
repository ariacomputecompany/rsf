#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use rsf_backup::{BackupItemKind, BackupPlanner, RestoreExecution, SnapshotManifest};
use rsf_core::{CommitVersion, ErrorCode, RsfError, RsfResult};
use rsf_snapshot::{DatabaseSnapshot, SnapshotRootKind};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RecoveryRootKind {
    Catalog,
    TransactionLog,
    Kv,
    Sql,
    Object,
    Blob,
    ChunkIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryRoot {
    pub kind: RecoveryRootKind,
    pub namespace: String,
    pub name: String,
    pub durable_version: CommitVersion,
    pub present: bool,
    pub checksum_ok: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryActionKind {
    Reuse,
    Replay,
    Quarantine,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryAction {
    pub kind: RecoveryActionKind,
    pub root: RecoveryRoot,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryPlan {
    pub checkpoint: CommitVersion,
    pub roots: Vec<RecoveryRoot>,
    pub actions: Vec<RecoveryAction>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestartStepKind {
    VerifyInventory,
    ReuseRoot,
    ReplayRoot,
    QuarantineRoot,
    OpenForTraffic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestartStep {
    pub kind: RestartStepKind,
    pub root: Option<RecoveryRoot>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseRestartPlan {
    pub checkpoint: CommitVersion,
    pub recovery: RecoveryPlan,
    pub steps: Vec<RestartStep>,
    pub reusable_roots: usize,
    pub replay_roots: usize,
    pub quarantined_roots: usize,
    pub opens_for_traffic: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseRestartCertificate {
    pub checkpoint: CommitVersion,
    pub snapshot_id: String,
    pub restore_manifest_id: String,
    pub restored_items: usize,
    pub restart_steps: usize,
    pub replay_roots: usize,
    pub reusable_roots: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RecoveryPlanner;

impl RecoveryPlanner {
    pub fn plan(
        &self,
        checkpoint: CommitVersion,
        roots: Vec<RecoveryRoot>,
    ) -> RsfResult<RecoveryPlan> {
        validate_roots(checkpoint, &roots)?;
        let actions = roots
            .iter()
            .map(|root| {
                if !root.present || !root.checksum_ok {
                    return RecoveryAction {
                        kind: RecoveryActionKind::Quarantine,
                        root: root.clone(),
                        detail: "optional root quarantined for operator repair".to_string(),
                    };
                }
                if root.durable_version == checkpoint {
                    return RecoveryAction {
                        kind: RecoveryActionKind::Reuse,
                        root: root.clone(),
                        detail: "root already matches checkpoint".to_string(),
                    };
                }
                RecoveryAction {
                    kind: RecoveryActionKind::Replay,
                    root: root.clone(),
                    detail: format!(
                        "replay from durable version {} to checkpoint {}",
                        root.durable_version.0, checkpoint.0
                    ),
                }
            })
            .collect();
        Ok(RecoveryPlan {
            checkpoint,
            roots,
            actions,
        })
    }

    pub fn plan_restart(
        &self,
        checkpoint: CommitVersion,
        roots: Vec<RecoveryRoot>,
    ) -> RsfResult<DatabaseRestartPlan> {
        let recovery = self.plan(checkpoint, roots)?;
        let mut reusable_roots = 0;
        let mut replay_roots = 0;
        let mut quarantined_roots = 0;
        let mut steps = vec![RestartStep {
            kind: RestartStepKind::VerifyInventory,
            root: None,
            detail: format!(
                "verified {} recovery roots for checkpoint {}",
                recovery.roots.len(),
                checkpoint.0
            ),
        }];

        for action in &recovery.actions {
            match action.kind {
                RecoveryActionKind::Reuse => {
                    reusable_roots += 1;
                    steps.push(RestartStep {
                        kind: RestartStepKind::ReuseRoot,
                        root: Some(action.root.clone()),
                        detail: action.detail.clone(),
                    });
                }
                RecoveryActionKind::Replay => {
                    replay_roots += 1;
                    steps.push(RestartStep {
                        kind: RestartStepKind::ReplayRoot,
                        root: Some(action.root.clone()),
                        detail: action.detail.clone(),
                    });
                }
                RecoveryActionKind::Quarantine => {
                    quarantined_roots += 1;
                    steps.push(RestartStep {
                        kind: RestartStepKind::QuarantineRoot,
                        root: Some(action.root.clone()),
                        detail: action.detail.clone(),
                    });
                }
            }
        }

        let required_keys = required_root_keys(&recovery.roots);
        let opens_for_traffic = recovery
            .actions
            .iter()
            .filter(|action| required_keys.contains(&root_key(&action.root)))
            .all(|action| action.kind != RecoveryActionKind::Quarantine);
        if opens_for_traffic {
            steps.push(RestartStep {
                kind: RestartStepKind::OpenForTraffic,
                root: None,
                detail: format!(
                    "all required roots are durable at checkpoint {}",
                    checkpoint.0
                ),
            });
        }

        Ok(DatabaseRestartPlan {
            checkpoint,
            recovery,
            steps,
            reusable_roots,
            replay_roots,
            quarantined_roots,
            opens_for_traffic,
        })
    }

    pub fn certify_database_restart(
        &self,
        snapshot: &DatabaseSnapshot,
        manifest: &SnapshotManifest,
        execution: &RestoreExecution,
        roots: Vec<RecoveryRoot>,
    ) -> RsfResult<DatabaseRestartCertificate> {
        if snapshot.checkpoint != manifest.checkpoint {
            return Err(invalid(
                "database restart snapshot checkpoint does not match backup manifest",
            ));
        }
        if execution.checkpoint != snapshot.checkpoint {
            return Err(invalid(
                "database restart restore execution checkpoint does not match snapshot",
            ));
        }
        if !execution.covers_manifest(manifest) {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "database restart restore execution does not cover backup manifest",
            ));
        }

        validate_snapshot_manifest_alignment(snapshot, manifest)?;
        validate_snapshot_recovery_alignment(snapshot, &roots)?;
        let restart = self.plan_restart(snapshot.checkpoint, roots)?;
        if !restart.opens_for_traffic {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "database restart cannot open for traffic before all required roots recover",
            ));
        }

        Ok(DatabaseRestartCertificate {
            checkpoint: snapshot.checkpoint,
            snapshot_id: snapshot.id.clone(),
            restore_manifest_id: manifest.id.clone(),
            restored_items: execution.restored_items,
            restart_steps: restart.step_count(),
            replay_roots: restart.replay_roots,
            reusable_roots: restart.reusable_roots,
        })
    }

    pub fn certify_restored_database_restart(
        &self,
        snapshot: &DatabaseSnapshot,
        manifest: &SnapshotManifest,
        roots: Vec<RecoveryRoot>,
    ) -> RsfResult<DatabaseRestartCertificate> {
        let backup = BackupPlanner;
        let plan = backup.plan_restore(manifest)?;
        let execution = backup.execute_restore(manifest, &plan)?;
        self.certify_database_restart(snapshot, manifest, &execution, roots)
    }
}

impl RecoveryPlan {
    pub fn replay_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|action| action.kind == RecoveryActionKind::Replay)
            .count()
    }

    pub fn quarantine_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|action| action.kind == RecoveryActionKind::Quarantine)
            .count()
    }

    pub fn action_count(&self) -> usize {
        self.actions.len()
    }
}

impl DatabaseRestartPlan {
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }
}

impl RecoveryRootKind {
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

impl RestartStepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VerifyInventory => "verify-inventory",
            Self::ReuseRoot => "reuse-root",
            Self::ReplayRoot => "replay-root",
            Self::QuarantineRoot => "quarantine-root",
            Self::OpenForTraffic => "open-for-traffic",
        }
    }
}

fn validate_roots(checkpoint: CommitVersion, roots: &[RecoveryRoot]) -> RsfResult<()> {
    if roots.is_empty() {
        return Err(invalid("database recovery requires root inventory"));
    }

    let mut seen = BTreeSet::new();
    for root in roots {
        validate_root(checkpoint, root)?;
        let key = (root.kind, root.namespace.clone(), root.name.clone());
        if !seen.insert(key) {
            return Err(invalid(format!(
                "duplicate recovery root {}:{}",
                root.kind.as_str(),
                root.name
            )));
        }
    }

    for required in [
        RecoveryRootKind::Catalog,
        RecoveryRootKind::TransactionLog,
        RecoveryRootKind::Kv,
        RecoveryRootKind::Sql,
        RecoveryRootKind::Object,
        RecoveryRootKind::Blob,
        RecoveryRootKind::ChunkIndex,
    ] {
        let Some(root) = roots.iter().find(|root| root.kind == required) else {
            return Err(invalid(format!(
                "database recovery missing required {} root",
                required.as_str()
            )));
        };
        if !root.present {
            return Err(invalid(format!(
                "database recovery required {} root is not present",
                required.as_str()
            )));
        }
        if !root.checksum_ok {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "database recovery required {} root failed checksum",
                    required.as_str()
                ),
            ));
        }
    }
    Ok(())
}

fn validate_root(checkpoint: CommitVersion, root: &RecoveryRoot) -> RsfResult<()> {
    if root.namespace.trim().is_empty() {
        return Err(invalid("recovery root namespace must not be empty"));
    }
    if root.name.trim().is_empty() {
        return Err(invalid("recovery root name must not be empty"));
    }
    if root.durable_version > checkpoint {
        return Err(invalid(format!(
            "recovery root {} version {} is newer than checkpoint {}",
            root.name, root.durable_version.0, checkpoint.0
        )));
    }
    Ok(())
}

fn required_root_keys(roots: &[RecoveryRoot]) -> BTreeSet<(RecoveryRootKind, String, String)> {
    let mut keys = BTreeSet::new();
    for required in [
        RecoveryRootKind::Catalog,
        RecoveryRootKind::TransactionLog,
        RecoveryRootKind::Kv,
        RecoveryRootKind::Sql,
        RecoveryRootKind::Object,
        RecoveryRootKind::Blob,
        RecoveryRootKind::ChunkIndex,
    ] {
        if let Some(root) = roots.iter().find(|root| root.kind == required) {
            keys.insert(root_key(root));
        }
    }
    keys
}

fn root_key(root: &RecoveryRoot) -> (RecoveryRootKind, String, String) {
    (root.kind, root.namespace.clone(), root.name.clone())
}

fn validate_snapshot_manifest_alignment(
    snapshot: &DatabaseSnapshot,
    manifest: &SnapshotManifest,
) -> RsfResult<()> {
    let manifest_keys = manifest
        .items
        .iter()
        .map(|item| {
            (
                recovery_kind_from_backup_kind(&item.kind),
                item.namespace.clone(),
                item.name.clone(),
                item.version,
            )
        })
        .collect::<BTreeSet<_>>();
    for root in &snapshot.roots {
        let key = (
            recovery_kind_from_snapshot_kind(&root.kind),
            root.namespace.clone(),
            root.name.clone(),
            root.version,
        );
        if !manifest_keys.contains(&key) {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "backup manifest does not cover snapshot root {}:{} at version {}",
                    root.kind.as_str(),
                    root.name,
                    root.version.0
                ),
            ));
        }
    }
    Ok(())
}

fn validate_snapshot_recovery_alignment(
    snapshot: &DatabaseSnapshot,
    roots: &[RecoveryRoot],
) -> RsfResult<()> {
    let recovery_keys = roots
        .iter()
        .map(|root| {
            (
                root.kind,
                root.namespace.clone(),
                root.name.clone(),
                root.durable_version,
            )
        })
        .collect::<BTreeSet<_>>();
    for root in &snapshot.roots {
        let key = (
            recovery_kind_from_snapshot_kind(&root.kind),
            root.namespace.clone(),
            root.name.clone(),
            root.version,
        );
        if !recovery_keys.contains(&key) {
            return Err(invalid(format!(
                "recovery inventory does not contain snapshot root {}:{} at version {}",
                root.kind.as_str(),
                root.name,
                root.version.0
            )));
        }
    }
    Ok(())
}

fn recovery_kind_from_snapshot_kind(kind: &SnapshotRootKind) -> RecoveryRootKind {
    match kind {
        SnapshotRootKind::Catalog => RecoveryRootKind::Catalog,
        SnapshotRootKind::TransactionLog => RecoveryRootKind::TransactionLog,
        SnapshotRootKind::Kv => RecoveryRootKind::Kv,
        SnapshotRootKind::Sql => RecoveryRootKind::Sql,
        SnapshotRootKind::Object => RecoveryRootKind::Object,
        SnapshotRootKind::Blob => RecoveryRootKind::Blob,
        SnapshotRootKind::ChunkIndex => RecoveryRootKind::ChunkIndex,
    }
}

fn recovery_kind_from_backup_kind(kind: &BackupItemKind) -> RecoveryRootKind {
    match kind {
        BackupItemKind::CatalogRoot => RecoveryRootKind::Catalog,
        BackupItemKind::TransactionLog => RecoveryRootKind::TransactionLog,
        BackupItemKind::KvRange => RecoveryRootKind::Kv,
        BackupItemKind::SqlTable => RecoveryRootKind::Sql,
        BackupItemKind::ObjectManifest => RecoveryRootKind::Object,
        BackupItemKind::BlobChunk => RecoveryRootKind::Blob,
        BackupItemKind::ChunkIndex => RecoveryRootKind::ChunkIndex,
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_backup::{BackupItem, BackupPlanner};
    use rsf_core::Digest;
    use rsf_snapshot::{SnapshotBarrierKind, SnapshotCoordinator, SnapshotRequest, SnapshotRoot};

    #[test]
    fn plans_database_replay_to_checkpoint() {
        let plan = RecoveryPlanner
            .plan(CommitVersion(20), roots(CommitVersion(20)))
            .expect("plan");

        assert_eq!(plan.roots.len(), 7);
        assert_eq!(plan.action_count(), 7);
        assert_eq!(plan.replay_count(), 4);
        assert_eq!(plan.quarantine_count(), 0);
    }

    #[test]
    fn plans_restart_sequence_from_verified_inventory_to_open() {
        let plan = RecoveryPlanner
            .plan_restart(CommitVersion(20), roots(CommitVersion(20)))
            .expect("restart plan");

        assert_eq!(plan.checkpoint, CommitVersion(20));
        assert_eq!(plan.reusable_roots, 3);
        assert_eq!(plan.replay_roots, 4);
        assert_eq!(plan.quarantined_roots, 0);
        assert_eq!(plan.recovery.action_count(), 7);
        assert!(plan.opens_for_traffic);
        assert_eq!(plan.step_count(), 9);
        assert_eq!(plan.steps[0].kind, RestartStepKind::VerifyInventory);
        assert_eq!(
            plan.steps.last().map(|step| step.kind),
            Some(RestartStepKind::OpenForTraffic)
        );
        assert!(plan.steps[0].root.is_none());
        assert!(plan.steps[1].root.is_some());
    }

    #[test]
    fn restart_planner_preserves_optional_quarantine_before_open() {
        let mut roots = roots(CommitVersion(20));
        roots.push(RecoveryRoot {
            kind: RecoveryRootKind::Blob,
            namespace: "app".to_string(),
            name: "blob/expired-unreferenced".to_string(),
            durable_version: CommitVersion(20),
            present: true,
            checksum_ok: false,
        });

        let plan = RecoveryPlanner
            .plan_restart(CommitVersion(20), roots)
            .expect("restart plan with optional quarantine");

        assert_eq!(plan.quarantined_roots, 1);
        assert!(plan.opens_for_traffic);
        assert!(plan.steps.iter().any(|step| {
            step.kind == RestartStepKind::QuarantineRoot
                && step
                    .root
                    .as_ref()
                    .is_some_and(|root| root.name == "blob/expired-unreferenced")
        }));
        assert_eq!(
            plan.steps.last().map(|step| step.kind),
            Some(RestartStepKind::OpenForTraffic)
        );
    }

    #[test]
    fn optional_corrupt_root_is_quarantined() {
        let mut roots = roots(CommitVersion(20));
        roots.push(RecoveryRoot {
            kind: RecoveryRootKind::Blob,
            namespace: "app".to_string(),
            name: "blob/expired-unreferenced".to_string(),
            durable_version: CommitVersion(20),
            present: true,
            checksum_ok: false,
        });

        let plan = RecoveryPlanner
            .plan(CommitVersion(20), roots)
            .expect("plan");

        assert_eq!(plan.quarantine_count(), 1);
        assert_eq!(plan.action_count(), 8);
    }

    #[test]
    fn required_corrupt_root_rejects_recovery() {
        let mut roots = roots(CommitVersion(20));
        roots
            .iter_mut()
            .find(|root| root.kind == RecoveryRootKind::Object)
            .unwrap()
            .checksum_ok = false;

        let err = RecoveryPlanner
            .plan(CommitVersion(20), roots)
            .expect_err("required root corruption must reject recovery");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("object"));
    }

    #[test]
    fn missing_each_required_root_rejects_recovery() {
        for required in required_root_kinds() {
            let mut roots = roots(CommitVersion(20));
            roots.retain(|root| root.kind != required);

            let err = RecoveryPlanner
                .plan(CommitVersion(20), roots)
                .expect_err("missing required root must reject recovery");

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(err.message.contains(required.as_str()));
        }
    }

    #[test]
    fn future_root_version_rejects_recovery() {
        let mut roots = roots(CommitVersion(20));
        roots[2].durable_version = CommitVersion(21);

        let err = RecoveryPlanner
            .plan(CommitVersion(20), roots)
            .expect_err("future root must reject recovery");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("newer than checkpoint"));
    }

    #[test]
    fn certifies_whole_database_restart_from_snapshot_backup_restore_and_roots() {
        let checkpoint = CommitVersion(20);
        let snapshot = database_snapshot(checkpoint);
        let manifest = backup_manifest_for_snapshot(&snapshot);
        let execution = BackupPlanner
            .execute_restore(
                &manifest,
                &BackupPlanner.plan_restore(&manifest).expect("plan"),
            )
            .expect("restore");

        let certificate = RecoveryPlanner
            .certify_database_restart(
                &snapshot,
                &manifest,
                &execution,
                recovery_roots_for_snapshot(&snapshot),
            )
            .expect("restart certificate");

        assert_eq!(certificate.checkpoint, checkpoint);
        assert_eq!(certificate.snapshot_id, snapshot.id);
        assert_eq!(certificate.restore_manifest_id, manifest.id);
        assert_eq!(certificate.restored_items, 7);
        assert_eq!(certificate.restart_steps, 9);
        assert_eq!(certificate.replay_roots, 4);
        assert_eq!(certificate.reusable_roots, 3);
    }

    #[test]
    fn restored_database_restart_rejects_corrupt_manifest_before_opening_roots() {
        let snapshot = database_snapshot(CommitVersion(20));
        let mut manifest = backup_manifest_for_snapshot(&snapshot);
        manifest.items[0].bytes += 1;

        let err = RecoveryPlanner
            .certify_restored_database_restart(
                &snapshot,
                &manifest,
                recovery_roots_for_snapshot(&snapshot),
            )
            .expect_err("corrupt manifest must reject restart");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("manifest checksum"));
    }

    #[test]
    fn restored_database_restart_rejects_incomplete_manifest() {
        let snapshot = database_snapshot(CommitVersion(20));
        let mut items = backup_items_for_snapshot(&snapshot);
        items.retain(|item| item.kind != BackupItemKind::SqlTable);
        let err = BackupPlanner
            .create_manifest("snapshot-app-0001", snapshot.checkpoint, 1, items)
            .expect_err("incomplete manifest rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("sql-table"));
    }

    #[test]
    fn restart_certificate_rejects_recovery_inventory_that_does_not_match_snapshot_roots() {
        let snapshot = database_snapshot(CommitVersion(20));
        let manifest = backup_manifest_for_snapshot(&snapshot);
        let mut roots = recovery_roots_for_snapshot(&snapshot);
        roots
            .iter_mut()
            .find(|root| root.kind == RecoveryRootKind::Sql)
            .expect("sql root")
            .name = "sql/other-table".to_string();

        let err = RecoveryPlanner
            .certify_restored_database_restart(&snapshot, &manifest, roots)
            .expect_err("mismatched inventory rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("sql"));
    }

    fn roots(checkpoint: CommitVersion) -> Vec<RecoveryRoot> {
        vec![
            root(RecoveryRootKind::Catalog, "catalog/root", CommitVersion(18)),
            root(RecoveryRootKind::TransactionLog, "fabric/wal", checkpoint),
            root(RecoveryRootKind::Kv, "kv/app/config", CommitVersion(19)),
            root(RecoveryRootKind::Sql, "sql/app/users", CommitVersion(19)),
            root(
                RecoveryRootKind::Object,
                "object/media/hero.png",
                CommitVersion(17),
            ),
            root(RecoveryRootKind::Blob, "blob/snapshot-chunk", checkpoint),
            root(RecoveryRootKind::ChunkIndex, "chunk/index/main", checkpoint),
        ]
    }

    fn root(kind: RecoveryRootKind, name: &str, durable_version: CommitVersion) -> RecoveryRoot {
        RecoveryRoot {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            durable_version,
            present: true,
            checksum_ok: true,
        }
    }

    fn required_root_kinds() -> [RecoveryRootKind; 7] {
        [
            RecoveryRootKind::Catalog,
            RecoveryRootKind::TransactionLog,
            RecoveryRootKind::Kv,
            RecoveryRootKind::Sql,
            RecoveryRootKind::Object,
            RecoveryRootKind::Blob,
            RecoveryRootKind::ChunkIndex,
        ]
    }

    fn database_snapshot(checkpoint: CommitVersion) -> rsf_snapshot::DatabaseSnapshot {
        SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint,
                roots: snapshot_roots(checkpoint),
            })
            .expect("snapshot")
    }

    fn snapshot_roots(checkpoint: CommitVersion) -> Vec<SnapshotRoot> {
        vec![
            snapshot_root(
                SnapshotRootKind::Catalog,
                "catalog/root",
                CommitVersion(18),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            snapshot_root(
                SnapshotRootKind::TransactionLog,
                "fabric/wal",
                checkpoint,
                SnapshotBarrierKind::CopyOnWrite,
            ),
            snapshot_root(
                SnapshotRootKind::Kv,
                "kv/app/config",
                CommitVersion(19),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            snapshot_root(
                SnapshotRootKind::Sql,
                "sql/app/users",
                CommitVersion(19),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            snapshot_root(
                SnapshotRootKind::Object,
                "object/media/hero.png",
                CommitVersion(17),
                SnapshotBarrierKind::ImmutablePin,
            ),
            snapshot_root(
                SnapshotRootKind::Blob,
                "blob/snapshot-chunk",
                checkpoint,
                SnapshotBarrierKind::ImmutablePin,
            ),
            snapshot_root(
                SnapshotRootKind::ChunkIndex,
                "chunk/index/main",
                checkpoint,
                SnapshotBarrierKind::CopyOnWrite,
            ),
        ]
    }

    fn snapshot_root(
        kind: SnapshotRootKind,
        name: &str,
        version: CommitVersion,
        barrier: SnapshotBarrierKind,
    ) -> SnapshotRoot {
        SnapshotRoot {
            kind,
            namespace: "app".to_string(),
            name: name.to_string(),
            version,
            barrier,
        }
    }

    fn backup_manifest_for_snapshot(snapshot: &rsf_snapshot::DatabaseSnapshot) -> SnapshotManifest {
        BackupPlanner
            .create_manifest(
                snapshot.id.clone(),
                snapshot.checkpoint,
                1,
                backup_items_for_snapshot(snapshot),
            )
            .expect("manifest")
    }

    fn backup_items_for_snapshot(snapshot: &rsf_snapshot::DatabaseSnapshot) -> Vec<BackupItem> {
        snapshot
            .roots
            .iter()
            .map(|root| BackupItem {
                kind: backup_kind_from_snapshot_kind(&root.kind),
                namespace: root.namespace.clone(),
                name: root.name.clone(),
                version: root.version,
                checksum: Digest::blake3_bytes(
                    format!(
                        "backup:{}:{}:{}",
                        root.kind.as_str(),
                        root.name,
                        root.version.0
                    )
                    .as_bytes(),
                ),
                bytes: 128,
            })
            .collect()
    }

    fn recovery_roots_for_snapshot(snapshot: &rsf_snapshot::DatabaseSnapshot) -> Vec<RecoveryRoot> {
        snapshot
            .roots
            .iter()
            .map(|root| RecoveryRoot {
                kind: recovery_kind_from_snapshot_kind(&root.kind),
                namespace: root.namespace.clone(),
                name: root.name.clone(),
                durable_version: root.version,
                present: true,
                checksum_ok: true,
            })
            .collect()
    }

    fn backup_kind_from_snapshot_kind(kind: &SnapshotRootKind) -> BackupItemKind {
        match kind {
            SnapshotRootKind::Catalog => BackupItemKind::CatalogRoot,
            SnapshotRootKind::TransactionLog => BackupItemKind::TransactionLog,
            SnapshotRootKind::Kv => BackupItemKind::KvRange,
            SnapshotRootKind::Sql => BackupItemKind::SqlTable,
            SnapshotRootKind::Object => BackupItemKind::ObjectManifest,
            SnapshotRootKind::Blob => BackupItemKind::BlobChunk,
            SnapshotRootKind::ChunkIndex => BackupItemKind::ChunkIndex,
        }
    }
}
