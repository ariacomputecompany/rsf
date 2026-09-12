#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use rsf_core::{CommitVersion, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SnapshotRootKind {
    Catalog,
    TransactionLog,
    Kv,
    Sql,
    Object,
    Blob,
    ChunkIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRoot {
    pub kind: SnapshotRootKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub barrier: SnapshotBarrierKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRequest {
    pub id: String,
    pub namespace: String,
    pub checkpoint: CommitVersion,
    pub roots: Vec<SnapshotRoot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBarrier {
    pub root: SnapshotRootKind,
    pub namespace: String,
    pub name: String,
    pub kind: SnapshotBarrierKind,
    pub version: CommitVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotBarrierKind {
    CopyOnWrite,
    ImmutablePin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseSnapshot {
    pub id: String,
    pub namespace: String,
    pub checkpoint: CommitVersion,
    pub roots: Vec<SnapshotRoot>,
    pub barriers: Vec<SnapshotBarrier>,
}

#[derive(Clone, Debug, Default)]
pub struct SnapshotCoordinator;

impl SnapshotCoordinator {
    pub fn create(&self, request: SnapshotRequest) -> RsfResult<DatabaseSnapshot> {
        validate_request(&request)?;
        let barriers: Vec<_> = request
            .roots
            .iter()
            .map(|root| SnapshotBarrier {
                root: root.kind.clone(),
                namespace: root.namespace.clone(),
                name: root.name.clone(),
                kind: root.barrier,
                version: root.version,
            })
            .collect();
        validate_barriers(&request, &barriers)?;
        Ok(DatabaseSnapshot {
            id: request.id,
            namespace: request.namespace,
            checkpoint: request.checkpoint,
            roots: request.roots,
            barriers,
        })
    }
}

impl SnapshotRootKind {
    pub fn as_str(&self) -> &'static str {
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

    pub fn required(&self) -> bool {
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

impl SnapshotBarrierKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CopyOnWrite => "copy-on-write",
            Self::ImmutablePin => "immutable-pin",
        }
    }
}

impl DatabaseSnapshot {
    pub fn root_count(&self) -> usize {
        self.roots.len()
    }

    pub fn barrier_count(&self) -> usize {
        self.barriers.len()
    }

    pub fn root(&self, kind: SnapshotRootKind) -> Option<&SnapshotRoot> {
        self.roots.iter().find(|root| root.kind == kind)
    }
}

fn validate_request(request: &SnapshotRequest) -> RsfResult<()> {
    if request.id.trim().is_empty() {
        return Err(invalid("snapshot id must not be empty"));
    }
    if request.namespace.trim().is_empty() {
        return Err(invalid("snapshot namespace must not be empty"));
    }
    if request.roots.is_empty() {
        return Err(invalid("snapshot must include at least one root"));
    }

    let mut seen = BTreeSet::new();
    for root in &request.roots {
        validate_root(root, request.checkpoint)?;
        let key = (root.kind.clone(), root.namespace.clone(), root.name.clone());
        if !seen.insert(key) {
            return Err(invalid(format!(
                "duplicate snapshot root {}:{}",
                root.kind.as_str(),
                root.name
            )));
        }
    }

    for required in [
        SnapshotRootKind::Catalog,
        SnapshotRootKind::TransactionLog,
        SnapshotRootKind::Kv,
        SnapshotRootKind::Sql,
        SnapshotRootKind::Object,
        SnapshotRootKind::Blob,
        SnapshotRootKind::ChunkIndex,
    ] {
        if !request.roots.iter().any(|root| root.kind == required) {
            return Err(invalid(format!(
                "snapshot missing required {} root",
                required.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_root(root: &SnapshotRoot, checkpoint: CommitVersion) -> RsfResult<()> {
    if root.namespace.trim().is_empty() {
        return Err(invalid("snapshot root namespace must not be empty"));
    }
    if root.name.trim().is_empty() {
        return Err(invalid("snapshot root name must not be empty"));
    }
    if root.version > checkpoint {
        return Err(invalid(format!(
            "snapshot root {} is newer than checkpoint {}",
            root.name, checkpoint.0
        )));
    }
    match (&root.kind, root.barrier) {
        (SnapshotRootKind::Object | SnapshotRootKind::Blob, SnapshotBarrierKind::CopyOnWrite) => {
            return Err(invalid(format!(
                "snapshot root {} must use immutable pin barrier",
                root.name
            )));
        }
        (
            SnapshotRootKind::Catalog
            | SnapshotRootKind::TransactionLog
            | SnapshotRootKind::Kv
            | SnapshotRootKind::Sql
            | SnapshotRootKind::ChunkIndex,
            SnapshotBarrierKind::ImmutablePin,
        ) => {
            return Err(invalid(format!(
                "snapshot root {} must use copy-on-write barrier",
                root.name
            )));
        }
        _ => {}
    }
    Ok(())
}

fn validate_barriers(request: &SnapshotRequest, barriers: &[SnapshotBarrier]) -> RsfResult<()> {
    if barriers.len() != request.roots.len() {
        return Err(invalid("snapshot barriers must cover every root"));
    }

    let mut seen = BTreeSet::new();
    for barrier in barriers {
        if barrier.namespace.trim().is_empty() {
            return Err(invalid("snapshot barrier namespace must not be empty"));
        }
        if barrier.name.trim().is_empty() {
            return Err(invalid("snapshot barrier name must not be empty"));
        }
        if barrier.version > request.checkpoint {
            return Err(invalid(format!(
                "snapshot barrier {} is newer than checkpoint {}",
                barrier.name, request.checkpoint.0
            )));
        }
        let key = (
            barrier.root.clone(),
            barrier.namespace.clone(),
            barrier.name.clone(),
        );
        if !seen.insert(key.clone()) {
            return Err(invalid(format!(
                "duplicate snapshot barrier {}:{}",
                barrier.root.as_str(),
                barrier.name
            )));
        }
        let Some(root) = request.roots.iter().find(|root| {
            root.kind == barrier.root
                && root.namespace == barrier.namespace
                && root.name == barrier.name
        }) else {
            return Err(invalid(format!(
                "snapshot barrier {} has no matching root",
                barrier.name
            )));
        };
        if barrier.version != root.version {
            return Err(invalid(format!(
                "snapshot barrier {} version does not match root",
                barrier.name
            )));
        }
        if barrier.kind != root.barrier {
            return Err(invalid(format!(
                "snapshot barrier {} kind does not match root",
                barrier.name
            )));
        }
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_consistent_snapshot_with_copy_on_write_barriers() {
        let snapshot = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots: roots(CommitVersion(20)),
            })
            .expect("snapshot");

        assert_eq!(snapshot.root_count(), 7);
        assert_eq!(snapshot.barrier_count(), 7);
        assert_eq!(
            snapshot.root(SnapshotRootKind::Catalog).unwrap().version,
            CommitVersion(18)
        );
        assert!(snapshot.barriers.iter().any(|barrier| {
            barrier.root == SnapshotRootKind::Object
                && barrier.kind == SnapshotBarrierKind::ImmutablePin
        }));
    }

    #[test]
    fn rejects_snapshot_without_each_required_root() {
        for required in required_root_kinds() {
            let mut roots = roots(CommitVersion(20));
            roots.retain(|root| root.kind != required);

            let err = SnapshotCoordinator
                .create(SnapshotRequest {
                    id: "snapshot-app-0001".to_string(),
                    namespace: "app".to_string(),
                    checkpoint: CommitVersion(20),
                    roots,
                })
                .expect_err("missing required root");

            assert!(err.message.contains(required.as_str()));
        }
    }

    #[test]
    fn rejects_wrong_barrier_kind_for_mutable_root() {
        let mut roots = roots(CommitVersion(20));
        roots
            .iter_mut()
            .find(|root| root.kind == SnapshotRootKind::Kv)
            .unwrap()
            .barrier = SnapshotBarrierKind::ImmutablePin;

        let err = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots,
            })
            .expect_err("wrong mutable barrier");

        assert!(err.message.contains("copy-on-write"));
    }

    #[test]
    fn rejects_future_root_versions() {
        let mut roots = roots(CommitVersion(20));
        roots[0].version = CommitVersion(21);

        let err = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots,
            })
            .expect_err("future root");

        assert!(err.message.contains("newer than checkpoint"));
    }

    #[test]
    fn rejects_duplicate_roots() {
        let mut roots = roots(CommitVersion(20));
        roots.push(roots[0].clone());

        let err = SnapshotCoordinator
            .create(SnapshotRequest {
                id: "snapshot-app-0001".to_string(),
                namespace: "app".to_string(),
                checkpoint: CommitVersion(20),
                roots,
            })
            .expect_err("duplicate root");

        assert!(err.message.contains("duplicate"));
    }

    fn roots(checkpoint: CommitVersion) -> Vec<SnapshotRoot> {
        vec![
            root(
                SnapshotRootKind::Catalog,
                "catalog-root",
                CommitVersion(18),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::TransactionLog,
                "txn-log",
                checkpoint,
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::Kv,
                "kv-config",
                CommitVersion(19),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::Sql,
                "sql-users",
                CommitVersion(19),
                SnapshotBarrierKind::CopyOnWrite,
            ),
            root(
                SnapshotRootKind::Object,
                "object-media",
                CommitVersion(17),
                SnapshotBarrierKind::ImmutablePin,
            ),
            root(
                SnapshotRootKind::Blob,
                "blob-chunks",
                CommitVersion(16),
                SnapshotBarrierKind::ImmutablePin,
            ),
            root(
                SnapshotRootKind::ChunkIndex,
                "chunk-index",
                checkpoint,
                SnapshotBarrierKind::CopyOnWrite,
            ),
        ]
    }

    fn required_root_kinds() -> [SnapshotRootKind; 7] {
        [
            SnapshotRootKind::Catalog,
            SnapshotRootKind::TransactionLog,
            SnapshotRootKind::Kv,
            SnapshotRootKind::Sql,
            SnapshotRootKind::Object,
            SnapshotRootKind::Blob,
            SnapshotRootKind::ChunkIndex,
        ]
    }

    fn root(
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
}
