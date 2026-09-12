use std::collections::BTreeSet;

use rsf_checkpoint::{CheckpointRoot, CheckpointRootKind};
use rsf_core::{ClusterId, CommitVersion, Digest, Epoch};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BootRootKind {
    Catalog,
    TransactionLog,
    Kv,
    Sql,
    Object,
    Blob,
    ChunkIndex,
}

impl BootRootKind {
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

impl From<CheckpointRootKind> for BootRootKind {
    fn from(kind: CheckpointRootKind) -> Self {
        match kind {
            CheckpointRootKind::Catalog => Self::Catalog,
            CheckpointRootKind::TransactionLog => Self::TransactionLog,
            CheckpointRootKind::Kv => Self::Kv,
            CheckpointRootKind::Sql => Self::Sql,
            CheckpointRootKind::Object => Self::Object,
            CheckpointRootKind::Blob => Self::Blob,
            CheckpointRootKind::ChunkIndex => Self::ChunkIndex,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootRoot {
    pub kind: BootRootKind,
    pub namespace: String,
    pub name: String,
    pub version: CommitVersion,
    pub bytes: u64,
    pub checksum: Digest,
}

impl BootRoot {
    pub fn new(
        kind: BootRootKind,
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

    pub fn from_checkpoint(root: &CheckpointRoot) -> Self {
        Self {
            kind: BootRootKind::from(root.kind),
            namespace: root.namespace.clone(),
            name: root.name.clone(),
            version: root.version,
            bytes: root.bytes,
            checksum: root.checksum,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Superblock {
    pub cluster: ClusterId,
    pub generation: u64,
    pub layout_epoch: Epoch,
    pub checkpoint: CommitVersion,
    pub roots: Vec<BootRoot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SuperblockDecision {
    Accepted {
        generation: u64,
        checkpoint: CommitVersion,
        roots: usize,
    },
    RootVerified {
        kind: BootRootKind,
        name: String,
        version: CommitVersion,
    },
    Advanced {
        generation: u64,
        checkpoint: CommitVersion,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct SuperblockStore {
    current: Superblock,
}

impl SuperblockStore {
    pub fn open(superblock: Superblock) -> SuperblockDecision {
        match validate_superblock(&superblock) {
            Ok(()) => SuperblockDecision::Accepted {
                generation: superblock.generation,
                checkpoint: superblock.checkpoint,
                roots: superblock.roots.len(),
            },
            Err(reason) => SuperblockDecision::Rejected { reason },
        }
    }

    pub fn new(superblock: Superblock) -> Result<Self, String> {
        validate_superblock(&superblock)?;
        Ok(Self {
            current: superblock,
        })
    }

    pub fn verify_roots(&self) -> Vec<SuperblockDecision> {
        self.current
            .roots
            .iter()
            .map(|root| SuperblockDecision::RootVerified {
                kind: root.kind,
                name: root.name.clone(),
                version: root.version,
            })
            .collect()
    }

    pub fn advance(&mut self, next: Superblock) -> SuperblockDecision {
        if next.generation <= self.current.generation {
            return SuperblockDecision::Rejected {
                reason: "superblock generation must advance".to_string(),
            };
        }
        if next.layout_epoch.0 < self.current.layout_epoch.0 {
            return SuperblockDecision::Rejected {
                reason: "superblock layout epoch cannot move backward".to_string(),
            };
        }
        if next.checkpoint.0 < self.current.checkpoint.0 {
            return SuperblockDecision::Rejected {
                reason: "superblock checkpoint cannot move backward".to_string(),
            };
        }
        if let Err(reason) = validate_superblock(&next) {
            return SuperblockDecision::Rejected { reason };
        }
        self.current = next;
        SuperblockDecision::Advanced {
            generation: self.current.generation,
            checkpoint: self.current.checkpoint,
        }
    }

    pub fn current(&self) -> &Superblock {
        &self.current
    }
}

fn validate_superblock(superblock: &Superblock) -> Result<(), String> {
    if superblock.cluster.as_str().trim().is_empty() {
        return Err("superblock cluster id must not be empty".to_string());
    }
    if superblock.generation == 0 {
        return Err("superblock generation must be non-zero".to_string());
    }
    if superblock.roots.is_empty() {
        return Err("superblock root inventory must not be empty".to_string());
    }
    let mut seen = BTreeSet::new();
    for root in &superblock.roots {
        if root.namespace.trim().is_empty() {
            return Err("superblock root namespace must not be empty".to_string());
        }
        if root.name.trim().is_empty() {
            return Err("superblock root name must not be empty".to_string());
        }
        if root.bytes == 0 {
            return Err(format!("superblock root {} is empty", root.name));
        }
        if root.version.0 > superblock.checkpoint.0 {
            return Err(format!(
                "superblock root {} is newer than checkpoint",
                root.name
            ));
        }
        if root.checksum.validate().is_err() {
            return Err(format!("superblock root {} has empty checksum", root.name));
        }
        let key = (root.kind, root.namespace.clone(), root.name.clone());
        if !seen.insert(key) {
            return Err(format!(
                "duplicate superblock root {}:{}",
                root.kind.as_str(),
                root.name
            ));
        }
    }
    for kind in [
        BootRootKind::Catalog,
        BootRootKind::TransactionLog,
        BootRootKind::Kv,
        BootRootKind::Sql,
        BootRootKind::Object,
        BootRootKind::Blob,
        BootRootKind::ChunkIndex,
    ] {
        if !superblock.roots.iter().any(|root| root.kind == kind) {
            return Err(format!(
                "superblock missing required {} root",
                kind.as_str()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_superblock(generation: u64, checkpoint: u64) -> Superblock {
        Superblock {
            cluster: ClusterId::new("rsf-dev").expect("cluster"),
            generation,
            layout_epoch: Epoch(4),
            checkpoint: CommitVersion(checkpoint),
            roots: vec![
                root(BootRootKind::Catalog, "catalog/main", checkpoint, 512, 1),
                root(
                    BootRootKind::TransactionLog,
                    "fabric/wal",
                    checkpoint,
                    2_048,
                    2,
                ),
                root(BootRootKind::Kv, "kv/app/config", checkpoint, 1_024, 3),
                root(BootRootKind::Sql, "sql/app/users", checkpoint, 4_096, 4),
                root(
                    BootRootKind::Object,
                    "object/media/hero.png",
                    checkpoint,
                    3_072,
                    5,
                ),
                root(BootRootKind::Blob, "blob/sha256-demo", checkpoint, 1_024, 6),
                root(
                    BootRootKind::ChunkIndex,
                    "chunk/index/main",
                    checkpoint,
                    256,
                    7,
                ),
            ],
        }
    }

    #[test]
    fn accepts_valid_boot_record() {
        assert_eq!(
            SuperblockStore::open(valid_superblock(1, 22)),
            SuperblockDecision::Accepted {
                generation: 1,
                checkpoint: CommitVersion(22),
                roots: 7
            }
        );
    }

    #[test]
    fn rejects_missing_each_required_root() {
        for required in required_root_kinds() {
            let mut superblock = valid_superblock(1, 22);
            superblock.roots.retain(|root| root.kind != required);

            assert!(matches!(
                SuperblockStore::open(superblock),
                SuperblockDecision::Rejected { reason } if reason.contains(required.as_str())
            ));
        }
    }

    #[test]
    fn rejects_backward_generation() {
        let mut store = SuperblockStore::new(valid_superblock(2, 22)).unwrap();

        assert!(matches!(
            store.advance(valid_superblock(1, 23)),
            SuperblockDecision::Rejected { .. }
        ));
    }

    #[test]
    fn advances_checkpoint() {
        let mut store = SuperblockStore::new(valid_superblock(1, 22)).unwrap();

        assert_eq!(
            store.advance(valid_superblock(2, 23)),
            SuperblockDecision::Advanced {
                generation: 2,
                checkpoint: CommitVersion(23)
            }
        );
    }

    #[test]
    fn derives_boot_roots_from_checkpoint_roots() {
        let checkpoint = CheckpointRoot::new(
            CheckpointRootKind::Object,
            "app",
            "object/media/hero.png",
            CommitVersion(22),
            3_072,
            Digest::blake3_bytes(b"object-root"),
        );

        assert_eq!(
            BootRoot::from_checkpoint(&checkpoint),
            BootRoot::new(
                BootRootKind::Object,
                "app",
                "object/media/hero.png",
                CommitVersion(22),
                3_072,
                Digest::blake3_bytes(b"object-root"),
            )
        );
    }

    fn root(kind: BootRootKind, name: &str, version: u64, bytes: u64, checksum: u8) -> BootRoot {
        BootRoot::new(
            kind,
            "app",
            name,
            CommitVersion(version),
            bytes,
            Digest::blake3_bytes(format!("boot:{name}:{checksum}").as_bytes()),
        )
    }

    fn required_root_kinds() -> [BootRootKind; 7] {
        [
            BootRootKind::Catalog,
            BootRootKind::TransactionLog,
            BootRootKind::Kv,
            BootRootKind::Sql,
            BootRootKind::Object,
            BootRootKind::Blob,
            BootRootKind::ChunkIndex,
        ]
    }
}
