//! Peer-owned application writers. Successful commits stay resident instead of
//! reopening and replaying a namespace's complete application log on every RPC.
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use rsf_core::{Epoch, RsfResult};
use rsf_protocol::{ApplicationSurface, ReplicatedFabricCommand};

const MAX_OPEN_STORES: usize = 64;

#[derive(Debug)]
enum Store {
    Kv(rsf_kv::DurableKvEngine),
    Sql(rsf_sql::DurableSqlEngine),
    Object(rsf_object::DurableObjectStore),
    Blob(rsf_blob::DurableBlobEngine),
}

impl Store {
    fn open(root: &Path, namespace: &str, surface: ApplicationSurface) -> RsfResult<Self> {
        let root = crate::security::namespace_root(root, namespace);
        Ok(match surface {
            ApplicationSurface::Kv => Self::Kv(rsf_kv::DurableKvEngine::open_replicated(
                root.join("kv"),
                Epoch(1),
                namespace,
            )?),
            ApplicationSurface::Sql => Self::Sql(rsf_sql::DurableSqlEngine::open_replicated(
                root.join("sql"),
                namespace,
            )?),
            ApplicationSurface::Object => Self::Object(
                rsf_object::DurableObjectStore::open_replicated(root.join("object"), namespace)?,
            ),
            ApplicationSurface::Blob => Self::Blob(rsf_blob::DurableBlobEngine::open_replicated(
                root.join("blob"),
                namespace,
            )?),
        })
    }

    fn apply(&mut self, command: &ReplicatedFabricCommand) -> RsfResult<()> {
        match self {
            Self::Kv(store) => {
                store.apply_application_command(command)?;
            }
            Self::Sql(store) => {
                store.apply_application_command(command)?;
            }
            Self::Object(store) => {
                store.apply_application_command(command)?;
            }
            Self::Blob(store) => {
                store.apply_application_command(command)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Resident {
    namespace: String,
    surface: ApplicationSurface,
    store: Store,
}

#[derive(Debug)]
pub(super) struct ApplicationMaterializer {
    root: PathBuf,
    // Bounded LRU order also bounds open ownership locks/file descriptors. Each
    // engine syncs its application receipt before returning from apply.
    stores: VecDeque<Resident>,
    capacity: usize,
}

impl ApplicationMaterializer {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            stores: VecDeque::new(),
            capacity: MAX_OPEN_STORES,
        }
    }

    pub(super) fn apply(&mut self, command: &ReplicatedFabricCommand) -> Result<(), String> {
        let namespace = &command.application.namespace;
        let surface = command.application.surface;
        let position = self
            .stores
            .iter()
            .position(|store| store.namespace == *namespace && store.surface == surface);
        let mut resident = match position {
            Some(position) => self
                .stores
                .remove(position)
                .expect("resident position exists"),
            None => {
                // Release the old writer's descriptors before opening another
                // store, including when already near the host's descriptor cap.
                if self.stores.len() == self.capacity {
                    self.stores.pop_front();
                }
                Resident {
                    namespace: namespace.clone(),
                    surface,
                    store: Store::open(&self.root, namespace, surface)
                        .map_err(|err| err.message)?,
                }
            }
        };
        let result = resident.store.apply(command).map_err(|err| err.message);
        // Keep failed writers fenced as well. Eviction/restart must perform
        // normal recovery; an RPC failure never swaps in an empty engine.
        self.stores.push_back(resident);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{CommitVersion, NodeId, PlacementGroupId};
    use rsf_kv::{DurableKvEngine, KvBatch, KvMutation};
    use rsf_protocol::{ClientRequestId, VsrBackedFabricRouter};

    #[test]
    fn eviction_releases_ownership_and_recovers_values_and_retry_receipts() {
        let root = std::env::temp_dir().join(format!(
            "rsf-resident-materializer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut router = VsrBackedFabricRouter::open(
            root.join("protocol"),
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            [NodeId(0)],
        )
        .unwrap();
        for (index, namespace) in ["alpha", "beta"].iter().enumerate() {
            let mut source = DurableKvEngine::open_replicated(
                root.join(format!("source-{namespace}")),
                Epoch(1),
                namespace,
            )
            .unwrap();
            let snapshot = source.snapshot(&router);
            source
                .commit_batch(
                    &mut router,
                    PlacementGroupId(1),
                    KvBatch {
                        client_request: Some(ClientRequestId::new("materializer", index as u64)),
                        snapshot,
                        mutations: vec![KvMutation::Set {
                            key: b"key".to_vec(),
                            value: namespace.as_bytes().to_vec(),
                        }],
                    },
                )
                .unwrap();
        }
        let commands = router.committed_application_commands();
        let mut materializer = ApplicationMaterializer::new(root.join("target"));
        materializer.capacity = 1;
        materializer.apply(&commands[0]).unwrap();
        let first = &commands[0].application.namespace;
        let path = crate::security::namespace_root(&root.join("target"), first).join("kv");
        assert!(DurableKvEngine::open_replicated(&path, Epoch(1), first).is_err());
        materializer.apply(&commands[1]).unwrap();
        {
            let recovered = DurableKvEngine::open_replicated(&path, Epoch(1), first).unwrap();
            assert_eq!(recovered.get(b"key").unwrap().value, first.as_bytes());
        }
        materializer.apply(&commands[0]).unwrap();
        materializer.apply(&commands[0]).unwrap();
        assert_eq!(materializer.stores.len(), 1);
        drop(materializer);
        let recovered = DurableKvEngine::open_replicated(&path, Epoch(1), first).unwrap();
        assert_eq!(recovered.changes_since(CommitVersion(0)).len(), 1);
        drop(recovered);
        drop(router);
        std::fs::remove_dir_all(root).unwrap();
    }
}
