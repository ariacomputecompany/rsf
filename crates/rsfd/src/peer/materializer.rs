//! Peer-owned application writers. Successful commits stay resident instead of
//! reopening and replaying a namespace's complete application log on every RPC.
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use rsf_core::{Epoch, RsfResult};
use rsf_protocol::{ApplicationSurface, ReplicatedFabricCommand};

const MAX_OPEN_STORES: usize = 64;

#[derive(Debug)]
enum Store {
    Kv(Box<rsf_kv::DurableKvEngine>),
    Sql(Box<rsf_sql::DurableSqlEngine>),
    Object(Box<rsf_object::DurableObjectStore>),
    Blob(Box<rsf_blob::DurableBlobEngine>),
}

impl Store {
    fn open(root: &Path, namespace: &str, surface: ApplicationSurface) -> RsfResult<Self> {
        let root = crate::security::namespace_root(root, namespace);
        Ok(match surface {
            ApplicationSurface::Kv => Self::Kv(Box::new(rsf_kv::DurableKvEngine::open_replicated(
                root.join("kv"),
                Epoch(1),
                namespace,
            )?)),
            ApplicationSurface::Sql => Self::Sql(Box::new(
                rsf_sql::DurableSqlEngine::open_replicated(root.join("sql"), namespace)?,
            )),
            ApplicationSurface::Object => Self::Object(Box::new(
                rsf_object::DurableObjectStore::open_replicated(root.join("object"), namespace)?,
            )),
            ApplicationSurface::Blob => Self::Blob(Box::new(
                rsf_blob::DurableBlobEngine::open_replicated(root.join("blob"), namespace)?,
            )),
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
    needs_replay: bool,
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
        self.with_store(
            &command.application.namespace,
            command.application.surface,
            |store, _| store.apply(command),
        )
        .map_err(|err| err.message)
    }

    pub(super) fn execute<T: rsf_vsr::VsrTransport>(
        &mut self,
        envelope: &rsf_wire::RequestEnvelope,
        router: &mut rsf_protocol::VsrBackedFabricRouter<T>,
    ) -> RsfResult<crate::data::DataPlaneResponse> {
        use rsf_wire::WireProtocol;
        let surface = match envelope.protocol {
            WireProtocol::Sql => ApplicationSurface::Sql,
            WireProtocol::Kv => ApplicationSurface::Kv,
            WireProtocol::Object => ApplicationSurface::Object,
            WireProtocol::Blob => ApplicationSurface::Blob,
            _ => {
                return Err(rsf_core::RsfError::new(
                    rsf_core::ErrorCode::InvalidInput,
                    "unsupported client surface",
                ));
            }
        };
        let namespace = envelope.namespace.as_deref().ok_or_else(|| {
            rsf_core::RsfError::new(rsf_core::ErrorCode::InvalidInput, "namespace is required")
        })?;
        self.with_store(namespace, surface, |store, needs_replay| {
            if *needs_replay {
                // Reopening a writer must not trust a process-wide cursor: its
                // derived files may have been evicted/lost independently. Rebuild
                // this namespace/surface from certified commands before reading.
                for command in router.committed_application_commands() {
                    if command.application.namespace == namespace
                        && command.application.surface == surface
                    {
                        store.apply(&command)?;
                    }
                }
                *needs_replay = false;
            }
            match store {
                Store::Kv(kv) => {
                    kv.reconcile(router)?;
                    crate::data::execution::kv(envelope, router, kv)
                }
                Store::Sql(sql) => {
                    sql.reconcile(router)?;
                    crate::data::execution::sql(envelope, router, sql)
                }
                Store::Object(objects) => {
                    objects.reconcile(router)?;
                    crate::data::execution::object(envelope, router, objects)
                }
                Store::Blob(blobs) => crate::data::execution::blob(envelope, router, blobs),
            }
        })
    }

    fn with_store<R>(
        &mut self,
        namespace: &str,
        surface: ApplicationSurface,
        operation: impl FnOnce(&mut Store, &mut bool) -> RsfResult<R>,
    ) -> RsfResult<R> {
        let position = self
            .stores
            .iter()
            .position(|store| store.namespace == namespace && store.surface == surface);
        let mut resident = match position {
            Some(position) => self
                .stores
                .remove(position)
                .expect("resident position exists"),
            None => {
                if self.stores.len() == self.capacity {
                    self.stores.pop_front();
                }
                Resident {
                    namespace: namespace.to_owned(),
                    surface,
                    store: Store::open(&self.root, namespace, surface)?,
                    needs_replay: true,
                }
            }
        };
        let result = operation(&mut resident.store, &mut resident.needs_replay);
        // Error paths retain the engine and its durable-failure fence as well.
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
    #[test]
    fn reopened_missing_materializer_store_recovers_before_returning_a_read() {
        use rsf_wire::{RequestEnvelope, WireOperationKind, WireProtocol};
        let root = std::env::temp_dir().join(format!(
            "rsf-materializer-missing-{}-{}",
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
                        client_request: Some(ClientRequestId::new("missing-store", index as u64)),
                        snapshot,
                        mutations: vec![KvMutation::Set {
                            key: b"key".to_vec(),
                            value: namespace.as_bytes().to_vec(),
                        }],
                    },
                )
                .unwrap();
        }
        let mut materializer = ApplicationMaterializer::new(root.join("target"));
        materializer.capacity = 1;
        for command in router.committed_application_commands() {
            materializer.apply(&command).unwrap();
        }
        let path = crate::security::namespace_root(&root.join("target"), "alpha").join("kv");
        std::fs::remove_dir_all(&path).unwrap();
        let envelope = RequestEnvelope {
            protocol: WireProtocol::Kv,
            principal: "app-client".into(),
            namespace: Some("alpha".into()),
            operation: "GET key".into(),
            operation_kind: WireOperationKind::Read,
            idempotency_key: None,
            body_bytes: 0,
            body: vec![],
        };
        let response = materializer.execute(&envelope, &mut router).unwrap();
        let crate::data::DataPlaneResponse::Executed { payload, .. } = response else {
            panic!("read did not execute")
        };
        assert_eq!(payload, b"alpha");
        drop(materializer);
        drop(router);
        std::fs::remove_dir_all(root).unwrap();
    }
}
