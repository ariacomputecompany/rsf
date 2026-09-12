#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{
    CommitConsistency, CommitEvidence, CommitVersion, ErrorCode, PlacementGroupId, RsfError,
    RsfResult,
    catalog::{ColumnDescriptor, NamespaceName, TableDescriptor},
};
use rsf_protocol::{
    ClientRequestId, CommitCertificate, ConsistencyMode, FabricOperation, FabricRouterPort,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Namespace {
    pub name: NamespaceName,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectBucket {
    pub namespace: NamespaceName,
    pub name: String,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementBinding {
    pub namespace: NamespaceName,
    pub surface: CatalogSurface,
    pub placement_group: PlacementGroupId,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogSurface {
    Relational,
    Object,
    Blob,
    Kv,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogSnapshot {
    pub version: CommitVersion,
    pub namespaces: usize,
    pub tables: usize,
    pub buckets: usize,
    pub bindings: usize,
}

#[derive(Clone, Debug, Default)]
pub struct CatalogEngine {
    namespaces: BTreeMap<String, Namespace>,
    tables: BTreeMap<(String, String), VersionedTable>,
    buckets: BTreeMap<(String, String), ObjectBucket>,
    bindings: BTreeMap<(String, CatalogSurface), PlacementBinding>,
}

impl CatalogEngine {
    pub fn create_namespace(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        name: impl Into<String>,
    ) -> RsfResult<Namespace> {
        let name = name.into();
        validate_name("namespace", &name)?;
        if let Some(existing) = self.namespaces.get(&name) {
            return Ok(existing.clone());
        }

        let evidence = commit_catalog_mutation(router, metadata_pg, &name, "namespace")?;
        let namespace = Namespace {
            name: NamespaceName(name.clone()),
            version: evidence.version,
            evidence,
        };
        self.namespaces.insert(name, namespace.clone());
        Ok(namespace)
    }

    pub fn create_table(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        descriptor: TableDescriptor,
    ) -> RsfResult<TableDescriptor> {
        let namespace = descriptor.namespace.0.clone();
        let table = descriptor.name.0.clone();
        self.require_namespace(&namespace)?;
        validate_table(&descriptor)?;
        if let Some(existing) = self.tables.get(&(namespace.clone(), table.clone())) {
            return Ok(existing.descriptor.clone());
        }

        let evidence =
            commit_catalog_mutation(router, metadata_pg, &namespace, &format!("table/{table}"))?;
        self.tables.insert(
            (namespace, table),
            VersionedTable {
                descriptor: descriptor.clone(),
                version: evidence.version,
                evidence,
            },
        );
        Ok(descriptor)
    }

    pub fn create_bucket(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> RsfResult<ObjectBucket> {
        let namespace = namespace.into();
        let name = name.into();
        self.require_namespace(&namespace)?;
        validate_name("bucket", &name)?;
        if let Some(existing) = self.buckets.get(&(namespace.clone(), name.clone())) {
            return Ok(existing.clone());
        }

        let evidence =
            commit_catalog_mutation(router, metadata_pg, &namespace, &format!("bucket/{name}"))?;
        let bucket = ObjectBucket {
            namespace: NamespaceName(namespace.clone()),
            name: name.clone(),
            version: evidence.version,
            evidence,
        };
        self.buckets.insert((namespace, name), bucket.clone());
        Ok(bucket)
    }

    pub fn bind_surface(
        &mut self,
        router: &mut impl FabricRouterPort,
        metadata_pg: PlacementGroupId,
        namespace: impl Into<String>,
        surface: CatalogSurface,
        placement_group: PlacementGroupId,
    ) -> RsfResult<PlacementBinding> {
        let namespace = namespace.into();
        self.require_namespace(&namespace)?;
        let item = format!("binding/{}", surface.as_str());
        let evidence = commit_catalog_mutation(router, metadata_pg, &namespace, &item)?;
        let binding = PlacementBinding {
            namespace: NamespaceName(namespace.clone()),
            surface: surface.clone(),
            placement_group,
            version: evidence.version,
            evidence,
        };
        self.bindings.insert((namespace, surface), binding.clone());
        Ok(binding)
    }

    pub fn table(&self, namespace: &str, table: &str) -> Option<&TableDescriptor> {
        self.tables
            .get(&(namespace.to_string(), table.to_string()))
            .map(|entry| &entry.descriptor)
    }

    pub fn table_version(&self, namespace: &str, table: &str) -> Option<CommitVersion> {
        self.tables
            .get(&(namespace.to_string(), table.to_string()))
            .map(|entry| entry.version)
    }

    pub fn bucket(&self, namespace: &str, name: &str) -> Option<&ObjectBucket> {
        self.buckets.get(&(namespace.to_string(), name.to_string()))
    }

    pub fn binding(&self, namespace: &str, surface: CatalogSurface) -> Option<&PlacementBinding> {
        self.bindings.get(&(namespace.to_string(), surface))
    }

    pub fn snapshot(&self, router: &impl FabricRouterPort) -> CatalogSnapshot {
        CatalogSnapshot {
            version: router.latest_version(),
            namespaces: self.namespaces.len(),
            tables: self.tables.len(),
            buckets: self.buckets.len(),
            bindings: self.bindings.len(),
        }
    }

    pub fn item_count(&self) -> usize {
        self.namespaces.len() + self.tables.len() + self.buckets.len() + self.bindings.len()
    }

    fn require_namespace(&self, namespace: &str) -> RsfResult<()> {
        if self.namespaces.contains_key(namespace) {
            return Ok(());
        }
        Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("namespace {namespace} does not exist"),
        ))
    }
}

impl CatalogSurface {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Relational => "relational",
            Self::Object => "object",
            Self::Blob => "blob",
            Self::Kv => "kv",
        }
    }
}

impl Ord for CatalogSurface {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for CatalogSurface {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VersionedTable {
    descriptor: TableDescriptor,
    version: CommitVersion,
    evidence: CommitEvidence,
}

fn validate_table(descriptor: &TableDescriptor) -> RsfResult<()> {
    validate_name("namespace", &descriptor.namespace.0)?;
    validate_name("table", &descriptor.name.0)?;
    if descriptor.columns.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "table must define at least one column",
        ));
    }
    if descriptor.primary_key.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "table must define a primary key",
        ));
    }
    for key in &descriptor.primary_key {
        if !descriptor.columns.iter().any(|column| column.name == *key) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("primary key column {key} is not in table descriptor"),
            ));
        }
    }
    for column in &descriptor.columns {
        validate_column(column)?;
    }
    Ok(())
}

fn commit_catalog_mutation(
    router: &mut impl FabricRouterPort,
    metadata_pg: PlacementGroupId,
    namespace: &str,
    item: &str,
) -> RsfResult<CommitEvidence> {
    let commit = router.commit_request(
        metadata_pg,
        ClientRequestId::new(format!("catalog:{namespace}:{item}"), 0),
        FabricOperation::CatalogMutation {
            namespace: namespace.to_string(),
            item: item.to_string(),
        },
    )?;
    Ok(commit_evidence(commit.certificate))
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

fn validate_column(column: &ColumnDescriptor) -> RsfResult<()> {
    validate_name("column", &column.name)
}

fn validate_name(kind: &str, value: &str) -> RsfResult<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!("{kind} name must be non-empty ASCII alphanumeric, '_' or '-'"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rsf_core::{
        Epoch, NodeId,
        catalog::{ColumnType, TableName},
    };
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn table() -> TableDescriptor {
        TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("users".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "id".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "name".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    #[test]
    fn namespace_table_bucket_and_bindings_commit_through_protocol() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);
        let mut catalog = CatalogEngine::default();

        let namespace = catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
            .unwrap();
        let descriptor = catalog
            .create_table(&mut router, PlacementGroupId(1), table())
            .unwrap();
        let bucket = catalog
            .create_bucket(&mut router, PlacementGroupId(1), "app", "media")
            .unwrap();
        let binding = catalog
            .bind_surface(
                &mut router,
                PlacementGroupId(1),
                "app",
                CatalogSurface::Relational,
                PlacementGroupId(2),
            )
            .unwrap();

        assert_eq!(namespace.version, CommitVersion(1));
        assert_eq!(descriptor.name.0, "users");
        assert_eq!(
            catalog.table_version("app", "users"),
            Some(CommitVersion(2))
        );
        assert_eq!(bucket.version, CommitVersion(3));
        assert_eq!(binding.version, CommitVersion(4));
        assert_eq!(catalog.snapshot(&router).version, CommitVersion(4));
        assert_eq!(catalog.item_count(), 4);
    }

    #[test]
    fn catalog_commits_through_vsr_backed_router_and_recovers_certificates() {
        let path = test_path("catalog_commits_through_vsr_backed_router");
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");
            let mut catalog = CatalogEngine::default();

            catalog
                .create_namespace(&mut router, PlacementGroupId(1), "app")
                .expect("namespace");
            catalog
                .create_table(&mut router, PlacementGroupId(1), table())
                .expect("table");
            catalog
                .create_bucket(&mut router, PlacementGroupId(1), "app", "media")
                .expect("bucket");
            catalog
                .bind_surface(
                    &mut router,
                    PlacementGroupId(1),
                    "app",
                    CatalogSurface::Blob,
                    PlacementGroupId(2),
                )
                .expect("binding");

            assert_eq!(catalog.snapshot(&router).version, CommitVersion(4));
            assert_eq!(router.latest_version(), CommitVersion(4));
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router");

        assert_eq!(recovered.latest_version(), CommitVersion(4));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            4
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn catalog_retries_replay_vsr_request_certificates_without_new_commits() {
        let path = test_path("catalog_retries_replay_vsr_request_certificates");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut catalog = CatalogEngine::default();

            catalog
                .create_namespace(&mut router, PlacementGroupId(1), "app")
                .expect("namespace");
            catalog
                .create_table(&mut router, PlacementGroupId(1), table())
                .expect("table");
            catalog
                .create_bucket(&mut router, PlacementGroupId(1), "app", "media")
                .expect("bucket");
            catalog
                .bind_surface(
                    &mut router,
                    PlacementGroupId(1),
                    "app",
                    CatalogSurface::Blob,
                    PlacementGroupId(2),
                )
                .expect("binding");

            assert_eq!(router.latest_version(), CommitVersion(4));
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut retried = CatalogEngine::default();

            let namespace = retried
                .create_namespace(&mut router, PlacementGroupId(1), "app")
                .expect("namespace replay");
            let descriptor = retried
                .create_table(&mut router, PlacementGroupId(1), table())
                .expect("table replay");
            let bucket = retried
                .create_bucket(&mut router, PlacementGroupId(1), "app", "media")
                .expect("bucket replay");
            let binding = retried
                .bind_surface(
                    &mut router,
                    PlacementGroupId(1),
                    "app",
                    CatalogSurface::Blob,
                    PlacementGroupId(2),
                )
                .expect("binding replay");

            assert_eq!(namespace.version, CommitVersion(1));
            assert_eq!(
                retried.table_version("app", &descriptor.name.0),
                Some(CommitVersion(2))
            );
            assert_eq!(bucket.version, CommitVersion(3));
            assert_eq!(binding.version, CommitVersion(4));
            assert_eq!(router.latest_version(), CommitVersion(4));
            assert_eq!(retried.item_count(), 4);
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            voters,
        )
        .expect("recover router again");

        assert_eq!(recovered.latest_version(), CommitVersion(4));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            4
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn create_is_idempotent_for_existing_items() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut catalog = CatalogEngine::default();

        let first = catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
            .unwrap();
        let second = catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(router.latest_version(), CommitVersion(1));
    }

    #[test]
    fn table_requires_existing_namespace() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut catalog = CatalogEngine::default();

        let err = catalog
            .create_table(&mut router, PlacementGroupId(1), table())
            .expect_err("missing namespace must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn table_primary_key_must_reference_a_column() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut catalog = CatalogEngine::default();
        catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
            .unwrap();
        let mut descriptor = table();
        descriptor.primary_key = vec!["missing".to_string()];

        let err = catalog
            .create_table(&mut router, PlacementGroupId(1), descriptor)
            .expect_err("invalid primary key must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-catalog-{name}-{}-{nonce}", std::process::id()))
    }
}
