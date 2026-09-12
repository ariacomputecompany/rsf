#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult, storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const TENANCY_PG: PlacementGroupId = PlacementGroupId(49);
const TENANCY_WAL_MAGIC: &[u8; 7] = b"RSFTEN1";
const TENANCY_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TenantName(pub String);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatabaseName(pub String);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NamespaceName(pub String);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DatabaseSurface {
    Relational,
    Kv,
    Object,
    Blob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantDescriptor {
    pub name: TenantName,
    pub default_region: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseDescriptor {
    pub tenant: TenantName,
    pub name: DatabaseName,
    pub surfaces: BTreeSet<DatabaseSurface>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceBinding {
    pub tenant: TenantName,
    pub database: DatabaseName,
    pub namespace: NamespaceName,
}

#[derive(Clone, Debug, Default)]
pub struct TenancyCatalog {
    tenants: BTreeMap<TenantName, TenantDescriptor>,
    databases: BTreeMap<(TenantName, DatabaseName), DatabaseDescriptor>,
    namespaces: BTreeMap<NamespaceName, NamespaceBinding>,
}

impl TenancyCatalog {
    pub fn create_tenant(
        &mut self,
        name: impl Into<String>,
        default_region: impl Into<String>,
    ) -> RsfResult<TenantDescriptor> {
        let name = TenantName(validate_name("tenant", name.into())?);
        let default_region = validate_name("region", default_region.into())?;
        if let Some(existing) = self.tenants.get(&name) {
            return Ok(existing.clone());
        }
        let descriptor = TenantDescriptor {
            name: name.clone(),
            default_region,
        };
        self.tenants.insert(name, descriptor.clone());
        Ok(descriptor)
    }

    pub fn create_database(
        &mut self,
        tenant: &TenantName,
        name: impl Into<String>,
        surfaces: impl IntoIterator<Item = DatabaseSurface>,
    ) -> RsfResult<DatabaseDescriptor> {
        if !self.tenants.contains_key(tenant) {
            return Err(invalid(format!("tenant {} does not exist", tenant.0)));
        }
        let name = DatabaseName(validate_name("database", name.into())?);
        let surfaces = surfaces.into_iter().collect::<BTreeSet<_>>();
        if surfaces.is_empty() {
            return Err(invalid("database must enable at least one surface"));
        }
        let key = (tenant.clone(), name.clone());
        if let Some(existing) = self.databases.get(&key) {
            return Ok(existing.clone());
        }
        let descriptor = DatabaseDescriptor {
            tenant: tenant.clone(),
            name,
            surfaces,
        };
        self.databases.insert(key, descriptor.clone());
        Ok(descriptor)
    }

    pub fn bind_namespace(
        &mut self,
        tenant: &TenantName,
        database: &DatabaseName,
        namespace: impl Into<String>,
    ) -> RsfResult<NamespaceBinding> {
        if !self
            .databases
            .contains_key(&(tenant.clone(), database.clone()))
        {
            return Err(invalid(format!(
                "database {}.{} does not exist",
                tenant.0, database.0
            )));
        }
        let namespace = NamespaceName(validate_name("namespace", namespace.into())?);
        if let Some(existing) = self.namespaces.get(&namespace) {
            if existing.tenant != *tenant || existing.database != *database {
                return Err(invalid(format!(
                    "namespace {} already belongs to {}.{}",
                    namespace.0, existing.tenant.0, existing.database.0
                )));
            }
            return Ok(existing.clone());
        }
        let binding = NamespaceBinding {
            tenant: tenant.clone(),
            database: database.clone(),
            namespace: namespace.clone(),
        };
        self.namespaces.insert(namespace, binding.clone());
        Ok(binding)
    }

    pub fn requires_surface(
        &self,
        tenant: &TenantName,
        database: &DatabaseName,
        surface: DatabaseSurface,
    ) -> RsfResult<()> {
        let descriptor = self
            .databases
            .get(&(tenant.clone(), database.clone()))
            .ok_or_else(|| {
                invalid(format!(
                    "database {}.{} does not exist",
                    tenant.0, database.0
                ))
            })?;
        if descriptor.surfaces.contains(&surface) {
            return Ok(());
        }
        Err(invalid(format!(
            "database {}.{} does not enable {}",
            tenant.0,
            database.0,
            surface.as_str()
        )))
    }

    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    pub fn database_count(&self) -> usize {
        self.databases.len()
    }

    pub fn namespace_count(&self) -> usize {
        self.namespaces.len()
    }
}

#[derive(Debug)]
pub struct DurableTenancyCatalog {
    catalog: TenancyCatalog,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableTenancyCatalog {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(TENANCY_PG),
        })?;
        let mut catalog = TenancyCatalog::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Tenancy)
        {
            apply_tenancy_record(&mut catalog, decode_tenancy_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            catalog,
            store,
            epoch,
            next_index,
        })
    }

    pub fn create_tenant(
        &mut self,
        name: impl Into<String>,
        default_region: impl Into<String>,
    ) -> RsfResult<TenantDescriptor> {
        let name = name.into();
        let default_region = default_region.into();
        let mut catalog = self.catalog.clone();
        let before = catalog.tenant_count();
        let descriptor = catalog.create_tenant(name, default_region)?;
        if catalog.tenant_count() > before {
            self.append_record(&TenancyWalRecord::TenantCreated {
                descriptor: descriptor.clone(),
            })?;
            self.catalog = catalog;
        }
        Ok(descriptor)
    }

    pub fn create_database(
        &mut self,
        tenant: &TenantName,
        name: impl Into<String>,
        surfaces: impl IntoIterator<Item = DatabaseSurface>,
    ) -> RsfResult<DatabaseDescriptor> {
        let name = name.into();
        let surfaces = surfaces.into_iter().collect::<BTreeSet<_>>();
        let mut catalog = self.catalog.clone();
        let before = catalog.database_count();
        let descriptor = catalog.create_database(tenant, name, surfaces.clone())?;
        if catalog.database_count() > before {
            self.append_record(&TenancyWalRecord::DatabaseCreated {
                descriptor: descriptor.clone(),
            })?;
            self.catalog = catalog;
        }
        Ok(descriptor)
    }

    pub fn bind_namespace(
        &mut self,
        tenant: &TenantName,
        database: &DatabaseName,
        namespace: impl Into<String>,
    ) -> RsfResult<NamespaceBinding> {
        let namespace = namespace.into();
        let mut catalog = self.catalog.clone();
        let before = catalog.namespace_count();
        let binding = catalog.bind_namespace(tenant, database, namespace)?;
        if catalog.namespace_count() > before {
            self.append_record(&TenancyWalRecord::NamespaceBound {
                binding: binding.clone(),
            })?;
            self.catalog = catalog;
        }
        Ok(binding)
    }

    pub fn requires_surface(
        &self,
        tenant: &TenantName,
        database: &DatabaseName,
        surface: DatabaseSurface,
    ) -> RsfResult<()> {
        self.catalog.requires_surface(tenant, database, surface)
    }

    pub fn catalog(&self) -> &TenancyCatalog {
        &self.catalog
    }

    fn append_record(&mut self, record: &TenancyWalRecord) -> RsfResult<()> {
        let payload = encode_tenancy_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(TENANCY_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: TENANCY_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Tenancy,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TenancyWalRecord {
    TenantCreated { descriptor: TenantDescriptor },
    DatabaseCreated { descriptor: DatabaseDescriptor },
    NamespaceBound { binding: NamespaceBinding },
}

fn apply_tenancy_record(catalog: &mut TenancyCatalog, record: TenancyWalRecord) -> RsfResult<()> {
    match record {
        TenancyWalRecord::TenantCreated { descriptor } => {
            catalog.create_tenant(descriptor.name.0, descriptor.default_region)?;
            Ok(())
        }
        TenancyWalRecord::DatabaseCreated { descriptor } => {
            catalog.create_database(&descriptor.tenant, descriptor.name.0, descriptor.surfaces)?;
            Ok(())
        }
        TenancyWalRecord::NamespaceBound { binding } => {
            catalog.bind_namespace(&binding.tenant, &binding.database, binding.namespace.0)?;
            Ok(())
        }
    }
}

fn encode_tenancy_wal_payload(record: &TenancyWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(TENANCY_WAL_MAGIC);
    out.push(TENANCY_WAL_VERSION);
    match record {
        TenancyWalRecord::TenantCreated { descriptor } => {
            out.push(1);
            write_string(&descriptor.name.0, &mut out)?;
            write_string(&descriptor.default_region, &mut out)?;
        }
        TenancyWalRecord::DatabaseCreated { descriptor } => {
            out.push(2);
            write_string(&descriptor.tenant.0, &mut out)?;
            write_string(&descriptor.name.0, &mut out)?;
            write_len(
                descriptor.surfaces.len(),
                "database surface count",
                &mut out,
            )?;
            for surface in &descriptor.surfaces {
                out.push(surface_to_u8(*surface));
            }
        }
        TenancyWalRecord::NamespaceBound { binding } => {
            out.push(3);
            write_string(&binding.tenant.0, &mut out)?;
            write_string(&binding.database.0, &mut out)?;
            write_string(&binding.namespace.0, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_tenancy_wal_payload(bytes: &[u8]) -> RsfResult<TenancyWalRecord> {
    if bytes.len() < TENANCY_WAL_MAGIC.len() + 2 {
        return Err(corrupt("tenancy WAL payload is too short"));
    }
    if &bytes[..TENANCY_WAL_MAGIC.len()] != TENANCY_WAL_MAGIC {
        return Err(corrupt("tenancy WAL payload has invalid magic"));
    }
    let mut offset = TENANCY_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != TENANCY_WAL_VERSION {
        return Err(corrupt(format!(
            "tenancy WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => TenancyWalRecord::TenantCreated {
            descriptor: TenantDescriptor {
                name: TenantName(validate_name("tenant", read_string(bytes, &mut offset)?)?),
                default_region: validate_name("region", read_string(bytes, &mut offset)?)?,
            },
        },
        2 => {
            let tenant = TenantName(validate_name("tenant", read_string(bytes, &mut offset)?)?);
            let name = DatabaseName(validate_name("database", read_string(bytes, &mut offset)?)?);
            let surface_len = read_len(bytes, &mut offset, "database surface count")?;
            let mut surfaces = BTreeSet::new();
            for _ in 0..surface_len {
                surfaces.insert(surface_from_u8(read_u8(bytes, &mut offset)?)?);
            }
            if surfaces.is_empty() {
                return Err(corrupt("tenancy database record has no surfaces"));
            }
            TenancyWalRecord::DatabaseCreated {
                descriptor: DatabaseDescriptor {
                    tenant,
                    name,
                    surfaces,
                },
            }
        }
        3 => TenancyWalRecord::NamespaceBound {
            binding: NamespaceBinding {
                tenant: TenantName(validate_name("tenant", read_string(bytes, &mut offset)?)?),
                database: DatabaseName(validate_name(
                    "database",
                    read_string(bytes, &mut offset)?,
                )?),
                namespace: NamespaceName(validate_name(
                    "namespace",
                    read_string(bytes, &mut offset)?,
                )?),
            },
        },
        other => {
            return Err(corrupt(format!(
                "tenancy WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("tenancy WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn surface_to_u8(surface: DatabaseSurface) -> u8 {
    match surface {
        DatabaseSurface::Relational => 1,
        DatabaseSurface::Kv => 2,
        DatabaseSurface::Object => 3,
        DatabaseSurface::Blob => 4,
    }
}

fn surface_from_u8(value: u8) -> RsfResult<DatabaseSurface> {
    match value {
        1 => Ok(DatabaseSurface::Relational),
        2 => Ok(DatabaseSurface::Kv),
        3 => Ok(DatabaseSurface::Object),
        4 => Ok(DatabaseSurface::Blob),
        other => Err(corrupt(format!(
            "tenancy database surface {other} is invalid"
        ))),
    }
}

impl DatabaseSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Relational => "relational",
            Self::Kv => "kv",
            Self::Object => "object",
            Self::Blob => "blob",
        }
    }
}

fn validate_name(kind: &str, value: String) -> RsfResult<String> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(invalid(format!(
            "{kind} name must be non-empty ASCII alphanumeric, '_' or '-'"
        )));
    }
    Ok(value)
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| invalid("tenancy string is too large for WAL"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len = usize::try_from(len).map_err(|_| corrupt("tenancy string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("tenancy WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("tenancy WAL payload has invalid UTF-8"))
}

fn write_len(value: usize, label: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let value = u32::try_from(value).map_err(|_| invalid(format!("{label} is too large")))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_len(bytes: &[u8], offset: &mut usize, label: &str) -> RsfResult<usize> {
    let len = read_u32(bytes, offset)?;
    usize::try_from(len).map_err(|_| corrupt(format!("{label} is too large")))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("tenancy WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("tenancy WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_tenant_database_and_namespace_binding() {
        let mut catalog = TenancyCatalog::default();
        let tenant = catalog.create_tenant("acme", "iad").unwrap();
        let database = catalog
            .create_database(
                &tenant.name,
                "prod",
                [
                    DatabaseSurface::Relational,
                    DatabaseSurface::Kv,
                    DatabaseSurface::Object,
                    DatabaseSurface::Blob,
                ],
            )
            .unwrap();
        let binding = catalog
            .bind_namespace(&tenant.name, &database.name, "app")
            .unwrap();

        assert_eq!(binding.namespace, NamespaceName("app".to_string()));
        assert_eq!(catalog.tenant_count(), 1);
        assert_eq!(catalog.database_count(), 1);
        assert_eq!(catalog.namespace_count(), 1);
        catalog
            .requires_surface(&tenant.name, &database.name, DatabaseSurface::Relational)
            .unwrap();
    }

    #[test]
    fn database_requires_existing_tenant_and_surface() {
        let mut catalog = TenancyCatalog::default();
        let err = catalog
            .create_database(
                &TenantName("missing".to_string()),
                "prod",
                [DatabaseSurface::Relational],
            )
            .expect_err("missing tenant");
        assert_eq!(err.code, ErrorCode::InvalidInput);

        let tenant = catalog.create_tenant("acme", "iad").unwrap();
        let err = catalog
            .create_database(&tenant.name, "empty", [])
            .expect_err("surface required");
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn namespace_cannot_be_rebound_to_another_database() {
        let mut catalog = TenancyCatalog::default();
        let tenant = catalog.create_tenant("acme", "iad").unwrap();
        let first = catalog
            .create_database(&tenant.name, "prod", [DatabaseSurface::Relational])
            .unwrap();
        let second = catalog
            .create_database(&tenant.name, "analytics", [DatabaseSurface::Kv])
            .unwrap();
        catalog
            .bind_namespace(&tenant.name, &first.name, "app")
            .unwrap();

        let err = catalog
            .bind_namespace(&tenant.name, &second.name, "app")
            .expect_err("namespace already owned");
        assert!(err.message.contains("already belongs"));
    }

    #[test]
    fn durable_tenancy_recovers_tenant_database_namespace_and_surface_gate() {
        let path = test_path("durable_tenancy_recovers_tenant_database_namespace_and_surface_gate");

        {
            let mut catalog =
                DurableTenancyCatalog::open(&path, Epoch(6)).expect("open tenancy catalog");
            let tenant = catalog.create_tenant("acme", "iad").expect("tenant");
            let database = catalog
                .create_database(
                    &tenant.name,
                    "prod",
                    [
                        DatabaseSurface::Relational,
                        DatabaseSurface::Kv,
                        DatabaseSurface::Object,
                    ],
                )
                .expect("database");
            catalog
                .bind_namespace(&tenant.name, &database.name, "app")
                .expect("namespace");
        }

        let recovered =
            DurableTenancyCatalog::open(&path, Epoch(6)).expect("recover tenancy catalog");
        assert_eq!(recovered.catalog().tenant_count(), 1);
        assert_eq!(recovered.catalog().database_count(), 1);
        assert_eq!(recovered.catalog().namespace_count(), 1);
        recovered
            .requires_surface(
                &TenantName("acme".to_string()),
                &DatabaseName("prod".to_string()),
                DatabaseSurface::Object,
            )
            .expect("surface recovered");

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_tenancy_rejects_database_before_recovered_tenant() {
        let path = test_path("durable_tenancy_rejects_database_before_recovered_tenant");
        let mut store = SegmentStore::open(&path).expect("open store");
        let payload = encode_tenancy_wal_payload(&TenancyWalRecord::DatabaseCreated {
            descriptor: DatabaseDescriptor {
                tenant: TenantName("missing".to_string()),
                name: DatabaseName("prod".to_string()),
                surfaces: [DatabaseSurface::Relational].into_iter().collect(),
            },
        })
        .expect("encode database");
        store
            .append_wal(WalAppend {
                pg: TENANCY_PG,
                epoch: Epoch(7),
                index: LogIndex(1),
                kind: WalRecordKind::Tenancy,
                previous_hash: [0; 32],
                payload: &payload,
            })
            .expect("append malformed tenancy record");

        let err = DurableTenancyCatalog::open(&path, Epoch(7)).expect_err("replay rejects");

        assert_eq!(err.code, ErrorCode::InvalidInput);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn tenancy_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_tenancy_wal_payload(&TenancyWalRecord::TenantCreated {
            descriptor: TenantDescriptor {
                name: TenantName("acme".to_string()),
                default_region: "iad".to_string(),
            },
        })
        .expect("encode");
        payload.push(9);

        let err = decode_tenancy_wal_payload(&payload).expect_err("trailing bytes reject");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-tenancy-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
