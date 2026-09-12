use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult, SurfaceId,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const AUTH_PG: PlacementGroupId = PlacementGroupId(48);
const AUTH_WAL_MAGIC: &[u8; 7] = b"RSFAUTH";
const AUTH_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PrincipalId(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrincipalKind {
    User,
    Service,
    Node,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Principal {
    pub id: PrincipalId,
    pub kind: PrincipalKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Capability {
    ClusterJoin,
    ClusterAdmin,
    NamespaceRead,
    NamespaceWrite,
    ObjectRead,
    ObjectWrite,
    BlobRead,
    BlobWrite,
    QueryExecute,
    TransactionCommit,
    RepairOperate,
    RetentionOperate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resource {
    Cluster,
    Namespace(String),
    Surface {
        namespace: String,
        surface: SurfaceId,
    },
}

impl Resource {
    pub fn surface(namespace: impl Into<String>, surface: impl Into<String>) -> RsfResult<Self> {
        Ok(Self::Surface {
            namespace: namespace.into(),
            surface: SurfaceId::new(surface.into())?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Grant {
    pub principal: PrincipalId,
    pub capability: Capability,
    pub resource: Resource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessRequest {
    pub principal: PrincipalId,
    pub capability: Capability,
    pub resource: Resource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthToken {
    pub principal: PrincipalId,
    pub issued_at: CommitVersion,
    pub expires_at: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Authorization {
    Allowed,
    Denied { reason: String },
}

impl Authorization {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }
}

#[derive(Clone, Debug, Default)]
pub struct AuthEngine {
    principals: BTreeMap<PrincipalId, Principal>,
    grants: Vec<Grant>,
}

impl AuthEngine {
    pub fn register_principal(&mut self, principal: Principal) -> RsfResult<()> {
        validate_principal(&principal)?;
        self.principals.insert(principal.id.clone(), principal);
        Ok(())
    }

    pub fn grant(&mut self, grant: Grant) -> RsfResult<()> {
        validate_grant_shape(&grant)?;
        if !self.principals.contains_key(&grant.principal) {
            return Err(invalid(format!(
                "unknown principal {} cannot receive grant",
                grant.principal.0
            )));
        }
        self.grants.push(grant);
        Ok(())
    }

    pub fn issue_token(
        &self,
        principal: &PrincipalId,
        issued_at: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<AuthToken> {
        if ttl_versions == 0 {
            return Err(invalid("token ttl must be greater than zero"));
        }
        if !self.principals.contains_key(principal) {
            return Err(invalid(format!("unknown principal {}", principal.0)));
        }
        Ok(AuthToken {
            principal: principal.clone(),
            issued_at,
            expires_at: CommitVersion(issued_at.0 + ttl_versions),
        })
    }

    pub fn authorize(
        &self,
        token: &AuthToken,
        now: CommitVersion,
        request: AccessRequest,
    ) -> RsfResult<Authorization> {
        if !self.principals.contains_key(&token.principal) {
            return Err(invalid(format!(
                "token principal {} is not registered",
                token.principal.0
            )));
        }
        if token.principal != request.principal {
            return Ok(Authorization::Denied {
                reason: "token principal does not match access request".to_string(),
            });
        }
        if now > token.expires_at {
            return Ok(Authorization::Denied {
                reason: "token expired".to_string(),
            });
        }
        if self.grants.iter().any(|grant| grant.allows(&request)) {
            return Ok(Authorization::Allowed);
        }
        Ok(Authorization::Denied {
            reason: "no matching grant".to_string(),
        })
    }

    pub fn grant_count(&self) -> usize {
        self.grants.len()
    }

    pub fn principal_count(&self) -> usize {
        self.principals.len()
    }
}

#[derive(Debug)]
pub struct DurableAuthEngine {
    auth: AuthEngine,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableAuthEngine {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(AUTH_PG) })?;
        let mut auth = AuthEngine::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Auth)
        {
            apply_auth_record(&mut auth, decode_auth_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            auth,
            store,
            epoch,
            next_index,
        })
    }

    pub fn register_principal(&mut self, principal: Principal) -> RsfResult<()> {
        let mut auth = self.auth.clone();
        auth.register_principal(principal.clone())?;
        self.append_record(&AuthWalRecord::PrincipalRegistered { principal })?;
        self.auth = auth;
        Ok(())
    }

    pub fn grant(&mut self, grant: Grant) -> RsfResult<()> {
        let mut auth = self.auth.clone();
        auth.grant(grant.clone())?;
        self.append_record(&AuthWalRecord::GrantAdded { grant })?;
        self.auth = auth;
        Ok(())
    }

    pub fn issue_token(
        &self,
        principal: &PrincipalId,
        issued_at: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<AuthToken> {
        self.auth.issue_token(principal, issued_at, ttl_versions)
    }

    pub fn authorize(
        &self,
        token: &AuthToken,
        now: CommitVersion,
        request: AccessRequest,
    ) -> RsfResult<Authorization> {
        self.auth.authorize(token, now, request)
    }

    pub fn engine(&self) -> &AuthEngine {
        &self.auth
    }

    fn append_record(&mut self, record: &AuthWalRecord) -> RsfResult<()> {
        let payload = encode_auth_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(AUTH_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: AUTH_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Auth,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AuthWalRecord {
    PrincipalRegistered { principal: Principal },
    GrantAdded { grant: Grant },
}

fn apply_auth_record(auth: &mut AuthEngine, record: AuthWalRecord) -> RsfResult<()> {
    match record {
        AuthWalRecord::PrincipalRegistered { principal } => auth.register_principal(principal),
        AuthWalRecord::GrantAdded { grant } => auth.grant(grant),
    }
}

fn encode_auth_wal_payload(record: &AuthWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(AUTH_WAL_MAGIC);
    out.push(AUTH_WAL_VERSION);
    match record {
        AuthWalRecord::PrincipalRegistered { principal } => {
            out.push(1);
            encode_principal(principal, &mut out)?;
        }
        AuthWalRecord::GrantAdded { grant } => {
            out.push(2);
            encode_grant(grant, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_auth_wal_payload(bytes: &[u8]) -> RsfResult<AuthWalRecord> {
    if bytes.len() < AUTH_WAL_MAGIC.len() + 2 {
        return Err(corrupt("auth WAL payload is too short"));
    }
    if &bytes[..AUTH_WAL_MAGIC.len()] != AUTH_WAL_MAGIC {
        return Err(corrupt("auth WAL payload has invalid magic"));
    }
    let mut offset = AUTH_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != AUTH_WAL_VERSION {
        return Err(corrupt(format!(
            "auth WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => AuthWalRecord::PrincipalRegistered {
            principal: decode_principal(bytes, &mut offset)?,
        },
        2 => AuthWalRecord::GrantAdded {
            grant: decode_grant(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "auth WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("auth WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn encode_principal(principal: &Principal, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_principal(principal)?;
    write_string(&principal.id.0, out)?;
    out.push(match &principal.kind {
        PrincipalKind::User => 1,
        PrincipalKind::Service => 2,
        PrincipalKind::Node => 3,
    });
    Ok(())
}

fn decode_principal(bytes: &[u8], offset: &mut usize) -> RsfResult<Principal> {
    let principal = Principal {
        id: PrincipalId(read_string(bytes, offset)?),
        kind: match read_u8(bytes, offset)? {
            1 => PrincipalKind::User,
            2 => PrincipalKind::Service,
            3 => PrincipalKind::Node,
            other => return Err(corrupt(format!("auth principal kind {other} is invalid"))),
        },
    };
    validate_principal(&principal)?;
    Ok(principal)
}

fn encode_grant(grant: &Grant, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_grant_shape(grant)?;
    write_string(&grant.principal.0, out)?;
    encode_capability(grant.capability.clone(), out);
    encode_resource(&grant.resource, out)?;
    Ok(())
}

fn decode_grant(bytes: &[u8], offset: &mut usize) -> RsfResult<Grant> {
    let grant = Grant {
        principal: PrincipalId(read_string(bytes, offset)?),
        capability: decode_capability(read_u8(bytes, offset)?)?,
        resource: decode_resource(bytes, offset)?,
    };
    validate_grant_shape(&grant)?;
    Ok(grant)
}

fn encode_capability(capability: Capability, out: &mut Vec<u8>) {
    out.push(match capability {
        Capability::ClusterJoin => 1,
        Capability::ClusterAdmin => 2,
        Capability::NamespaceRead => 3,
        Capability::NamespaceWrite => 4,
        Capability::ObjectRead => 5,
        Capability::ObjectWrite => 6,
        Capability::BlobRead => 7,
        Capability::BlobWrite => 8,
        Capability::QueryExecute => 9,
        Capability::TransactionCommit => 10,
        Capability::RepairOperate => 11,
        Capability::RetentionOperate => 12,
    });
}

fn decode_capability(value: u8) -> RsfResult<Capability> {
    match value {
        1 => Ok(Capability::ClusterJoin),
        2 => Ok(Capability::ClusterAdmin),
        3 => Ok(Capability::NamespaceRead),
        4 => Ok(Capability::NamespaceWrite),
        5 => Ok(Capability::ObjectRead),
        6 => Ok(Capability::ObjectWrite),
        7 => Ok(Capability::BlobRead),
        8 => Ok(Capability::BlobWrite),
        9 => Ok(Capability::QueryExecute),
        10 => Ok(Capability::TransactionCommit),
        11 => Ok(Capability::RepairOperate),
        12 => Ok(Capability::RetentionOperate),
        other => Err(corrupt(format!("auth capability {other} is invalid"))),
    }
}

fn encode_resource(resource: &Resource, out: &mut Vec<u8>) -> RsfResult<()> {
    match resource {
        Resource::Cluster => out.push(1),
        Resource::Namespace(namespace) => {
            out.push(2);
            write_string(namespace, out)?;
        }
        Resource::Surface { namespace, surface } => {
            out.push(3);
            write_string(namespace, out)?;
            write_string(surface.as_str(), out)?;
        }
    }
    Ok(())
}

fn decode_resource(bytes: &[u8], offset: &mut usize) -> RsfResult<Resource> {
    match read_u8(bytes, offset)? {
        1 => Ok(Resource::Cluster),
        2 => Ok(Resource::Namespace(read_string(bytes, offset)?)),
        3 => Resource::surface(read_string(bytes, offset)?, read_string(bytes, offset)?),
        other => Err(corrupt(format!("auth resource kind {other} is invalid"))),
    }
}

fn validate_principal(principal: &Principal) -> RsfResult<()> {
    validate_principal_id(&principal.id)
}

fn validate_grant_shape(grant: &Grant) -> RsfResult<()> {
    validate_principal_id(&grant.principal)?;
    validate_resource(&grant.resource)
}

fn validate_principal_id(principal: &PrincipalId) -> RsfResult<()> {
    if principal.0.trim().is_empty() {
        return Err(invalid("principal id must not be empty"));
    }
    Ok(())
}

fn validate_resource(resource: &Resource) -> RsfResult<()> {
    match resource {
        Resource::Cluster => Ok(()),
        Resource::Namespace(namespace) => validate_namespace(namespace),
        Resource::Surface { namespace, .. } => validate_namespace(namespace),
    }
}

fn validate_namespace(namespace: &str) -> RsfResult<()> {
    if namespace.trim().is_empty() {
        return Err(invalid("auth resource namespace must not be empty"));
    }
    Ok(())
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| invalid("auth string is too large for WAL"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len = usize::try_from(len).map_err(|_| corrupt("auth string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("auth WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("auth WAL payload has invalid UTF-8"))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("auth WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("auth WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

impl Grant {
    fn allows(&self, request: &AccessRequest) -> bool {
        self.principal == request.principal
            && self.capability == request.capability
            && resource_matches(&self.resource, &request.resource)
    }
}

fn resource_matches(grant: &Resource, request: &Resource) -> bool {
    match (grant, request) {
        (Resource::Cluster, Resource::Cluster) => true,
        (Resource::Namespace(left), Resource::Namespace(right)) => left == right,
        (Resource::Namespace(left), Resource::Surface { namespace, .. }) => left == namespace,
        (
            Resource::Surface {
                namespace: left_ns,
                surface: left_surface,
            },
            Resource::Surface {
                namespace: right_ns,
                surface: right_surface,
            },
        ) => left_ns == right_ns && left_surface == right_surface,
        _ => false,
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_grant_authorizes_surface_request() {
        let mut auth = AuthEngine::default();
        let principal = Principal {
            id: PrincipalId("app-client".to_string()),
            kind: PrincipalKind::User,
        };
        auth.register_principal(principal.clone()).unwrap();
        auth.grant(Grant {
            principal: principal.id.clone(),
            capability: Capability::ObjectWrite,
            resource: Resource::Namespace("app".to_string()),
        })
        .unwrap();

        let token = auth
            .issue_token(&principal.id, CommitVersion(10), 10)
            .unwrap();
        let decision = auth
            .authorize(
                &token,
                CommitVersion(11),
                AccessRequest {
                    principal: principal.id,
                    capability: Capability::ObjectWrite,
                    resource: Resource::Surface {
                        namespace: "app".to_string(),
                        surface: SurfaceId::new("object").unwrap(),
                    },
                },
            )
            .unwrap();

        assert!(decision.is_allowed());
    }

    #[test]
    fn expired_token_is_denied() {
        let mut auth = AuthEngine::default();
        let principal = register_user(&mut auth, "app-client");
        auth.grant(Grant {
            principal: principal.clone(),
            capability: Capability::QueryExecute,
            resource: Resource::Namespace("app".to_string()),
        })
        .unwrap();
        let token = auth.issue_token(&principal, CommitVersion(1), 2).unwrap();

        let decision = auth
            .authorize(
                &token,
                CommitVersion(4),
                AccessRequest {
                    principal,
                    capability: Capability::QueryExecute,
                    resource: Resource::Namespace("app".to_string()),
                },
            )
            .unwrap();

        assert_eq!(
            decision,
            Authorization::Denied {
                reason: "token expired".to_string()
            }
        );
    }

    #[test]
    fn principal_mismatch_is_denied() {
        let mut auth = AuthEngine::default();
        let app = register_user(&mut auth, "app-client");
        let other = register_user(&mut auth, "other-client");
        let token = auth.issue_token(&app, CommitVersion(1), 10).unwrap();

        let decision = auth
            .authorize(
                &token,
                CommitVersion(2),
                AccessRequest {
                    principal: other,
                    capability: Capability::NamespaceRead,
                    resource: Resource::Namespace("app".to_string()),
                },
            )
            .unwrap();

        assert!(!decision.is_allowed());
    }

    #[test]
    fn missing_grant_is_denied() {
        let mut auth = AuthEngine::default();
        let principal = register_user(&mut auth, "app-client");
        let token = auth.issue_token(&principal, CommitVersion(1), 10).unwrap();

        let decision = auth
            .authorize(
                &token,
                CommitVersion(2),
                AccessRequest {
                    principal,
                    capability: Capability::RepairOperate,
                    resource: Resource::Cluster,
                },
            )
            .unwrap();

        assert_eq!(
            decision,
            Authorization::Denied {
                reason: "no matching grant".to_string()
            }
        );
    }

    #[test]
    fn surface_resource_rejects_invalid_surface_id() {
        let err = Resource::surface("app", "object store").expect_err("invalid surface id");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn empty_namespace_grant_is_rejected() {
        let mut auth = AuthEngine::default();
        let principal = register_user(&mut auth, "app-client");

        let err = auth
            .grant(Grant {
                principal,
                capability: Capability::NamespaceRead,
                resource: Resource::Namespace(" ".to_string()),
            })
            .expect_err("empty namespace rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn unknown_principal_is_rejected() {
        let auth = AuthEngine::default();
        let err = auth
            .issue_token(&PrincipalId("ghost".to_string()), CommitVersion(1), 10)
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn durable_auth_recovers_principals_grants_and_authorizes() {
        let path = test_path("durable_auth_recovers_principals_grants_and_authorizes");
        let principal = Principal {
            id: PrincipalId("app-service".to_string()),
            kind: PrincipalKind::Service,
        };

        {
            let mut auth = DurableAuthEngine::open(&path, Epoch(4)).expect("open durable auth");
            auth.register_principal(principal.clone())
                .expect("register principal");
            auth.grant(Grant {
                principal: principal.id.clone(),
                capability: Capability::BlobWrite,
                resource: Resource::surface("app", "blob").expect("resource"),
            })
            .expect("grant");
        }

        let recovered = DurableAuthEngine::open(&path, Epoch(4)).expect("recover durable auth");
        assert_eq!(recovered.engine().principal_count(), 1);
        assert_eq!(recovered.engine().grant_count(), 1);
        let token = recovered
            .issue_token(&principal.id, CommitVersion(10), 5)
            .expect("issue token after recovery");
        let decision = recovered
            .authorize(
                &token,
                CommitVersion(11),
                AccessRequest {
                    principal: principal.id,
                    capability: Capability::BlobWrite,
                    resource: Resource::surface("app", "blob").expect("resource"),
                },
            )
            .expect("authorize");

        assert_eq!(decision, Authorization::Allowed);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_auth_rejects_grant_for_unknown_recovered_principal() {
        let path = test_path("durable_auth_rejects_grant_for_unknown_recovered_principal");
        let mut store = SegmentStore::open(&path).expect("open store");
        let payload = encode_auth_wal_payload(&AuthWalRecord::GrantAdded {
            grant: Grant {
                principal: PrincipalId("missing".to_string()),
                capability: Capability::ClusterAdmin,
                resource: Resource::Cluster,
            },
        })
        .expect("encode grant");
        store
            .append_wal(WalAppend {
                pg: AUTH_PG,
                epoch: Epoch(5),
                index: LogIndex(1),
                kind: WalRecordKind::Auth,
                previous_hash: [0; 32],
                payload: &payload,
            })
            .expect("append malformed auth record");

        let err = DurableAuthEngine::open(&path, Epoch(5)).expect_err("recover rejects grant");

        assert_eq!(err.code, ErrorCode::InvalidInput);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn auth_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_auth_wal_payload(&AuthWalRecord::PrincipalRegistered {
            principal: Principal {
                id: PrincipalId("audit-service".to_string()),
                kind: PrincipalKind::Service,
            },
        })
        .expect("encode");
        payload.push(99);

        let err = decode_auth_wal_payload(&payload).expect_err("trailing bytes reject");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn register_user(auth: &mut AuthEngine, id: &str) -> PrincipalId {
        let principal = Principal {
            id: PrincipalId(id.to_string()),
            kind: PrincipalKind::User,
        };
        auth.register_principal(principal.clone()).unwrap();
        principal.id
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-auth-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
