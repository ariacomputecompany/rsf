use std::path::{Path, PathBuf};

use rsf_auth::{
    AccessRequest, AuthEngine, Authorization, Capability, DurableAuthEngine, Grant, Principal,
    PrincipalId, PrincipalKind, Resource,
};
use rsf_core::{ErrorCode, RsfError, RsfResult};
use rsf_tenancy::{DatabaseSurface, DurableTenancyCatalog, TenancyCatalog};
use rsf_wire::{RequestEnvelope, WireOperationKind, WireProtocol};
use sha2::{Digest, Sha256};

pub fn namespace_root(state: &Path, namespace: &str) -> PathBuf {
    // Fixed-width, separator-free components avoid traversal and filename limits.
    let digest = Sha256::digest(namespace.as_bytes());
    let key = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    state.join("namespaces").join(key)
}

pub fn authorize(
    state: &Path,
    fingerprint: &[u8; 32],
    envelope: &RequestEnvelope,
) -> RsfResult<()> {
    let namespace = envelope
        .namespace
        .as_deref()
        .ok_or_else(|| invalid("namespace is required"))?;
    let (surface, name, capability) = match envelope.protocol {
        WireProtocol::Kv => (
            DatabaseSurface::Kv,
            "kv",
            if envelope.operation_kind == WireOperationKind::Read {
                Capability::NamespaceRead
            } else {
                Capability::NamespaceWrite
            },
        ),
        WireProtocol::Sql => (DatabaseSurface::Relational, "sql", Capability::QueryExecute),
        WireProtocol::Object => (
            DatabaseSurface::Object,
            "object",
            if envelope.operation_kind == WireOperationKind::Read {
                Capability::ObjectRead
            } else {
                Capability::ObjectWrite
            },
        ),
        WireProtocol::Blob => (
            DatabaseSurface::Blob,
            "blob",
            if envelope.operation_kind == WireOperationKind::Read {
                Capability::BlobRead
            } else {
                Capability::BlobWrite
            },
        ),
        WireProtocol::Admin => {
            return Err(invalid(
                "admin requests are not accepted on the data listener",
            ));
        }
    };
    let auth = AuthEngine::recover(state.join("auth"))?;
    let check = |capability| {
        let request = AccessRequest {
            principal: PrincipalId(envelope.principal.clone()),
            capability,
            resource: Resource::surface(namespace, name)?,
        };
        match auth.authorize_certificate(fingerprint, &request) {
            Authorization::Allowed => Ok(()),
            Authorization::Denied { reason } => Err(invalid(reason)),
        }
    };
    check(capability)?;
    if envelope.protocol == WireProtocol::Sql {
        // Authorization and execution use the same parser, so text classifiers
        // cannot disguise a mutation as a read-only query.
        check(if rsf_sql::statement_is_read_only(&envelope.operation)? {
            Capability::NamespaceRead
        } else {
            Capability::NamespaceWrite
        })?;
    }
    let tenancy = TenancyCatalog::recover(state.join("tenancy"))?;
    let binding = tenancy.namespace(namespace)?;
    tenancy.requires_surface(&binding.tenant, &binding.database, surface)?;
    if ["sql", "kv", "object", "blob"]
        .iter()
        .any(|surface| state.join(surface).exists())
    {
        return Err(invalid(
            "legacy unscoped data requires explicit namespace migration before serving requests",
        ));
    }
    Ok(())
}

fn capability(value: &str) -> Result<Capability, String> {
    Ok(match value {
        "namespace-read" => Capability::NamespaceRead,
        "namespace-write" => Capability::NamespaceWrite,
        "query-execute" => Capability::QueryExecute,
        "object-read" => Capability::ObjectRead,
        "object-write" => Capability::ObjectWrite,
        "blob-read" => Capability::BlobRead,
        "blob-write" => Capability::BlobWrite,
        _ => return Err("unknown data capability".into()),
    })
}

fn surfaces(value: &str) -> Result<Vec<DatabaseSurface>, String> {
    value
        .split(',')
        .map(|name| match name {
            "sql" => Ok(DatabaseSurface::Relational),
            "kv" => Ok(DatabaseSurface::Kv),
            "object" => Ok(DatabaseSurface::Object),
            "blob" => Ok(DatabaseSurface::Blob),
            _ => Err("surfaces must be a comma-separated list of sql,kv,object,blob".into()),
        })
        .collect()
}

pub fn run_cli(command: &str, args: &[String]) -> Result<(), String> {
    let state = crate::required_flag(args, "--state")?;
    let admin = rsf_admin::AdminPlane::recover(Path::new(state).join("admin"))
        .map_err(|err| err.message)?;
    if admin.membership_view().is_none() {
        return Err("initialize the cluster before configuring access".into());
    }
    // Serialize local administrative writers across the auth and tenancy logs.
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(Path::new(state).join("security.lock"))
        .map_err(|err| err.to_string())?;
    lock.try_lock()
        .map_err(|_| "another security administration command is active")?;
    let action = args
        .first()
        .ok_or("auth or namespace subcommand is required")?;
    let epoch = admin.cluster_map().epoch;
    let mut certificate_id = None;
    if command == "namespace" {
        if action != "create" {
            return Err("usage: rsfd namespace create --state <dir> --tenant <name> --database <name> --namespace <name> --region <id> --surfaces <sql,kv,object,blob>".into());
        }
        let mut tenancy = DurableTenancyCatalog::open(Path::new(state).join("tenancy"), epoch)
            .map_err(|err| err.message)?;
        let region = crate::required_flag(args, "--region")?;
        let tenant = tenancy
            .create_tenant(crate::required_flag(args, "--tenant")?, region)
            .map_err(|err| err.message)?;
        if tenant.default_region != region {
            return Err("tenant already exists with another default region".into());
        }
        let selected = surfaces(crate::required_flag(args, "--surfaces")?)?;
        let database = tenancy
            .create_database(
                &tenant.name,
                crate::required_flag(args, "--database")?,
                selected.clone(),
            )
            .map_err(|err| err.message)?;
        if database.surfaces != selected.into_iter().collect() {
            return Err("database already exists with different enabled surfaces".into());
        }
        tenancy
            .bind_namespace(
                &tenant.name,
                &database.name,
                crate::required_flag(args, "--namespace")?,
            )
            .map_err(|err| err.message)?;
    } else {
        let mut auth = DurableAuthEngine::open(Path::new(state).join("auth"), epoch)
            .map_err(|err| err.message)?;
        match action.as_str() {
            "register" => auth
                .register_principal(Principal {
                    id: PrincipalId(crate::required_flag(args, "--principal")?.into()),
                    kind: PrincipalKind::Service,
                })
                .map_err(|err| err.message)?,
            "bind" | "revoke-certificate" => {
                let certificates = crate::tls::certificates(Path::new(crate::required_flag(
                    args,
                    "--certificate",
                )?))?;
                let fingerprint = crate::tls::certificate_fingerprint(certificates[0].as_ref());
                certificate_id = Some(
                    fingerprint
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>(),
                );
                if action == "bind" {
                    auth.bind_certificate(
                        fingerprint,
                        PrincipalId(crate::required_flag(args, "--principal")?.into()),
                    )
                    .map_err(|err| err.message)?;
                } else {
                    auth.revoke_certificate(fingerprint)
                        .map_err(|err| err.message)?;
                }
            }
            "grant" | "revoke-grant" => {
                let namespace = crate::required_flag(args, "--namespace")?;
                TenancyCatalog::recover(Path::new(state).join("tenancy"))
                    .map_err(|err| err.message)?
                    .namespace(namespace)
                    .map_err(|err| err.message)?;
                let resource = match crate::optional_flag(args, "--surface") {
                    Some(surface) if ["sql", "kv", "object", "blob"].contains(&surface) => {
                        Resource::surface(namespace, surface).map_err(|err| err.message)?
                    }
                    Some(_) => return Err("surface must be sql, kv, object, or blob".into()),
                    None => Resource::Namespace(namespace.into()),
                };
                let grant = Grant {
                    principal: PrincipalId(crate::required_flag(args, "--principal")?.into()),
                    capability: capability(crate::required_flag(args, "--capability")?)?,
                    resource,
                };
                if action == "grant" {
                    auth.grant(grant).map_err(|err| err.message)?;
                } else {
                    auth.revoke_grant(grant).map_err(|err| err.message)?;
                }
            }
            _ => {
                return Err(
                    "auth subcommands: register, bind, revoke-certificate, grant, revoke-grant"
                        .into(),
                );
            }
        }
    }
    let mut audit = rsf_observability::DurableAuditLog::open(Path::new(state).join("audit"))
        .map_err(|err| err.message)?;
    crate::audit_operator_event(
        &mut audit,
        format!("{command}.{action}"),
        None,
        None,
        None,
        format!(
            "principal={} namespace={} capability={} surface={} certificate_sha256={}",
            crate::optional_flag(args, "--principal").unwrap_or("-"),
            crate::optional_flag(args, "--namespace").unwrap_or("-"),
            crate::optional_flag(args, "--capability").unwrap_or("-"),
            crate::optional_flag(args, "--surface").unwrap_or("-"),
            certificate_id.as_deref().unwrap_or("-")
        ),
    )?;
    println!("{command} {action} persisted");
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}
