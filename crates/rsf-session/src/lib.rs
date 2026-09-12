#![forbid(unsafe_code)]

use rsf_auth::{
    AccessRequest, AuthEngine, AuthToken, Authorization, Capability, PrincipalId, Resource,
};
use rsf_catalog::{CatalogEngine, CatalogSurface};
use rsf_core::{
    CommitVersion, ErrorCode, PlacementGroupId, RsfError, RsfResult,
    catalog::{NamespaceName, TableDescriptor, TableName},
};
use rsf_protocol::ConsistencyMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionContext {
    pub session_id: String,
    pub principal: PrincipalId,
    pub namespace: NamespaceName,
}

impl SessionContext {
    pub fn new(
        session_id: impl Into<String>,
        principal: PrincipalId,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            principal,
            namespace: NamespaceName(namespace.into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundStatement {
    pub session_id: String,
    pub statement: String,
    pub class: StatementClass,
    pub table: Option<TableDescriptor>,
    pub capability: Capability,
    pub resource: Resource,
    pub consistency: ConsistencyMode,
    pub transaction: TransactionShape,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementClass {
    Query,
    Ddl,
    Dml,
}

impl StatementClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Ddl => "ddl",
            Self::Dml => "dml",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionShape {
    ReadOnly {
        snapshot: CommitVersion,
        placement_group: PlacementGroupId,
    },
    MetadataWrite {
        catalog_group: PlacementGroupId,
    },
    SinglePartitionWrite {
        placement_group: PlacementGroupId,
    },
}

impl TransactionShape {
    pub fn placement_group(&self) -> PlacementGroupId {
        match self {
            Self::ReadOnly {
                placement_group, ..
            }
            | Self::SinglePartitionWrite { placement_group } => *placement_group,
            Self::MetadataWrite { catalog_group } => *catalog_group,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly { .. } => "read-only",
            Self::MetadataWrite { .. } => "metadata-write",
            Self::SinglePartitionWrite { .. } => "single-partition-write",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SessionBinder;

impl SessionBinder {
    pub fn bind(
        &self,
        catalog: &CatalogEngine,
        context: &SessionContext,
        statement: &str,
        snapshot: CommitVersion,
        catalog_group: PlacementGroupId,
    ) -> RsfResult<BoundStatement> {
        validate_session(context)?;
        let parsed = ParsedSql::parse(statement)?;
        let binding = catalog
            .binding(&context.namespace.0, CatalogSurface::Relational)
            .ok_or_else(|| {
                invalid(format!(
                    "namespace {} has no relational placement binding",
                    context.namespace.0
                ))
            })?;

        let table = match parsed.table {
            Some(table_name) if parsed.requires_existing_table() => Some(
                catalog
                    .table(&context.namespace.0, &table_name)
                    .cloned()
                    .ok_or_else(|| {
                        invalid(format!(
                            "table {}.{} does not exist",
                            context.namespace.0, table_name
                        ))
                    })?,
            ),
            Some(table_name) => Some(TableDescriptor {
                namespace: context.namespace.clone(),
                name: TableName(table_name),
                columns: Vec::new(),
                primary_key: Vec::new(),
            }),
            None => None,
        };

        let transaction = match parsed.class {
            StatementClass::Query => TransactionShape::ReadOnly {
                snapshot,
                placement_group: binding.placement_group,
            },
            StatementClass::Ddl => TransactionShape::MetadataWrite { catalog_group },
            StatementClass::Dml => TransactionShape::SinglePartitionWrite {
                placement_group: binding.placement_group,
            },
        };

        let capability = match parsed.class {
            StatementClass::Query => Capability::QueryExecute,
            StatementClass::Ddl | StatementClass::Dml => Capability::NamespaceWrite,
        };

        Ok(BoundStatement {
            session_id: context.session_id.clone(),
            statement: statement.trim().trim_end_matches(';').trim().to_string(),
            class: parsed.class,
            table,
            capability,
            resource: Resource::surface(context.namespace.0.clone(), "relational")
                .expect("built-in surface id is valid"),
            consistency: ConsistencyMode::Strict,
            transaction,
        })
    }

    pub fn authorize(
        &self,
        auth: &AuthEngine,
        token: &AuthToken,
        now: CommitVersion,
        statement: &BoundStatement,
        principal: PrincipalId,
    ) -> RsfResult<Authorization> {
        auth.authorize(
            token,
            now,
            AccessRequest {
                principal,
                capability: statement.capability.clone(),
                resource: statement.resource.clone(),
            },
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedSql {
    class: StatementClass,
    table: Option<String>,
    requires_existing_table: bool,
}

impl ParsedSql {
    fn parse(statement: &str) -> RsfResult<Self> {
        let trimmed = statement.trim().trim_end_matches(';').trim();
        if trimmed.is_empty() {
            return Err(invalid("SQL statement must not be empty"));
        }

        let uppercase = trimmed.to_ascii_uppercase();
        if uppercase.starts_with("SELECT ") {
            return Ok(Self {
                class: StatementClass::Query,
                table: table_after_keyword(trimmed, "FROM")?,
                requires_existing_table: true,
            });
        }
        if uppercase.starts_with("CREATE TABLE ") {
            return Ok(Self {
                class: StatementClass::Ddl,
                table: Some(table_after_prefix(trimmed, "CREATE TABLE")?),
                requires_existing_table: false,
            });
        }
        if uppercase.starts_with("CREATE INDEX ") {
            return Ok(Self {
                class: StatementClass::Ddl,
                table: table_after_keyword(trimmed, "ON")?,
                requires_existing_table: true,
            });
        }
        if uppercase.starts_with("INSERT INTO ") {
            return Ok(Self {
                class: StatementClass::Dml,
                table: Some(table_after_prefix(trimmed, "INSERT INTO")?),
                requires_existing_table: true,
            });
        }
        if uppercase.starts_with("UPDATE ") {
            return Ok(Self {
                class: StatementClass::Dml,
                table: Some(table_after_prefix(trimmed, "UPDATE")?),
                requires_existing_table: true,
            });
        }
        if uppercase.starts_with("DELETE FROM ") {
            return Ok(Self {
                class: StatementClass::Dml,
                table: Some(table_after_prefix(trimmed, "DELETE FROM")?),
                requires_existing_table: true,
            });
        }

        Err(invalid("unsupported SQL statement for session binding"))
    }

    fn requires_existing_table(&self) -> bool {
        self.requires_existing_table
    }
}

fn table_after_keyword(statement: &str, keyword: &str) -> RsfResult<Option<String>> {
    let uppercase = statement.to_ascii_uppercase();
    let needle = format!(" {keyword} ");
    let Some(position) = uppercase.find(&needle) else {
        return Err(invalid(format!("SQL statement missing {keyword}")));
    };
    Ok(Some(first_identifier(
        &statement[position + needle.len()..],
    )?))
}

fn table_after_prefix(statement: &str, prefix: &str) -> RsfResult<String> {
    let rest = statement
        .get(prefix.len()..)
        .ok_or_else(|| invalid(format!("SQL statement missing {prefix} target")))?
        .trim();
    first_identifier(rest)
}

fn first_identifier(input: &str) -> RsfResult<String> {
    let identifier = input
        .trim()
        .trim_start_matches("ONLY ")
        .split(|ch: char| ch.is_whitespace() || ch == '(' || ch == ';')
        .next()
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    if identifier.is_empty() {
        return Err(invalid("SQL statement missing table name"));
    }
    Ok(identifier)
}

fn validate_session(context: &SessionContext) -> RsfResult<()> {
    if context.session_id.trim().is_empty() {
        return Err(invalid("session id must not be empty"));
    }
    if context.namespace.0.trim().is_empty() {
        return Err(invalid("session namespace must not be empty"));
    }
    if context.principal.0.trim().is_empty() {
        return Err(invalid("session principal must not be empty"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use rsf_auth::{Grant, Principal, PrincipalKind};
    use rsf_catalog::CatalogSurface;
    use rsf_core::catalog::{ColumnDescriptor, ColumnType};
    use rsf_protocol::FabricRouter;

    use super::*;

    #[test]
    fn select_binds_to_read_only_snapshot_and_query_capability() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);
        let catalog = catalog(&mut router);
        let context = context();

        let bound = SessionBinder
            .bind(
                &catalog,
                &context,
                "SELECT id, name FROM users WHERE id = 1",
                CommitVersion(11),
                PlacementGroupId(1),
            )
            .unwrap();

        assert_eq!(bound.class, StatementClass::Query);
        assert_eq!(bound.capability, Capability::QueryExecute);
        assert_eq!(
            bound.transaction,
            TransactionShape::ReadOnly {
                snapshot: CommitVersion(11),
                placement_group: PlacementGroupId(2)
            }
        );
        assert_eq!(bound.table.unwrap().name.0, "users");
    }

    #[test]
    fn mutation_binds_to_namespace_write_and_relational_pg() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);
        let catalog = catalog(&mut router);
        let context = context();

        let bound = SessionBinder
            .bind(
                &catalog,
                &context,
                "INSERT INTO users(id, name) VALUES (1, 'ada')",
                CommitVersion(11),
                PlacementGroupId(1),
            )
            .unwrap();

        assert_eq!(bound.class, StatementClass::Dml);
        assert_eq!(bound.capability, Capability::NamespaceWrite);
        assert_eq!(
            bound.transaction,
            TransactionShape::SinglePartitionWrite {
                placement_group: PlacementGroupId(2)
            }
        );
    }

    #[test]
    fn unknown_table_is_rejected_before_execution() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);
        let catalog = catalog(&mut router);
        let context = context();

        let err = SessionBinder
            .bind(
                &catalog,
                &context,
                "SELECT * FROM missing WHERE id = 1",
                CommitVersion(11),
                PlacementGroupId(1),
            )
            .expect_err("missing table must fail binding");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn authorization_uses_bound_statement_capability_and_resource() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);
        let catalog = catalog(&mut router);
        let context = context();
        let mut auth = AuthEngine::default();
        let principal = Principal {
            id: context.principal.clone(),
            kind: PrincipalKind::User,
        };
        auth.register_principal(principal.clone()).unwrap();
        auth.grant(Grant {
            principal: principal.id.clone(),
            capability: Capability::QueryExecute,
            resource: Resource::Namespace("app".to_string()),
        })
        .unwrap();
        let token = auth
            .issue_token(&principal.id, CommitVersion(10), 10)
            .unwrap();

        let bound = SessionBinder
            .bind(
                &catalog,
                &context,
                "SELECT id FROM users WHERE id = 1",
                CommitVersion(11),
                PlacementGroupId(1),
            )
            .unwrap();
        let decision = SessionBinder
            .authorize(&auth, &token, CommitVersion(11), &bound, principal.id)
            .unwrap();

        assert_eq!(decision, Authorization::Allowed);
    }

    fn context() -> SessionContext {
        SessionContext::new("session-1", PrincipalId("app-client".to_string()), "app")
    }

    fn catalog(router: &mut FabricRouter) -> CatalogEngine {
        let mut catalog = CatalogEngine::default();
        catalog
            .create_namespace(router, PlacementGroupId(1), "app")
            .unwrap();
        catalog
            .create_table(
                router,
                PlacementGroupId(1),
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
                },
            )
            .unwrap();
        catalog
            .bind_surface(
                router,
                PlacementGroupId(1),
                "app",
                CatalogSurface::Relational,
                PlacementGroupId(2),
            )
            .unwrap();
        catalog
    }
}
