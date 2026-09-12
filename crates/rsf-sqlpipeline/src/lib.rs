#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_auth::{AuthEngine, AuthToken, Authorization, PrincipalId};
use rsf_catalog::CatalogEngine;
use rsf_constraints::{ConstraintRule, ConstraintState};
use rsf_core::{
    CommitVersion, ErrorCode, PlacementGroupId, RsfError, RsfResult,
    catalog::{ColumnType, TableDescriptor, TableName},
    transaction::TransactionId,
};
use rsf_executor::{ExecutionReport, PhysicalExecutor};
use rsf_index::IndexDescriptor;
use rsf_isolation::IsolationTracker;
use rsf_kv::KvEngine;
use rsf_planner::{PhysicalPlan, PhysicalPlanner, PhysicalPlanningContext, TableStats};
use rsf_protocol::FabricRouterPort;
use rsf_query::{
    Predicate, Projection, QueryExecutor, QueryPlan, QueryResult, QueryRow, QueryValue,
};
use rsf_session::{BoundStatement, SessionBinder, SessionContext, StatementClass};
use rsf_sqldml::{
    SqlDmlPipeline, SqlDmlPipelineDecision, SqlDmlPipelineReport, SqlDmlPipelineRequest,
};
use rsf_txn::TransactionCoordinator;

#[derive(Clone, Debug)]
pub struct SqlPipelineRequest {
    pub context: SessionContext,
    pub principal: PrincipalId,
    pub statement: String,
    pub snapshot: CommitVersion,
    pub catalog_group: PlacementGroupId,
    pub now: CommitVersion,
    pub stats: TableStats,
    pub indexes: Vec<IndexDescriptor>,
    pub cacheable: bool,
    pub stream_threshold_bytes: u64,
    pub rows: Vec<QueryRow>,
    pub transaction_id: TransactionId,
    pub constraints: Vec<ConstraintRule>,
    pub constraint_state: ConstraintState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlPipelineReport {
    pub bound: BoundStatement,
    pub authorization: Authorization,
    pub logical: QueryPlan,
    pub physical: PhysicalPlan,
    pub execution: ExecutionReport,
    pub result: QueryResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlStatementReport {
    Query(SqlPipelineReport),
    Dml(SqlDmlPipelineReport),
}

impl SqlStatementReport {
    pub fn bound(&self) -> &BoundStatement {
        match self {
            Self::Query(report) => &report.bound,
            Self::Dml(report) => &report.bound,
        }
    }

    pub fn committed_version(&self) -> Option<CommitVersion> {
        match self {
            Self::Query(_) => None,
            Self::Dml(report) => Some(report.commit_version),
        }
    }
}

impl SqlPipelineReport {
    pub fn stage_count(&self) -> usize {
        self.execution.stages.len()
    }

    pub fn row_count(&self) -> usize {
        self.result.rows.len()
    }

    pub fn remote_reads(&self) -> u64 {
        self.execution.remote_reads
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlPipelineDecision {
    Bound {
        table: TableName,
        class: StatementClass,
        placement_group: PlacementGroupId,
    },
    Authorized {
        principal: PrincipalId,
    },
    LogicalPlanned {
        table: TableName,
        projection: usize,
        has_predicate: bool,
        limit: Option<usize>,
    },
    PhysicalPlanned {
        table: TableName,
        operators: usize,
        estimated_rows: u64,
        estimated_bytes: u64,
    },
    Executed {
        stages: usize,
        rows: usize,
        bytes: u64,
        remote_reads: u64,
    },
    Dml {
        decision: SqlDmlPipelineDecision,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct SqlPipeline;

impl SqlPipeline {
    pub fn execute_statement(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        transactions: &mut TransactionCoordinator,
        isolation: &mut IsolationTracker,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        request: SqlPipelineRequest,
    ) -> RsfResult<(SqlStatementReport, Vec<SqlPipelineDecision>)> {
        let bound = SessionBinder.bind(
            catalog,
            &request.context,
            &request.statement,
            request.snapshot,
            request.catalog_group,
        )?;
        match bound.class {
            StatementClass::Query => {
                let (report, decisions) = self.execute(catalog, auth, token, request)?;
                Ok((SqlStatementReport::Query(report), decisions))
            }
            StatementClass::Dml => {
                let request = SqlDmlPipelineRequest {
                    context: request.context,
                    principal: request.principal,
                    statement: request.statement,
                    snapshot: request.snapshot,
                    catalog_group: request.catalog_group,
                    now: request.now,
                    transaction_id: request.transaction_id,
                    constraints: request.constraints,
                    constraint_state: request.constraint_state,
                    indexes: request.indexes,
                    rows: dml_rows(request.rows),
                };
                let (report, decisions) = SqlDmlPipeline.execute(
                    catalog,
                    auth,
                    token,
                    transactions,
                    isolation,
                    kv,
                    router,
                    request,
                )?;
                Ok((
                    SqlStatementReport::Dml(report),
                    decisions
                        .into_iter()
                        .map(|decision| SqlPipelineDecision::Dml { decision })
                        .collect(),
                ))
            }
            StatementClass::Ddl => Err(invalid(
                "SQL statement pipeline does not execute DDL; use the catalog migration path",
            )),
        }
    }

    pub fn execute(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        request: SqlPipelineRequest,
    ) -> RsfResult<(SqlPipelineReport, Vec<SqlPipelineDecision>)> {
        let bound = SessionBinder.bind(
            catalog,
            &request.context,
            &request.statement,
            request.snapshot,
            request.catalog_group,
        )?;
        if bound.class != StatementClass::Query {
            return Err(invalid(
                "SQL query pipeline accepts SELECT statements; use execute_statement for DML",
            ));
        }
        let table = bound
            .table
            .clone()
            .ok_or_else(|| invalid("bound SELECT statement must include table metadata"))?;

        let authorization =
            SessionBinder.authorize(auth, token, request.now, &bound, request.principal.clone())?;
        if authorization != Authorization::Allowed {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL pipeline authorization denied query execution",
            ));
        }

        let logical_shape = lower_select(&request.statement, &table)?;
        let logical = rsf_query::QueryPlanner.select(
            &table,
            logical_shape.projection,
            logical_shape.predicate,
            logical_shape.limit,
        )?;
        let physical = PhysicalPlanner.plan(
            &table,
            &logical,
            &PhysicalPlanningContext {
                stats: request.stats,
                indexes: request.indexes,
                stream_threshold_bytes: request.stream_threshold_bytes,
                cacheable: request.cacheable,
            },
        )?;
        let execution = PhysicalExecutor.execute(&physical)?;
        let result = QueryExecutor.execute(&logical, &request.rows)?;

        let decisions = vec![
            SqlPipelineDecision::Bound {
                table: table.name.clone(),
                class: bound.class,
                placement_group: bound.transaction.placement_group(),
            },
            SqlPipelineDecision::Authorized {
                principal: request.principal,
            },
            SqlPipelineDecision::LogicalPlanned {
                table: logical.table.clone(),
                projection: projection_width(&logical.projection, &table),
                has_predicate: logical.predicate.is_some(),
                limit: logical.limit,
            },
            SqlPipelineDecision::PhysicalPlanned {
                table: physical.table.clone(),
                operators: physical.operators.len(),
                estimated_rows: physical.estimated_rows,
                estimated_bytes: physical.estimated_bytes,
            },
            SqlPipelineDecision::Executed {
                stages: execution.stages.len(),
                rows: result.rows.len(),
                bytes: execution.bytes,
                remote_reads: execution.remote_reads,
            },
        ];

        Ok((
            SqlPipelineReport {
                bound,
                authorization,
                logical,
                physical,
                execution,
                result,
            },
            decisions,
        ))
    }

    pub fn reject(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        request: SqlPipelineRequest,
    ) -> SqlPipelineDecision {
        match self.execute(catalog, auth, token, request) {
            Ok(_) => SqlPipelineDecision::Rejected {
                reason: "expected SQL pipeline request to be rejected".to_string(),
            },
            Err(err) => SqlPipelineDecision::Rejected {
                reason: err.message,
            },
        }
    }
}

fn dml_rows(rows: Vec<QueryRow>) -> BTreeMap<String, BTreeMap<String, String>> {
    rows.into_iter()
        .filter_map(|row| {
            let values = row
                .columns
                .into_iter()
                .map(|(column, value)| (column, query_value_to_dml_value(value)))
                .collect::<BTreeMap<_, _>>();
            values.get("id").cloned().map(|id| (id, values))
        })
        .collect()
}

fn query_value_to_dml_value(value: QueryValue) -> String {
    match value {
        QueryValue::Int64(value) => value.to_string(),
        QueryValue::Text(value) => value,
        QueryValue::Bool(value) => value.to_string(),
        QueryValue::Bytes(value) => value
            .into_iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LogicalSelect {
    projection: Projection,
    predicate: Option<Predicate>,
    limit: Option<usize>,
}

fn lower_select(statement: &str, table: &TableDescriptor) -> RsfResult<LogicalSelect> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let uppercase = trimmed.to_ascii_uppercase();
    if !uppercase.starts_with("SELECT ") {
        return Err(invalid("SQL pipeline only lowers SELECT statements"));
    }
    let from_pos = uppercase
        .find(" FROM ")
        .ok_or_else(|| invalid("SELECT missing FROM clause"))?;
    let projection = parse_projection(&trimmed["SELECT ".len()..from_pos])?;
    let after_from = &trimmed[from_pos + " FROM ".len()..];
    let uppercase_after = after_from.to_ascii_uppercase();
    let where_pos = uppercase_after.find(" WHERE ");
    let limit_pos = uppercase_after.find(" LIMIT ");

    let table_end = where_pos.or(limit_pos).unwrap_or(after_from.len());
    let target_table = after_from[..table_end].trim();
    if target_table != table.name.0 {
        return Err(invalid(format!(
            "SELECT targets {target_table}, not bound table {}",
            table.name.0
        )));
    }

    let predicate = if let Some(position) = where_pos {
        let where_start = position + " WHERE ".len();
        let where_end = limit_pos.unwrap_or(after_from.len());
        Some(parse_eq_predicate(
            &after_from[where_start..where_end],
            table,
        )?)
    } else {
        None
    };

    let limit = if let Some(position) = limit_pos {
        let value = after_from[position + " LIMIT ".len()..].trim();
        Some(
            value
                .parse::<usize>()
                .map_err(|_| invalid("SELECT LIMIT must be an integer"))?,
        )
    } else {
        None
    };

    Ok(LogicalSelect {
        projection,
        predicate,
        limit,
    })
}

fn parse_projection(input: &str) -> RsfResult<Projection> {
    let projection = input.trim();
    if projection == "*" {
        return Ok(Projection::All);
    }
    let columns = projection
        .split(',')
        .map(|column| column.trim().trim_matches('"').to_string())
        .collect::<Vec<_>>();
    if columns.iter().any(|column| column.is_empty()) {
        return Err(invalid("SELECT projection contains an empty column"));
    }
    Ok(Projection::Columns(columns))
}

fn parse_eq_predicate(input: &str, table: &TableDescriptor) -> RsfResult<Predicate> {
    let Some((column, literal)) = input.split_once('=') else {
        return Err(invalid("SQL pipeline only supports equality predicates"));
    };
    let column = column.trim().trim_matches('"');
    let column_type = table
        .columns
        .iter()
        .find(|descriptor| descriptor.name == column)
        .map(|descriptor| &descriptor.ty)
        .ok_or_else(|| invalid(format!("unknown predicate column {column}")))?;
    Ok(Predicate::Eq {
        column: column.to_string(),
        value: parse_literal(literal.trim(), column_type)?,
    })
}

fn parse_literal(input: &str, ty: &ColumnType) -> RsfResult<QueryValue> {
    match ty {
        ColumnType::Int64 => input
            .parse::<i64>()
            .map(QueryValue::Int64)
            .map_err(|_| invalid("integer literal is invalid")),
        ColumnType::Text => Ok(QueryValue::Text(
            input.trim_matches('\'').trim_matches('"').to_string(),
        )),
        ColumnType::Bool => match input.to_ascii_lowercase().as_str() {
            "true" => Ok(QueryValue::Bool(true)),
            "false" => Ok(QueryValue::Bool(false)),
            _ => Err(invalid("boolean literal is invalid")),
        },
        ColumnType::Bytes => Ok(QueryValue::Bytes(input.as_bytes().to_vec())),
    }
}

fn projection_width(projection: &Projection, table: &TableDescriptor) -> usize {
    match projection {
        Projection::All => table.columns.len(),
        Projection::Columns(columns) => columns.len(),
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use rsf_auth::{Capability, Grant, Principal, PrincipalKind, Resource};
    use rsf_catalog::CatalogSurface;
    use rsf_core::{
        CommitConsistency, Epoch, NodeId,
        catalog::{ColumnDescriptor, NamespaceName},
    };
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn executes_bound_authorized_select_pipeline() {
        let fixture = Fixture::new();
        let (report, decisions) = SqlPipeline
            .execute(
                &fixture.catalog,
                &fixture.auth,
                &fixture.token,
                fixture.request("SELECT id, name FROM users WHERE active = true LIMIT 1"),
            )
            .unwrap();

        assert_eq!(report.bound.class, StatementClass::Query);
        assert_eq!(report.logical.limit, Some(1));
        assert_eq!(report.physical.operators.len(), 2);
        assert_eq!(report.stage_count(), 2);
        assert_eq!(report.row_count(), 1);
        assert_eq!(report.remote_reads(), 1);
        assert_eq!(decisions.len(), 5);
    }

    #[test]
    fn rejects_unknown_projection_column() {
        let fixture = Fixture::new();
        let decision = SqlPipeline.reject(
            &fixture.catalog,
            &fixture.auth,
            &fixture.token,
            fixture.request("SELECT missing FROM users WHERE id = 1"),
        );

        assert!(matches!(
            decision,
            SqlPipelineDecision::Rejected { reason } if reason.contains("unknown query column")
        ));
    }

    #[test]
    fn statement_pipeline_commits_insert_through_dml_runtime() {
        let fixture = Fixture::new();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();

        let (report, decisions) = SqlPipeline
            .execute_statement(
                &fixture.catalog,
                &fixture.auth,
                &fixture.token,
                &mut transactions,
                &mut isolation,
                &mut kv,
                &mut router,
                fixture.request("INSERT INTO users(id, name, active) VALUES (3, 'lin', true)"),
            )
            .unwrap();

        let SqlStatementReport::Dml(report) = report else {
            panic!("INSERT must execute through the DML statement path");
        };
        assert_eq!(report.commit_version, CommitVersion(1));
        assert_eq!(report.applied_mutations, 2);
        assert_eq!(transactions.committed().len(), 1);
        assert_eq!(isolation.committed().len(), 1);
        assert_eq!(kv.visible_key_count(), 2);
        assert_eq!(router.committed().len(), 1);
        assert_eq!(decisions.len(), 8);
        assert!(matches!(
            decisions[5],
            SqlPipelineDecision::Dml {
                decision: SqlDmlPipelineDecision::TransactionCommitted { .. }
            }
        ));
    }

    #[test]
    fn statement_pipeline_recovers_vsr_dml_commit_certificate() {
        let fixture = Fixture::new();
        let path = test_path("statement_pipeline_recovers_vsr_dml_commit_certificate");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            let (report, _decisions) = SqlPipeline
                .execute_statement(
                    &fixture.catalog,
                    &fixture.auth,
                    &fixture.token,
                    &mut transactions,
                    &mut isolation,
                    &mut kv,
                    &mut router,
                    fixture
                        .request("INSERT INTO users(id, name, active) VALUES (4, 'lamarr', true)"),
                )
                .unwrap();
            let SqlStatementReport::Dml(report) = report else {
                panic!("INSERT must execute through the DML statement path");
            };
            assert_eq!(report.commit_version, CommitVersion(1));
            assert_eq!(report.commit_evidence.placement_group, PlacementGroupId(1));
            assert_eq!(
                report.commit_evidence.consistency,
                CommitConsistency::Strict
            );
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            voters,
        )
        .unwrap()
        .recovered_certificates(PlacementGroupId(1))
        .expect("statement-level DML certificate must recover");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].version, CommitVersion(1));
    }

    #[test]
    fn query_api_points_dml_callers_to_statement_pipeline() {
        let fixture = Fixture::new();
        let decision = SqlPipeline.reject(
            &fixture.catalog,
            &fixture.auth,
            &fixture.token,
            fixture.request("INSERT INTO users(id, name) VALUES (3, 'lin')"),
        );

        assert!(matches!(
            decision,
            SqlPipelineDecision::Rejected { reason } if reason.contains("use execute_statement for DML")
        ));
    }

    struct Fixture {
        catalog: CatalogEngine,
        auth: AuthEngine,
        token: AuthToken,
    }

    impl Fixture {
        fn new() -> Self {
            let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);
            let mut catalog = CatalogEngine::default();
            catalog
                .create_namespace(&mut router, PlacementGroupId(1), "app")
                .unwrap();
            catalog
                .create_table(&mut router, PlacementGroupId(1), table())
                .unwrap();
            catalog
                .bind_surface(
                    &mut router,
                    PlacementGroupId(1),
                    "app",
                    CatalogSurface::Relational,
                    PlacementGroupId(1),
                )
                .unwrap();

            let mut auth = AuthEngine::default();
            let principal = Principal {
                id: PrincipalId("app-client".to_string()),
                kind: PrincipalKind::User,
            };
            auth.register_principal(principal.clone()).unwrap();
            auth.grant(Grant {
                principal: principal.id.clone(),
                capability: Capability::QueryExecute,
                resource: Resource::Namespace("app".to_string()),
            })
            .unwrap();
            auth.grant(Grant {
                principal: principal.id.clone(),
                capability: Capability::NamespaceWrite,
                resource: Resource::surface("app", "relational").unwrap(),
            })
            .unwrap();
            let token = auth
                .issue_token(&principal.id, CommitVersion(10), 10)
                .unwrap();

            Self {
                catalog,
                auth,
                token,
            }
        }

        fn request(&self, statement: &str) -> SqlPipelineRequest {
            SqlPipelineRequest {
                context: SessionContext::new(
                    "session-1",
                    PrincipalId("app-client".to_string()),
                    "app",
                ),
                principal: PrincipalId("app-client".to_string()),
                statement: statement.to_string(),
                snapshot: CommitVersion(11),
                catalog_group: PlacementGroupId(1),
                now: CommitVersion(11),
                stats: TableStats {
                    table: TableName("users".to_string()),
                    rows: 1_000,
                    average_row_bytes: 512,
                },
                indexes: vec![IndexDescriptor {
                    table: TableName("users".to_string()),
                    name: "users_active".to_string(),
                    columns: vec!["active".to_string()],
                    unique: false,
                }],
                cacheable: true,
                stream_threshold_bytes: 8_192,
                rows: vec![row(1, "ada", true), row(2, "grace", false)],
                transaction_id: TransactionId([0x61; 16]),
                constraints: vec![
                    ConstraintRule::PrimaryKey {
                        columns: vec!["id".to_string()],
                    },
                    ConstraintRule::Unique {
                        name: "users_name".to_string(),
                        columns: vec!["name".to_string()],
                    },
                ],
                constraint_state: ConstraintState::default(),
            }
        }
    }

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
                ColumnDescriptor {
                    name: "active".to_string(),
                    ty: ColumnType::Bool,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    fn row(id: i64, name: &str, active: bool) -> QueryRow {
        QueryRow {
            table: TableName("users".to_string()),
            columns: BTreeMap::from([
                ("id".to_string(), QueryValue::Int64(id)),
                ("name".to_string(), QueryValue::Text(name.to_string())),
                ("active".to_string(), QueryValue::Bool(active)),
            ]),
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-sqlpipeline-{name}-{nanos}"))
    }
}
