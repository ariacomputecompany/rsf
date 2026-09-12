#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_auth::{AuthEngine, AuthToken, Authorization, PrincipalId};
use rsf_catalog::CatalogEngine;
use rsf_constraints::{ConstraintRule, ConstraintState, ConstraintValidator};
use rsf_core::{
    CommitEvidence, CommitVersion, ErrorCode, PlacementGroupId, RsfError, RsfResult,
    catalog::TableName,
    transaction::{Mutation, TransactionId},
};
use rsf_index::{IndexDescriptor, RowImage};
use rsf_isolation::{IsolationDecision, IsolationTracker, IsolationTransaction};
use rsf_kv::KvEngine;
use rsf_protocol::{FabricRouter, FabricRouterPort};
use rsf_relcommit::{CommitGroupDecision, RelationalCommitAuditor, RelationalCommitGroup};
use rsf_session::{BoundStatement, SessionBinder, SessionContext, StatementClass};
use rsf_sessiontxn::SessionTransactionEngine;
use rsf_txn::{PreparedTransaction, TransactionCoordinator};
use rsf_write::{WriteDecision, WritePlan, WritePlanner, WriteRequest};

#[derive(Clone, Debug)]
pub struct SqlDmlPipelineRequest {
    pub context: SessionContext,
    pub principal: PrincipalId,
    pub statement: String,
    pub snapshot: CommitVersion,
    pub catalog_group: PlacementGroupId,
    pub now: CommitVersion,
    pub transaction_id: TransactionId,
    pub constraints: Vec<ConstraintRule>,
    pub constraint_state: ConstraintState,
    pub indexes: Vec<IndexDescriptor>,
    pub rows: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlDmlPipelineReport {
    pub bound: BoundStatement,
    pub authorization: Authorization,
    pub commit_group: RelationalCommitGroup,
    pub write_plan: WritePlan,
    pub prepared: PreparedTransaction,
    pub commit_version: CommitVersion,
    pub commit_evidence: CommitEvidence,
    pub isolation: IsolationDecision,
    pub applied_mutations: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlDmlSessionReport {
    pub bound: BoundStatement,
    pub authorization: Authorization,
    pub commit_group: RelationalCommitGroup,
    pub write_plan: WritePlan,
    pub staged_mutations: usize,
    pub overlay_verified: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlDmlPipelineDecision {
    Bound {
        table: TableName,
        placement_group: PlacementGroupId,
    },
    Authorized {
        principal: PrincipalId,
    },
    WritePlanned {
        operation: DmlOperation,
        table: TableName,
        steps: usize,
        mutations: usize,
        write_conflicts: usize,
    },
    CommitGroupAccepted {
        table: String,
        row_mutations: usize,
        index_mutations: usize,
        conflict_ranges: usize,
        intent_mutations: usize,
    },
    TransactionPrepared {
        transaction: TransactionId,
        read_version: CommitVersion,
        read_conflicts: usize,
        write_conflicts: usize,
    },
    TransactionCommitted {
        transaction: TransactionId,
        commit_version: CommitVersion,
        evidence: CommitEvidence,
    },
    IsolationAccepted {
        transaction: TransactionId,
        ranges: usize,
    },
    KvApplied {
        commit_version: CommitVersion,
        mutations: usize,
    },
    SessionStaged {
        session_id: String,
        mutations: usize,
    },
    SessionOverlayVerified {
        session_id: String,
        reads: usize,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlOperation {
    Insert,
    Update,
    Delete,
}

impl DmlOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SqlDmlPipeline;

impl SqlDmlPipeline {
    pub fn execute(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        transactions: &mut TransactionCoordinator,
        isolation: &mut IsolationTracker,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        request: SqlDmlPipelineRequest,
    ) -> RsfResult<(SqlDmlPipelineReport, Vec<SqlDmlPipelineDecision>)> {
        let planned = self.plan(catalog, auth, token, request)?;
        let placement_group = planned.bound.transaction.placement_group();
        let bound = planned.bound;
        let authorization = planned.authorization;
        let principal = planned.principal;
        let operation = planned.operation;
        let write_plan = planned.write_plan;
        let commit_group = planned.commit_group;
        let table_name = write_plan.table.clone();

        let read = transactions.begin(router);
        let prepared = transactions.prepare(read.clone(), write_plan.intent.clone())?;
        let commit = transactions.commit(router, placement_group, prepared.clone())?;
        let isolation_decision = isolation.commit(
            IsolationTransaction {
                id: write_plan.intent.id.clone(),
                read_version: read.version,
                read_ranges: write_plan.intent.read_conflicts.clone(),
                write_ranges: write_plan.intent.write_conflicts.clone(),
            },
            commit.version,
        )?;
        if !isolation_decision.is_accepted() {
            return Err(invalid("SQL DML isolation validation rejected transaction"));
        }
        apply_mutations(kv, commit.version, &write_plan.intent.mutations);

        let decisions = vec![
            SqlDmlPipelineDecision::Bound {
                table: table_name.clone(),
                placement_group,
            },
            SqlDmlPipelineDecision::Authorized { principal },
            SqlDmlPipelineDecision::WritePlanned {
                operation,
                table: table_name,
                steps: write_plan.steps.len(),
                mutations: write_plan.intent.mutations.len(),
                write_conflicts: write_plan.intent.write_conflicts.len(),
            },
            SqlDmlPipelineDecision::CommitGroupAccepted {
                table: commit_group.table.clone(),
                row_mutations: commit_group.row_mutations,
                index_mutations: commit_group.index_mutations,
                conflict_ranges: commit_group.conflict_ranges,
                intent_mutations: commit_group.intent_mutations,
            },
            SqlDmlPipelineDecision::TransactionPrepared {
                transaction: write_plan.intent.id.clone(),
                read_version: read.version,
                read_conflicts: write_plan.intent.read_conflicts.len(),
                write_conflicts: write_plan.intent.write_conflicts.len(),
            },
            SqlDmlPipelineDecision::TransactionCommitted {
                transaction: write_plan.intent.id.clone(),
                commit_version: commit.version,
                evidence: commit.evidence.clone(),
            },
            SqlDmlPipelineDecision::IsolationAccepted {
                transaction: write_plan.intent.id.clone(),
                ranges: write_plan.intent.read_conflicts.len()
                    + write_plan.intent.write_conflicts.len(),
            },
            SqlDmlPipelineDecision::KvApplied {
                commit_version: commit.version,
                mutations: write_plan.intent.mutations.len(),
            },
        ];

        Ok((
            SqlDmlPipelineReport {
                bound,
                authorization,
                commit_group,
                write_plan: write_plan.clone(),
                prepared,
                commit_version: commit.version,
                commit_evidence: commit.evidence,
                isolation: isolation_decision,
                applied_mutations: write_plan.intent.mutations.len(),
            },
            decisions,
        ))
    }

    pub fn stage_in_session_transaction(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        session_transactions: &mut SessionTransactionEngine,
        kv: &KvEngine,
        request: SqlDmlPipelineRequest,
    ) -> RsfResult<(SqlDmlSessionReport, Vec<SqlDmlPipelineDecision>)> {
        let planned = self.plan(catalog, auth, token, request)?;
        let session_id = planned.bound.session_id.clone();
        let mut overlay_verified = 0;

        for mutation in &planned.write_plan.intent.mutations {
            match mutation {
                Mutation::Set { key, value } => {
                    session_transactions.stage_set(&session_id, key.clone(), value.clone())?;
                    session_transactions.read(&session_id, kv, key.clone())?;
                    overlay_verified += 1;
                }
                Mutation::Clear { key } => {
                    session_transactions.stage_clear(&session_id, key.clone())?;
                    session_transactions.read(&session_id, kv, key.clone())?;
                    overlay_verified += 1;
                }
            }
        }

        let decisions = vec![
            SqlDmlPipelineDecision::Bound {
                table: planned
                    .bound
                    .table
                    .as_ref()
                    .expect("DML table")
                    .name
                    .clone(),
                placement_group: planned.bound.transaction.placement_group(),
            },
            SqlDmlPipelineDecision::Authorized {
                principal: planned.principal,
            },
            SqlDmlPipelineDecision::WritePlanned {
                operation: planned.operation,
                table: planned.write_plan.table.clone(),
                steps: planned.write_plan.steps.len(),
                mutations: planned.write_plan.intent.mutations.len(),
                write_conflicts: planned.write_plan.intent.write_conflicts.len(),
            },
            SqlDmlPipelineDecision::CommitGroupAccepted {
                table: planned.commit_group.table.clone(),
                row_mutations: planned.commit_group.row_mutations,
                index_mutations: planned.commit_group.index_mutations,
                conflict_ranges: planned.commit_group.conflict_ranges,
                intent_mutations: planned.commit_group.intent_mutations,
            },
            SqlDmlPipelineDecision::SessionStaged {
                session_id: session_id.clone(),
                mutations: planned.write_plan.intent.mutations.len(),
            },
            SqlDmlPipelineDecision::SessionOverlayVerified {
                session_id,
                reads: overlay_verified,
            },
        ];

        Ok((
            SqlDmlSessionReport {
                bound: planned.bound,
                authorization: planned.authorization,
                commit_group: planned.commit_group,
                write_plan: planned.write_plan.clone(),
                staged_mutations: planned.write_plan.intent.mutations.len(),
                overlay_verified,
            },
            decisions,
        ))
    }

    pub fn reject(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        request: SqlDmlPipelineRequest,
    ) -> SqlDmlPipelineDecision {
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        match self.execute(
            catalog,
            auth,
            token,
            &mut transactions,
            &mut isolation,
            &mut kv,
            &mut router,
            request,
        ) {
            Ok(_) => SqlDmlPipelineDecision::Rejected {
                reason: "expected SQL DML pipeline request to be rejected".to_string(),
            },
            Err(err) => SqlDmlPipelineDecision::Rejected {
                reason: err.message,
            },
        }
    }

    fn plan(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        request: SqlDmlPipelineRequest,
    ) -> RsfResult<PlannedDml> {
        let bound = SessionBinder.bind(
            catalog,
            &request.context,
            &request.statement,
            request.snapshot,
            request.catalog_group,
        )?;
        if bound.class != StatementClass::Dml {
            return Err(invalid("SQL DML pipeline only accepts DML statements"));
        }
        let table = bound
            .table
            .clone()
            .ok_or_else(|| invalid("bound DML statement must include table metadata"))?;

        let authorization =
            SessionBinder.authorize(auth, token, request.now, &bound, request.principal.clone())?;
        if authorization != Authorization::Allowed {
            return Err(invalid(
                "SQL DML pipeline authorization denied write execution",
            ));
        }

        let dml = parse_dml(&request.statement)?;
        if dml.table() != table.name.0 {
            return Err(invalid(format!(
                "{} targets {}, not bound table {}",
                dml.operation().as_str(),
                dml.table(),
                table.name.0
            )));
        }

        let constraints = ConstraintValidator::new(&table, request.constraints)?;
        let planner = WritePlanner::new(&table, constraints, request.indexes)?;
        let operation = dml.operation();
        let decision = match dml {
            DmlStatement::Insert(insert) => planner.plan_insert(
                &table,
                WriteRequest {
                    id: request.transaction_id.clone(),
                    table: table.name.clone(),
                    primary_key: insert.primary_key,
                    values: insert.values,
                },
                &request.constraint_state,
            )?,
            DmlStatement::Update(update) => {
                let mut values = request
                    .rows
                    .get(&update.primary_key)
                    .cloned()
                    .ok_or_else(|| invalid("UPDATE target row is not present"))?;
                for (column, value) in update.assignments {
                    values.insert(column, value);
                }
                planner.plan_update(
                    &table,
                    RowImage {
                        primary_key: update.primary_key.clone(),
                        values: request
                            .rows
                            .get(&update.primary_key)
                            .cloned()
                            .expect("UPDATE row image checked above"),
                    },
                    WriteRequest {
                        id: request.transaction_id.clone(),
                        table: table.name.clone(),
                        primary_key: update.primary_key,
                        values,
                    },
                    &request.constraint_state,
                )?
            }
            DmlStatement::Delete(delete) => planner.plan_delete(
                &table,
                RowImage {
                    primary_key: delete.primary_key.clone(),
                    values: request
                        .rows
                        .get(&delete.primary_key)
                        .cloned()
                        .ok_or_else(|| invalid("DELETE target row is not present"))?,
                },
                request.transaction_id.clone(),
            )?,
        };
        let WriteDecision::Planned(write_plan) = decision else {
            return Err(invalid(
                "SQL DML write planning rejected constraint validation",
            ));
        };
        let commit_group = match RelationalCommitAuditor.audit(&write_plan) {
            CommitGroupDecision::Accepted(group) => group,
            CommitGroupDecision::Rejected { reason, .. } => {
                return Err(invalid(format!(
                    "SQL DML commit group rejected write plan: {reason}"
                )));
            }
        };

        Ok(PlannedDml {
            bound,
            authorization,
            principal: request.principal,
            operation,
            commit_group,
            write_plan,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PlannedDml {
    bound: BoundStatement,
    authorization: Authorization,
    principal: PrincipalId,
    operation: DmlOperation,
    commit_group: RelationalCommitGroup,
    write_plan: WritePlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InsertStatement {
    table: String,
    primary_key: String,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UpdateStatement {
    table: String,
    primary_key: String,
    assignments: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DeleteStatement {
    table: String,
    primary_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DmlStatement {
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl DmlStatement {
    fn operation(&self) -> DmlOperation {
        match self {
            Self::Insert(_) => DmlOperation::Insert,
            Self::Update(_) => DmlOperation::Update,
            Self::Delete(_) => DmlOperation::Delete,
        }
    }

    fn table(&self) -> &str {
        match self {
            Self::Insert(statement) => &statement.table,
            Self::Update(statement) => &statement.table,
            Self::Delete(statement) => &statement.table,
        }
    }
}

fn parse_dml(statement: &str) -> RsfResult<DmlStatement> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("INSERT INTO ") {
        return parse_insert(trimmed).map(DmlStatement::Insert);
    }
    if upper.starts_with("UPDATE ") {
        return parse_update(trimmed).map(DmlStatement::Update);
    }
    if upper.starts_with("DELETE FROM ") {
        return parse_delete(trimmed).map(DmlStatement::Delete);
    }
    Err(invalid(
        "SQL DML pipeline supports INSERT, UPDATE, and DELETE",
    ))
}

fn parse_insert(statement: &str) -> RsfResult<InsertStatement> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("INSERT INTO ") {
        return Err(invalid(
            "SQL DML INSERT parser received a non-INSERT statement",
        ));
    }

    let after_into = &trimmed["INSERT INTO ".len()..];
    let open_columns = after_into
        .find('(')
        .ok_or_else(|| invalid("INSERT must include a column list"))?;
    let table = after_into[..open_columns].trim().to_string();
    if table.is_empty() {
        return Err(invalid("INSERT table must not be empty"));
    }
    let close_columns = after_into[open_columns..]
        .find(')')
        .map(|offset| open_columns + offset)
        .ok_or_else(|| invalid("INSERT column list must be closed"))?;
    let columns = split_csv(&after_into[open_columns + 1..close_columns]);

    let after_columns = after_into[close_columns + 1..].trim();
    if !after_columns.to_ascii_uppercase().starts_with("VALUES ") {
        return Err(invalid("INSERT must include VALUES"));
    }
    let values_expr = after_columns["VALUES ".len()..].trim();
    let values_expr = values_expr
        .strip_prefix('(')
        .and_then(|expr| expr.strip_suffix(')'))
        .ok_or_else(|| invalid("INSERT VALUES must be parenthesized"))?;
    let values = split_csv(values_expr)
        .into_iter()
        .map(|value| unquote(&value))
        .collect::<Vec<_>>();
    if columns.len() != values.len() {
        return Err(invalid("INSERT column/value count mismatch"));
    }

    let values = columns.into_iter().zip(values).collect::<BTreeMap<_, _>>();
    let primary_key = values
        .get("id")
        .cloned()
        .ok_or_else(|| invalid("INSERT must include id primary key"))?;

    Ok(InsertStatement {
        table,
        primary_key,
        values,
    })
}

fn parse_update(statement: &str) -> RsfResult<UpdateStatement> {
    let after_update = &statement["UPDATE ".len()..];
    let upper_after = after_update.to_ascii_uppercase();
    let set_pos = upper_after
        .find(" SET ")
        .ok_or_else(|| invalid("UPDATE must include SET"))?;
    let where_pos = upper_after
        .find(" WHERE ")
        .ok_or_else(|| invalid("UPDATE must include WHERE id = ..."))?;
    if where_pos <= set_pos {
        return Err(invalid("UPDATE WHERE must follow SET"));
    }
    let table = after_update[..set_pos].trim().to_string();
    let assignments = parse_assignments(&after_update[set_pos + " SET ".len()..where_pos])?;
    let primary_key = parse_where_id(&after_update[where_pos + " WHERE ".len()..])?;
    Ok(UpdateStatement {
        table,
        primary_key,
        assignments,
    })
}

fn parse_delete(statement: &str) -> RsfResult<DeleteStatement> {
    let after_delete = &statement["DELETE FROM ".len()..];
    let upper_after = after_delete.to_ascii_uppercase();
    let where_pos = upper_after
        .find(" WHERE ")
        .ok_or_else(|| invalid("DELETE must include WHERE id = ..."))?;
    let table = after_delete[..where_pos].trim().to_string();
    let primary_key = parse_where_id(&after_delete[where_pos + " WHERE ".len()..])?;
    Ok(DeleteStatement { table, primary_key })
}

fn parse_assignments(input: &str) -> RsfResult<BTreeMap<String, String>> {
    let mut assignments = BTreeMap::new();
    for part in split_csv(input) {
        let (column, value) = part
            .split_once('=')
            .ok_or_else(|| invalid("assignment must use column = value"))?;
        assignments.insert(column.trim().to_string(), unquote(value));
    }
    if assignments.is_empty() {
        return Err(invalid("DML assignment list must not be empty"));
    }
    Ok(assignments)
}

fn parse_where_id(input: &str) -> RsfResult<String> {
    let (column, value) = input
        .split_once('=')
        .ok_or_else(|| invalid("WHERE clause must use id = value"))?;
    if column.trim() != "id" {
        return Err(invalid("DML WHERE clause must target id"));
    }
    Ok(unquote(value))
}

fn split_csv(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

fn unquote(value: &str) -> String {
    value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string()
}

fn apply_mutations(kv: &mut KvEngine, version: CommitVersion, mutations: &[Mutation]) {
    for mutation in mutations {
        match mutation {
            Mutation::Set { key, value } => {
                kv.apply_committed_set(key.clone(), value.clone(), version);
            }
            Mutation::Clear { key } => {
                kv.apply_committed_clear(key.clone(), version);
            }
        }
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use rsf_auth::{Capability, Grant, Principal, PrincipalKind, Resource};
    use rsf_catalog::{CatalogEngine, CatalogSurface};
    use rsf_core::{
        CommitConsistency, Epoch, NodeId,
        catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor},
    };
    use rsf_protocol::VsrBackedFabricRouter;
    use rsf_sessiontxn::SessionTransactionDecision;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn insert_runs_through_write_txn_isolation_and_kv() {
        let (catalog, auth, token, table, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();

        let (report, decisions) = SqlDmlPipeline
            .execute(
                &catalog,
                &auth,
                &token,
                &mut transactions,
                &mut isolation,
                &mut kv,
                &mut router,
                request(
                    "INSERT INTO users (id, name, active) VALUES (7, 'grace', true)",
                    index,
                ),
            )
            .unwrap();

        assert_eq!(report.commit_version, CommitVersion(1));
        assert_eq!(report.applied_mutations, 2);
        assert_eq!(transactions.committed().len(), 1);
        assert_eq!(isolation.committed().len(), 1);
        assert_eq!(kv.visible_key_count(), 2);
        assert_eq!(router.committed().len(), 1);
        assert_eq!(decisions.len(), 8);
        assert_eq!(report.write_plan.table, table.name);
        assert_eq!(report.commit_group.row_mutations, 1);
        assert_eq!(report.commit_group.index_mutations, 1);
        assert!(matches!(
            decisions[3],
            SqlDmlPipelineDecision::CommitGroupAccepted {
                row_mutations: 1,
                index_mutations: 1,
                intent_mutations: 2,
                ..
            }
        ));
    }

    #[test]
    fn dml_commits_through_vsr_backed_router_and_recovers_certificate() {
        let (catalog, auth, token, _table, index) = fixtures();
        let path = test_path("rsf-sqldml-vsr-router");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            let (report, decisions) = SqlDmlPipeline
                .execute(
                    &catalog,
                    &auth,
                    &token,
                    &mut transactions,
                    &mut isolation,
                    &mut kv,
                    &mut router,
                    request(
                        "INSERT INTO users (id, name, active) VALUES (9, 'lovelace', true)",
                        index,
                    ),
                )
                .unwrap();

            assert_eq!(report.commit_version, CommitVersion(1));
            assert_eq!(report.commit_evidence.version, CommitVersion(1));
            assert_eq!(report.commit_evidence.placement_group, PlacementGroupId(1));
            assert_eq!(
                report.commit_evidence.consistency,
                CommitConsistency::Strict
            );
            assert_eq!(report.applied_mutations, 2);
            assert_eq!(kv.visible_key_count(), 2);
            let SqlDmlPipelineDecision::TransactionCommitted {
                commit_version,
                evidence,
                ..
            } = &decisions[5]
            else {
                panic!("expected transaction commit decision");
            };
            assert_eq!(*commit_version, CommitVersion(1));
            assert_eq!(*evidence, report.commit_evidence);
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1), NodeId(0), voters)
                .unwrap()
                .recovered_certificates(PlacementGroupId(1))
                .expect("DML transaction commit certificate recovers");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].version, CommitVersion(1));
        assert_eq!(recovered[0].pg, PlacementGroupId(1));
    }

    #[test]
    fn duplicate_unique_key_rejects_before_transaction_commit() {
        let (catalog, auth, token, _table, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();
        let mut constraints = ConstraintState::default();
        constraints.add_unique_key("users_name", ["grace"]);
        let mut request = request(
            "INSERT INTO users (id, name, active) VALUES (8, 'grace', true)",
            index,
        );
        request.constraint_state = constraints;

        let err = SqlDmlPipeline
            .execute(
                &catalog,
                &auth,
                &token,
                &mut transactions,
                &mut isolation,
                &mut kv,
                &mut router,
                request,
            )
            .expect_err("duplicate unique value must reject");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(transactions.committed().len(), 0);
        assert_eq!(router.committed().len(), 0);
    }

    #[test]
    fn update_runs_through_same_transaction_path_and_replaces_index() {
        let (catalog, auth, token, _table, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();
        let mut request = request(
            "UPDATE users SET name = 'hopper', active = false WHERE id = 7",
            index,
        );
        request.rows = BTreeMap::from([("7".to_string(), row_values("7", "grace", true))]);
        request.transaction_id = TransactionId([0x56; 16]);

        let (report, decisions) = SqlDmlPipeline
            .execute(
                &catalog,
                &auth,
                &token,
                &mut transactions,
                &mut isolation,
                &mut kv,
                &mut router,
                request,
            )
            .unwrap();

        assert_eq!(report.commit_version, CommitVersion(1));
        assert_eq!(report.applied_mutations, 3);
        assert_eq!(transactions.committed().len(), 1);
        assert_eq!(isolation.committed().len(), 1);
        assert_eq!(kv.visible_key_count(), 2);
        assert!(matches!(
            decisions[2],
            SqlDmlPipelineDecision::WritePlanned {
                operation: DmlOperation::Update,
                ..
            }
        ));
    }

    #[test]
    fn delete_runs_through_same_transaction_path_and_clears_row() {
        let (catalog, auth, token, _table, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();
        let mut request = request("DELETE FROM users WHERE id = 7", index);
        request.rows = BTreeMap::from([("7".to_string(), row_values("7", "hopper", false))]);
        request.transaction_id = TransactionId([0x57; 16]);

        let (report, decisions) = SqlDmlPipeline
            .execute(
                &catalog,
                &auth,
                &token,
                &mut transactions,
                &mut isolation,
                &mut kv,
                &mut router,
                request,
            )
            .unwrap();

        assert_eq!(report.commit_version, CommitVersion(1));
        assert_eq!(report.applied_mutations, 2);
        assert_eq!(transactions.committed().len(), 1);
        assert_eq!(isolation.committed().len(), 1);
        assert_eq!(kv.visible_key_count(), 0);
        assert!(matches!(
            decisions[2],
            SqlDmlPipelineDecision::WritePlanned {
                operation: DmlOperation::Delete,
                ..
            }
        ));
    }

    #[test]
    fn dml_can_stage_into_session_transaction_overlay_before_commit() {
        let (catalog, auth, token, _table, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut session_transactions = SessionTransactionEngine::default();
        session_transactions
            .begin("session-sqldml", &kv, &router)
            .unwrap();

        let (report, decisions) = SqlDmlPipeline
            .stage_in_session_transaction(
                &catalog,
                &auth,
                &token,
                &mut session_transactions,
                &kv,
                request(
                    "INSERT INTO users (id, name, active) VALUES (7, 'grace', true)",
                    index,
                ),
            )
            .unwrap();

        assert_eq!(report.staged_mutations, 2);
        assert_eq!(report.overlay_verified, 2);
        assert_eq!(report.commit_group.intent_mutations, 2);
        assert_eq!(kv.visible_key_count(), 0);
        assert!(matches!(
            decisions[3],
            SqlDmlPipelineDecision::CommitGroupAccepted {
                row_mutations: 1,
                index_mutations: 1,
                ..
            }
        ));
        assert!(matches!(
            decisions[4],
            SqlDmlPipelineDecision::SessionStaged { mutations: 2, .. }
        ));
        let overlay = session_transactions
            .read("session-sqldml", &kv, b"table/users/pk/7".to_vec())
            .unwrap();
        assert!(matches!(
            overlay,
            SessionTransactionDecision::OverlayRead { value: Some(_), .. }
        ));

        let committed = session_transactions
            .commit("session-sqldml", &mut kv, &mut router, PlacementGroupId(1))
            .unwrap();
        assert!(matches!(
            committed,
            SessionTransactionDecision::Committed {
                version: CommitVersion(1),
                writes: 2,
                ..
            }
        ));
        assert_eq!(kv.visible_key_count(), 2);
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn insert_without_secondary_indexes_uses_valid_row_only_commit_group() {
        let (catalog, auth, token, _table, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut transactions = TransactionCoordinator::default();
        let mut isolation = IsolationTracker::default();
        let mut kv = KvEngine::default();
        let mut request = request(
            "INSERT INTO users (id, name, active) VALUES (17, 'date', true)",
            index,
        );
        request.constraints = vec![ConstraintRule::PrimaryKey {
            columns: vec!["id".to_string()],
        }];
        request.indexes = Vec::new();

        let (report, decisions) = SqlDmlPipeline
            .execute(
                &catalog,
                &auth,
                &token,
                &mut transactions,
                &mut isolation,
                &mut kv,
                &mut router,
                request,
            )
            .unwrap();

        assert_eq!(report.applied_mutations, 1);
        assert_eq!(report.commit_group.row_mutations, 1);
        assert_eq!(report.commit_group.index_mutations, 0);
        assert_eq!(report.commit_group.intent_mutations, 1);
        assert_eq!(kv.visible_key_count(), 1);
        assert!(matches!(
            decisions[3],
            SqlDmlPipelineDecision::CommitGroupAccepted {
                row_mutations: 1,
                index_mutations: 0,
                intent_mutations: 1,
                ..
            }
        ));
    }

    fn fixtures() -> (
        CatalogEngine,
        AuthEngine,
        AuthToken,
        TableDescriptor,
        IndexDescriptor,
    ) {
        let table = TableDescriptor {
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
        };
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut catalog = CatalogEngine::default();
        catalog
            .create_namespace(&mut router, PlacementGroupId(1), "app")
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
        catalog
            .create_table(&mut router, PlacementGroupId(1), table.clone())
            .unwrap();

        let mut auth = AuthEngine::default();
        let principal = PrincipalId("app-client".to_string());
        auth.register_principal(Principal {
            id: principal.clone(),
            kind: PrincipalKind::User,
        })
        .unwrap();
        auth.grant(Grant {
            principal: principal.clone(),
            capability: Capability::NamespaceWrite,
            resource: Resource::surface("app", "relational").unwrap(),
        })
        .unwrap();
        let token = auth
            .issue_token(&principal, CommitVersion(100), 100)
            .unwrap();
        let index = IndexDescriptor {
            table: table.name.clone(),
            name: "users_name".to_string(),
            columns: vec!["name".to_string()],
            unique: true,
        };
        (catalog, auth, token, table, index)
    }

    fn request(statement: &str, index: IndexDescriptor) -> SqlDmlPipelineRequest {
        SqlDmlPipelineRequest {
            context: SessionContext::new(
                "session-sqldml",
                PrincipalId("app-client".to_string()),
                "app",
            ),
            principal: PrincipalId("app-client".to_string()),
            statement: statement.to_string(),
            snapshot: CommitVersion(0),
            catalog_group: PlacementGroupId(1),
            now: CommitVersion(1),
            transaction_id: TransactionId([0x55; 16]),
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
            indexes: vec![index],
            rows: BTreeMap::new(),
        }
    }

    fn row_values(id: &str, name: &str, active: bool) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("id".to_string(), id.to_string()),
            ("name".to_string(), name.to_string()),
            ("active".to_string(), active.to_string()),
        ])
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock moves forward")
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{}-{unique}", std::process::id()))
    }
}
