#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_auth::{AuthEngine, AuthToken, PrincipalId};
use rsf_catalog::CatalogEngine;
use rsf_constraints::{ConstraintRule, ConstraintState};
use rsf_core::{
    Bytes, CommitEvidence, CommitVersion, ErrorCode, PlacementGroupId, RsfError, RsfResult,
    catalog::TableName, transaction::TransactionId,
};
use rsf_deferred::{
    CommittedReferences, DeferredConstraintDecision, DeferredConstraintEngine,
    DeferredReferenceRule, DeferredRow,
};
use rsf_index::IndexDescriptor;
use rsf_kv::KvEngine;
use rsf_protocol::FabricRouterPort;
use rsf_session::SessionContext;
use rsf_sessiontxn::{SessionTransactionDecision, SessionTransactionEngine};
use rsf_sqldml::{SqlDmlPipeline, SqlDmlPipelineDecision, SqlDmlPipelineRequest};

#[derive(Clone, Debug)]
pub struct SqlTransactionScriptRequest {
    pub context: SessionContext,
    pub principal: PrincipalId,
    pub catalog_group: PlacementGroupId,
    pub transaction_group: PlacementGroupId,
    pub now: CommitVersion,
    pub statements: Vec<String>,
    pub constraints: Vec<ConstraintRule>,
    pub constraint_state: ConstraintState,
    pub indexes: Vec<IndexDescriptor>,
    pub rows: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlTransactionScriptReport {
    pub session_id: String,
    pub statements: usize,
    pub began: bool,
    pub dml_staged: usize,
    pub overlay_reads: usize,
    pub snapshot_reads: usize,
    pub commits: usize,
    pub rollbacks: usize,
    pub rollback_absent: usize,
    pub savepoints: usize,
    pub savepoint_rewinds: usize,
    pub savepoint_absent: usize,
    pub deferred_obligations: usize,
    pub deferred_transaction_references: usize,
    pub deferred_commits: usize,
    pub deferred_rejections: usize,
    pub committed_version: Option<CommitVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlTransactionDecision {
    Began {
        session_id: String,
        snapshot: CommitVersion,
    },
    DmlStaged {
        statement: String,
        mutations: usize,
    },
    SavepointCreated {
        session_id: String,
        name: String,
        pending_writes: usize,
    },
    SavepointRolledBack {
        session_id: String,
        name: String,
        rewound_writes: usize,
        pending_writes: usize,
    },
    SavepointAbsent {
        name: String,
        key: Bytes,
    },
    DeferredObligationStaged {
        transaction: TransactionId,
        rule: String,
        key: Vec<String>,
    },
    DeferredReferenceStaged {
        transaction: TransactionId,
        reference: String,
        key: Vec<String>,
    },
    DeferredConstraintsAccepted {
        transaction: TransactionId,
        obligations: usize,
        transaction_references: usize,
        committed_references: usize,
    },
    DeferredConstraintsRejected {
        transaction: TransactionId,
        violations: usize,
    },
    OverlayRead {
        statement: String,
        key: Bytes,
        value: Option<Bytes>,
    },
    SnapshotRead {
        statement: String,
        key: Bytes,
        value: Option<Bytes>,
        version: Option<CommitVersion>,
    },
    Committed {
        session_id: String,
        version: CommitVersion,
        evidence: CommitEvidence,
        writes: usize,
    },
    RolledBack {
        session_id: String,
        discarded_writes: usize,
    },
    RollbackAbsent {
        key: Bytes,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct SqlTransactionScriptEngine {
    dml: SqlDmlPipeline,
}

impl SqlTransactionScriptEngine {
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep the existing borrowed execution ports explicit in the public script API"
    )]
    pub fn execute(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        session_transactions: &mut SessionTransactionEngine,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        request: SqlTransactionScriptRequest,
    ) -> RsfResult<(SqlTransactionScriptReport, Vec<SqlTransactionDecision>)> {
        let mut decisions = Vec::new();
        let mut began = false;
        let mut dml_staged = 0;
        let mut overlay_reads = 0;
        let mut snapshot_reads = 0;
        let mut commits = 0;
        let mut rollbacks = 0;
        let mut rollback_absent = 0;
        let mut savepoints = 0;
        let mut savepoint_rewinds = 0;
        let mut savepoint_absent = 0;
        let mut deferred_obligations = 0;
        let mut deferred_transaction_references = 0;
        let mut deferred_commits = 0;
        let deferred_rejections = 0;
        let mut committed_version = None;
        let mut next_transaction_id = 0u8;
        let mut rollback_probes = Vec::new();
        let mut transaction_rows = initial_transaction_rows(&request.rows);
        let mut savepoint_probe_offsets = BTreeMap::new();
        let mut row_savepoints = BTreeMap::new();
        let mut deferred = DeferredSqlTransaction::default();
        let deferred_constraints = deferred_constraints(&request.constraints);
        let immediate_constraints = immediate_constraints(&request.constraints);
        let session_id = request.context.session_id.clone();

        for statement in &request.statements {
            match SqlTransactionStatement::parse(statement)? {
                SqlTransactionStatement::Begin => {
                    if began {
                        return Err(invalid("SQL transaction already began"));
                    }
                    match session_transactions.begin(&session_id, kv, router)? {
                        SessionTransactionDecision::Began {
                            session_id,
                            snapshot,
                        } => {
                            began = true;
                            deferred.begin(transaction_id(0), committed_references(&request)?)?;
                            decisions.push(SqlTransactionDecision::Began {
                                session_id,
                                snapshot,
                            });
                        }
                        _ => return Err(invalid("session transaction begin returned wrong event")),
                    }
                }
                SqlTransactionStatement::Dml(dml_statement) => {
                    if !began || commits > 0 || rollbacks > 0 {
                        return Err(invalid("DML statement requires an active SQL transaction"));
                    }
                    if let Some(key) = inserted_row_key(&dml_statement)? {
                        rollback_probes.push(key);
                    }
                    apply_transaction_row_effect(
                        &mut transaction_rows,
                        &dml_statement,
                        &request.rows,
                    )?;
                    next_transaction_id = next_transaction_id.saturating_add(1);
                    let (_report, dml_decisions) = self.dml.stage_in_session_transaction(
                        catalog,
                        auth,
                        token,
                        session_transactions,
                        kv,
                        SqlDmlPipelineRequest {
                            context: request.context.clone(),
                            principal: request.principal.clone(),
                            statement: dml_statement.clone(),
                            snapshot: router.latest_version(),
                            catalog_group: request.catalog_group,
                            now: request.now,
                            transaction_id: transaction_id(next_transaction_id),
                            constraints: immediate_constraints.clone(),
                            constraint_state: request.constraint_state.clone(),
                            indexes: request.indexes.clone(),
                            rows: request.rows.clone(),
                        },
                    )?;
                    for decision in deferred.stage_statement(
                        &dml_statement,
                        &request.rows,
                        &deferred_constraints,
                    )? {
                        match decision {
                            SqlTransactionDecision::DeferredObligationStaged { .. } => {
                                deferred_obligations += 1;
                            }
                            SqlTransactionDecision::DeferredReferenceStaged { .. } => {
                                deferred_transaction_references += 1;
                            }
                            _ => {}
                        }
                        decisions.push(decision);
                    }
                    let mutations = dml_decisions
                        .iter()
                        .find_map(|decision| match decision {
                            SqlDmlPipelineDecision::SessionStaged { mutations, .. } => {
                                Some(*mutations)
                            }
                            _ => None,
                        })
                        .ok_or_else(|| invalid("SQL DML did not stage into session overlay"))?;
                    dml_staged += 1;
                    decisions.push(SqlTransactionDecision::DmlStaged {
                        statement: dml_statement,
                        mutations,
                    });
                }
                SqlTransactionStatement::Savepoint(name) => {
                    if !began || commits > 0 || rollbacks > 0 {
                        return Err(invalid("SAVEPOINT requires an active SQL transaction"));
                    }
                    match session_transactions.savepoint(&session_id, name.clone())? {
                        SessionTransactionDecision::SavepointCreated {
                            session_id,
                            name,
                            pending_writes,
                        } => {
                            savepoints += 1;
                            savepoint_probe_offsets.insert(name.clone(), rollback_probes.len());
                            row_savepoints.insert(name.clone(), transaction_rows.clone());
                            deferred.savepoint(name.clone())?;
                            decisions.push(SqlTransactionDecision::SavepointCreated {
                                session_id,
                                name,
                                pending_writes,
                            });
                        }
                        _ => {
                            return Err(invalid(
                                "session transaction savepoint returned wrong event",
                            ));
                        }
                    }
                }
                SqlTransactionStatement::RollbackToSavepoint(name) => {
                    if !began || commits > 0 || rollbacks > 0 {
                        return Err(invalid(
                            "ROLLBACK TO SAVEPOINT requires an active SQL transaction",
                        ));
                    }
                    let probe_start = *savepoint_probe_offsets
                        .get(&name)
                        .ok_or_else(|| invalid(format!("unknown SQL savepoint {name}")))?;
                    match session_transactions.rollback_to_savepoint(&session_id, &name)? {
                        SessionTransactionDecision::SavepointRolledBack {
                            session_id,
                            name,
                            rewound_writes,
                            pending_writes,
                        } => {
                            savepoint_rewinds += 1;
                            decisions.push(SqlTransactionDecision::SavepointRolledBack {
                                session_id: session_id.clone(),
                                name: name.clone(),
                                rewound_writes,
                                pending_writes,
                            });
                            for key in rollback_probes.iter().skip(probe_start) {
                                let read =
                                    session_transactions.read(&session_id, kv, key.clone())?;
                                match read {
                                    SessionTransactionDecision::SnapshotRead {
                                        value: None,
                                        ..
                                    } => {}
                                    SessionTransactionDecision::OverlayRead { .. }
                                    | SessionTransactionDecision::SnapshotRead {
                                        value: Some(_),
                                        ..
                                    } => {
                                        return Err(invalid(
                                            "SQL savepoint rewind left discarded INSERT visible",
                                        ));
                                    }
                                    _ => {
                                        return Err(invalid(
                                            "session transaction savepoint probe returned wrong event",
                                        ));
                                    }
                                }
                                if kv.get(key).is_some() {
                                    return Err(invalid(
                                        "SQL savepoint rewind made discarded INSERT globally visible",
                                    ));
                                }
                                savepoint_absent += 1;
                                decisions.push(SqlTransactionDecision::SavepointAbsent {
                                    name: name.clone(),
                                    key: key.clone(),
                                });
                            }
                            rollback_probes.truncate(probe_start);
                            transaction_rows =
                                row_savepoints.get(&name).cloned().ok_or_else(|| {
                                    invalid(format!("unknown SQL row savepoint {name}"))
                                })?;
                            deferred.rollback_to_savepoint(&name)?;
                        }
                        _ => {
                            return Err(invalid(
                                "session transaction rollback-to-savepoint returned wrong event",
                            ));
                        }
                    }
                }
                SqlTransactionStatement::Select(select) => {
                    if !began || commits > 0 || rollbacks > 0 {
                        return Err(invalid("SELECT requires an active SQL transaction"));
                    }
                    let keys = select_row_keys(&select, &transaction_rows)?;
                    for key in keys {
                        let read = session_transactions.read(&session_id, kv, key.clone())?;
                        match read {
                            SessionTransactionDecision::OverlayRead { key, value, .. } => {
                                overlay_reads += 1;
                                decisions.push(SqlTransactionDecision::OverlayRead {
                                    statement: statement.trim().trim_end_matches(';').to_string(),
                                    key,
                                    value,
                                });
                            }
                            SessionTransactionDecision::SnapshotRead {
                                key,
                                version,
                                value,
                                ..
                            } => {
                                snapshot_reads += 1;
                                decisions.push(SqlTransactionDecision::SnapshotRead {
                                    statement: statement.trim().trim_end_matches(';').to_string(),
                                    key,
                                    value,
                                    version,
                                });
                            }
                            _ => {
                                return Err(invalid(
                                    "session transaction read returned wrong event",
                                ));
                            }
                        }
                    }
                }
                SqlTransactionStatement::Commit => {
                    if !began || commits > 0 || rollbacks > 0 {
                        return Err(invalid("COMMIT requires one active SQL transaction"));
                    }
                    match deferred.commit()? {
                        DeferredConstraintDecision::CommitAccepted {
                            transaction,
                            obligations,
                            transaction_references,
                            committed_references,
                        } => {
                            deferred_commits += 1;
                            decisions.push(SqlTransactionDecision::DeferredConstraintsAccepted {
                                transaction,
                                obligations,
                                transaction_references,
                                committed_references,
                            });
                        }
                        DeferredConstraintDecision::CommitRejected {
                            transaction,
                            violations,
                        } => {
                            decisions.push(SqlTransactionDecision::DeferredConstraintsRejected {
                                transaction,
                                violations: violations.len(),
                            });
                            return Err(invalid(format!(
                                "deferred constraints rejected COMMIT with {} violation(s)",
                                violations.len()
                            )));
                        }
                        _ => {
                            return Err(invalid("deferred constraint commit returned wrong event"));
                        }
                    }
                    match session_transactions.commit(
                        &session_id,
                        kv,
                        router,
                        request.transaction_group,
                    )? {
                        SessionTransactionDecision::Committed {
                            session_id,
                            version,
                            evidence,
                            writes,
                        } => {
                            commits += 1;
                            committed_version = Some(version);
                            decisions.push(SqlTransactionDecision::Committed {
                                session_id,
                                version,
                                evidence,
                                writes,
                            });
                        }
                        _ => {
                            return Err(invalid("session transaction commit returned wrong event"));
                        }
                    }
                }
                SqlTransactionStatement::Rollback => {
                    if !began || commits > 0 || rollbacks > 0 {
                        return Err(invalid("ROLLBACK requires one active SQL transaction"));
                    }
                    match session_transactions.rollback(&session_id)? {
                        SessionTransactionDecision::RolledBack {
                            session_id,
                            discarded_writes,
                        } => {
                            rollbacks += 1;
                            deferred.rollback();
                            decisions.push(SqlTransactionDecision::RolledBack {
                                session_id,
                                discarded_writes,
                            });
                            for key in &rollback_probes {
                                if kv.get(key).is_some() {
                                    return Err(invalid(
                                        "rolled-back SQL INSERT became globally visible",
                                    ));
                                }
                                rollback_absent += 1;
                                decisions.push(SqlTransactionDecision::RollbackAbsent {
                                    key: key.clone(),
                                });
                            }
                        }
                        _ => {
                            return Err(invalid(
                                "session transaction rollback returned wrong event",
                            ));
                        }
                    }
                }
            }
        }

        if began && commits == 0 && rollbacks == 0 {
            return Err(invalid(
                "SQL transaction script ended before COMMIT or ROLLBACK",
            ));
        }

        Ok((
            SqlTransactionScriptReport {
                session_id,
                statements: request.statements.len(),
                began,
                dml_staged,
                overlay_reads,
                snapshot_reads,
                commits,
                rollbacks,
                rollback_absent,
                savepoints,
                savepoint_rewinds,
                savepoint_absent,
                deferred_obligations,
                deferred_transaction_references,
                deferred_commits,
                deferred_rejections,
                committed_version,
            },
            decisions,
        ))
    }

    pub fn reject(
        &self,
        catalog: &CatalogEngine,
        auth: &AuthEngine,
        token: &AuthToken,
        kv: &mut KvEngine,
        router: &mut impl FabricRouterPort,
        request: SqlTransactionScriptRequest,
    ) -> SqlTransactionDecision {
        let mut session_transactions = SessionTransactionEngine::default();
        match self.execute(
            catalog,
            auth,
            token,
            &mut session_transactions,
            kv,
            router,
            request,
        ) {
            Ok(_) => SqlTransactionDecision::Rejected {
                reason: "expected SQL transaction script to be rejected".to_string(),
            },
            Err(err) => SqlTransactionDecision::Rejected {
                reason: err.message,
            },
        }
    }
}

#[derive(Clone, Debug, Default)]
struct DeferredSqlTransaction {
    transaction: Option<TransactionId>,
    engine: DeferredConstraintEngine,
    committed: CommittedReferences,
    savepoints: BTreeMap<String, DeferredSqlSavepoint>,
}

#[derive(Clone, Debug)]
struct DeferredSqlSavepoint {
    engine: DeferredConstraintEngine,
    committed: CommittedReferences,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DmlEffect {
    table: String,
    old: Option<BTreeMap<String, String>>,
    new: Option<BTreeMap<String, String>>,
}

impl DeferredSqlTransaction {
    fn begin(
        &mut self,
        transaction: TransactionId,
        committed: CommittedReferences,
    ) -> RsfResult<()> {
        self.engine.begin(transaction.clone())?;
        self.transaction = Some(transaction);
        self.committed = committed;
        self.savepoints.clear();
        Ok(())
    }

    fn stage_statement(
        &mut self,
        statement: &str,
        rows: &BTreeMap<String, BTreeMap<String, String>>,
        constraints: &[ConstraintRule],
    ) -> RsfResult<Vec<SqlTransactionDecision>> {
        if constraints.is_empty() {
            return Ok(Vec::new());
        }
        let transaction = self
            .transaction
            .clone()
            .ok_or_else(|| invalid("deferred constraint transaction has not begun"))?;
        let effect = dml_effect(statement, rows)?;
        let rules = deferred_rules_for_table(&effect.table, constraints);
        let mut decisions = Vec::new();

        for rule in rules {
            if effect.table == rule.referenced_table.0 {
                if let Some(old) = &effect.old {
                    self.committed.remove(
                        &rule.referenced_table,
                        &rule.referenced_columns,
                        key_values(&rule.referenced_columns, old)?,
                    )?;
                }
                if let Some(new) = &effect.new {
                    let decision = self.engine.stage_referenced_key(
                        &transaction,
                        &rule.referenced_table,
                        &rule.referenced_columns,
                        key_values(&rule.referenced_columns, new)?,
                    )?;
                    if let DeferredConstraintDecision::ReferencedKeyStaged {
                        transaction,
                        reference,
                        key,
                    } = decision
                    {
                        decisions.push(SqlTransactionDecision::DeferredReferenceStaged {
                            transaction,
                            reference,
                            key,
                        });
                    }
                }
            }

            if effect.table == rule.table.0
                && let Some(new) = &effect.new
                && has_columns(new, &rule.columns)
            {
                let row = DeferredRow {
                    table: rule.table.clone(),
                    values: new.clone(),
                };
                let decision = self
                    .engine
                    .defer_foreign_key(&transaction, rule.clone(), &row)?;
                if let DeferredConstraintDecision::ObligationStaged {
                    transaction,
                    rule,
                    key,
                } = decision
                {
                    decisions.push(SqlTransactionDecision::DeferredObligationStaged {
                        transaction,
                        rule,
                        key,
                    });
                }
            }
        }

        Ok(decisions)
    }

    fn savepoint(&mut self, name: String) -> RsfResult<()> {
        if self.transaction.is_none() {
            return Err(invalid("deferred constraint transaction has not begun"));
        }
        self.savepoints.insert(
            name,
            DeferredSqlSavepoint {
                engine: self.engine.clone(),
                committed: self.committed.clone(),
            },
        );
        Ok(())
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> RsfResult<()> {
        let Some(savepoint) = self.savepoints.get(name).cloned() else {
            return Err(invalid(format!(
                "deferred constraint transaction has no savepoint {name}"
            )));
        };
        self.engine = savepoint.engine;
        self.committed = savepoint.committed;
        Ok(())
    }

    fn commit(&mut self) -> RsfResult<DeferredConstraintDecision> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| invalid("deferred constraint transaction has not begun"))?;
        self.engine.commit(&transaction, &self.committed)
    }

    fn rollback(&mut self) {
        self.transaction = None;
        self.engine = DeferredConstraintEngine::default();
        self.committed = CommittedReferences::default();
        self.savepoints.clear();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SqlTransactionStatement {
    Begin,
    Dml(String),
    Savepoint(String),
    RollbackToSavepoint(String),
    Select(TransactionalSelect),
    Commit,
    Rollback,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TransactionalSelect {
    table: String,
    predicate_column: String,
    predicate_value: String,
}

impl SqlTransactionStatement {
    fn parse(statement: &str) -> RsfResult<Self> {
        let trimmed = statement.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        if upper == "BEGIN" || upper == "BEGIN TRANSACTION" {
            return Ok(Self::Begin);
        }
        if upper == "COMMIT" {
            return Ok(Self::Commit);
        }
        if upper == "ROLLBACK" {
            return Ok(Self::Rollback);
        }
        if upper.starts_with("SAVEPOINT ") {
            let name = trimmed["SAVEPOINT ".len()..].trim();
            if name.is_empty() {
                return Err(invalid("SAVEPOINT requires a name"));
            }
            return Ok(Self::Savepoint(name.to_string()));
        }
        if upper.starts_with("ROLLBACK TO SAVEPOINT ") {
            let name = trimmed["ROLLBACK TO SAVEPOINT ".len()..].trim();
            if name.is_empty() {
                return Err(invalid("ROLLBACK TO SAVEPOINT requires a name"));
            }
            return Ok(Self::RollbackToSavepoint(name.to_string()));
        }
        if upper.starts_with("ROLLBACK TO ") {
            let name = trimmed["ROLLBACK TO ".len()..].trim();
            if name.is_empty() {
                return Err(invalid("ROLLBACK TO requires a savepoint name"));
            }
            return Ok(Self::RollbackToSavepoint(name.to_string()));
        }
        if upper.starts_with("INSERT INTO ")
            || upper.starts_with("UPDATE ")
            || upper.starts_with("DELETE FROM ")
        {
            return Ok(Self::Dml(trimmed.to_string()));
        }
        if upper.starts_with("SELECT ") {
            return parse_select(trimmed).map(Self::Select);
        }
        Err(invalid("SQL transaction script statement is unsupported"))
    }
}

fn parse_select(statement: &str) -> RsfResult<TransactionalSelect> {
    let upper = statement.to_ascii_uppercase();
    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| invalid("SELECT must include FROM"))?;
    let where_pos = upper
        .find(" WHERE ")
        .ok_or_else(|| invalid("transactional SELECT must include an equality predicate"))?;
    if where_pos <= from_pos {
        return Err(invalid("SELECT WHERE must follow FROM"));
    }
    let table = statement[from_pos + " FROM ".len()..where_pos]
        .trim()
        .to_string();
    let where_expr = statement[where_pos + " WHERE ".len()..].trim();
    let (column, value) = where_expr
        .split_once('=')
        .ok_or_else(|| invalid("transactional SELECT WHERE must use column = value"))?;
    let predicate_column = column.trim().trim_matches('"').to_string();
    if predicate_column.is_empty() {
        return Err(invalid(
            "transactional SELECT predicate column must not be empty",
        ));
    }
    let predicate_value = value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();
    Ok(TransactionalSelect {
        table,
        predicate_column,
        predicate_value,
    })
}

fn row_key(table: &str, primary_key: &str) -> Bytes {
    rsf_core::relational_key::RelationalKeyFormat::default().row_key(table, primary_key)
}

fn initial_transaction_rows(
    rows: &BTreeMap<String, BTreeMap<String, String>>,
) -> BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>> {
    let mut by_table = BTreeMap::new();
    by_table.insert("users".to_string(), rows.clone());
    by_table
}

fn select_row_keys(
    select: &TransactionalSelect,
    rows: &BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
) -> RsfResult<Vec<Bytes>> {
    let table_rows = rows.get(&select.table).ok_or_else(|| {
        invalid(format!(
            "transactional SELECT has no row images for {}",
            select.table
        ))
    })?;
    if select.predicate_column == "id" {
        return Ok(vec![row_key(&select.table, &select.predicate_value)]);
    }
    let keys = table_rows
        .iter()
        .filter_map(|(primary_key, values)| {
            values
                .get(&select.predicate_column)
                .filter(|value| *value == &select.predicate_value)
                .map(|_| row_key(&select.table, primary_key))
        })
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Err(invalid(format!(
            "transactional SELECT found no {} row where {} = {}",
            select.table, select.predicate_column, select.predicate_value
        )));
    }
    Ok(keys)
}

fn apply_transaction_row_effect(
    rows: &mut BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
    statement: &str,
    base_rows: &BTreeMap<String, BTreeMap<String, String>>,
) -> RsfResult<()> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("INSERT INTO ") {
        let insert = parse_insert_row(trimmed)?;
        rows.entry(insert.table)
            .or_default()
            .insert(insert.primary_key, insert.values);
        return Ok(());
    }
    if upper.starts_with("UPDATE ") {
        let update = parse_update_row(trimmed)?;
        let table_rows = rows.entry(update.table.clone()).or_default();
        let mut values = table_rows
            .get(&update.primary_key)
            .cloned()
            .or_else(|| base_rows.get(&update.primary_key).cloned())
            .ok_or_else(|| invalid("UPDATE target row is not present"))?;
        for (column, value) in update.assignments {
            values.insert(column, value);
        }
        table_rows.insert(update.primary_key, values);
        return Ok(());
    }
    if upper.starts_with("DELETE FROM ") {
        let delete = parse_delete_row(trimmed)?;
        if let Some(table_rows) = rows.get_mut(&delete.table) {
            table_rows.remove(&delete.primary_key);
        }
        return Ok(());
    }
    Err(invalid("transactional row effect only supports DML"))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InsertRowEffect {
    table: String,
    primary_key: String,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UpdateRowEffect {
    table: String,
    primary_key: String,
    assignments: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DeleteRowEffect {
    table: String,
    primary_key: String,
}

fn parse_insert_row(statement: &str) -> RsfResult<InsertRowEffect> {
    let after_into = &statement["INSERT INTO ".len()..];
    let open_columns = after_into
        .find('(')
        .ok_or_else(|| invalid("INSERT must include a column list"))?;
    let table = after_into[..open_columns].trim().to_string();
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
        .map(|value| {
            value
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string()
        })
        .collect::<Vec<_>>();
    if columns.len() != values.len() {
        return Err(invalid("INSERT column/value count mismatch"));
    }
    let values = columns.into_iter().zip(values).collect::<BTreeMap<_, _>>();
    let primary_key = values
        .get("id")
        .cloned()
        .ok_or_else(|| invalid("INSERT must include id primary key"))?;
    Ok(InsertRowEffect {
        table,
        primary_key,
        values,
    })
}

fn parse_update_row(statement: &str) -> RsfResult<UpdateRowEffect> {
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
    Ok(UpdateRowEffect {
        table,
        primary_key,
        assignments,
    })
}

fn parse_delete_row(statement: &str) -> RsfResult<DeleteRowEffect> {
    let after_delete = &statement["DELETE FROM ".len()..];
    let upper_after = after_delete.to_ascii_uppercase();
    let where_pos = upper_after
        .find(" WHERE ")
        .ok_or_else(|| invalid("DELETE must include WHERE id = ..."))?;
    let table = after_delete[..where_pos].trim().to_string();
    let primary_key = parse_where_id(&after_delete[where_pos + " WHERE ".len()..])?;
    Ok(DeleteRowEffect { table, primary_key })
}

fn inserted_row_key(statement: &str) -> RsfResult<Option<Bytes>> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("INSERT INTO ") {
        return Ok(None);
    }
    let after_into = &trimmed["INSERT INTO ".len()..];
    let open_columns = after_into
        .find('(')
        .ok_or_else(|| invalid("INSERT must include a column list"))?;
    let table = after_into[..open_columns].trim();
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
        .map(|value| {
            value
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string()
        })
        .collect::<Vec<_>>();
    if columns.len() != values.len() {
        return Err(invalid("INSERT column/value count mismatch"));
    }
    let id = columns
        .into_iter()
        .zip(values)
        .find_map(|(column, value)| (column == "id").then_some(value))
        .ok_or_else(|| invalid("INSERT must include id primary key"))?;
    Ok(Some(row_key(table, &id)))
}

fn deferred_constraints(constraints: &[ConstraintRule]) -> Vec<ConstraintRule> {
    constraints
        .iter()
        .filter(|rule| matches!(rule, ConstraintRule::ForeignKey { .. }))
        .cloned()
        .collect()
}

fn deferred_rules_for_table(
    table: &str,
    constraints: &[ConstraintRule],
) -> Vec<DeferredReferenceRule> {
    constraints
        .iter()
        .filter_map(|rule| match rule {
            ConstraintRule::ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_columns,
            } => Some(DeferredReferenceRule {
                name: name.clone(),
                table: TableName(table.to_string()),
                columns: columns.clone(),
                referenced_table: referenced_table.clone(),
                referenced_columns: referenced_columns.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn immediate_constraints(constraints: &[ConstraintRule]) -> Vec<ConstraintRule> {
    constraints
        .iter()
        .filter(|rule| !matches!(rule, ConstraintRule::ForeignKey { .. }))
        .cloned()
        .collect()
}

fn committed_references(request: &SqlTransactionScriptRequest) -> RsfResult<CommittedReferences> {
    let mut committed = CommittedReferences::default();
    for (reference, keys) in request.constraint_state.referenced_key_entries() {
        for key in keys {
            committed.add_reference(reference.clone(), key)?;
        }
    }
    Ok(committed)
}

fn dml_effect(
    statement: &str,
    rows: &BTreeMap<String, BTreeMap<String, String>>,
) -> RsfResult<DmlEffect> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("INSERT INTO ") {
        let (table, values) = parse_insert_values(trimmed)?;
        return Ok(DmlEffect {
            table,
            old: None,
            new: Some(values),
        });
    }
    if upper.starts_with("UPDATE ") {
        let (table, primary_key, assignments) = parse_update_values(trimmed)?;
        let old = rows
            .get(&primary_key)
            .cloned()
            .ok_or_else(|| invalid("UPDATE target row is not present"))?;
        let mut new = old.clone();
        for (column, value) in assignments {
            new.insert(column, value);
        }
        return Ok(DmlEffect {
            table,
            old: Some(old),
            new: Some(new),
        });
    }
    if upper.starts_with("DELETE FROM ") {
        let (table, primary_key) = parse_delete_values(trimmed)?;
        let old = rows
            .get(&primary_key)
            .cloned()
            .ok_or_else(|| invalid("DELETE target row is not present"))?;
        return Ok(DmlEffect {
            table,
            old: Some(old),
            new: None,
        });
    }
    Err(invalid(
        "deferred constraint tracking supports INSERT, UPDATE, and DELETE",
    ))
}

fn parse_insert_values(statement: &str) -> RsfResult<(String, BTreeMap<String, String>)> {
    let after_into = &statement["INSERT INTO ".len()..];
    let open_columns = after_into
        .find('(')
        .ok_or_else(|| invalid("INSERT must include a column list"))?;
    let table = after_into[..open_columns].trim().to_string();
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
    Ok((table, columns.into_iter().zip(values).collect()))
}

fn parse_update_values(statement: &str) -> RsfResult<(String, String, BTreeMap<String, String>)> {
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
    Ok((table, primary_key, assignments))
}

fn parse_delete_values(statement: &str) -> RsfResult<(String, String)> {
    let after_delete = &statement["DELETE FROM ".len()..];
    let upper_after = after_delete.to_ascii_uppercase();
    let where_pos = upper_after
        .find(" WHERE ")
        .ok_or_else(|| invalid("DELETE must include WHERE id = ..."))?;
    let table = after_delete[..where_pos].trim().to_string();
    let primary_key = parse_where_id(&after_delete[where_pos + " WHERE ".len()..])?;
    Ok((table, primary_key))
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

fn key_values(columns: &[String], values: &BTreeMap<String, String>) -> RsfResult<Vec<String>> {
    columns
        .iter()
        .map(|column| {
            values
                .get(column)
                .cloned()
                .ok_or_else(|| invalid(format!("deferred key column {column} is missing")))
        })
        .collect()
}

fn has_columns(values: &BTreeMap<String, String>, columns: &[String]) -> bool {
    columns.iter().all(|column| values.contains_key(column))
}

fn unquote(value: &str) -> String {
    value
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string()
}

fn split_csv(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

fn transaction_id(sequence: u8) -> TransactionId {
    TransactionId([sequence; 16])
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_auth::{Capability, Grant, Principal, PrincipalKind, Resource};
    use rsf_catalog::CatalogSurface;
    use rsf_core::{
        Epoch, NodeId,
        catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName},
    };
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn script_stages_reads_overlay_and_commits() {
        let (catalog, auth, token, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let (report, decisions) = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                request(
                    vec![
                        "BEGIN",
                        "INSERT INTO users (id, name, active) VALUES (11, 'hopper', true)",
                        "SELECT * FROM users WHERE id = 11",
                        "COMMIT",
                    ],
                    index,
                ),
            )
            .unwrap();

        assert_eq!(report.statements, 4);
        assert!(report.began);
        assert_eq!(report.dml_staged, 1);
        assert_eq!(report.overlay_reads, 1);
        assert_eq!(report.commits, 1);
        assert_eq!(kv.visible_key_count(), 2);
        assert_eq!(sessions.report().pending_writes, 2);
        assert_eq!(sessions.report().overlay_reads, 3);
        assert!(matches!(
            decisions[2],
            SqlTransactionDecision::OverlayRead { ref value, .. } if value.is_some()
        ));
    }

    #[test]
    fn transactional_select_reads_staged_rows_by_non_primary_key() {
        let (catalog, auth, token, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let (report, decisions) = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                request(
                    vec![
                        "BEGIN",
                        "INSERT INTO users (id, name, active) VALUES (21, 'curie', true)",
                        "SELECT * FROM users WHERE name = 'curie'",
                        "COMMIT",
                    ],
                    index,
                ),
            )
            .unwrap();

        assert_eq!(report.overlay_reads, 1);
        assert_eq!(report.snapshot_reads, 0);
        assert_eq!(report.commits, 1);
        assert!(decisions.iter().any(|decision| matches!(
            decision,
            SqlTransactionDecision::OverlayRead { key, value: Some(_), .. }
                if key == &row_key("users", "21")
        )));
    }

    #[test]
    fn transactional_select_reads_snapshot_rows_by_non_primary_key() {
        let (catalog, auth, token, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        kv.apply_committed_set(
            row_key("users", "22"),
            b"id=22,name=franklin,active=true".to_vec(),
            CommitVersion(0),
        );
        let mut sessions = SessionTransactionEngine::default();
        let mut request = request(
            vec![
                "BEGIN",
                "SELECT * FROM users WHERE name = 'franklin'",
                "ROLLBACK",
            ],
            index,
        );
        request.rows = BTreeMap::from([("22".to_string(), row_values("22", "franklin", true))]);

        let (report, decisions) = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                request,
            )
            .unwrap();

        assert_eq!(report.overlay_reads, 0);
        assert_eq!(report.snapshot_reads, 1);
        assert_eq!(report.rollbacks, 1);
        assert!(decisions.iter().any(|decision| matches!(
            decision,
            SqlTransactionDecision::SnapshotRead {
                key,
                value: Some(_),
                version: Some(CommitVersion(0)),
                ..
            } if key == &row_key("users", "22")
        )));
    }

    #[test]
    fn script_commits_through_vsr_backed_router_and_recovers_certificate() {
        let (catalog, auth, token, index) = fixtures();
        let path = test_path("rsf-sqltxn-vsr-router");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .unwrap();
            let (report, decisions) = SqlTransactionScriptEngine::default()
                .execute(
                    &catalog,
                    &auth,
                    &token,
                    &mut sessions,
                    &mut kv,
                    &mut router,
                    request(
                        vec![
                            "BEGIN",
                            "INSERT INTO users (id, name, active) VALUES (15, 'codd', true)",
                            "SELECT * FROM users WHERE id = 15",
                            "COMMIT",
                        ],
                        index,
                    ),
                )
                .unwrap();

            assert_eq!(report.commits, 1);
            assert_eq!(report.committed_version, Some(CommitVersion(1)));
            assert_eq!(kv.visible_key_count(), 2);
            assert!(matches!(
                decisions.last(),
                Some(SqlTransactionDecision::Committed {
                    version: CommitVersion(1),
                    writes: 2,
                    ..
                })
            ));
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1), NodeId(0), voters)
                .unwrap()
                .recovered_certificates(PlacementGroupId(1))
                .expect("SQL transaction script commit certificate recovers");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].version, CommitVersion(1));
    }

    #[test]
    fn dml_without_begin_is_rejected() {
        let (catalog, auth, token, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let err = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                request(
                    vec!["INSERT INTO users (id, name, active) VALUES (11, 'hopper', true)"],
                    index,
                ),
            )
            .expect_err("DML without BEGIN should fail");

        assert!(err.message.contains("requires an active SQL transaction"));
    }

    #[test]
    fn rollback_discards_staged_sql_writes() {
        let (catalog, auth, token, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let (report, decisions) = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                request(
                    vec![
                        "BEGIN",
                        "INSERT INTO users (id, name, active) VALUES (12, 'lamport', true)",
                        "SELECT * FROM users WHERE id = 12",
                        "ROLLBACK",
                    ],
                    index,
                ),
            )
            .unwrap();

        assert_eq!(report.dml_staged, 1);
        assert_eq!(report.overlay_reads, 1);
        assert_eq!(report.commits, 0);
        assert_eq!(report.rollbacks, 1);
        assert_eq!(report.rollback_absent, 1);
        assert_eq!(kv.visible_key_count(), 0);
        assert!(matches!(
            decisions.last(),
            Some(SqlTransactionDecision::RollbackAbsent { .. })
        ));
        assert_eq!(sessions.report().rollbacks, 1);
    }

    #[test]
    fn savepoint_rewind_keeps_earlier_sql_writes_and_discards_later_ones() {
        let (catalog, auth, token, index) = fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let (report, decisions) = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                request(
                    vec![
                        "BEGIN",
                        "INSERT INTO users (id, name, active) VALUES (13, 'stonebraker', true)",
                        "SAVEPOINT after_first",
                        "INSERT INTO users (id, name, active) VALUES (14, 'gray', true)",
                        "SELECT * FROM users WHERE id = 14",
                        "ROLLBACK TO SAVEPOINT after_first",
                        "COMMIT",
                    ],
                    index,
                ),
            )
            .unwrap();

        assert_eq!(report.dml_staged, 2);
        assert_eq!(report.overlay_reads, 1);
        assert_eq!(report.savepoints, 1);
        assert_eq!(report.savepoint_rewinds, 1);
        assert_eq!(report.savepoint_absent, 1);
        assert_eq!(report.commits, 1);
        assert_eq!(kv.visible_key_count(), 2);
        assert!(kv.get(&row_key("users", "13")).is_some());
        assert!(kv.get(&row_key("users", "14")).is_none());
        assert!(decisions.iter().any(|decision| matches!(
            decision,
            SqlTransactionDecision::SavepointAbsent { key, .. }
                if key == &row_key("users", "14")
        )));
        assert_eq!(sessions.report().savepoints, 1);
        assert_eq!(sessions.report().savepoint_rewinds, 1);
    }

    #[test]
    fn deferred_foreign_key_accepts_transaction_local_parent_at_commit() {
        let (catalog, auth, token) = deferred_fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let (report, decisions) = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                deferred_request(vec![
                    "BEGIN",
                    "INSERT INTO orders (id, user_id, total) VALUES (50, 42, 19)",
                    "INSERT INTO users (id, name) VALUES (42, 'ada')",
                    "COMMIT",
                ]),
            )
            .unwrap();

        assert_eq!(report.dml_staged, 2);
        assert_eq!(report.deferred_obligations, 1);
        assert_eq!(report.deferred_transaction_references, 1);
        assert_eq!(report.deferred_commits, 1);
        assert_eq!(report.commits, 1);
        assert!(kv.get(&row_key("orders", "50")).is_some());
        assert!(kv.get(&row_key("users", "42")).is_some());
        assert!(decisions.iter().any(|decision| matches!(
            decision,
            SqlTransactionDecision::DeferredConstraintsAccepted {
                obligations: 1,
                transaction_references: 1,
                ..
            }
        )));
    }

    #[test]
    fn deferred_foreign_key_rejects_missing_parent_before_session_commit() {
        let (catalog, auth, token) = deferred_fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let err = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                deferred_request(vec![
                    "BEGIN",
                    "INSERT INTO orders (id, user_id, total) VALUES (51, 404, 21)",
                    "COMMIT",
                ]),
            )
            .expect_err("missing deferred parent should reject COMMIT");

        assert!(
            err.message
                .contains("deferred constraints rejected COMMIT with 1 violation")
        );
        assert!(kv.get(&row_key("orders", "51")).is_none());
        assert_eq!(sessions.report().commits, 0);
    }

    #[test]
    fn deferred_foreign_key_savepoint_rewind_discards_child_obligation() {
        let (catalog, auth, token) = deferred_fixtures();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut kv = KvEngine::default();
        let mut sessions = SessionTransactionEngine::default();

        let (report, _decisions) = SqlTransactionScriptEngine::default()
            .execute(
                &catalog,
                &auth,
                &token,
                &mut sessions,
                &mut kv,
                &mut router,
                deferred_request(vec![
                    "BEGIN",
                    "INSERT INTO users (id, name) VALUES (77, 'stonebraker')",
                    "SAVEPOINT before_child",
                    "INSERT INTO orders (id, user_id, total) VALUES (52, 999, 33)",
                    "ROLLBACK TO SAVEPOINT before_child",
                    "COMMIT",
                ]),
            )
            .unwrap();

        assert_eq!(report.dml_staged, 2);
        assert_eq!(report.savepoint_rewinds, 1);
        assert_eq!(report.deferred_obligations, 1);
        assert_eq!(report.deferred_commits, 1);
        assert_eq!(report.commits, 1);
        assert!(kv.get(&row_key("orders", "52")).is_none());
        assert!(kv.get(&row_key("users", "77")).is_some());
    }

    fn fixtures() -> (CatalogEngine, AuthEngine, AuthToken, IndexDescriptor) {
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
            table: table.name,
            name: "users_name".to_string(),
            columns: vec!["name".to_string()],
            unique: true,
        };
        (catalog, auth, token, index)
    }

    fn deferred_fixtures() -> (CatalogEngine, AuthEngine, AuthToken) {
        let users = TableDescriptor {
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
        };
        let orders = TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("orders".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "id".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "user_id".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "total".to_string(),
                    ty: ColumnType::Int64,
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
            .create_table(&mut router, PlacementGroupId(1), users)
            .unwrap();
        catalog
            .create_table(&mut router, PlacementGroupId(1), orders)
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
        (catalog, auth, token)
    }

    fn request(statements: Vec<&str>, index: IndexDescriptor) -> SqlTransactionScriptRequest {
        SqlTransactionScriptRequest {
            context: SessionContext::new(
                "sql-transaction-script",
                PrincipalId("app-client".to_string()),
                "app",
            ),
            principal: PrincipalId("app-client".to_string()),
            catalog_group: PlacementGroupId(1),
            transaction_group: PlacementGroupId(1),
            now: CommitVersion(1),
            statements: statements.into_iter().map(str::to_string).collect(),
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

    fn deferred_request(statements: Vec<&str>) -> SqlTransactionScriptRequest {
        SqlTransactionScriptRequest {
            context: SessionContext::new(
                "sql-transaction-script",
                PrincipalId("app-client".to_string()),
                "app",
            ),
            principal: PrincipalId("app-client".to_string()),
            catalog_group: PlacementGroupId(1),
            transaction_group: PlacementGroupId(1),
            now: CommitVersion(1),
            statements: statements.into_iter().map(str::to_string).collect(),
            constraints: vec![ConstraintRule::ForeignKey {
                name: "orders_user".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: TableName("users".to_string()),
                referenced_columns: vec!["id".to_string()],
            }],
            constraint_state: ConstraintState::default(),
            indexes: Vec::new(),
            rows: BTreeMap::new(),
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock moves forward")
            .as_nanos();
        std::env::temp_dir().join(format!("{name}-{}-{unique}", std::process::id()))
    }
}
