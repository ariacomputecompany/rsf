//! Durable dependencies outlive savepoint rewinds and coordinator processes.
use super::*;
use rsf_query::{Projection, QueryExecutor, QueryPlan, QueryPlanner, QueryRow};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(in crate::durable) enum Dependency {
    Row { table: String, key: String },
    Query { statement: String },
}
impl Dependency {
    pub fn validate(&self) -> RsfResult<()> {
        match self {
            Self::Row { table, key } => {
                validate_name(table, "dependency table", MAX_SQL_SESSION_BYTES)?;
                if key.len() > MAX_SQL_SESSION_BYTES {
                    return Err(invalid("SQL dependency key exceeds limit"));
                }
            }
            Self::Query { statement } => {
                if statement.len() > MAX_SQL_SESSION_BYTES
                    || !matches!(parse_statement(statement)?, ParsedStatement::Select { .. })
                {
                    return Err(corrupt("SQL read dependency is not a bounded SELECT"));
                }
            }
        }
        Ok(())
    }
}

pub(super) fn read_request(
    sql: &DurableSqlEngine,
    mut request: SessionRequest,
) -> RsfResult<SessionRequest> {
    let state = super::plan::active(sql, &request)?;
    // A SELECT identity binds its data view, not accumulated dependency metadata
    // or savepoints. Identical reads of an unchanged overlay reuse one receipt.
    let identity = encode(&(
        sql.namespace.as_deref(),
        &request,
        &state.statements,
        state.snapshot,
        state.schema,
        state.pg,
    ))?;
    request.client = format!(
        "sql-session-read:{}",
        hex_bytes(Digest::blake3_bytes(&identity).as_bytes())
    );
    Ok(request)
}

enum Check {
    Row(Vec<u8>),
    Query { table: String, plan: QueryPlan },
}

pub(in crate::durable) fn validate(
    sql: &DurableSqlEngine,
    state: &SessionState,
    upper: Option<CommitVersion>,
) -> RsfResult<()> {
    let changes = sql.engine.kv.changes_since(CommitVersion(state.snapshot));
    let changes = changes
        .iter()
        .filter(|change| upper.is_none_or(|upper| change.version < upper));
    let Some(dependencies) = &state.dependencies else {
        if changes.count() != 0 {
            return Err(conflict());
        }
        return Ok(());
    };
    let mut checks = Vec::new();
    for dependency in dependencies {
        match dependency {
            Dependency::Row { table, key } => {
                checks.push(Check::Row(crate::logical_row_key(table, key)))
            }
            Dependency::Query { statement } => {
                let ParsedStatement::Select {
                    table, predicate, ..
                } = parse_statement(statement)?
                else {
                    return Err(corrupt("SQL dependency is not SELECT"));
                };
                // A table created only inside the overlay has no external rows.
                // Concurrent catalog changes are checked before this validator.
                if let Ok(record) = sql.engine.table_record(&table) {
                    let plan = QueryPlanner.select(
                        &record.descriptor,
                        Projection::All,
                        crate::type_select_predicate(record, predicate)?,
                        None,
                    )?;
                    checks.push(Check::Query { table, plan });
                }
            }
        }
    }
    for change in changes {
        for check in &checks {
            match check {
                Check::Row(key) if *key == change.key => return Err(conflict()),
                Check::Query { table, plan }
                    if change
                        .key
                        .starts_with(crate::table_row_prefix(table).as_bytes()) =>
                {
                    let key = crate::primary_key_from_logical_row_key(table, &change.key)?;
                    if let Some(before) = sql.engine.kv.get_at(
                        &change.key,
                        rsf_kv::ReadSnapshot {
                            version: CommitVersion(state.snapshot),
                        },
                    ) && matches_row(
                        &sql.engine,
                        table,
                        &key,
                        &before.value,
                        before.version,
                        plan,
                    )? {
                        return Err(conflict());
                    }
                    if let Some(value) = &change.value
                        && matches_row(&sql.engine, table, &key, value, change.version, plan)?
                    {
                        return Err(conflict());
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn matches_row(
    engine: &SqlEngine,
    table: &str,
    key: &str,
    bytes: &[u8],
    version: CommitVersion,
    plan: &QueryPlan,
) -> RsfResult<bool> {
    let record = engine.table_record(table)?;
    let row = engine.row_from_physical_bytes(record, key, bytes, version)?;
    let mut columns = BTreeMap::new();
    for column in &record.descriptor.columns {
        if let Some(value) = row.columns.get(&column.name) {
            columns.insert(
                column.name.clone(),
                crate::query_value_from_sql_string(&column.ty, value, &column.name)?,
            );
        }
    }
    let row = QueryRow {
        table: record.descriptor.name.clone(),
        columns,
    };
    Ok(!QueryExecutor.execute(plan, &[row])?.rows.is_empty())
}
fn conflict() -> RsfError {
    invalid(
        "SQL serializable snapshot conflict; a row or predicate dependency changed; roll back and retry",
    )
}
