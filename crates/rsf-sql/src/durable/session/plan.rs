use super::model::{RowEffect, SessionResult, SessionRow};
use super::*;
use std::collections::BTreeSet;

pub(in crate::durable) struct PreparedSession {
    pub engine: SqlEngine,
    pub key: String,
    pub after: Option<SessionState>,
}

fn schema(engine: &SqlEngine) -> RsfResult<[u8; 32]> {
    let metadata = SqlCheckpoint {
        key_format: engine.key_format(),
        tables: engine.recovered_tables_for_checkpoint(),
        kv: vec![],
        receipts: BTreeMap::new(),
        namespace: None,
        commands: BTreeMap::new(),
        journal: None,
        sessions: BTreeMap::new(),
    };
    Ok(Digest::blake3_bytes(encode_checkpoint(&metadata)?.as_bytes()).bytes)
}
fn target(engine: &SqlEngine, statement: &ParsedStatement) -> RsfResult<Option<(String, String)>> {
    Ok(match statement {
        ParsedStatement::Insert { table, columns } => {
            Some((table.clone(), engine.primary_key_value(table, columns)?))
        }
        ParsedStatement::Update {
            table, predicate, ..
        }
        | ParsedStatement::Delete { table, predicate } => Some((
            table.clone(),
            engine.primary_key_from_predicate(table, predicate)?,
        )),
        _ => None,
    })
}
struct Working {
    base: SqlEngine,
    engine: SqlEngine,
    touched: BTreeSet<(String, String)>,
}
fn working(engine: &SqlEngine, state: &SessionState) -> RsfResult<Working> {
    state.validate()?;
    if schema(engine)? != state.schema {
        return Err(invalid(
            "SQL transaction schema changed; roll back and begin again",
        ));
    }
    let mut kv = KvEngine::default();
    for (key, versions) in engine.committed_kv_versions_for_checkpoint() {
        for value in versions
            .into_iter()
            .filter(|value| value.version.0 <= state.snapshot)
        {
            let mutation = if value.deleted {
                KvMutation::Clear { key: key.clone() }
            } else {
                KvMutation::Set {
                    key: key.clone(),
                    value: value.value,
                }
            };
            kv.apply_committed_batch(value.version, vec![mutation])?;
        }
    }
    let (base, _) = SqlEngine::recover_from_committed_kv_with_format(
        kv,
        engine.recovered_tables_for_checkpoint(),
        engine.key_format(),
    )?;
    let mut next = base.clone();
    let mut touched = BTreeSet::new();
    for (position, statement) in state.statements.iter().enumerate() {
        let parsed = parse_statement(statement)?;
        if let Some(key) = target(&next, &parsed)? {
            touched.insert(key);
        }
        let version = state
            .snapshot
            .checked_add(position as u64 + 1)
            .map(CommitVersion)
            .ok_or_else(|| invalid("SQL transaction version exhausted"))?;
        next = super::super::replicated::plan::prepare(
            &next,
            statement,
            PlacementGroupId(state.pg),
            version,
        )?;
    }
    Ok(Working {
        base,
        engine: next,
        touched,
    })
}
pub(super) fn active<'a>(
    sql: &'a DurableSqlEngine,
    request: &SessionRequest,
) -> RsfResult<&'a SessionState> {
    request.validate()?;
    let state = sql
        .sessions
        .get(&request.key())
        .ok_or_else(|| invalid("SQL session has no active transaction"))?;
    if request.context.transaction.as_deref() != Some(state.token.as_str())
        || state.pg != request.pg
    {
        return Err(invalid(
            "SQL session transaction token or placement group differs",
        ));
    }
    Ok(state)
}
fn transaction_token(namespace: &str, request: &SessionRequest) -> RsfResult<String> {
    let mut identity = namespace.as_bytes().to_vec();
    identity.extend(encode(request)?);
    Ok(hex_bytes(Digest::blake3_bytes(&identity).as_bytes()))
}
pub(in crate::durable) fn validate_serializable_snapshot(
    sql: &DurableSqlEngine,
    snapshot: u64,
) -> RsfResult<()> {
    if !sql
        .engine
        .kv
        .changes_since(CommitVersion(snapshot))
        .is_empty()
    {
        return Err(invalid(
            "SQL serializable snapshot conflict; namespace data changed after BEGIN; roll back and retry",
        ));
    }
    Ok(())
}
fn effects(
    sql: &DurableSqlEngine,
    state: &SessionState,
    work: &Working,
    serializable: bool,
) -> RsfResult<Vec<RowEffect>> {
    if serializable {
        dependencies::validate(sql, state, None)?;
    }
    let mut effects = Vec::new();
    for (table, key) in &work.touched {
        let row = work.engine.select_pk(table, key);
        if row.is_none() && work.base.select_pk(table, key).is_none() {
            continue;
        }
        effects.push(RowEffect {
            table: table.clone(),
            key: key.clone(),
            columns: row.map(|row| row.columns.clone()),
        });
    }
    if !serializable {
        let keys = effects
            .iter()
            .map(|effect| sql.engine.row_key(&effect.table, &effect.key))
            .collect::<BTreeSet<_>>();
        if sql
            .engine
            .kv
            .changes_since(CommitVersion(state.snapshot))
            .iter()
            .any(|change| keys.contains(&change.key))
        {
            return Err(invalid(
                "SQL snapshot write conflict; roll back and retry the transaction",
            ));
        }
    }
    Ok(effects)
}

pub(in crate::durable) fn propose(
    sql: &DurableSqlEngine,
    request: SessionRequest,
    begin_snapshot: CommitVersion,
    serializable: bool,
    tracked: bool,
) -> RsfResult<SessionProposal> {
    request.validate()?;
    let parsed = parse_statement(&request.statement)?;
    let before = sql.sessions.get(&request.key()).cloned();
    let (after, result, effects) = match &parsed {
        ParsedStatement::Begin => {
            if before.is_some() || request.context.transaction.is_some() {
                return Err(invalid(
                    "SQL session already has an active transaction or unexpected token",
                ));
            }
            if sql.sessions.len() >= MAX_ACTIVE_SQL_SESSIONS {
                return Err(invalid("active SQL session limit reached"));
            }
            let namespace = sql
                .namespace
                .as_deref()
                .ok_or_else(|| invalid("SQL sessions require a bound namespace"))?;
            let token = transaction_token(namespace, &request)?;
            let state = SessionState {
                token: token.clone(),
                snapshot: begin_snapshot.0,
                schema: schema(&sql.engine)?,
                pg: request.pg,
                statements: vec![],
                savepoints: vec![],
                dependencies: tracked.then(BTreeSet::new),
            };
            (
                Some(state),
                SessionResult::Began {
                    token,
                    snapshot: begin_snapshot.0,
                },
                vec![],
            )
        }
        ParsedStatement::Rollback => {
            let state = active(sql, &request)?;
            (
                None,
                SessionResult::RolledBack {
                    token: state.token.clone(),
                    discarded: state.statements.len(),
                },
                vec![],
            )
        }
        ParsedStatement::Commit => {
            let state = active(sql, &request)?;
            let work = working(&sql.engine, state)?;
            // Without durable predicate read sets, validate the entire namespace
            // snapshot. An accepted command then serializes at its commit: no
            // intervening data/schema change can invalidate any earlier read.
            let effects = effects(sql, state, &work, serializable)?;
            let schemas = state
                .statements
                .iter()
                .map(|s| parse_statement(s))
                .collect::<RsfResult<Vec<_>>>()?
                .iter()
                .filter(|s| {
                    matches!(
                        s,
                        ParsedStatement::CreateTable { .. } | ParsedStatement::CreateIndex { .. }
                    )
                })
                .count();
            let result = SessionResult::Committed {
                token: state.token.clone(),
                mutations: effects.len() + schemas,
            };
            (None, result, effects)
        }
        ParsedStatement::Savepoint { name } => {
            let mut state = active(sql, &request)?.clone();
            working(&sql.engine, &state)?;
            crate::validate_savepoint_name(name)?;
            if state.savepoints.len() >= MAX_SQL_SAVEPOINTS {
                return Err(invalid("SQL session savepoint limit reached"));
            }
            state
                .savepoints
                .push((name.clone(), state.statements.len()));
            let result = SessionResult::Savepoint {
                token: state.token.clone(),
                name: name.clone(),
                staged: state.statements.len(),
            };
            (Some(state), result, vec![])
        }
        ParsedStatement::RollbackToSavepoint { name } => {
            let mut state = active(sql, &request)?.clone();
            working(&sql.engine, &state)?;
            let position = state
                .savepoints
                .iter()
                .rposition(|(saved, _)| saved == name)
                .ok_or_else(|| invalid("SQL session has no such savepoint"))?;
            state.statements.truncate(state.savepoints[position].1);
            state.savepoints.truncate(position + 1);
            let result = SessionResult::RollbackTo {
                token: state.token.clone(),
                name: name.clone(),
                staged: state.statements.len(),
            };
            (Some(state), result, vec![])
        }
        ParsedStatement::Select { .. } => {
            if !tracked {
                return Err(invalid("legacy SQL session reads are not replicated"));
            }
            let mut state = active(sql, &request)?.clone();
            let execution = read(sql, request.clone())?;
            let SqlOutcome::Selected { rows } = execution.outcome else {
                return Err(corrupt("SQL read has no rows"));
            };
            let rows = rows
                .into_iter()
                .map(|row| SessionRow {
                    table: row.table,
                    key: row.primary_key,
                    columns: row.columns,
                })
                .collect::<Vec<_>>();
            if encode(&rows)?.len() > MAX_SQL_SESSION_READ_BYTES {
                return Err(invalid(
                    "SQL session SELECT result exceeds limit; use a smaller LIMIT",
                ));
            }
            if let Some(dependencies) = &mut state.dependencies {
                dependencies.insert(dependencies::Dependency::Query {
                    statement: request.statement.clone(),
                });
            }
            let result = SessionResult::Selected {
                token: state.token.clone(),
                snapshot: state.snapshot,
                rows,
            };
            (Some(state), result, vec![])
        }
        _ => {
            let mut state = active(sql, &request)?.clone();
            state.statements.push(request.statement.clone());
            state.validate()?;
            let work = working(&sql.engine, &state)?;
            let token = state.token.clone();
            let snapshot = state.snapshot;
            let result = match &parsed {
                ParsedStatement::CreateTable { table, .. } => SessionResult::Schema {
                    token,
                    snapshot,
                    table: table.clone(),
                    item: "table".into(),
                },
                ParsedStatement::CreateIndex { table, column } => SessionResult::Schema {
                    token,
                    snapshot,
                    table: table.clone(),
                    item: format!("index:{column}"),
                },
                ParsedStatement::Insert { .. } | ParsedStatement::Update { .. } => {
                    let (table, key) = target(&work.engine, &parsed)?
                        .ok_or_else(|| corrupt("SQL staged row has no key"))?;
                    let row = work
                        .engine
                        .select_pk(&table, &key)
                        .ok_or_else(|| corrupt("SQL staged row disappeared"))?;
                    let row = SessionRow {
                        table,
                        key,
                        columns: row.columns.clone(),
                    };
                    if matches!(parsed, ParsedStatement::Insert { .. }) {
                        SessionResult::Inserted {
                            token,
                            snapshot,
                            row,
                        }
                    } else {
                        SessionResult::Updated {
                            token,
                            snapshot,
                            row,
                        }
                    }
                }
                ParsedStatement::Delete { .. } => {
                    let (table, key) = target(&work.engine, &parsed)?
                        .ok_or_else(|| corrupt("SQL staged delete has no key"))?;
                    SessionResult::Deleted {
                        token,
                        snapshot,
                        table,
                        key,
                    }
                }
                _ => return Err(invalid("unsupported SQL session statement")),
            };
            if let Some(dependencies) = &mut state.dependencies {
                let row = match &result {
                    SessionResult::Inserted { row, .. } | SessionResult::Updated { row, .. } => {
                        Some((row.table.clone(), row.key.clone()))
                    }
                    SessionResult::Deleted { table, key, .. } => Some((table.clone(), key.clone())),
                    _ => None,
                };
                if let Some((table, key)) = row {
                    dependencies.insert(dependencies::Dependency::Row { table, key });
                }
            }
            (Some(state), result, vec![])
        }
    };
    let proposal = SessionProposal {
        serializable,
        tracked,
        request,
        before,
        after,
        result,
        effects,
    };
    proposal.validate()?;
    Ok(proposal)
}

pub(in crate::durable) fn prepare(
    sql: &DurableSqlEngine,
    proposal: &SessionProposal,
    version: CommitVersion,
) -> RsfResult<PreparedSession> {
    proposal.validate()?;
    let snapshot = proposal
        .after
        .as_ref()
        .or(proposal.before.as_ref())
        .map_or(0, |state| state.snapshot);
    if snapshot >= version.0 {
        return Err(corrupt(
            "SQL session snapshot is not older than its command",
        ));
    }
    let expected = propose(
        sql,
        proposal.request.clone(),
        CommitVersion(snapshot),
        proposal.serializable,
        proposal.tracked,
    )?;
    if expected != *proposal {
        return Err(corrupt(
            "SQL session transition differs from deterministic replay",
        ));
    }
    let engine = if matches!(proposal.result, SessionResult::Committed { .. }) {
        let state = proposal
            .before
            .as_ref()
            .ok_or_else(|| corrupt("SQL commit has no staged transaction"))?;
        let work = working(&sql.engine, state)?;
        let mut kv = sql.engine.kv.clone();
        let mut mutations = Vec::new();
        for effect in &proposal.effects {
            let key = sql.engine.row_key(&effect.table, &effect.key);
            mutations.push(match &effect.columns {
                Some(columns) => KvMutation::Set {
                    key,
                    value: work.engine.encode_row_payload(
                        &effect.table,
                        &effect.key,
                        columns,
                        version,
                    )?,
                },
                None => KvMutation::Clear { key },
            });
        }
        if !mutations.is_empty() {
            kv.apply_committed_batch(version, mutations)?;
        }
        let (mut engine, _) = SqlEngine::recover_from_committed_kv_with_format(
            kv,
            work.engine.recovered_tables_for_checkpoint(),
            sql.engine.key_format(),
        )?;
        engine.schema_version = version;
        engine
    } else {
        sql.engine.clone()
    };
    Ok(PreparedSession {
        engine,
        key: proposal.request.key(),
        after: proposal.after.clone(),
    })
}

pub(in crate::durable) fn read(
    sql: &DurableSqlEngine,
    request: SessionRequest,
) -> RsfResult<DurableSqlExecution> {
    let state = active(sql, &request)?;
    let mut work = working(&sql.engine, state)?;
    let ParsedStatement::Select {
        table,
        projection,
        predicate,
        limit,
    } = parse_statement(&request.statement)?
    else {
        return Err(invalid("SQL session inspection requires SELECT"));
    };
    let mut rows = work
        .engine
        .execute_select(&table, projection, predicate, limit)?;
    for row in &mut rows {
        if row.version.0 > state.snapshot {
            row.version = CommitVersion(state.snapshot);
        }
    }
    Ok(execution(SqlOutcome::Selected { rows }, false))
}

pub(in crate::durable) fn validate_recovered(sql: &DurableSqlEngine) -> RsfResult<()> {
    let namespace = sql
        .namespace
        .as_deref()
        .ok_or_else(|| corrupt("SQL sessions have no namespace"))?;
    let mut expected = BTreeMap::new();
    let mut catalog = SqlEngine {
        key_format: sql.engine.key_format(),
        ..SqlEngine::default()
    };
    let mut previous = CommitVersion::GENESIS;
    let mut commands = sql.commands.values().collect::<Vec<_>>();
    commands.sort_by_key(|command| command.commit.version);
    let versions = sql
        .engine
        .committed_kv_versions_for_checkpoint()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    for command in commands {
        let request = super::super::replicated::decode_request(command, namespace)?;
        if let Some(proposal) = request.session {
            let key = proposal.request.key();
            if expected.get(&key) != proposal.before.as_ref() {
                return Err(corrupt("SQL checkpoint session transition chain differs"));
            }
            if let SessionResult::Began {
                snapshot,
                ref token,
            } = proposal.result
            {
                if snapshot < previous.0 || snapshot >= command.commit.version.0 {
                    return Err(corrupt(
                        "SQL session BEGIN snapshot conflicts with command history",
                    ));
                }
                let catalog_digest = schema(&catalog)?;
                if *token != transaction_token(namespace, &proposal.request)?
                    || proposal
                        .after
                        .as_ref()
                        .is_none_or(|state| state.schema != catalog_digest)
                {
                    return Err(corrupt(
                        "SQL session BEGIN token or catalog differs from history",
                    ));
                }
            }
            let state = proposal
                .before
                .as_ref()
                .or(proposal.after.as_ref())
                .ok_or_else(|| corrupt("SQL session command has no state"))?;
            if state.snapshot >= command.commit.version.0 {
                return Err(corrupt(
                    "SQL session snapshot is not older than its command",
                ));
            }
            if matches!(proposal.result, SessionResult::Committed { .. }) {
                if proposal.serializable && proposal.tracked {
                    dependencies::validate(sql, state, Some(command.commit.version)).map_err(
                        |_| {
                            corrupt("serializable SQL session committed across a dependency change")
                        },
                    )?;
                } else if proposal.serializable
                    && versions.values().flatten().any(|value| {
                        state.snapshot < value.version.0 && value.version < command.commit.version
                    })
                {
                    return Err(corrupt(
                        "serializable SQL session committed across a namespace data change",
                    ));
                }
                if state.schema != schema(&catalog)? {
                    return Err(corrupt("SQL session committed across a catalog change"));
                }
                for statement in &state.statements {
                    recover_catalog(
                        &mut catalog,
                        statement,
                        command.commit.pg,
                        command.commit.version,
                    )?;
                }
            }
            let mut effect_keys = BTreeSet::new();
            for effect in &proposal.effects {
                let key = sql.engine.row_key(&effect.table, &effect.key);
                effect_keys.insert(key.clone());
                let value = versions
                    .get(&key)
                    .and_then(|values| {
                        values
                            .iter()
                            .find(|value| value.version == command.commit.version)
                    })
                    .ok_or_else(|| corrupt("SQL session commit is missing a row version"))?;
                match &effect.columns {
                    None if value.deleted => {}
                    Some(columns) if !value.deleted => {
                        let row = sql.engine.row_from_physical_bytes(
                            sql.engine.table_record(&effect.table)?,
                            &effect.key,
                            &value.value,
                            value.version,
                        )?;
                        if &row.columns != columns {
                            return Err(corrupt(
                                "SQL session commit row differs from its complete command",
                            ));
                        }
                    }
                    _ => return Err(corrupt("SQL session commit row kind differs")),
                }
            }
            let actual = versions
                .iter()
                .filter(|(_, values)| {
                    values
                        .iter()
                        .any(|value| value.version == command.commit.version)
                })
                .map(|(key, _)| key.clone())
                .collect::<BTreeSet<_>>();
            if actual != effect_keys {
                return Err(corrupt("SQL session command has unaccounted row effects"));
            }
            if let Some(after) = proposal.after {
                expected.insert(key, after);
            } else {
                expected.remove(&key);
            }
            if expected.len() > MAX_ACTIVE_SQL_SESSIONS {
                return Err(corrupt("SQL command history exceeds active session limit"));
            }
        } else {
            recover_catalog(
                &mut catalog,
                &request.statement,
                command.commit.pg,
                command.commit.version,
            )?;
        }
        previous = command.commit.version;
    }
    if expected != sql.sessions {
        return Err(corrupt(
            "SQL checkpoint session inventory differs from committed commands",
        ));
    }
    if schema(&catalog)? != schema(&sql.engine)? {
        return Err(corrupt(
            "SQL checkpoint catalog differs from committed commands",
        ));
    }
    Ok(())
}

fn recover_catalog(
    engine: &mut SqlEngine,
    statement: &str,
    pg: PlacementGroupId,
    version: CommitVersion,
) -> RsfResult<()> {
    if matches!(
        parse_statement(statement)?,
        ParsedStatement::CreateTable { .. } | ParsedStatement::CreateIndex { .. }
    ) {
        *engine = super::super::replicated::plan::prepare(engine, statement, pg, version)?;
    }
    Ok(())
}
