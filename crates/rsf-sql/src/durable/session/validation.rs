//! Checks that do not require row materialization also apply to checkpoint reads.
use super::model::SessionResult;
use super::*;

pub(super) fn validate(proposal: &SessionProposal) -> RsfResult<()> {
    let statement = parse_statement(&proposal.request.statement)?;
    let fail = || corrupt("SQL session command has an inconsistent transition or result");
    if let ParsedStatement::Begin = statement {
        let Some(after) = &proposal.after else {
            return Err(fail());
        };
        if proposal.before.is_some()
            || proposal.request.context.transaction.is_some()
            || after.pg != proposal.request.pg
            || !after.statements.is_empty()
            || !after.savepoints.is_empty()
            || proposal.result
                != (SessionResult::Began {
                    token: after.token.clone(),
                    snapshot: after.snapshot,
                })
        {
            return Err(fail());
        }
        return Ok(());
    }
    let before = proposal.before.as_ref().ok_or_else(fail)?;
    if proposal.request.context.transaction.as_deref() != Some(&before.token)
        || proposal.request.pg != before.pg
    {
        return Err(fail());
    }
    let mut after = Some(before.clone());
    let valid = match (&statement, &proposal.result) {
        (ParsedStatement::Commit, SessionResult::Committed { token, mutations }) => {
            after = None;
            let schemas = before.statements.iter().try_fold(0, |count, sql| {
                Ok::<_, RsfError>(
                    count
                        + usize::from(matches!(
                            parse_statement(sql)?,
                            ParsedStatement::CreateTable { .. }
                                | ParsedStatement::CreateIndex { .. }
                        )),
                )
            })?;
            let mut previous = None;
            for effect in &proposal.effects {
                let key = (&effect.table, &effect.key);
                if effect.table.is_empty() || previous.is_some_and(|previous| previous >= key) {
                    return Err(fail());
                }
                previous = Some(key);
            }
            token == &before.token && *mutations == proposal.effects.len() + schemas
        }
        (ParsedStatement::Rollback, SessionResult::RolledBack { token, discarded }) => {
            after = None;
            token == &before.token && *discarded == before.statements.len()
        }
        (
            ParsedStatement::Savepoint { name },
            SessionResult::Savepoint {
                token,
                name: result,
                staged,
            },
        ) => {
            after
                .as_mut()
                .unwrap()
                .savepoints
                .push((name.clone(), before.statements.len()));
            token == &before.token && result == name && *staged == before.statements.len()
        }
        (
            ParsedStatement::RollbackToSavepoint { name },
            SessionResult::RollbackTo {
                token,
                name: result,
                staged,
            },
        ) => {
            let position = before
                .savepoints
                .iter()
                .rposition(|(saved, _)| saved == name)
                .ok_or_else(fail)?;
            let state = after.as_mut().unwrap();
            state.statements.truncate(before.savepoints[position].1);
            state.savepoints.truncate(position + 1);
            token == &before.token && result == name && *staged == state.statements.len()
        }
        (
            ParsedStatement::CreateTable { table, .. },
            SessionResult::Schema {
                token,
                snapshot,
                table: result,
                item,
            },
        ) => {
            token == &before.token
                && snapshot == &before.snapshot
                && result == table
                && item == "table"
        }
        (
            ParsedStatement::CreateIndex { table, column },
            SessionResult::Schema {
                token,
                snapshot,
                table: result,
                item,
            },
        ) => {
            token == &before.token
                && snapshot == &before.snapshot
                && result == table
                && item == &format!("index:{column}")
        }
        (
            ParsedStatement::Insert { table, columns },
            SessionResult::Inserted {
                token,
                snapshot,
                row,
            },
        ) => {
            token == &before.token
                && snapshot == &before.snapshot
                && &row.table == table
                && columns
                    .iter()
                    .all(|(key, value)| row.columns.get(key) == Some(value))
        }
        (
            ParsedStatement::Update {
                table, assignments, ..
            },
            SessionResult::Updated {
                token,
                snapshot,
                row,
            },
        ) => {
            token == &before.token
                && snapshot == &before.snapshot
                && &row.table == table
                && assignments
                    .iter()
                    .all(|(key, value)| row.columns.get(key) == Some(value))
        }
        (
            ParsedStatement::Delete { table, .. },
            SessionResult::Deleted {
                token,
                snapshot,
                table: result,
                key: _,
            },
        ) => token == &before.token && snapshot == &before.snapshot && result == table,
        _ => false,
    };
    if matches!(
        statement,
        ParsedStatement::CreateTable { .. }
            | ParsedStatement::CreateIndex { .. }
            | ParsedStatement::Insert { .. }
            | ParsedStatement::Update { .. }
            | ParsedStatement::Delete { .. }
    ) {
        after
            .as_mut()
            .unwrap()
            .statements
            .push(proposal.request.statement.clone());
    }
    if !valid || after != proposal.after {
        return Err(fail());
    }
    Ok(())
}
