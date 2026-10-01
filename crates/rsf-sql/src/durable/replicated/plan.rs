//! Plan an autocommit transition on a private engine copy. Prospective versions
//! are not certificates; the copy is published only after matching commit evidence.
use super::*;

pub(super) fn operation(engine: &SqlEngine, statement: &str) -> RsfResult<FabricOperation> {
    let mutation = |table: String, key: String| FabricOperation::RelationalMutation {
        key: crate::logical_row_key(&table, &key),
        table,
    };
    Ok(match parse_statement(statement)? {
        ParsedStatement::CreateTable { table, .. } => FabricOperation::RelationalCatalog {
            table,
            item: "table".into(),
        },
        ParsedStatement::CreateIndex { table, column } => FabricOperation::RelationalCatalog {
            table,
            item: format!("index:{column}"),
        },
        ParsedStatement::Insert { table, columns } => {
            let key = engine.primary_key_value(&table, &columns)?;
            mutation(table, key)
        }
        ParsedStatement::Update {
            table, predicate, ..
        }
        | ParsedStatement::Delete { table, predicate } => {
            let key = engine.primary_key_from_predicate(&table, &predicate)?;
            mutation(table, key)
        }
        _ => {
            return Err(invalid(
                "replicated SQL commands require a supported autocommit mutation",
            ));
        }
    })
}

pub(super) fn prepare(
    engine: &SqlEngine,
    statement: &str,
    pg: PlacementGroupId,
    version: CommitVersion,
) -> RsfResult<SqlEngine> {
    version.validate_committed()?;
    // Physical page/tombstone history is not checkpointed. Rebuild the same
    // canonical base on primary and follower so restart cannot change capacity.
    let (mut next, _) = SqlEngine::recover_from_committed_kv(
        engine.kv.clone(),
        engine.recovered_tables_for_checkpoint(),
    )?;
    match parse_statement(statement)? {
        ParsedStatement::CreateTable {
            table,
            primary_key,
            columns,
        } => {
            if next.tables.contains_key(&table) {
                return Err(invalid(format!("table {table} already exists")));
            }
            next.create_table_with_columns(table, primary_key, columns)?;
            next.schema_version = version;
        }
        ParsedStatement::CreateIndex { table, column } => {
            next.validate_index_column(&table, &column)?;
            next.create_index(&table, &column)?;
            next.schema_version = version;
        }
        ParsedStatement::Insert { table, columns } => {
            let key = next.primary_key_value(&table, &columns)?;
            next.apply_transaction_insert(&table, &key, &columns, pg, version)?;
        }
        ParsedStatement::Update {
            table,
            predicate,
            assignments,
        } => {
            let key = next.primary_key_from_predicate(&table, &predicate)?;
            next.validate_assignment_columns(&table, &assignments)?;
            let mut columns = next
                .select_pk(&table, &key)
                .ok_or_else(|| invalid("row is not present"))?
                .columns
                .clone();
            columns.extend(assignments);
            next.apply_transaction_update(&table, &key, &columns, pg, version)?;
        }
        ParsedStatement::Delete { table, predicate } => {
            let key = next.primary_key_from_predicate(&table, &predicate)?;
            next.apply_transaction_delete(&table, &key, version)?;
        }
        _ => {
            return Err(invalid(
                "replicated SQL commands require a supported autocommit mutation",
            ));
        }
    }
    Ok(next)
}
