//! Pull-based SELECT execution. Candidate scans and transaction overlays retain
//! at most one row; callers choose whether to collect or stream the result.
use super::*;

impl SqlEngine {
    pub(crate) fn execute_select(
        &mut self,
        table: &str,
        projection: Projection,
        predicate: Option<Predicate>,
        limit: Option<usize>,
    ) -> RsfResult<Vec<Row>> {
        let mut rows = Vec::new();
        self.visit_select(table, projection, predicate, limit, |row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(rows)
    }

    pub(crate) fn visit_select(
        &mut self,
        table: &str,
        projection: Projection,
        predicate: Option<Predicate>,
        limit: Option<usize>,
        mut sink: impl FnMut(Row) -> RsfResult<()>,
    ) -> RsfResult<usize> {
        let table = self.table_record(table)?.clone();
        let requested = projection.clone();
        let logical = QueryPlanner.select(
            &table.descriptor,
            projection_with_primary(&table, projection),
            type_select_predicate(&table, predicate)?,
            limit,
        )?;
        let physical = PhysicalPlanner.plan(
            &table.descriptor,
            &logical,
            &PhysicalPlanningContext {
                stats: self.table_stats(&table),
                indexes: self.secondary_index_descriptors(&table),
                stream_threshold_bytes: 64 * 1024,
                cacheable: false,
            },
        )?;
        let execution = PhysicalExecutor.execute(&physical)?;
        let prefix = self.row_prefix(&table.name);
        let count = QueryExecutor.visit(
            &logical,
            self.query_candidates(&table, &logical, &physical, &prefix)?,
            |query_row| {
                let mut row = row_from_query_row(&table, query_row)?;
                apply_requested_projection(std::slice::from_mut(&mut row), &requested)?;
                sink(row)
            },
        )?;
        self.query_reports = Some(SqlQueryReport {
            logical,
            physical,
            execution,
        });
        self.query_report_count = self.query_report_count.saturating_add(1);
        Ok(count)
    }

    fn query_candidates<'a>(
        &'a self,
        table: &'a Table,
        logical: &'a QueryPlan,
        physical: &PhysicalPlan,
        prefix: &'a [u8],
    ) -> RsfResult<impl Iterator<Item = RsfResult<QueryRow>> + 'a> {
        let access = physical
            .operators
            .iter()
            .find_map(|operator| match operator.access {
                AccessPath::CacheLookup | AccessPath::StreamRead { .. } => None,
                _ => Some(&operator.access),
            })
            .ok_or_else(|| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    "physical query plan has no row-producing access operator",
                )
            })?;
        let point = match access {
            AccessPath::PrimaryKeyLookup { column } => {
                Some(equality_text_value(logical.predicate.as_ref(), column))
            }
            _ => None,
        };
        let mut point =
            point.map(|key| key.and_then(|key| self.read_query_point(table, &key).transpose()));
        let mut committed = self.kv.iter_prefix(prefix).peekable();
        let mut staged = self
            .transaction
            .iter()
            .flat_map(|state| state.staged.iter())
            .filter(move |((name, _), _)| name == &table.name)
            .peekable();
        Ok(std::iter::from_fn(move || {
            if let Some(point) = &mut point {
                return point
                    .take()
                    .map(|row| row.and_then(|row| query_row_from_row(table, &row)));
            }
            loop {
                let primary = match committed.peek() {
                    Some((key, _)) => match self.primary_key_from_row_key(&table.name, key) {
                        Ok(key) => Some(key),
                        Err(error) => {
                            committed.next();
                            return Some(Err(error));
                        }
                    },
                    None => None,
                };
                if let Some(((.., key), _)) = staged.peek()
                    && primary.as_ref().is_none_or(|primary| key <= primary)
                {
                    let ((_, key), mutation) = staged.next().expect("peeked staged row");
                    if primary.as_ref() == Some(key) {
                        committed.next();
                    }
                    match mutation {
                        SqlTransactionMutation::Delete => continue,
                        SqlTransactionMutation::Set(columns) => {
                            return Some(query_row_from_row(
                                table,
                                &Row {
                                    table: table.name.clone(),
                                    primary_key: key.clone(),
                                    columns: columns.clone(),
                                    version: self
                                        .transaction
                                        .as_ref()
                                        .expect("staged transaction")
                                        .snapshot,
                                },
                            ));
                        }
                    }
                }
                let (_, value) = committed.next()?;
                return Some(
                    self.row_from_physical_bytes(
                        table,
                        &primary.expect("committed primary"),
                        &value.value,
                        value.version,
                    )
                    .and_then(|row| query_row_from_row(table, &row)),
                );
            }
        }))
    }

    fn read_query_point(&self, table: &Table, key: &str) -> RsfResult<Option<Row>> {
        if let Some(state) = &self.transaction
            && let Some(mutation) = state.staged.get(&(table.name.clone(), key.to_string()))
        {
            return Ok(match mutation {
                SqlTransactionMutation::Delete => None,
                SqlTransactionMutation::Set(columns) => Some(Row {
                    table: table.name.clone(),
                    primary_key: key.into(),
                    columns: columns.clone(),
                    version: state.snapshot,
                }),
            });
        }
        self.kv
            .get(&self.row_key(&table.name, key))
            .map(|value| self.row_from_physical_bytes(table, key, &value.value, value.version))
            .transpose()
    }

    fn table_stats(&self, table: &Table) -> TableStats {
        if let Some(estimate) = self.stats.estimate(&table.descriptor.name)
            && estimate.rows > 0
        {
            return TableStats {
                table: estimate.table.clone(),
                rows: estimate.rows,
                average_row_bytes: estimate.average_row_bytes().max(1),
            };
        }
        // Planning needs a size estimate, not decoded copies of the table.
        let prefix = self.row_prefix(&table.name);
        let (rows, bytes) =
            self.kv
                .iter_prefix(&prefix)
                .fold((0_u64, 0_u64), |(rows, bytes), (_, value)| {
                    (
                        rows.saturating_add(1),
                        bytes.saturating_add(value.value.len() as u64),
                    )
                });
        TableStats {
            table: table.descriptor.name.clone(),
            rows,
            average_row_bytes: bytes.checked_div(rows).unwrap_or(1).max(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_protocol::FabricRouter;
    fn setup(statements: &[&str]) -> (SqlEngine, FabricRouter) {
        let mut sql = SqlEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        for statement in statements {
            sql.execute(&mut router, PlacementGroupId(1), statement)
                .unwrap();
        }
        (sql, router)
    }
    #[test]
    fn sql_pull_limit_does_not_decode_later_rows_and_sink_failure_stops() {
        let (mut sql, _) = setup(&[
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "INSERT INTO users (id, name) VALUES (1, first)",
            "INSERT INTO users (id) VALUES (2)",
        ]);
        let mut keys = Vec::new();
        assert_eq!(
            sql.visit_select("users", Projection::All, None, Some(1), |row| {
                keys.push(row.primary_key);
                Ok(())
            })
            .unwrap(),
            1
        );
        assert_eq!(keys, ["1"]);
        assert!(
            sql.visit_select("users", Projection::All, None, None, |_| Err(
                RsfError::new(ErrorCode::InvalidInput, "sink closed")
            ))
            .unwrap_err()
            .message
            .contains("sink closed")
        );
        assert!(
            sql.visit_select("users", Projection::All, None, None, |_| Ok(()))
                .is_err()
        );
    }
    #[test]
    fn sql_pull_merges_ordered_overlay_and_retains_only_latest_report() {
        let (mut sql, mut router) = setup(&[
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "INSERT INTO users (id, name) VALUES (1, first)",
            "INSERT INTO users (id, name) VALUES (3, third)",
            "BEGIN",
            "DELETE FROM users WHERE id=1",
            "UPDATE users SET name=changed WHERE id=3",
            "INSERT INTO users (id, name) VALUES (2, second)",
        ]);
        for _ in 0..130 {
            let mut actual = Vec::new();
            sql.visit_select("users", Projection::All, None, None, |row| {
                actual.push((row.primary_key, row.columns["name"].clone()));
                Ok(())
            })
            .unwrap();
            assert_eq!(
                actual,
                [
                    ("2".into(), "second".into()),
                    ("3".into(), "changed".into())
                ]
            );
        }
        assert_eq!(sql.query_report_count(), 130);
        assert!(sql.query_reports.is_some());
        sql.execute(&mut router, PlacementGroupId(1), "ROLLBACK")
            .unwrap();
        let rows = sql
            .execute_select("users", Projection::All, None, None)
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.primary_key.as_str())
                .collect::<Vec<_>>(),
            ["1", "3"]
        );
    }
}
