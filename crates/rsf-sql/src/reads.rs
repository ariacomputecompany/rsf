//! Pull-based SELECT execution. Candidate scans and transaction overlays retain
//! at most one row; callers choose whether to collect or stream the result.
use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct QuerySize {
    pub(super) rows: u64,
    pub(super) bytes: u64,
}
impl QuerySize {
    fn change(self, before: Option<usize>, after: Option<usize>) -> RsfResult<Self> {
        let rows = self
            .rows
            .checked_sub(u64::from(before.is_some()))
            .and_then(|rows| rows.checked_add(u64::from(after.is_some())));
        let bytes = self
            .bytes
            .checked_sub(before.unwrap_or(0) as u64)
            .and_then(|bytes| bytes.checked_add(after.unwrap_or(0) as u64));
        match (rows, bytes) {
            (Some(rows), Some(bytes)) => Ok(Self { rows, bytes }),
            _ => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "SQL planning size metadata is inconsistent",
            )),
        }
    }
}

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
        let secondary = match access {
            AccessPath::SecondaryIndexLookup { column, .. } => Some((
                column.clone(),
                equality_text_value(logical.predicate.as_ref(), column),
            )),
            _ => None,
        };
        let mut point =
            point.map(|key| key.and_then(|key| self.read_query_point(table, &key).transpose()));
        // Both sources expose ordered primary keys and borrow committed values.
        // Index lookup allocates no collection of matching primary keys.
        type Source<'a> = Box<dyn Iterator<Item = RsfResult<(String, &'a KvVersionedValue)>> + 'a>;
        let committed: Source<'a> =
            if let AccessPath::SecondaryIndexLookup { index, column } = access {
                let descriptor = secondary_index_descriptor(table, column);
                if &descriptor.name != index {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "SQL index plan differs from table descriptor",
                    ));
                }
                if let Some(value) = equality_text_value(logical.predicate.as_ref(), column) {
                    let image = RowImage {
                        primary_key: String::new(),
                        values: BTreeMap::from([(column.clone(), value)]),
                    };
                    let key = encode_index_key(&descriptor, &image)?;
                    Box::new(self.indexes.lookup_iter(index, &key).map(move |primary| {
                        let value = self
                            .kv
                            .get(&self.row_key(&table.name, primary))
                            .ok_or_else(|| {
                                RsfError::new(
                                    ErrorCode::CorruptionDetected,
                                    "SQL index references a missing row",
                                )
                            })?;
                        Ok((primary.to_string(), value))
                    }))
                } else {
                    Box::new(std::iter::empty())
                }
            } else {
                Box::new(self.kv.iter_prefix(prefix).map(move |(key, value)| {
                    Ok((self.primary_key_from_row_key(&table.name, key)?, value))
                }))
            };
        let mut committed = committed.peekable();
        let indexed_predicate = secondary.clone();
        let mut staged = self
            .transaction
            .iter()
            .flat_map(|state| state.staged.iter())
            .filter(move |((name, _), _)| name == &table.name)
            .peekable();
        Ok(std::iter::from_fn(move || {
            if let Some(point) = &mut point {
                return point.take();
            }
            loop {
                let primary = match committed.peek() {
                    Some(Ok((primary, _))) => Some(primary.clone()),
                    Some(Err(_)) => {
                        return Some(Err(committed
                            .next()
                            .expect("peeked committed error")
                            .unwrap_err()));
                    }
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
                            let row = Row {
                                table: table.name.clone(),
                                primary_key: key.clone(),
                                columns: columns.clone(),
                                version: self
                                    .transaction
                                    .as_ref()
                                    .expect("staged transaction")
                                    .snapshot,
                            };
                            return Some(row_image(table, &row).map(|image| Row {
                                columns: image.values,
                                ..row
                            }));
                        }
                    }
                }
                let (_, value) = committed.next()?.expect("peeked committed value");
                let row = self.row_from_physical_bytes(
                    table,
                    &primary.expect("committed primary"),
                    &value.value,
                    value.version,
                );
                return Some(row.and_then(|row| {
                    if indexed_predicate.as_ref().is_some_and(|(column, value)| {
                        value
                            .as_ref()
                            .is_none_or(|value| row.columns.get(column) != Some(value))
                    }) {
                        return Err(RsfError::new(
                            ErrorCode::CorruptionDetected,
                            "SQL index entry differs from its committed row",
                        ));
                    }
                    Ok(row)
                }));
            }
        })
        .filter_map(move |row| match row {
            Ok(row)
                if secondary.as_ref().is_some_and(|(column, value)| {
                    value
                        .as_ref()
                        .is_none_or(|value| row.columns.get(column) != Some(value))
                }) =>
            {
                None
            }
            Ok(row) => Some(query_row_from_row(table, &row)),
            Err(error) => Some(Err(error)),
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
        let size = self
            .query_sizes
            .get(&table.name)
            .copied()
            .unwrap_or_default();
        TableStats {
            table: table.descriptor.name.clone(),
            rows: size.rows,
            average_row_bytes: size.bytes.checked_div(size.rows).unwrap_or(1).max(1),
        }
    }

    pub(super) fn update_query_size(
        &mut self,
        table: &str,
        key: &[u8],
        after: Option<usize>,
    ) -> RsfResult<()> {
        let before = self.kv.get(key).map(|value| value.value.len());
        let previous = self.query_sizes.get(table).copied().unwrap_or_default();
        self.query_sizes
            .insert(table.to_string(), previous.change(before, after)?);
        Ok(())
    }

    pub(super) fn rebuild_query_sizes(&mut self) -> RsfResult<()> {
        let mut sizes = BTreeMap::new();
        for table in self.tables.keys() {
            let prefix = self.row_prefix(table);
            let size = self
                .kv
                .iter_prefix(&prefix)
                .try_fold(QuerySize::default(), |size, (_, value)| {
                    size.change(None, Some(value.value.len()))
                })?;
            sizes.insert(table.clone(), size);
        }
        self.query_sizes = sizes;
        Ok(())
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
    fn sql_pull_index_filter_skips_nonmatching_nullable_rows_before_projection() {
        let (mut sql, mut router) = setup(&[
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "INSERT INTO users (id, name) VALUES (1, first)",
            "INSERT INTO users (id) VALUES (2)",
            "CREATE INDEX users_name ON users(name)",
        ]);
        let rows = sql
            .execute_select(
                "users",
                Projection::All,
                Some(Predicate::Eq {
                    column: "name".into(),
                    value: QueryValue::Text("first".into()),
                }),
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].primary_key, "1");
        sql.execute(&mut router, PlacementGroupId(1), "BEGIN")
            .unwrap();
        sql.execute(
            &mut router,
            PlacementGroupId(1),
            "INSERT INTO users (id) VALUES (3)",
        )
        .unwrap();
        let rows = sql
            .execute_select(
                "users",
                Projection::All,
                Some(Predicate::Eq {
                    column: "name".into(),
                    value: QueryValue::Text("first".into()),
                }),
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].primary_key, "1");
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

#[cfg(test)]
mod indexed_tests;
