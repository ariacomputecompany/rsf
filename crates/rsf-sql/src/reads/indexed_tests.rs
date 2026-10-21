use super::*;
use rsf_protocol::FabricRouter;
fn execute(sql: &mut SqlEngine, router: &mut FabricRouter, statement: &str) {
    sql.execute(router, PlacementGroupId(1), statement).unwrap();
}
#[test]
fn indexed_select_uses_only_matching_candidates_and_stops_at_limit() {
    let mut sql = SqlEngine::default();
    let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
    for statement in [
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "CREATE INDEX names ON users(name)",
        "INSERT INTO users (id, name) VALUES (1, wanted)",
        "INSERT INTO users (id, name) VALUES (2, other)",
        "INSERT INTO users (id, name) VALUES (3, wanted)",
    ] {
        execute(&mut sql, &mut router, statement);
    }
    let key = sql.row_key("users", "2");
    sql.kv
        .apply_committed_set(key, b"unrelated corrupt row".to_vec(), CommitVersion(100));
    let predicate = || {
        Some(Predicate::Eq {
            column: "name".into(),
            value: QueryValue::Text("wanted".into()),
        })
    };
    let rows = sql
        .execute_select("users", Projection::All, predicate(), None)
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.primary_key.as_str())
            .collect::<Vec<_>>(),
        ["1", "3"]
    );
    let key = sql.row_key("users", "3");
    sql.kv.apply_committed_set(
        key,
        b"later matching corrupt row".to_vec(),
        CommitVersion(101),
    );
    assert_eq!(
        sql.execute_select("users", Projection::All, predicate(), Some(1))
            .unwrap()
            .len(),
        1
    );
    assert!(
        sql.execute_select("users", Projection::All, predicate(), None)
            .is_err()
    );
}

#[test]
fn typed_secondary_index_keys_are_canonical_across_writes_and_recovery() {
    let mut sql = SqlEngine::default();
    let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
    sql.register_table_descriptor(TableDescriptor {
        namespace: NamespaceName("app".into()),
        name: TableName("typed".into()),
        columns: vec![
            ColumnDescriptor {
                name: "id".into(),
                ty: ColumnType::Text,
                nullable: false,
            },
            ColumnDescriptor {
                name: "n".into(),
                ty: ColumnType::Int64,
                nullable: true,
            },
            ColumnDescriptor {
                name: "b".into(),
                ty: ColumnType::Bool,
                nullable: true,
            },
        ],
        primary_key: vec!["id".into()],
    })
    .unwrap();
    for statement in [
        "CREATE INDEX numbers ON typed(n)",
        "CREATE INDEX flags ON typed(b)",
        "INSERT INTO typed (id, n, b) VALUES (1, 001, TRUE)",
    ] {
        execute(&mut sql, &mut router, statement);
    }
    assert_eq!(sql.select_eq("typed", "n", "1").len(), 1);
    assert_eq!(sql.select_eq("typed", "b", "true").len(), 1);
    execute(
        &mut sql,
        &mut router,
        "UPDATE typed SET n=002, b=FALSE WHERE id=1",
    );
    assert!(sql.select_eq("typed", "n", "1").is_empty());
    assert!(sql.select_eq("typed", "b", "true").is_empty());
    assert_eq!(sql.select_eq("typed", "n", "2").len(), 1);
    assert_eq!(sql.select_eq("typed", "b", "false").len(), 1);
    execute(&mut sql, &mut router, "BEGIN");
    execute(
        &mut sql,
        &mut router,
        "UPDATE typed SET n=003, b=TRUE WHERE id=1",
    );
    for (column, value) in [
        ("n", QueryValue::Text("3".into())),
        ("b", QueryValue::Text("true".into())),
    ] {
        assert_eq!(
            sql.execute_select(
                "typed",
                Projection::All,
                Some(Predicate::Eq {
                    column: column.into(),
                    value
                }),
                None
            )
            .unwrap()
            .len(),
            1
        );
    }
    execute(&mut sql, &mut router, "ROLLBACK");
    let (mut recovered, _) =
        SqlEngine::recover_from_committed_kv(sql.kv.clone(), sql.recovered_tables_for_checkpoint())
            .unwrap();
    assert_eq!(recovered.select_eq("typed", "n", "2").len(), 1);
    assert_eq!(recovered.select_eq("typed", "b", "false").len(), 1);
    execute(&mut recovered, &mut router, "DELETE FROM typed WHERE id=1");
    assert!(recovered.select_eq("typed", "n", "2").is_empty());
    assert!(recovered.select_eq("typed", "b", "false").is_empty());
}

#[test]
fn indexed_transaction_overlay_merges_entries_movements_and_savepoint_rewinds() {
    let mut sql = SqlEngine::default();
    let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
    for statement in [
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "CREATE INDEX names ON users(name)",
        "INSERT INTO users (id, name) VALUES (1, wanted)",
        "INSERT INTO users (id, name) VALUES (2, other)",
        "INSERT INTO users (id, name) VALUES (3, wanted)",
        "BEGIN",
        "SAVEPOINT start",
        "UPDATE users SET name=other WHERE id=1",
        "UPDATE users SET name=wanted WHERE id=2",
        "DELETE FROM users WHERE id=3",
        "INSERT INTO users (id, name) VALUES (0, wanted)",
    ] {
        execute(&mut sql, &mut router, statement);
    }
    let predicate = || {
        Some(Predicate::Eq {
            column: "name".into(),
            value: QueryValue::Text("wanted".into()),
        })
    };
    let keys = |sql: &mut SqlEngine| {
        sql.execute_select("users", Projection::All, predicate(), None)
            .unwrap()
            .into_iter()
            .map(|row| row.primary_key)
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&mut sql), ["0", "2"]);
    execute(&mut sql, &mut router, "ROLLBACK TO start");
    assert_eq!(keys(&mut sql), ["1", "3"]);
    execute(
        &mut sql,
        &mut router,
        "UPDATE users SET name=wanted WHERE id=2",
    );
    execute(&mut sql, &mut router, "COMMIT");
    assert_eq!(keys(&mut sql), ["1", "2", "3"]);
    let (mut recovered, _) =
        SqlEngine::recover_from_committed_kv(sql.kv.clone(), sql.recovered_tables_for_checkpoint())
            .unwrap();
    assert_eq!(keys(&mut recovered), ["1", "2", "3"]);
}

#[test]
fn indexed_reads_reject_missing_or_mismatched_committed_entries() {
    for missing in [false, true] {
        let mut sql = SqlEngine::default();
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        for statement in [
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "CREATE INDEX names ON users(name)",
            "INSERT INTO users (id, name) VALUES (1, other)",
        ] {
            execute(&mut sql, &mut router, statement);
        }
        let table = sql.tables["users"].clone();
        let descriptor = secondary_index_descriptor(&table, "name");
        let fake = RowImage {
            primary_key: if missing { "missing" } else { "1" }.into(),
            values: BTreeMap::from([("name".into(), "wanted".into())]),
        };
        let plan = IndexPlanner::new(&table.descriptor, vec![descriptor])
            .unwrap()
            .plan_insert(&table.descriptor, &fake)
            .unwrap();
        sql.indexes.apply(&plan);
        let error = sql
            .execute_select(
                "users",
                Projection::All,
                Some(Predicate::Eq {
                    column: "name".into(),
                    value: QueryValue::Text("wanted".into()),
                }),
                None,
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::CorruptionDetected);
    }
}

#[test]
fn query_size_metadata_tracks_physical_writes_transactions_and_recovery() {
    fn check(sql: &SqlEngine) {
        let mut total = 0;
        for table in sql.tables.keys() {
            let prefix = sql.row_prefix(table);
            let actual =
                sql.kv
                    .iter_prefix(&prefix)
                    .fold(QuerySize::default(), |size, (_, value)| QuerySize {
                        rows: size.rows + 1,
                        bytes: size.bytes + value.value.len() as u64,
                    });
            assert_eq!(sql.query_sizes[table], actual);
            assert_eq!(sql.table_stats(&sql.tables[table]).rows, actual.rows);
            total += actual.rows as usize;
        }
        assert_eq!(sql.row_count(), total);
    }
    let mut sql = SqlEngine::default();
    let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
    for statement in [
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "CREATE TABLE empty(id PRIMARY KEY)",
        "INSERT INTO users (id, name) VALUES (1, first)",
        "UPDATE users SET name=longer_name WHERE id=1",
        "BEGIN",
        "INSERT INTO users (id, name) VALUES (2, staged)",
        "ROLLBACK",
        "BEGIN",
        "INSERT INTO users (id, name) VALUES (3, committed)",
        "COMMIT",
        "DELETE FROM users WHERE id=1",
    ] {
        execute(&mut sql, &mut router, statement);
        check(&sql);
    }
    let (mut recovered, _) =
        SqlEngine::recover_from_committed_kv(sql.kv.clone(), sql.recovered_tables_for_checkpoint())
            .unwrap();
    check(&recovered);
    execute(&mut recovered, &mut router, "DELETE FROM users WHERE id=3");
    check(&recovered);
    assert_eq!(recovered.row_count(), 0);
}
