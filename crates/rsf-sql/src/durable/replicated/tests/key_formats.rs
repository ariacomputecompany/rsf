use super::*;

#[test]
fn sql_index_rebuilds_do_not_clear_another_tables_same_named_index() {
    let root = root("index-boundaries");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    for (statement, id) in [
        ("CREATE TABLE a_b(id PRIMARY KEY, c)", "left-table"),
        ("CREATE TABLE a(id PRIMARY KEY, b_c)", "right-table"),
        ("INSERT INTO a_b (id, c) VALUES (1, left)", "left-row"),
        ("INSERT INTO a (id, b_c) VALUES (2, right)", "right-row"),
        ("CREATE INDEX first ON a_b(c)", "left-index"),
        ("CREATE INDEX second ON a(b_c)", "right-index"),
    ] {
        execute(&mut sql, &mut router, statement, id).unwrap();
    }
    assert_eq!(
        sql.engine.select_eq("a_b", "c", "left").len(),
        1,
        "index rebuild must preserve the other table's index entry"
    );
    assert_eq!(sql.engine.select_eq("a", "b_c", "right").len(), 1);
    for statement in [
        "SELECT * FROM a_b WHERE c=left",
        "SELECT * FROM a WHERE b_c=right",
    ] {
        assert_eq!(sql.read_snapshot(statement).unwrap().rows, 1, "{statement}");
    }
    sql.checkpoint().unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        sql.read_snapshot("SELECT * FROM a_b WHERE c=left")
            .unwrap()
            .rows,
        1
    );
    assert_eq!(
        sql.read_snapshot("SELECT * FROM a WHERE b_c=right")
            .unwrap()
            .rows,
        1
    );
}

#[test]
fn legacy_key_namespaces_reject_aliasing_before_submission_and_cold_followers_inherit_format() {
    let root = root("legacy-key-namespace");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = crate::durable::legacy_replicated(&path, "tenant");
    execute(
        &mut sql,
        &mut router,
        "CREATE TABLE a(id PRIMARY KEY, name)",
        "create",
    )
    .unwrap();
    let count = router.committed_application_commands().len();
    assert!(
        execute(
            &mut sql,
            &mut router,
            "CREATE TABLE a/pk/b(id PRIMARY KEY, name)",
            "alias"
        )
        .unwrap_err()
        .message
        .contains("key namespaces overlap")
    );
    assert!(sql.pending.is_none());
    assert_eq!(router.committed_application_commands().len(), count);
    let inserted = execute(
        &mut sql,
        &mut router,
        "INSERT INTO a (id, name) VALUES ('b/pk/c', preserved)",
        "insert",
    )
    .unwrap();
    sql.checkpoint().unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        sql.engine.key_format(),
        RelationalKeyFormat::LegacyDelimitedV1
    );
    assert_eq!(
        execute(
            &mut sql,
            &mut router,
            "INSERT INTO a (id, name) VALUES ('b/pk/c', preserved)",
            "insert"
        )
        .unwrap()
        .outcome,
        inserted.outcome
    );
    let follower_path = root.join("follower");
    let mut follower = DurableSqlEngine::open_replicated(&follower_path, "tenant").unwrap();
    assert_eq!(
        follower.engine.key_format(),
        RelationalKeyFormat::LengthPrefixedV2
    );
    for command in router.committed_application_commands() {
        follower.apply_application_command(&command).unwrap();
        drop(follower);
        follower = DurableSqlEngine::open(&follower_path).unwrap();
        assert_eq!(
            follower.engine.key_format(),
            RelationalKeyFormat::LegacyDelimitedV1
        );
    }
    assert_eq!(follower.read_snapshot("SELECT * FROM a").unwrap().rows, 1);
}

#[test]
fn overlapping_legacy_catalog_recovery_fails_repeatedly_without_rewriting_metadata() {
    let root = root("unsafe-legacy-catalog");
    fs::create_dir_all(&root).unwrap();
    let mut engine = SqlEngine::default();
    engine
        .create_table_with_columns("a", "id", ["id", "name"])
        .unwrap();
    engine
        .create_table_with_columns("a/pk/b", "id", ["id", "name"])
        .unwrap();
    let checkpoint = SqlCheckpoint {
        key_format: RelationalKeyFormat::LegacyDelimitedV1,
        tables: engine.recovered_tables_for_checkpoint(),
        kv: vec![],
        receipts: BTreeMap::new(),
        namespace: None,
        commands: BTreeMap::new(),
        journal: None,
        sessions: BTreeMap::new(),
    };
    let encoded = encode_checkpoint(&checkpoint).unwrap();
    fs::write(root.join(STATE_FILE), &encoded).unwrap();
    for _ in 0..2 {
        let error = DurableSqlEngine::open(&root).unwrap_err();
        assert_eq!(error.code, ErrorCode::CorruptionDetected);
        assert!(error.message.contains("key namespaces overlap"));
        assert_eq!(fs::read_to_string(root.join(STATE_FILE)).unwrap(), encoded);
    }
}

#[test]
fn framed_key_recovery_rejects_mixed_or_unowned_rows_and_format_downgrades() {
    let root = root("key-format-fences");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create").unwrap();
    let original = execute(&mut sql, &mut router, INSERT, "insert").unwrap();
    let tables = sql.engine.recovered_tables_for_checkpoint();
    let mut mixed = sql.engine.kv.clone();
    mixed
        .apply_committed_batch(
            CommitVersion(99),
            vec![rsf_kv::KvMutation::Set {
                key: RelationalKeyFormat::LegacyDelimitedV1.row_key("users", "foreign"),
                value: b"foreign legacy row".to_vec(),
            }],
        )
        .unwrap();
    assert!(SqlEngine::recover_from_committed_kv(mixed.clone(), tables.clone()).is_err());
    assert!(
        SqlEngine::recover_from_committed_kv_with_format(
            mixed,
            tables.clone(),
            RelationalKeyFormat::LengthPrefixedV2
        )
        .is_err()
    );
    let mut orphan = sql.engine.kv.clone();
    orphan
        .apply_committed_batch(
            CommitVersion(99),
            vec![rsf_kv::KvMutation::Set {
                key: RelationalKeyFormat::default().row_key("unknown", "1"),
                value: b"orphan row".to_vec(),
            }],
        )
        .unwrap();
    assert!(
        SqlEngine::recover_from_committed_kv_with_format(
            orphan,
            tables,
            RelationalKeyFormat::LengthPrefixedV2
        )
        .is_err()
    );
    sql.checkpoint().unwrap();
    drop(sql);
    let original_bytes = fs::read_to_string(path.join(STATE_FILE)).unwrap();
    assert!(original_bytes.starts_with(FRAMED_JOURNALED_FORMAT));
    let mut downgraded = verify_checksum(&original_bytes).unwrap().replacen(
        FRAMED_JOURNALED_FORMAT,
        JOURNALED_FORMAT,
        1,
    );
    append_checksum(&mut downgraded);
    fs::write(path.join(STATE_FILE), &downgraded).unwrap();
    for _ in 0..2 {
        assert!(DurableSqlEngine::open(&path).is_err());
        assert_eq!(
            fs::read_to_string(path.join(STATE_FILE)).unwrap(),
            downgraded
        );
    }
    fs::write(path.join(STATE_FILE), original_bytes).unwrap();
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        execute(&mut sql, &mut router, INSERT, "insert")
            .unwrap()
            .outcome,
        original.outcome
    );
}

#[test]
fn framed_command_key_format_is_bound_and_cannot_switch_a_nonempty_namespace() {
    let root = root("command-key-format");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create").unwrap();
    let command = router.committed_application_commands().remove(0);
    let mut request = decode_request(&command, "tenant").unwrap();
    assert_eq!(request.key_format, RelationalKeyFormat::LengthPrefixedV2);
    request.key_format = RelationalKeyFormat::LegacyDelimitedV1;
    let mut confused = command.clone();
    confused.application.bytes = FRAMED_MAGIC.to_vec();
    confused.application.bytes.push(0);
    confused
        .application
        .bytes
        .extend(request.encode().unwrap().as_bytes());
    let mut follower = DurableSqlEngine::open_replicated(root.join("follower"), "tenant").unwrap();
    assert!(follower.apply_application_command(&confused).is_err());
    assert!(follower.commands.is_empty());
    follower.apply_application_command(&command).unwrap();
    let legacy_root = root.join("legacy");
    let mut legacy_router = self::router(&legacy_root);
    let mut legacy = crate::durable::legacy_replicated(legacy_root.join("sql"), "tenant");
    execute(
        &mut legacy,
        &mut legacy_router,
        "CREATE TABLE other(id PRIMARY KEY)",
        "other",
    )
    .unwrap();
    let legacy_command = legacy_router.committed_application_commands().remove(0);
    assert!(follower.apply_application_command(&legacy_command).is_err());
    assert_eq!(follower.commands.len(), 1);
    assert_eq!(
        follower.engine.key_format(),
        RelationalKeyFormat::LengthPrefixedV2
    );
}
