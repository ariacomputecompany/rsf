use super::*;
use rsf_core::{Epoch, NodeId};
use rsf_protocol::VsrBackedFabricRouter;

mod serializable;

#[test]
fn bound_v5_checkpoint_upgrades_without_changing_autocommit_receipts() {
    let root = root("v5");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    let statement = "CREATE TABLE users(id PRIMARY KEY, name)";
    let original = sql
        .execute(&mut router, PlacementGroupId(1), statement, "create")
        .unwrap();
    sql.checkpoint().unwrap();
    drop(sql);
    let encoded = fs::read_to_string(path.join(STATE_FILE)).unwrap();
    let mut legacy =
        verify_checksum(&encoded)
            .unwrap()
            .replacen("RSF_SQL_STATE_V6", "RSF_SQL_STATE_V5", 1);
    append_checksum(&mut legacy);
    fs::write(path.join(STATE_FILE), legacy).unwrap();
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        sql.execute(&mut router, PlacementGroupId(1), statement, "create")
            .unwrap()
            .outcome,
        original.outcome
    );
    let context = begin(&mut sql, &mut router, "alice", "work", "begin");
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK",
        "rollback",
    )
    .unwrap();
    sql.checkpoint().unwrap();
    drop(sql);
    assert!(
        fs::read_to_string(path.join(STATE_FILE))
            .unwrap()
            .starts_with("RSF_SQL_STATE_V6\n")
    );
    DurableSqlEngine::open(&path).unwrap();
}

#[test]
fn session_limits_reject_before_intents_and_leave_transactions_usable() {
    let root = root("limits");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "table",
    );
    let context = begin(&mut sql, &mut router, "alice", "bounded", "begin");
    for n in 0..MAX_SQL_SESSION_STATEMENTS {
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            &format!("INSERT INTO users (id, name) VALUES ({n}, value)"),
            &format!("insert-{n}"),
        )
        .unwrap();
    }
    let count = router.committed_application_commands().len();
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            "INSERT INTO users (id, name) VALUES (extra, value)",
            "excess"
        )
        .is_err()
    );
    assert!(sql.pending.is_none());
    assert_eq!(router.committed_application_commands().len(), count);
    for n in 0..MAX_SQL_SAVEPOINTS {
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            &format!("SAVEPOINT s{n}"),
            &format!("save-{n}"),
        )
        .unwrap();
    }
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            "SAVEPOINT excess",
            "excess-save"
        )
        .is_err()
    );
    assert!(sql.pending.is_none());
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK",
        "rollback",
    )
    .unwrap();
    assert_eq!(sql.read_snapshot("SELECT * FROM users").unwrap().rows, 0);
    for n in 0..MAX_ACTIVE_SQL_SESSIONS {
        begin(
            &mut sql,
            &mut router,
            "alice",
            &format!("s{n}"),
            &format!("begin-{n}"),
        );
    }
    let count = router.committed_application_commands().len();
    let extra = SqlSessionContext {
        name: "excess".into(),
        transaction: None,
    };
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &extra,
            "BEGIN",
            "excess-begin"
        )
        .is_err()
    );
    assert!(sql.pending.is_none());
    assert_eq!(router.committed_application_commands().len(), count);
    drop(sql);
    let sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    assert_eq!(sql.sessions.len(), MAX_ACTIVE_SQL_SESSIONS);
}

#[test]
fn session_recovery_rejects_divergent_state_results_and_catalog_and_allows_schema_abort() {
    let root = root("validation");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "table",
    );
    let context = begin(&mut sql, &mut router, "alice", "work", "begin");
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "INSERT INTO users (id, name) VALUES (1, ada)",
        "insert",
    )
    .unwrap();
    let proposal = sql.receipts["insert"].request.session.as_ref().unwrap();
    let mut bad = proposal.clone();
    bad.after.as_mut().unwrap().pg += 1;
    assert!(bad.validate().is_err());
    let mut bad = proposal.clone();
    bad.result = super::model::SessionResult::RolledBack {
        token: context.transaction.clone().unwrap(),
        discarded: 1,
    };
    assert!(bad.validate().is_err());
    let mut bad = proposal.clone();
    bad.request.statement = "COMMIT".into();
    assert!(bad.validate().is_err());
    let state = sql.sessions.values().next().unwrap().clone();
    let mut bad_state = state.clone();
    bad_state.statements = vec![format!(
        "INSERT INTO users (id, name) VALUES (1, '{}')",
        "x".repeat(MAX_SQL_SESSION_BYTES)
    )];
    assert!(bad_state.validate().is_err());
    sql.sessions.clear();
    assert!(validate_recovered(&sql).is_err());
    sql.sessions.insert(key("alice", "work"), state);
    validate_recovered(&sql).unwrap();
    let saved = sql.engine.clone();
    sql.engine = SqlEngine::default();
    assert!(validate_recovered(&sql).is_err());
    sql.engine = saved;
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE other(id PRIMARY KEY)",
        "other-table",
    );
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            "SELECT * FROM users",
            "read"
        )
        .is_err()
    );
    assert!(run(&mut sql, &mut router, "alice", &context, "COMMIT", "commit").is_err());
    assert!(sql.pending.is_none());
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK",
        "rollback",
    )
    .unwrap();
    drop(sql);
    let sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    assert!(sql.sessions.is_empty());
}

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-sql-session-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn router(root: &Path) -> VsrBackedFabricRouter {
    VsrBackedFabricRouter::open(
        root.join("router"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0)],
    )
    .unwrap()
}
fn run(
    sql: &mut DurableSqlEngine,
    router: &mut VsrBackedFabricRouter,
    owner: &str,
    context: &SqlSessionContext,
    statement: &str,
    id: &str,
) -> RsfResult<DurableSqlExecution> {
    sql.execute_session(router, PlacementGroupId(1), owner, context, statement, id)
}
fn begin(
    sql: &mut DurableSqlEngine,
    router: &mut VsrBackedFabricRouter,
    owner: &str,
    name: &str,
    id: &str,
) -> SqlSessionContext {
    let mut context = SqlSessionContext {
        name: name.into(),
        transaction: None,
    };
    let result = run(sql, router, owner, &context, "BEGIN", id).unwrap();
    let SqlOutcome::TransactionBegan { session_id, .. } = result.outcome else {
        panic!("BEGIN result")
    };
    assert!(result.persisted);
    assert!(result.version.is_some());
    context.transaction = Some(session_id);
    context
}
fn names(
    sql: &mut DurableSqlEngine,
    router: &mut VsrBackedFabricRouter,
    owner: &str,
    context: &SqlSessionContext,
) -> Vec<String> {
    let SqlOutcome::Selected { rows } =
        run(sql, router, owner, context, "SELECT * FROM users", "read")
            .unwrap()
            .outcome
    else {
        panic!("SELECT result")
    };
    rows.into_iter()
        .map(|row| row.columns["name"].clone())
        .collect()
}
fn auto(sql: &mut DurableSqlEngine, router: &mut VsrBackedFabricRouter, statement: &str, id: &str) {
    sql.execute(router, PlacementGroupId(1), statement, id)
        .unwrap();
}

#[test]
fn durable_sessions_commit_schema_rows_and_savepoints_atomically_and_replay_original_results() {
    let root = root("atomic");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    let context = begin(&mut sql, &mut router, "alice", "work", "begin");
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "create",
    )
    .unwrap();
    let inserted = run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "INSERT INTO users (id, name) VALUES (1, ada)",
        "insert",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "SAVEPOINT keep",
        "savepoint",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "UPDATE users SET name=grace WHERE id=1",
        "update",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "INSERT INTO users (id, name) VALUES (2, linus)",
        "second",
    )
    .unwrap();
    assert_eq!(
        names(&mut sql, &mut router, "alice", &context),
        vec!["grace", "linus"]
    );
    assert!(sql.read_snapshot("SELECT * FROM users").is_err());
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        names(&mut sql, &mut router, "alice", &context),
        vec!["grace", "linus"]
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK TO keep",
        "rollback-to",
    )
    .unwrap();
    assert_eq!(names(&mut sql, &mut router, "alice", &context), vec!["ada"]);
    let committed = run(&mut sql, &mut router, "alice", &context, "COMMIT", "commit").unwrap();
    assert!(matches!(
        committed.outcome,
        SqlOutcome::TransactionCommitted { mutations: 2, .. }
    ));
    assert_eq!(sql.read_snapshot("SELECT * FROM users").unwrap().rows, 1);
    let versions = sql.engine.committed_kv_versions_for_checkpoint();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].1.len(), 1);
    assert_eq!(versions[0].1[0].version, committed.version.unwrap());
    sql.checkpoint().unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    let count = router.committed_application_commands().len();
    let replay = run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "INSERT INTO users (id, name) VALUES (1, ada)",
        "insert",
    )
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.outcome, inserted.outcome);
    assert_eq!(replay.version, inserted.version);
    let replay = run(&mut sql, &mut router, "alice", &context, "COMMIT", "commit").unwrap();
    assert_eq!(replay.outcome, committed.outcome);
    assert!(replay.replayed);
    assert_eq!(router.committed_application_commands().len(), count);
    assert!(sql.sessions.is_empty());
}

#[test]
fn durable_sessions_isolate_owners_detect_snapshot_conflicts_and_preserve_unrelated_commits() {
    let root = root("isolation");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "create",
    );
    auto(
        &mut sql,
        &mut router,
        "INSERT INTO users (id, name) VALUES (1, ada)",
        "insert",
    );
    let alice = begin(&mut sql, &mut router, "alice", "work", "alice-begin");
    let bob = begin(&mut sql, &mut router, "bob", "work", "bob-begin");
    run(
        &mut sql,
        &mut router,
        "alice",
        &alice,
        "UPDATE users SET name=grace WHERE id=1",
        "alice-update",
    )
    .unwrap();
    assert_eq!(names(&mut sql, &mut router, "bob", &bob), vec!["ada"]);
    assert!(run(&mut sql, &mut router, "bob", &alice, "ROLLBACK", "steal").is_err());
    assert!(
        sql.execute_session(
            &mut router,
            PlacementGroupId(2),
            "alice",
            &alice,
            "ROLLBACK",
            "wrong-pg"
        )
        .is_err()
    );
    auto(
        &mut sql,
        &mut router,
        "UPDATE users SET name=linus WHERE id=1",
        "concurrent-update",
    );
    let before = router.latest_version();
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &alice,
            "COMMIT",
            "conflicting-commit"
        )
        .unwrap_err()
        .message
        .contains("conflict")
    );
    assert_eq!(router.latest_version(), before);
    run(
        &mut sql,
        &mut router,
        "bob",
        &bob,
        "INSERT INTO users (id, name) VALUES (2, barbara)",
        "bob-insert",
    )
    .unwrap();
    assert!(run(&mut sql, &mut router, "bob", &bob, "COMMIT", "bob-commit").is_err());
    run(&mut sql, &mut router, "bob", &bob, "ROLLBACK", "bob-abort").unwrap();
    let bob = begin(&mut sql, &mut router, "bob", "work", "bob-fresh");
    run(
        &mut sql,
        &mut router,
        "bob",
        &bob,
        "INSERT INTO users (id, name) VALUES (2, barbara)",
        "bob-fresh-insert",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "bob",
        &bob,
        "COMMIT",
        "bob-fresh-commit",
    )
    .unwrap();
    let SqlOutcome::Selected { rows } = sql.read_snapshot("SELECT * FROM users").unwrap().outcome
    else {
        panic!("SELECT")
    };
    assert_eq!(
        rows.iter()
            .map(|row| row.columns["name"].as_str())
            .collect::<Vec<_>>(),
        vec!["linus", "barbara"]
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &alice,
        "ROLLBACK",
        "alice-rollback",
    )
    .unwrap();
    let fresh = begin(&mut sql, &mut router, "alice", "work", "alice-fresh");
    assert_ne!(fresh.transaction, alice.transaction);
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &alice,
            "COMMIT",
            "stale-commit"
        )
        .is_err()
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &fresh,
        "ROLLBACK",
        "fresh-rollback",
    )
    .unwrap();
}

#[test]
fn durable_session_commands_replay_active_transactions_and_indexes_after_each_follower_restart() {
    let root = root("follower");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "create",
    );
    auto(
        &mut sql,
        &mut router,
        "INSERT INTO users (id, name) VALUES (1, ada)",
        "insert",
    );
    let context = begin(&mut sql, &mut router, "alice", "work", "begin");
    for (id, statement) in [
        ("update", "UPDATE users SET name=grace WHERE id=1"),
        ("index", "CREATE INDEX users_name ON users(name)"),
        ("commit", "COMMIT"),
    ] {
        run(&mut sql, &mut router, "alice", &context, statement, id).unwrap();
    }
    let path = root.join("follower");
    let mut follower = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    for command in router.committed_application_commands() {
        follower.apply_application_command(&command).unwrap();
        drop(follower);
        follower = DurableSqlEngine::open(&path).unwrap();
    }
    assert_eq!(
        follower
            .read_snapshot("SELECT * FROM users WHERE name=grace")
            .unwrap()
            .rows,
        1
    );
    assert!(follower.sessions.is_empty());
    for command in router.committed_application_commands() {
        assert!(
            follower
                .apply_application_command(&command)
                .unwrap()
                .replayed
        );
    }
}

#[test]
fn session_context_rejects_noncanonical_unknown_and_oversized_inputs() {
    let value = SqlSessionContext {
        name: "work".into(),
        transaction: None,
    };
    assert_eq!(
        SqlSessionContext::decode(&value.encode().unwrap()).unwrap(),
        value
    );
    for text in [
        "{\"name\":\"work\",\"transaction\":null,\"owner\":\"bob\"}",
        "{\"name\":\"work\", \"transaction\":null}",
        "{\"name\":\"work\",\"transaction\":\"stale\"}",
    ] {
        let mut bytes = CONTEXT_MAGIC.to_vec();
        bytes.extend_from_slice(text.as_bytes());
        assert!(SqlSessionContext::decode(&bytes).is_err());
    }
    assert!(SqlSessionContext::decode(&vec![0; 4097]).is_err());
    assert!(
        SqlSessionContext {
            name: "bad\nname".into(),
            transaction: None
        }
        .encode()
        .is_err()
    );
}
