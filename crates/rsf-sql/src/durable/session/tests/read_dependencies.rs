use super::*;

fn setup(sql: &mut DurableSqlEngine, router: &mut VsrBackedFabricRouter) {
    for (statement, id) in [
        ("CREATE TABLE users(id PRIMARY KEY, name)", "table"),
        ("INSERT INTO users (id, name) VALUES (1, left)", "left"),
        ("INSERT INTO users (id, name) VALUES (2, right)", "right"),
    ] {
        auto(sql, router, statement, id);
    }
}

#[test]
fn tracked_reads_allow_disjoint_commits_and_replay_after_every_follower_restart() {
    let root = root("disjoint");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    setup(&mut sql, &mut router);
    let alice = begin(&mut sql, &mut router, "alice", "work", "alice-begin");
    let bob = begin(&mut sql, &mut router, "bob", "work", "bob-begin");
    let query = "SELECT name FROM users WHERE id=1";
    let selected = run(&mut sql, &mut router, "alice", &alice, query, "read").unwrap();
    assert!(selected.persisted && selected.version.is_some());
    assert!(selected.read_snapshot.is_some());
    assert!(selected.read_snapshot < selected.version);
    let count = router.committed_application_commands().len();
    let retry = run(&mut sql, &mut router, "alice", &alice, query, "read").unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.outcome, selected.outcome);
    assert_eq!(router.committed_application_commands().len(), count);
    run(
        &mut sql,
        &mut router,
        "bob",
        &bob,
        "SELECT * FROM users WHERE id=2",
        "read",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &alice,
        "UPDATE users SET name=alice WHERE id=1",
        "alice-update",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "bob",
        &bob,
        "UPDATE users SET name=bob WHERE id=2",
        "bob-update",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &alice,
        "COMMIT",
        "alice-commit",
    )
    .unwrap();
    sql.checkpoint().unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    // The first transaction changed a row outside Bob's point predicate/write set.
    run(&mut sql, &mut router, "bob", &bob, "COMMIT", "bob-commit").unwrap();
    let follower_path = root.join("follower");
    let mut follower = DurableSqlEngine::open_replicated(&follower_path, "tenant").unwrap();
    for command in router.committed_application_commands() {
        follower.apply_application_command(&command).unwrap();
        drop(follower);
        follower = DurableSqlEngine::open(&follower_path).unwrap();
    }
    let SqlOutcome::Selected { rows } = follower
        .read_snapshot("SELECT * FROM users")
        .unwrap()
        .outcome
    else {
        panic!("rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row.columns["name"].as_str())
            .collect::<Vec<_>>(),
        vec!["alice", "bob"]
    );
    assert!(follower.sessions.is_empty());
}

#[test]
fn savepoint_rewinds_retain_predicates_and_implicit_absence_reads_after_restart() {
    let root = root("savepoint-reads");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    setup(&mut sql, &mut router);
    let context = begin(&mut sql, &mut router, "alice", "query", "begin");
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "SAVEPOINT keep",
        "keep",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "SELECT * FROM users WHERE name=left",
        "read",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "INSERT INTO users (id, name) VALUES (9, temporary)",
        "temp",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK TO keep",
        "rewind",
    )
    .unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    // Moving a row OUT of the observed predicate must invalidate the old read.
    auto(
        &mut sql,
        &mut router,
        "UPDATE users SET name=departed WHERE id=1",
        "move-out",
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "UPDATE users SET name=influenced WHERE id=2",
        "influenced",
    )
    .unwrap();
    assert!(
        run(&mut sql, &mut router, "alice", &context, "COMMIT", "commit")
            .unwrap_err()
            .message
            .contains("serializable snapshot conflict")
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK",
        "abort",
    )
    .unwrap();
    let context = begin(&mut sql, &mut router, "alice", "absence", "absence-begin");
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "SAVEPOINT keep",
        "absence-keep",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "INSERT INTO users (id, name) VALUES (9, temporary)",
        "absence-temp",
    )
    .unwrap();
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK TO keep",
        "absence-rewind",
    )
    .unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    // The insert's successful absence check was observed even though rewound.
    auto(
        &mut sql,
        &mut router,
        "INSERT INTO users (id, name) VALUES (9, external)",
        "external",
    );
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            "COMMIT",
            "absence-commit"
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
        "absence-abort",
    )
    .unwrap();
}

#[test]
fn tracked_read_limits_fail_before_intents_and_identical_reads_reuse_certificates() {
    let root = root("read-limits");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "table",
    );
    let context = begin(&mut sql, &mut router, "alice", "bounded", "begin");
    for n in 0..MAX_SQL_SESSION_DEPENDENCIES {
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            &format!("SELECT * FROM users WHERE id=missing{n}"),
            "read",
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
            "SELECT * FROM users WHERE id=excess",
            "read"
        )
        .is_err()
    );
    assert!(sql.pending.is_none());
    assert_eq!(router.committed_application_commands().len(), count);
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK",
        "abort",
    )
    .unwrap();
    for n in 0..10 {
        auto(
            &mut sql,
            &mut router,
            &format!(
                "INSERT INTO users (id, name) VALUES ({n}, '{}')",
                "x".repeat(3000)
            ),
            &format!("row-{n}"),
        );
    }
    let context = begin(&mut sql, &mut router, "alice", "rows", "rows-begin");
    let count = router.committed_application_commands().len();
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            "SELECT * FROM users",
            "read"
        )
        .unwrap_err()
        .message
        .contains("result exceeds limit")
    );
    assert!(sql.pending.is_none());
    assert_eq!(router.committed_application_commands().len(), count);
    let first = run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "SELECT * FROM users LIMIT 1",
        "read",
    )
    .unwrap();
    let count = router.committed_application_commands().len();
    let again = run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "SELECT * FROM users LIMIT 1",
        "read",
    )
    .unwrap();
    assert!(again.replayed);
    assert_eq!(again.version, first.version);
    assert_eq!(again.outcome, first.outcome);
    assert_eq!(router.committed_application_commands().len(), count);
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK",
        "rows-abort",
    )
    .unwrap();
}

#[test]
fn legacy_active_sessions_keep_coarse_validation_and_original_rsfsp2_receipts() {
    let root = root("coarse-compatibility");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    setup(&mut sql, &mut router);
    let mut context = SqlSessionContext {
        name: "old".into(),
        transaction: None,
    };
    let SqlOutcome::TransactionBegan { session_id, .. } = super::serializable::old_format(
        &mut sql,
        &mut router,
        &context,
        "BEGIN",
        "old-begin",
        true,
    )
    .outcome
    else {
        panic!("BEGIN");
    };
    context.transaction = Some(session_id);
    let old_commit = super::serializable::old_format(
        &mut sql,
        &mut router,
        &context,
        "COMMIT",
        "old-commit",
        true,
    );
    sql.checkpoint().unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            "COMMIT",
            "old-commit"
        )
        .unwrap()
        .outcome,
        old_commit.outcome
    );
    context.transaction = None;
    let SqlOutcome::TransactionBegan { session_id, .. } = super::serializable::old_format(
        &mut sql,
        &mut router,
        &context,
        "BEGIN",
        "old-active",
        true,
    )
    .outcome
    else {
        panic!("BEGIN");
    };
    context.transaction = Some(session_id);
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "SELECT * FROM users WHERE id=1",
        "read",
    )
    .unwrap();
    auto(
        &mut sql,
        &mut router,
        "UPDATE users SET name=other WHERE id=2",
        "other-update",
    );
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &context,
            "COMMIT",
            "old-active-commit"
        )
        .is_err()
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &context,
        "ROLLBACK",
        "old-active-abort",
    )
    .unwrap();
}
