use super::*;

#[test]
fn serializable_sessions_reject_write_skew_after_restart_and_follower_recovery() {
    let root = root("write-skew");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    for (statement, id) in [
        ("CREATE TABLE users(id PRIMARY KEY, name)", "table"),
        ("INSERT INTO users (id, name) VALUES (1, on)", "first"),
        ("INSERT INTO users (id, name) VALUES (2, on)", "second"),
    ] {
        auto(&mut sql, &mut router, statement, id);
    }
    let alice = begin(&mut sql, &mut router, "alice", "work", "alice-begin");
    let bob = begin(&mut sql, &mut router, "bob", "work", "bob-begin");
    assert_eq!(
        names(&mut sql, &mut router, "alice", &alice),
        vec!["on", "on"]
    );
    assert_eq!(names(&mut sql, &mut router, "bob", &bob), vec!["on", "on"]);
    run(
        &mut sql,
        &mut router,
        "alice",
        &alice,
        "UPDATE users SET name=off WHERE id=1",
        "alice-off",
    )
    .unwrap();
    let staged = run(
        &mut sql,
        &mut router,
        "bob",
        &bob,
        "UPDATE users SET name=off WHERE id=2",
        "bob-off",
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
    let count = router.committed_application_commands().len();
    let error = run(&mut sql, &mut router, "bob", &bob, "COMMIT", "bob-commit").unwrap_err();
    assert!(error.message.contains("serializable snapshot conflict"));
    assert!(sql.pending.is_none());
    assert_eq!(router.committed_application_commands().len(), count);
    assert_eq!(
        run(
            &mut sql,
            &mut router,
            "bob",
            &bob,
            "UPDATE users SET name=off WHERE id=2",
            "bob-off"
        )
        .unwrap()
        .outcome,
        staged.outcome
    );
    let follower_path = root.join("follower");
    let mut follower = DurableSqlEngine::open_replicated(&follower_path, "tenant").unwrap();
    for command in router.committed_application_commands() {
        follower.apply_application_command(&command).unwrap();
        drop(follower);
        follower = DurableSqlEngine::open(&follower_path).unwrap();
    }
    assert!(
        run(
            &mut follower,
            &mut router,
            "bob",
            &bob,
            "COMMIT",
            "follower-commit"
        )
        .unwrap_err()
        .message
        .contains("serializable snapshot conflict")
    );
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
        vec!["off", "on"]
    );
    run(
        &mut follower,
        &mut router,
        "bob",
        &bob,
        "ROLLBACK",
        "bob-rollback",
    )
    .unwrap();
}

#[test]
fn serializable_sessions_reject_phantoms_and_tombstones_including_read_only_commits() {
    let root = root("phantoms");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "table",
    );
    let first = begin(&mut sql, &mut router, "alice", "first", "begin-first");
    assert!(names(&mut sql, &mut router, "alice", &first).is_empty());
    auto(
        &mut sql,
        &mut router,
        "INSERT INTO users (id, name) VALUES (1, phantom)",
        "insert",
    );
    assert!(names(&mut sql, &mut router, "alice", &first).is_empty());
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &first,
            "COMMIT",
            "first-commit"
        )
        .is_err()
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &first,
        "ROLLBACK",
        "first-abort",
    )
    .unwrap();
    let second = begin(&mut sql, &mut router, "alice", "second", "begin-second");
    assert_eq!(
        names(&mut sql, &mut router, "alice", &second),
        vec!["phantom"]
    );
    auto(
        &mut sql,
        &mut router,
        "DELETE FROM users WHERE id=1",
        "delete",
    );
    assert_eq!(
        names(&mut sql, &mut router, "alice", &second),
        vec!["phantom"]
    );
    assert!(
        run(
            &mut sql,
            &mut router,
            "alice",
            &second,
            "COMMIT",
            "second-commit"
        )
        .is_err()
    );
    run(
        &mut sql,
        &mut router,
        "alice",
        &second,
        "ROLLBACK",
        "second-abort",
    )
    .unwrap();
    let third = begin(&mut sql, &mut router, "alice", "third", "begin-third");
    let result = run(
        &mut sql,
        &mut router,
        "alice",
        &third,
        "COMMIT",
        "third-commit",
    )
    .unwrap();
    assert!(matches!(
        result.outcome,
        SqlOutcome::TransactionCommitted { mutations: 0, .. }
    ));
    assert!(sql.pending.is_none());
}

// Simulate an already certified pre-upgrade writer without using the new live
// submission guard. Complete commands/certificates still use the real router.
fn legacy(
    sql: &mut DurableSqlEngine,
    router: &mut VsrBackedFabricRouter,
    context: &SqlSessionContext,
    statement: &str,
    id: &str,
) -> DurableSqlExecution {
    let request = SessionRequest {
        owner: "alice".into(),
        context: context.clone(),
        client: id.into(),
        statement: statement.into(),
        pg: 1,
    };
    let proposal = propose(sql, request, router.latest_version(), false).unwrap();
    let encoded = proposal.encode().unwrap();
    assert!(encoded.starts_with(b"RSFSP1"));
    assert!(
        !std::str::from_utf8(&encoded)
            .unwrap()
            .contains("serializable")
    );
    assert_eq!(SessionProposal::decode(&encoded).unwrap(), proposal);
    let operation = proposal.operation().unwrap();
    let pending = PendingRequest {
        client: id.into(),
        statement: statement.into(),
        pg: PlacementGroupId(1),
        session: Some(Box::new(proposal)),
    };
    let mut bytes = b"RSFSC2".to_vec();
    bytes.extend(pending.encode().unwrap().as_bytes());
    let app = rsf_protocol::ApplicationPayload {
        namespace: "tenant".into(),
        surface: rsf_protocol::ApplicationSurface::Sql,
        bytes,
    };
    let request = ClientRequestId::new(id, 0);
    let commit = router
        .preview_application_request(
            PlacementGroupId(1),
            &request,
            rsf_protocol::ConsistencyMode::Strict,
            &operation,
            &app,
        )
        .unwrap();
    let receipt = router
        .commit_application_request(
            PlacementGroupId(1),
            request.clone(),
            rsf_protocol::ConsistencyMode::Strict,
            operation,
            app.clone(),
        )
        .unwrap();
    let command = rsf_protocol::ReplicatedFabricCommand {
        request,
        commit,
        application: app,
    };
    assert_eq!(command.certificate(), receipt.certificate);
    sql.apply_application_command(&command).unwrap()
}

#[test]
fn serializable_upgrade_preserves_legacy_receipts_and_fences_old_pending_commits() {
    let root = root("legacy-isolation");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    auto(
        &mut sql,
        &mut router,
        "CREATE TABLE users(id PRIMARY KEY, name)",
        "table",
    );
    let mut old = SqlSessionContext {
        name: "legacy".into(),
        transaction: None,
    };
    let SqlOutcome::TransactionBegan { session_id, .. } =
        legacy(&mut sql, &mut router, &old, "BEGIN", "old-begin").outcome
    else {
        panic!("BEGIN");
    };
    old.transaction = Some(session_id);
    legacy(
        &mut sql,
        &mut router,
        &old,
        "INSERT INTO users (id, name) VALUES (1, old)",
        "old-insert",
    );
    auto(
        &mut sql,
        &mut router,
        "INSERT INTO users (id, name) VALUES (2, concurrent)",
        "concurrent",
    );
    let old_commit = legacy(&mut sql, &mut router, &old, "COMMIT", "old-commit");
    sql.checkpoint().unwrap();
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        run(&mut sql, &mut router, "alice", &old, "COMMIT", "old-commit")
            .unwrap()
            .outcome,
        old_commit.outcome
    );
    // Checkpoint validation must enforce the new isolation promise itself, not
    // merely accept a consistent row/effect chain relabeled as serializable.
    let id = ClientRequestId::new("old-commit", 0);
    let original = sql.commands[&id].clone();
    let mut relabeled = original.clone();
    let mut request = crate::durable::replicated::decode_request(&relabeled, "tenant").unwrap();
    request.session.as_mut().unwrap().serializable = true;
    relabeled.application.bytes = b"RSFSC2".to_vec();
    relabeled
        .application
        .bytes
        .extend(request.encode().unwrap().as_bytes());
    relabeled.commit.operation = request.session.as_ref().unwrap().operation().unwrap();
    relabeled.commit.operation_id = relabeled.commit.operation.operation_id();
    sql.commands.insert(id.clone(), relabeled);
    assert!(
        validate_recovered(&sql)
            .unwrap_err()
            .message
            .contains("serializable SQL session committed across a namespace data change")
    );
    sql.commands.insert(id, original);
    validate_recovered(&sql).unwrap();
    let fresh = begin(&mut sql, &mut router, "alice", "fresh", "new-begin");
    let proposal = sql.receipts["new-begin"].request.session.as_ref().unwrap();
    assert!(proposal.serializable);
    let encoded = proposal.encode().unwrap();
    assert!(encoded.starts_with(b"RSFSP2"));
    let mut downgraded = encoded.clone();
    downgraded[..6].copy_from_slice(b"RSFSP1");
    assert!(SessionProposal::decode(&downgraded).is_err());
    let request = SessionRequest {
        owner: "alice".into(),
        context: fresh.clone(),
        client: "pending-old-commit".into(),
        statement: "COMMIT".into(),
        pg: 1,
    };
    let pending = PendingRequest {
        client: request.client.clone(),
        statement: request.statement.clone(),
        pg: PlacementGroupId(1),
        session: Some(Box::new(
            propose(&sql, request, router.latest_version(), false).unwrap(),
        )),
    };
    auto(
        &mut sql,
        &mut router,
        "INSERT INTO users (id, name) VALUES (3, later)",
        "later",
    );
    let count = router.committed_application_commands().len();
    assert!(
        sql.prepare_replicated(&mut router, &pending)
            .err()
            .unwrap()
            .message
            .contains("legacy pending SQL COMMIT conflicts with upgraded isolation")
    );
    assert_eq!(router.committed_application_commands().len(), count);
    run(
        &mut sql,
        &mut router,
        "alice",
        &fresh,
        "ROLLBACK",
        "new-abort",
    )
    .unwrap();
    let mut follower = DurableSqlEngine::open_replicated(root.join("follower"), "tenant").unwrap();
    for command in router.committed_application_commands() {
        follower.apply_application_command(&command).unwrap();
    }
    follower.checkpoint().unwrap();
    drop(follower);
    DurableSqlEngine::open(root.join("follower")).unwrap();
}
