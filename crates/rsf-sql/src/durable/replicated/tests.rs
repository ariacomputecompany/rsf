use super::*;
use rsf_core::{Epoch, NodeId};
use rsf_protocol::{CommitCertificate, FabricCommit, RequestCommitEvidence, VsrBackedFabricRouter};

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-sql-command-{label}-{}-{}",
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
fn execute(
    sql: &mut DurableSqlEngine,
    router: &mut impl FabricRouterPort,
    statement: &str,
    request: &str,
) -> RsfResult<DurableSqlExecution> {
    sql.execute(router, PlacementGroupId(1), statement, request)
}
const CREATE: &str = "CREATE TABLE users(id PRIMARY KEY, name)";
const INSERT: &str = "INSERT INTO users (id, name) VALUES (1, ada)";
const UPDATE: &str = "UPDATE users SET name=grace WHERE id=1";
const READ: &str = "SELECT * FROM users";

#[test]
fn sql_commands_replay_schema_rows_indexes_tombstones_and_original_results() {
    let root = root("replay");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    let mut results = Vec::new();
    for (i, statement) in [
        CREATE,
        INSERT,
        "CREATE INDEX users_name ON users(name)",
        UPDATE,
        "DELETE FROM users WHERE id=1",
    ]
    .iter()
    .enumerate()
    {
        results.push(execute(&mut sql, &mut router, statement, &format!("request-{i}")).unwrap());
    }
    let old = execute(&mut sql, &mut router, INSERT, "request-1").unwrap();
    assert_eq!(old.outcome, results[1].outcome);
    assert!(old.replayed);
    assert_eq!(
        execute(&mut sql, &mut router, READ, "read").unwrap().rows,
        0
    );
    assert!(execute(&mut sql, &mut router, UPDATE, "request-1").is_err());
    let commands = router.committed_application_commands();
    assert_eq!(commands.len(), 5);
    let mut follower = DurableSqlEngine::open_replicated(root.join("follower"), "tenant").unwrap();
    for (i, command) in commands.iter().enumerate() {
        assert_eq!(
            follower.apply_application_command(command).unwrap().outcome,
            results[i].outcome
        );
        // Restart every follower step while keeping the primary in memory.
        drop(follower);
        follower = DurableSqlEngine::open(root.join("follower")).unwrap();
    }
    for (i, command) in commands.iter().enumerate() {
        assert_eq!(
            follower.apply_application_command(command).unwrap().outcome,
            results[i].outcome
        );
    }
    assert_eq!(
        execute(&mut follower, &mut router, READ, "read")
            .unwrap()
            .rows,
        0
    );
    drop(sql);
    drop(router);
    let mut router = self::router(&root);
    let mut sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    assert_eq!(
        execute(&mut sql, &mut router, INSERT, "request-1")
            .unwrap()
            .outcome,
        results[1].outcome
    );
    assert_eq!(router.committed_application_commands().len(), 5);
    let mut empty = self::router(&root.join("rolled-back"));
    assert!(execute(&mut sql, &mut empty, INSERT, "request-1").is_err());
    assert!(empty.committed_application_commands().is_empty());
    drop(sql);
    assert!(DurableSqlEngine::open_replicated(root.join("sql"), "wrong").is_err());
}

#[test]
fn invalid_plans_do_not_create_intents_or_committed_commands() {
    let root = root("validation");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create").unwrap();
    for (i, statement) in [
        CREATE.into(),
        "UPDATE users SET name=x WHERE id=missing".into(),
        format!(
            "INSERT INTO users (id, name) VALUES (1, '{}')",
            "x".repeat(300_000)
        ),
        format!(
            "INSERT INTO users (id, name) VALUES (1, '{}')",
            "x".repeat(600_000)
        ),
    ]
    .iter()
    .enumerate()
    {
        assert!(
            execute(&mut sql, &mut router, statement, &format!("bad-{i}")).is_err(),
            "invalid case {i}"
        );
        assert!(sql.pending.is_none());
        assert!(!root.join("sql/sql.intent").exists());
        assert_eq!(router.committed_application_commands().len(), 1);
    }
    execute(&mut sql, &mut router, INSERT, "insert").unwrap();
    assert!(execute(&mut sql, &mut router, INSERT, "duplicate").is_err());
    let commands = router.committed_application_commands();
    let mut follower = DurableSqlEngine::open_replicated(root.join("follower"), "tenant").unwrap();
    let mut bad = commands[0].clone();
    bad.application.namespace = "wrong".into();
    assert!(follower.apply_application_command(&bad).is_err());
    follower.apply_application_command(&commands[0]).unwrap();
    let mut bad = commands[1].clone();
    bad.request.request.0 += 1;
    assert!(follower.apply_application_command(&bad).is_err());
    let mut bad = commands[1].clone();
    bad.application.bytes.truncate(7);
    assert!(follower.apply_application_command(&bad).is_err());
    assert_eq!(
        execute(&mut follower, &mut router, READ, "read")
            .unwrap()
            .rows,
        0
    );
    follower.apply_application_command(&commands[1]).unwrap();
    assert_eq!(
        execute(&mut follower, &mut router, READ, "read")
            .unwrap()
            .rows,
        1
    );
    let mut legacy = DurableSqlEngine::open(root.join("legacy")).unwrap();
    execute(&mut legacy, &mut router, CREATE, "legacy-create").unwrap();
    drop(legacy);
    let legacy_path = root.join("legacy").join(STATE_FILE);
    let encoded = fs::read_to_string(&legacy_path).unwrap();
    let mut v3 = verify_checksum(&encoded)
        .unwrap()
        .replacen(FORMAT, "RSF_SQL_STATE_V3", 1);
    append_checksum(&mut v3);
    fs::write(&legacy_path, v3).unwrap();
    assert!(DurableSqlEngine::open_replicated(root.join("legacy"), "tenant").is_err());
    assert_eq!(
        DurableSqlEngine::open(root.join("legacy"))
            .unwrap()
            .recovery()
            .tables,
        1
    );
}

struct Interrupt {
    inner: VsrBackedFabricRouter,
    stop: bool,
    stop_on_read: bool,
    fail_journal_head: Option<PathBuf>,
}
impl FabricRouterPort for Interrupt {
    fn route_operation(&self, op: &FabricOperation) -> Option<PlacementGroupId> {
        self.inner.route_operation(op)
    }
    fn commit(&mut self, pg: PlacementGroupId, op: FabricOperation) -> RsfResult<FabricCommit> {
        FabricRouterPort::commit(&mut self.inner, pg, op)
    }
    fn commit_with_mode(
        &mut self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        op: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        FabricRouterPort::commit_with_mode(&mut self.inner, pg, mode, op)
    }
    fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        op: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        FabricRouterPort::commit_request(&mut self.inner, pg, request, op)
    }
    fn commit_request_with_mode(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        op: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        FabricRouterPort::commit_request_with_mode(&mut self.inner, pg, request, mode, op)
    }
    fn request_certificate(&self, request: &ClientRequestId) -> Option<CommitCertificate> {
        self.inner.request_certificate(request)
    }
    fn latest_version(&self) -> CommitVersion {
        self.inner.latest_version()
    }
    fn preview_application_request(
        &mut self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        mode: ConsistencyMode,
        op: &FabricOperation,
        app: &ApplicationPayload,
    ) -> RsfResult<FabricCommit> {
        self.inner
            .preview_application_request(pg, request, mode, op, app)
    }
    fn commit_application_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        op: FabricOperation,
        app: ApplicationPayload,
    ) -> RsfResult<RequestCommitEvidence> {
        let is_session_commit = app.bytes.strip_prefix(SESSION_MAGIC).is_some_and(|bytes| {
            let text = std::str::from_utf8(bytes).unwrap();
            let pending = PendingRequest::decode(&text.split('\t').collect::<Vec<_>>()).unwrap();
            matches!(
                parse_statement(&pending.statement).unwrap(),
                ParsedStatement::Commit
            ) || (self.stop_on_read
                && matches!(
                    parse_statement(&pending.statement).unwrap(),
                    ParsedStatement::Select { .. }
                ))
        });
        let is_row = matches!(op, FabricOperation::RelationalMutation { .. }) || is_session_commit;
        let result = self
            .inner
            .commit_application_request(pg, request, mode, op, app)?;
        if is_row {
            if self.stop {
                println!("SQL_COMMAND_DURABLE");
                std::io::stdout().flush().unwrap();
                loop {
                    std::thread::park_timeout(std::time::Duration::from_secs(1));
                }
            }
            if let Some(path) = self.fail_journal_head.take() {
                fs::create_dir(path.join("sql.journal.head.tmp")).unwrap();
            }
        }
        Ok(result)
    }
}

#[test]
fn interrupted_writer_process() {
    let Some(root) = std::env::var_os("RSF_SQL_COMMAND_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut router = Interrupt {
        inner: router(&root),
        stop: true,
        stop_on_read: false,
        fail_journal_head: None,
    };
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create").unwrap();
    execute(&mut sql, &mut router, INSERT, "insert").unwrap();
}

#[test]
fn interrupted_session_commit_process() {
    let Some(root) = std::env::var_os("RSF_SQL_SESSION_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut router = Interrupt {
        inner: router(&root),
        stop: true,
        stop_on_read: false,
        fail_journal_head: None,
    };
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    let mut context = crate::SqlSessionContext {
        name: "work".into(),
        transaction: None,
    };
    let result = sql
        .execute_session(
            &mut router,
            PlacementGroupId(1),
            "alice",
            &context,
            "BEGIN",
            "begin",
        )
        .unwrap();
    let SqlOutcome::TransactionBegan { session_id, .. } = result.outcome else {
        panic!("BEGIN");
    };
    context.transaction = Some(session_id);
    for (statement, id) in [(CREATE, "create"), (INSERT, "insert"), ("COMMIT", "commit")] {
        sql.execute_session(
            &mut router,
            PlacementGroupId(1),
            "alice",
            &context,
            statement,
            id,
        )
        .unwrap();
    }
}

#[test]
fn killed_session_commit_recovers_atomic_rows_schema_and_original_receipt() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = root("session-kill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable::replicated::tests::interrupted_session_commit_process",
            "--nocapture",
        ])
        .env("RSF_SQL_SESSION_ROOT", &root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if line.unwrap().contains("SQL_COMMAND_DURABLE") {
                let _ = send.send(());
                return;
            }
        }
    });
    let arrived = receive.recv_timeout(std::time::Duration::from_secs(15));
    let _ = child.kill();
    child.wait().unwrap();
    reader.join().unwrap();
    arrived.unwrap();
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    assert!(
        sql.read_snapshot(READ).is_err(),
        "unpublished commit must not expose partial schema"
    );
    let context = crate::SqlSessionContext {
        name: "work".into(),
        transaction: Some(sql.sessions.values().next().unwrap().token.clone()),
    };
    assert_eq!(sql.reconcile(&mut router).unwrap(), 1);
    assert_eq!(sql.read_snapshot(READ).unwrap().rows, 1);
    assert!(sql.sessions.is_empty());
    let retry = sql
        .execute_session(
            &mut router,
            PlacementGroupId(1),
            "alice",
            &context,
            "COMMIT",
            "commit",
        )
        .unwrap();
    assert!(retry.replayed);
    assert!(matches!(
        retry.outcome,
        SqlOutcome::TransactionCommitted { mutations: 2, .. }
    ));
    drop(sql);
    let mut sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    let again = sql
        .execute_session(
            &mut router,
            PlacementGroupId(1),
            "alice",
            &context,
            "COMMIT",
            "commit",
        )
        .unwrap();
    assert_eq!(again.outcome, retry.outcome);
    assert_eq!(router.committed_application_commands().len(), 4);
}

#[test]
fn interrupted_session_read_process() {
    let Some(root) = std::env::var_os("RSF_SQL_READ_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut router = Interrupt {
        inner: router(&root),
        stop: true,
        stop_on_read: true,
        fail_journal_head: None,
    };
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create").unwrap();
    let mut context = crate::SqlSessionContext {
        name: "work".into(),
        transaction: None,
    };
    let SqlOutcome::TransactionBegan { session_id, .. } = sql
        .execute_session(
            &mut router,
            PlacementGroupId(1),
            "alice",
            &context,
            "BEGIN",
            "begin",
        )
        .unwrap()
        .outcome
    else {
        panic!("BEGIN");
    };
    context.transaction = Some(session_id);
    sql.execute_session(
        &mut router,
        PlacementGroupId(1),
        "alice",
        &context,
        "SELECT * FROM users WHERE id=missing",
        "read",
    )
    .unwrap();
}

#[test]
fn killed_session_read_recovers_certified_dependency_and_original_result_before_acknowledgement() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = root("read-kill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable::replicated::tests::interrupted_session_read_process",
            "--nocapture",
        ])
        .env("RSF_SQL_READ_ROOT", &root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if line.unwrap().contains("SQL_COMMAND_DURABLE") {
                let _ = send.send(());
                return;
            }
        }
    });
    let arrived = receive.recv_timeout(std::time::Duration::from_secs(15));
    let _ = child.kill();
    child.wait().unwrap();
    reader.join().unwrap();
    arrived.unwrap();
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    let context = crate::SqlSessionContext {
        name: "work".into(),
        transaction: Some(sql.sessions.values().next().unwrap().token.clone()),
    };
    assert_eq!(sql.reconcile(&mut router).unwrap(), 1);
    let result = sql
        .execute_session(
            &mut router,
            PlacementGroupId(1),
            "alice",
            &context,
            "SELECT * FROM users WHERE id=missing",
            "read",
        )
        .unwrap();
    assert!(result.replayed && result.persisted);
    assert_eq!(result.rows, 0);
    assert_eq!(router.committed_application_commands().len(), 3);
    execute(
        &mut sql,
        &mut router,
        "INSERT INTO users (id, name) VALUES (missing, phantom)",
        "phantom",
    )
    .unwrap();
    let error = sql
        .execute_session(
            &mut router,
            PlacementGroupId(1),
            "alice",
            &context,
            "COMMIT",
            "commit",
        )
        .unwrap_err();
    assert!(error.message.contains("serializable snapshot conflict"));
    assert!(sql.pending.is_none());
}

#[test]
fn killed_writer_recovers_authoritative_statement_before_local_checkpoint() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let root = root("kill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "durable::replicated::tests::interrupted_writer_process",
            "--nocapture",
        ])
        .env("RSF_SQL_COMMAND_ROOT", &root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if line.unwrap().contains("SQL_COMMAND_DURABLE") {
                let _ = send.send(());
                return;
            }
        }
    });
    let arrived = receive.recv_timeout(std::time::Duration::from_secs(15));
    let _ = child.kill();
    child.wait().unwrap();
    reader.join().unwrap();
    arrived.unwrap();
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    assert_eq!(sql.reconcile(&mut router).unwrap(), 1);
    assert_eq!(
        execute(&mut sql, &mut router, READ, "read").unwrap().rows,
        1
    );
    assert!(
        execute(&mut sql, &mut router, INSERT, "insert")
            .unwrap()
            .replayed
    );
    assert_eq!(router.committed_application_commands().len(), 2);
}

#[test]
fn journal_head_failure_fences_serving_and_replays_exact_command_after_reopen() {
    let root = root("checkpoint-failure");
    let sql_root = root.join("sql");
    let mut router = Interrupt {
        inner: router(&root),
        stop: false,
        stop_on_read: false,
        fail_journal_head: Some(sql_root.clone()),
    };
    let mut sql = DurableSqlEngine::open_replicated(&sql_root, "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create").unwrap();
    assert!(execute(&mut sql, &mut router, INSERT, "insert").is_err());
    assert!(execute(&mut sql, &mut router, READ, "read").is_err());
    drop(sql);
    fs::remove_dir(sql_root.join("sql.journal.head.tmp")).unwrap();
    let mut sql = DurableSqlEngine::open(&sql_root).unwrap();
    for command in router.inner.committed_application_commands() {
        sql.apply_application_command(&command).unwrap();
    }
    assert_eq!(
        execute(&mut sql, &mut router, READ, "read").unwrap().rows,
        1
    );
    assert!(
        execute(&mut sql, &mut router, INSERT, "insert")
            .unwrap()
            .replayed
    );
    assert!(sql.pending.is_none());
    assert_eq!(router.inner.committed_application_commands().len(), 2);
}

#[test]
fn multipage_sql_replicates_restarts_and_preserves_original_receipts() {
    let root = root("multipage");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create").unwrap();
    let mut statements = Vec::new();
    let mut results = Vec::new();
    for n in 0..24 {
        let statement = format!(
            "INSERT INTO users (id, name) VALUES ({n}, '{}')",
            "x".repeat(512)
        );
        results.push(execute(&mut sql, &mut router, &statement, &format!("insert-{n}")).unwrap());
        statements.push(statement);
    }
    assert!(sql.engine.page_wal_report().pages > 1);
    let grown = format!("UPDATE users SET name='{}' WHERE id=0", "y".repeat(3000));
    execute(&mut sql, &mut router, &grown, "grow").unwrap();
    execute(
        &mut sql,
        &mut router,
        "DELETE FROM users WHERE id=7",
        "delete",
    )
    .unwrap();
    let commands = router.committed_application_commands();
    let before = commands.len();
    let oversized = format!("UPDATE users SET name='{}' WHERE id=0", "z".repeat(5000));
    assert!(execute(&mut sql, &mut router, &oversized, "oversized").is_err());
    assert_eq!(router.committed_application_commands().len(), before);
    let mut follower = DurableSqlEngine::open_replicated(root.join("follower"), "tenant").unwrap();
    for command in &commands {
        follower.apply_application_command(command).unwrap();
        drop(follower);
        follower = DurableSqlEngine::open(root.join("follower")).unwrap();
    }
    assert_eq!(
        follower.engine.select_pk("users", "0").unwrap().columns["name"],
        "y".repeat(3000)
    );
    assert!(follower.engine.select_pk("users", "7").is_none());
    drop(sql);
    let mut sql = DurableSqlEngine::open(root.join("sql")).unwrap();
    for (n, statement) in statements.iter().enumerate() {
        let retry = execute(&mut sql, &mut router, statement, &format!("insert-{n}")).unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.outcome, results[n].outcome);
    }
    assert_eq!(
        execute(&mut sql, &mut router, READ, "read").unwrap().rows,
        23
    );
    execute(
        &mut sql,
        &mut router,
        "UPDATE users SET name='after restart' WHERE id=0",
        "post-restart",
    )
    .unwrap();
}
