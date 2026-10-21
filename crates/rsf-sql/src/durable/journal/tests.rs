use super::*;
use rsf_core::NodeId;
use rsf_protocol::VsrBackedFabricRouter;

const CREATE: &str = "CREATE TABLE users(id PRIMARY KEY, name)";
const INSERT: &str = "INSERT INTO users (id, name) VALUES (1, ada)";

fn root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rsf-sql-journal-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn router(root: &Path) -> VsrBackedFabricRouter {
    VsrBackedFabricRouter::open(root.join("router"), [PG], EPOCH, NodeId(0), [NodeId(0)]).unwrap()
}
fn execute(
    sql: &mut DurableSqlEngine,
    router: &mut VsrBackedFabricRouter,
    statement: &str,
    id: &str,
) -> DurableSqlExecution {
    sql.execute(router, PG, statement, id).unwrap()
}
fn segment(sql: &DurableSqlEngine) -> PathBuf {
    let journal = sql.journal.as_ref().unwrap();
    journal
        .store
        .root()
        .join("segments")
        .join(&journal.store.active_segment().file_name)
}

#[test]
fn incremental_sql_preserves_checkpoint_bytes_then_reclaims_and_replays_original_results() {
    let root = root("bounded-tail");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    let initial = fs::read(path.join(STATE_FILE)).unwrap();
    execute(&mut sql, &mut router, CREATE, "create");
    let inserted = execute(&mut sql, &mut router, INSERT, "insert");
    assert_eq!(fs::read(path.join(STATE_FILE)).unwrap(), initial);
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(sql.recovery.live_rows, 1);
    assert_eq!(
        sql.persistence_stats().unwrap().commands_since_checkpoint,
        2
    );
    for i in 0..70 {
        execute(
            &mut sql,
            &mut router,
            &format!("UPDATE users SET name=name{i} WHERE id=1"),
            &format!("update-{i}"),
        );
    }
    let stats = sql.persistence_stats().unwrap();
    assert_eq!(stats.journal_index, 73);
    assert_eq!(stats.checkpoint_index, 65);
    assert_eq!(stats.commands_since_checkpoint, 8);
    let recovered = sql
        .journal
        .as_ref()
        .unwrap()
        .store
        .recover_current()
        .unwrap()
        .recovery;
    assert_eq!(recovered.wal_records.len(), 8);
    assert_eq!(recovered.compacted_prefixes[&PG].index, LogIndex(65));
    assert_ne!(fs::read(path.join(STATE_FILE)).unwrap(), initial);
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(sql.read_snapshot("SELECT * FROM users").unwrap().rows, 1);
    let replayed = execute(&mut sql, &mut router, INSERT, "insert");
    assert!(replayed.replayed);
    assert_eq!(replayed.outcome, inserted.outcome);
    assert_eq!(sql.commands.len(), 72);
    sql.checkpoint().unwrap();
    assert_eq!(
        sql.persistence_stats().unwrap().commands_since_checkpoint,
        0
    );
    drop(sql);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(sql.persistence_stats().unwrap().checkpoint_index, 73);
    assert_eq!(
        execute(&mut sql, &mut router, INSERT, "insert").outcome,
        inserted.outcome
    );
}

#[test]
fn acknowledged_torn_journal_is_rejected_on_every_open() {
    let root = root("lost-acknowledgement");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create");
    execute(&mut sql, &mut router, INSERT, "insert");
    let segment = segment(&sql);
    drop(sql);
    let file = OpenOptions::new().write(true).open(segment).unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    file.sync_all().unwrap();
    drop(file);
    for _ in 0..2 {
        assert!(DurableSqlEngine::open(&path).is_err());
    }
}

#[test]
fn unacknowledged_torn_append_preserves_acknowledged_prefix() {
    let root = root("torn-unacknowledged");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create");
    let inserted = execute(&mut sql, &mut router, INSERT, "insert");
    let segment = segment(&sql);
    drop(sql);
    let mut file = OpenOptions::new().append(true).open(segment).unwrap();
    file.write_all(b"torn").unwrap();
    file.sync_all().unwrap();
    drop(file);
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(sql.recovery.live_rows, 1);
    assert_eq!(
        execute(&mut sql, &mut router, INSERT, "insert").outcome,
        inserted.outcome
    );
}

#[test]
fn checkpoint_failure_preserves_journal_and_fences_writer() {
    let root = root("checkpoint-failure");
    let path = root.join("sql");
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create");
    let inserted = execute(&mut sql, &mut router, INSERT, "insert");
    fs::create_dir(path.join(TMP_FILE)).unwrap();
    assert!(sql.checkpoint().is_err());
    assert!(sql.read_snapshot("SELECT * FROM users").is_err());
    assert_eq!(
        sql.journal
            .as_ref()
            .unwrap()
            .store
            .recover_current()
            .unwrap()
            .recovery
            .wal_records
            .len(),
        3
    );
    drop(sql);
    fs::remove_dir(path.join(TMP_FILE)).unwrap();
    let mut sql = DurableSqlEngine::open(&path).unwrap();
    assert_eq!(
        execute(&mut sql, &mut router, INSERT, "insert").outcome,
        inserted.outcome
    );
}

fn legacy_checkpoint(sql: &DurableSqlEngine) -> String {
    encode_checkpoint(&SqlCheckpoint {
        tables: sql.engine.recovered_tables_for_checkpoint(),
        kv: sql.engine.committed_kv_versions_for_checkpoint(),
        receipts: sql.receipts.clone(),
        namespace: sql.namespace.clone(),
        commands: sql.commands.clone(),
        journal: None,
    })
    .unwrap()
}

#[test]
fn bound_v4_upgrade_resumes_header_publication_without_changing_values_or_receipts() {
    for window in ["legacy", "header", "head"] {
        let root = root(window);
        let path = root.join("sql");
        let mut router = router(&root);
        let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
        execute(&mut sql, &mut router, CREATE, "create");
        let inserted = execute(&mut sql, &mut router, INSERT, "insert");
        let legacy = legacy_checkpoint(&sql);
        drop(sql);
        fs::remove_dir_all(path.join(DIRECTORY)).unwrap();
        fs::remove_file(path.join(HEAD_FILE)).unwrap();
        fs::write(path.join(STATE_FILE), &legacy).unwrap();
        if window != "legacy" {
            let mut store = SegmentStore::open(path.join(DIRECTORY)).unwrap();
            let header = format!(
                "{HEADER_FORMAT}\nnamespace\t{}\nbase\tblake3\t{}\n",
                hex_bytes(b"tenant"),
                hex_bytes(Digest::blake3_bytes(legacy.as_bytes()).as_bytes())
            );
            let (_, _, record) = store
                .append_next_wal(PG, EPOCH, WalRecordKind::Membership, header.as_bytes())
                .unwrap();
            if window == "head" {
                JournalFence {
                    index: record.index,
                    hash: record.record_hash,
                }
                .publish_head(&path)
                .unwrap();
            }
        }
        let mut sql = DurableSqlEngine::open(&path).unwrap();
        assert!(
            fs::read_to_string(path.join(STATE_FILE))
                .unwrap()
                .starts_with(JOURNALED_FORMAT)
        );
        assert_eq!(sql.recovery.live_rows, 1);
        assert_eq!(
            execute(&mut sql, &mut router, INSERT, "insert").outcome,
            inserted.outcome
        );
        execute(
            &mut sql,
            &mut router,
            "UPDATE users SET name=grace WHERE id=1",
            "update",
        );
        drop(sql);
        assert_eq!(DurableSqlEngine::open(&path).unwrap().recovery.live_rows, 1);
    }
}

#[test]
fn missing_journal_head_checkpoint_or_reclaimed_snapshot_fails_closed() {
    for missing in [
        "journal",
        "head",
        "checkpoint",
        "stale-checkpoint",
        "foreign-head",
        "legacy-rollback",
    ] {
        let root = root(missing);
        let path = root.join("sql");
        let mut router = router(&root);
        let mut sql = DurableSqlEngine::open_replicated(&path, "tenant").unwrap();
        let initial = fs::read(path.join(STATE_FILE)).unwrap();
        let legacy = legacy_checkpoint(&sql);
        execute(&mut sql, &mut router, CREATE, "create");
        execute(&mut sql, &mut router, INSERT, "insert");
        sql.checkpoint().unwrap();
        drop(sql);
        match missing {
            "journal" => fs::remove_dir_all(path.join(DIRECTORY)).unwrap(),
            "head" => fs::remove_file(path.join(HEAD_FILE)).unwrap(),
            "checkpoint" => fs::remove_file(path.join(STATE_FILE)).unwrap(),
            "stale-checkpoint" => fs::write(path.join(STATE_FILE), initial).unwrap(),
            "legacy-rollback" => fs::write(path.join(STATE_FILE), legacy).unwrap(),
            "foreign-head" => JournalFence {
                index: LogIndex(3),
                hash: [9; 32],
            }
            .publish_head(&path)
            .unwrap(),
            _ => unreachable!(),
        }
        for _ in 0..2 {
            assert!(DurableSqlEngine::open(&path).is_err(), "accepted {missing}");
        }
    }
}

#[test]
fn journal_writer_process() {
    let Some(root) = std::env::var_os("RSF_SQL_JOURNAL_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let boundary = std::env::var("RSF_SQL_JOURNAL_CRASH_BOUNDARY").unwrap();
    let mut router = router(&root);
    let mut sql = DurableSqlEngine::open_replicated(root.join("sql"), "tenant").unwrap();
    execute(&mut sql, &mut router, CREATE, "create");
    if boundary == "append" {
        // Stop after a real certified command's synced append, before its head.
        let mut donor = DurableSqlEngine::open_replicated(root.join("donor"), "tenant").unwrap();
        for command in router.committed_application_commands() {
            donor.apply_application_command(&command).unwrap();
        }
        execute(&mut donor, &mut router, INSERT, "insert");
        let command = router
            .committed_application_commands()
            .into_iter()
            .find(|command| command.request == ClientRequestId::new("insert", 0))
            .unwrap();
        PendingRequest {
            client: "insert".into(),
            statement: INSERT.into(),
            pg: PG,
        }
        .persist(&sql.root)
        .unwrap();
        sql.journal
            .as_mut()
            .unwrap()
            .store
            .append_next_wal(PG, EPOCH, WalRecordKind::Commit, &command.encode().unwrap())
            .unwrap();
    } else {
        execute(&mut sql, &mut router, INSERT, "insert");
        match boundary.as_str() {
            "head" => {}
            "checkpoint" => sql.persist().unwrap(),
            "compacted" => sql.checkpoint().unwrap(),
            _ => panic!("unknown crash boundary"),
        }
    }
    println!("SQL_JOURNAL_CRASH_BOUNDARY");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park_timeout(std::time::Duration::from_secs(1));
    }
}

#[test]
fn killed_writer_recovers_append_head_checkpoint_and_compaction_boundaries() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    for boundary in ["append", "head", "checkpoint", "compacted"] {
        let root = root(boundary);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "durable::journal::tests::journal_writer_process",
                "--nocapture",
            ])
            .env("RSF_SQL_JOURNAL_CRASH_ROOT", &root)
            .env("RSF_SQL_JOURNAL_CRASH_BOUNDARY", boundary)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if line.unwrap().contains("SQL_JOURNAL_CRASH_BOUNDARY") {
                    let _ = send.send(());
                    return;
                }
            }
        });
        let arrived = receive.recv_timeout(std::time::Duration::from_secs(20));
        let _ = child.kill();
        child.wait().unwrap();
        reader.join().unwrap();
        arrived.unwrap();
        let mut router = router(&root);
        let mut sql = DurableSqlEngine::open(root.join("sql")).unwrap();
        sql.reconcile(&mut router).unwrap();
        assert_eq!(sql.recovery.live_rows, 1, "{boundary}");
        let command = router
            .committed_application_commands()
            .into_iter()
            .find(|command| command.request == ClientRequestId::new("insert", 0))
            .unwrap();
        let retried = execute(&mut sql, &mut router, INSERT, "insert");
        assert!(retried.replayed, "{boundary}");
        assert_eq!(retried.rows, 1);
        assert_eq!(retried.version, Some(command.commit.version));
        assert_eq!(router.committed_application_commands().len(), 2);
        assert_eq!(
            sql.receipts["insert"].evidence,
            crate::commit_evidence(command.certificate())
        );
        assert!(sql.pending.is_none());
    }
}
