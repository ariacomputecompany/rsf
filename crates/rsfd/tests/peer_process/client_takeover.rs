//! Actual application listeners on independently owned peer stores. No manual
//! election API, copied primary WAL, or external recovery coordinator is used.
use super::automatic::{
    AutomaticProcess, await_primary, configure, reserve, reserve_listener, state,
};
use super::*;
use rsf_wire::WireProtocol::{Blob, Kv, Object, Sql};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn must_execute(
    address: SocketAddr,
    protocol: rsf_wire::WireProtocol,
    operation: &str,
    request: &str,
    bytes: &[u8],
) -> String {
    let response = surface_request(address, protocol, operation, request, bytes);
    assert!(
        response.starts_with("RSF/1 executed"),
        "{operation}: {response}"
    );
    response
}
fn begin_session(address: SocketAddr, name: &str, id: &str) -> Vec<u8> {
    let body = rsf_sql::SqlSessionContext {
        name: name.into(),
        transaction: None,
    }
    .encode()
    .unwrap();
    let reply = must_execute(address, Sql, "BEGIN", id, &body);
    let token = reply
        .split("session=")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    rsf_sql::SqlSessionContext {
        name: name.into(),
        transaction: Some(token),
    }
    .encode()
    .unwrap()
}

#[test]
fn automatic_client_takeover_recovers_every_surface_routes_followers_and_rechecks_security() {
    let started = std::time::Instant::now();
    let root = root();
    let mut peers = provision(&root);
    let control = initialize_network_state(&root, &peers);
    // The client service must derive coordination solely from its own peer cells.
    std::fs::remove_dir_all(control.join("protocol")).unwrap();
    std::fs::remove_dir_all(control.join("protocol.vsr")).unwrap();
    let mut reservations = reserve(&mut peers);
    configure(&root, &peers, true);
    let client_reservations = (0..3).map(|_| reserve_listener()).collect::<Vec<_>>();
    let addresses = client_reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect::<Vec<_>>();
    for node in 0..3 {
        let path = root.join(format!("auto-{node}.json"));
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["timeout_ms"] = 500.into();
        config["elections"] =
            serde_json::json!({"heartbeat_ms":200,"timeout_ms":5000,"rank_delay_ms":1500});
        config["client"] = serde_json::json!({"state":control,"listen":addresses[node].to_string(),"endpoints":addresses.iter().enumerate().map(|(node,address)|serde_json::json!({"node":node,"address":address.to_string()})).collect::<Vec<_>>()});
        std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
    }
    // Invalid timing fails before opening any peer store or serving clients.
    let valid = std::fs::read(root.join("auto-0.json")).unwrap();
    let mut invalid: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    invalid["elections"]["timeout_ms"] = 3000.into();
    let invalid_path = root.join("invalid-client.json");
    std::fs::write(&invalid_path, serde_json::to_vec(&invalid).unwrap()).unwrap();
    let rejected = Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(["peer", "serve", "--config"])
        .arg(&invalid_path)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("cumulative data I/O deadline"));
    assert!(!state(&root, 0).exists());
    drop(client_reservations);
    let mut processes = (0..3)
        .map(|node| {
            drop(reservations[node].take());
            Some(AutomaticProcess::start(
                &root.join(format!("auto-{node}.json")),
            ))
        })
        .collect::<Vec<_>>();
    let (primary, view) = await_primary(&root, &peers, &mut processes, &[0, 1, 2], 2);
    let requests = [
        (
            Kv,
            "SET client/key",
            "client-kv",
            b"kv before failover".as_slice(),
        ),
        (
            Sql,
            "CREATE TABLE users(id PRIMARY KEY, name)",
            "client-table",
            b"",
        ),
        (
            Sql,
            "INSERT INTO users (id, name) VALUES (1, ada)",
            "client-row",
            b"",
        ),
        (Object, "CREATE_BUCKET media", "client-bucket", b""),
        (
            Object,
            "PUT media/key",
            "client-object",
            b"object before failover",
        ),
        (Blob, "PUT initial", "client-blob", b"blob before failover"),
    ];
    let original = requests
        .iter()
        .map(|(protocol, operation, request, bytes)| {
            must_execute(
                addresses[primary as usize],
                *protocol,
                operation,
                request,
                bytes,
            )
        })
        .collect::<Vec<_>>();
    // Acknowledged session state survives primary data loss while its rows and
    // schema remain uncommitted. The replacement must resume the same token.
    let begin_context = rsf_sql::SqlSessionContext {
        name: "takeover".into(),
        transaction: None,
    };
    let begin_body = begin_context.encode().unwrap();
    let begin_reply = must_execute(
        addresses[primary as usize],
        Sql,
        "BEGIN",
        "session-begin",
        &begin_body,
    );
    let token = begin_reply
        .split("session=")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    let session = rsf_sql::SqlSessionContext {
        name: "takeover".into(),
        transaction: Some(token),
    };
    let session_body = session.encode().unwrap();
    let staged = [
        (
            "CREATE TABLE session_rows(id PRIMARY KEY, name)",
            "session-table",
        ),
        (
            "INSERT INTO session_rows (id, name) VALUES (1, durable)",
            "session-insert",
        ),
        ("SAVEPOINT keep", "session-savepoint"),
        (
            "UPDATE session_rows SET name=discarded WHERE id=1",
            "session-update",
        ),
    ];
    let staged_replies = staged
        .iter()
        .map(|(statement, id)| {
            must_execute(
                addresses[primary as usize],
                Sql,
                statement,
                id,
                &session_body,
            )
        })
        .collect::<Vec<_>>();
    assert!(staged_replies[1].contains("staged=true"));
    let precrash_body = begin_session(
        addresses[primary as usize],
        "precrash-predicate",
        "precrash-begin",
    );
    let precrash_query = "SELECT * FROM users WHERE name=precrashghost";
    let precrash_read = must_execute(
        addresses[primary as usize],
        Sql,
        precrash_query,
        "precrash-read",
        &precrash_body,
    );
    assert!(precrash_read.contains("select rows=0"));
    assert!(
        surface_request(
            addresses[primary as usize],
            Sql,
            "SELECT * FROM session_rows",
            "session-invisible",
            b""
        )
        .starts_with("RSF/1 rejected")
    );
    // Object-internal protocol pins must not become a user blob value when the
    // resident materializer advances its complete-command cursor.
    let internal = rsf_core::blob::ContentId::for_bytes(b"object before failover");
    let hidden = surface_request(
        addresses[primary as usize],
        Blob,
        &format!("GET blake3:{}", hex(internal.as_bytes())),
        "internal-pin-isolation",
        b"",
    );
    assert!(hidden.starts_with("RSF/1 rejected"), "{hidden}");
    // A follower publishes a usable endpoint and never attempts the mutation.
    let follower = (primary + 1) % 3;
    let redirect = surface_request(
        addresses[follower as usize],
        Kv,
        "SET must-not-exist",
        "redirect",
        b"denied",
    );
    assert!(redirect.starts_with("RSF/1 redirect"), "{redirect}");
    assert!(
        redirect.contains(&format!("address={}", addresses[primary as usize])),
        "{redirect}"
    );
    assert!(redirect.contains("mutation=not-attempted"));
    let routed: SocketAddr = redirect
        .split("address=")
        .nth(1)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    must_execute(routed, Kv, "GET client/key", "follow-redirect", b"");

    // Grow acknowledged history before the crash; hot coordination must remain
    // resident while application receipts and cell tails advance.
    let growing_started = std::time::Instant::now();
    for index in 0..128 {
        must_execute(
            addresses[primary as usize],
            Kv,
            &format!("SET growing/{index}"),
            &format!("growing-{index}"),
            format!("history-{index}").as_bytes(),
        );
    }
    let history_growth_ms = growing_started.elapsed().as_millis();
    println!("history growth completed in {history_growth_ms} ms");
    // Four clients keep requests in flight while the primary is killed. Every
    // acknowledgement must survive; every lost response is retried by identity.
    let (send, receive) = std::sync::mpsc::channel();
    let clients = (0..4)
        .map(|client| {
            let send = send.clone();
            let address = addresses[primary as usize];
            std::thread::spawn(move || {
                (0..8)
                    .map(|index| {
                        let key = format!("concurrent/{client}/{index}");
                        let request = format!("concurrent-{client}-{index}");
                        let value = format!("value-{client}-{index}").into_bytes();
                        let frame = rsf_wire::RawFrame::with_body(
                            Kv,
                            "app-client",
                            Some("app".into()),
                            format!("SET {key}"),
                            Some(request.clone()),
                            value.clone(),
                        );
                        let result = test_support::send(
                            address,
                            &rsf_wire::WireCodec.encode(&frame).unwrap(),
                            test_support::pki().client.clone(),
                        );
                        let acknowledged = result
                            .as_ref()
                            .is_ok_and(|response| response.starts_with("RSF/1 executed"));
                        send.send(acknowledged).unwrap();
                        (key, request, value, result)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    drop(send);
    let mut acknowledged = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while acknowledged < 4 {
        assert!(
            std::time::Instant::now() < deadline,
            "concurrent writes did not acknowledge before crash"
        );
        if receive.recv_timeout(Duration::from_secs(2)).unwrap() {
            acknowledged += 1;
        }
    }
    let cache = processes[primary as usize]
        .as_ref()
        .unwrap()
        .cache
        .lock()
        .unwrap()
        .clone()
        .expect("client cache evidence");
    let reuses = cache["reuses"].as_u64().unwrap();
    let recoveries = cache["full_recoveries"].as_u64().unwrap();
    assert!(
        reuses >= 128 && recoveries < reuses,
        "hot requests reconstructed coordination: {cache}"
    );
    drop(processes[primary as usize].take());
    std::fs::remove_dir_all(state(&root, primary)).unwrap(); // Physically lose its data.
    let survivors = (0..3).filter(|node| *node != primary).collect::<Vec<_>>();
    let concurrent = clients
        .into_iter()
        .flat_map(|client| client.join().unwrap())
        .collect::<Vec<_>>();
    let (replacement, _) = await_primary(&root, &peers, &mut processes, &survivors, view + 1);
    let address = addresses[replacement as usize];
    for (key, request, value, original) in &concurrent {
        let retry = must_execute(address, Kv, &format!("SET {key}"), request, value);
        if let Ok(original) = original
            && original.starts_with("RSF/1 executed")
        {
            assert_eq!(retry, original.replace("replayed=false", "replayed=true"));
        }
    }
    for ((protocol, operation, request, bytes), original) in requests.iter().zip(&original) {
        let retry = must_execute(address, *protocol, operation, request, bytes);
        let expected = original.replace("replayed=false", "replayed=true");
        let expected = if *protocol == Blob {
            expected.replace("deduped=false", "deduped=true")
        } else {
            expected
        };
        assert_eq!(retry, expected);
    }
    assert_eq!(
        must_execute(
            address,
            Sql,
            precrash_query,
            "precrash-read",
            &precrash_body
        ),
        precrash_read.replace("replayed=false", "replayed=true")
    );
    must_execute(
        address,
        Sql,
        "INSERT INTO users (id, name) VALUES (4, precrashghost)",
        "precrash-external",
        b"",
    );
    let rejected = surface_request(address, Sql, "COMMIT", "precrash-commit", &precrash_body);
    assert!(
        rejected.starts_with("RSF/1 rejected")
            && rejected.contains("serializable snapshot conflict"),
        "{rejected}"
    );
    must_execute(address, Sql, "ROLLBACK", "precrash-abort", &precrash_body);
    let resumed = must_execute(
        address,
        Sql,
        "SELECT * FROM session_rows",
        "session-resume",
        &session_body,
    );
    assert!(resumed.contains(&hex(b"discarded")), "{resumed}");
    must_execute(
        address,
        Sql,
        "ROLLBACK TO keep",
        "session-rollback-to",
        &session_body,
    );
    let committed = must_execute(address, Sql, "COMMIT", "session-commit", &session_body);
    let phantom_begin = rsf_sql::SqlSessionContext {
        name: "phantom-check".into(),
        transaction: None,
    }
    .encode()
    .unwrap();
    let reply = must_execute(address, Sql, "BEGIN", "phantom-begin", &phantom_begin);
    let token = reply
        .split("session=")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    let phantom_body = rsf_sql::SqlSessionContext {
        name: "phantom-check".into(),
        transaction: Some(token),
    }
    .encode()
    .unwrap();
    let empty = must_execute(
        address,
        Sql,
        "SELECT * FROM users WHERE name=phantom",
        "phantom-read",
        &phantom_body,
    );
    assert!(empty.contains("select rows=0"), "{empty}");
    must_execute(
        address,
        Sql,
        "UPDATE users SET name=must-not-commit WHERE id=1",
        "phantom-update",
        &phantom_body,
    );
    must_execute(
        address,
        Sql,
        "INSERT INTO users (id, name) VALUES (3, phantom)",
        "phantom-external",
        b"",
    );
    let rejected = surface_request(address, Sql, "COMMIT", "phantom-commit", &phantom_body);
    assert!(
        rejected.starts_with("RSF/1 rejected")
            && rejected.contains("serializable snapshot conflict"),
        "{rejected}"
    );
    must_execute(address, Sql, "ROLLBACK", "phantom-abort", &phantom_body);
    let unchanged = must_execute(
        address,
        Sql,
        "SELECT * FROM users WHERE id=1",
        "phantom-check",
        b"",
    );
    assert!(
        unchanged.contains(&hex(b"ada")) && !unchanged.contains(&hex(b"must-not-commit")),
        "{unchanged}"
    );
    for (statement, id) in [
        (
            "CREATE TABLE parallel_rows(id PRIMARY KEY, name)",
            "parallel-table",
        ),
        (
            "INSERT INTO parallel_rows (id, name) VALUES (1, left)",
            "parallel-left",
        ),
        (
            "INSERT INTO parallel_rows (id, name) VALUES (2, right)",
            "parallel-right",
        ),
    ] {
        must_execute(address, Sql, statement, id, b"");
    }
    let left = begin_session(address, "parallel-left", "parallel-left-begin");
    let right = begin_session(address, "parallel-right", "parallel-right-begin");
    must_execute(
        address,
        Sql,
        "SELECT * FROM parallel_rows WHERE id=1",
        "parallel-left-read",
        &left,
    );
    must_execute(
        address,
        Sql,
        "SELECT * FROM parallel_rows WHERE id=2",
        "parallel-right-read",
        &right,
    );
    must_execute(
        address,
        Sql,
        "UPDATE parallel_rows SET name=alpha WHERE id=1",
        "parallel-left-update",
        &left,
    );
    must_execute(
        address,
        Sql,
        "UPDATE parallel_rows SET name=beta WHERE id=2",
        "parallel-right-update",
        &right,
    );
    must_execute(address, Sql, "COMMIT", "parallel-left-commit", &left);
    must_execute(address, Sql, "COMMIT", "parallel-right-commit", &right);
    let read = must_execute(
        address,
        Sql,
        "SELECT * FROM session_rows",
        "session-visible",
        b"",
    );
    assert!(
        read.contains(&hex(b"durable")) && !read.contains(&hex(b"discarded")),
        "{read}"
    );
    assert_eq!(
        must_execute(address, Sql, "BEGIN", "session-begin", &begin_body),
        begin_reply.replace("replayed=false", "replayed=true")
    );
    for ((statement, id), reply) in staged.iter().zip(&staged_replies) {
        assert_eq!(
            must_execute(address, Sql, statement, id, &session_body),
            reply.replace("replayed=false", "replayed=true")
        );
    }
    assert_eq!(
        must_execute(address, Sql, "COMMIT", "session-commit", &session_body),
        committed.replace("replayed=false", "replayed=true")
    );
    for (protocol, operation, expected) in [
        (Kv, "GET client/key", b"kv before failover".as_slice()),
        (Object, "GET media/key", b"object before failover"),
    ] {
        let read = must_execute(address, protocol, operation, "read", b"");
        assert!(
            read.contains(&format!("payload_hex={}", hex(expected))),
            "{read}"
        );
    }
    let sql = must_execute(address, Sql, "SELECT * FROM users", "select", b"");
    assert!(sql.contains(&hex(b"ada")), "{sql}");
    let id = rsf_core::blob::ContentId::for_bytes(b"blob before failover");
    let blob = must_execute(
        address,
        Blob,
        &format!("GET blake3:{}", hex(id.as_bytes())),
        "blob-read",
        b"",
    );
    assert!(
        blob.contains(&format!("payload_hex={}", hex(b"blob before failover"))),
        "{blob}"
    );
    must_execute(address, Kv, "SET client/after", "after-kv", b"new kv");
    must_execute(
        address,
        Sql,
        "INSERT INTO users (id, name) VALUES (2, grace)",
        "after-row",
        b"",
    );
    must_execute(
        address,
        Object,
        "PUT media/after",
        "after-object",
        b"new object",
    );
    must_execute(address, Blob, "PUT after", "after-blob", b"new blob");
    // Warm materializers and recovered coordination must not cache an auth grant.
    let fingerprint = test_support::pki().fingerprint;
    let mut auth = rsf_auth::DurableAuthEngine::open(control.join("auth"), Epoch(1)).unwrap();
    auth.revoke_certificate(fingerprint).unwrap();
    let denied = surface_request(address, Kv, "GET client/key", "revoked", b"");
    assert!(
        denied.contains("RSF/1 rejected") && denied.contains("mutation=not-attempted"),
        "{denied}"
    );
    auth.bind_certificate(fingerprint, rsf_auth::PrincipalId("app-client".into()))
        .unwrap();
    drop(auth);
    // Restart the replacement after many custody handoffs and durable receipts.
    drop(processes[replacement as usize].take());
    processes[replacement as usize] = Some(AutomaticProcess::start(
        &root.join(format!("auto-{replacement}.json")),
    ));
    await_primary(&root, &peers, &mut processes, &survivors, view + 1);
    let read = must_execute(address, Object, "GET media/after", "after-read", b"");
    assert!(
        read.contains(&format!("payload_hex={}", hex(b"new object"))),
        "{read}"
    );
    let read = must_execute(address, Sql, "SELECT * FROM users", "after-select", b"");
    assert!(read.contains(&hex(b"grace")), "{read}");
    assert_eq!(
        must_execute(address, Sql, "COMMIT", "session-commit", &session_body),
        committed.replace("replayed=false", "replayed=true")
    );
    let id = rsf_core::blob::ContentId::for_bytes(b"new blob");
    let read = must_execute(
        address,
        Blob,
        &format!("GET blake3:{}", hex(id.as_bytes())),
        "after-blob-read",
        b"",
    );
    assert!(
        read.contains(&format!("payload_hex={}", hex(b"new blob"))),
        "{read}"
    );
    let standby = *survivors.iter().find(|node| **node != replacement).unwrap();
    drop(processes[standby as usize].take());
    let rejected = surface_request(address, Kv, "GET client/key", "minority", b"");
    assert!(rejected.starts_with("RSF/1 rejected"), "{rejected}");
    processes[standby as usize] = Some(AutomaticProcess::start(
        &root.join(format!("auto-{standby}.json")),
    ));
    await_primary(&root, &peers, &mut processes, &survivors, view + 1);
    must_execute(address, Kv, "GET client/after", "restored", b"");
    drop(processes);
    for node in survivors {
        let mut sql = rsf_sql::DurableSqlEngine::open_replicated(
            crate_namespace(&state(&root, node)).join("sql"),
            "app",
        )
        .unwrap();
        let result = sql.read_snapshot("SELECT * FROM session_rows").unwrap();
        let rsf_sql::SqlOutcome::Selected { rows } = result.outcome else {
            panic!("session rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns["name"], "durable");
        let result = sql.read_snapshot("SELECT * FROM parallel_rows").unwrap();
        let rsf_sql::SqlOutcome::Selected { rows } = result.outcome else {
            panic!("parallel rows");
        };
        assert_eq!(
            rows.iter()
                .map(|row| row.columns["name"].as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        let kv = rsf_kv::DurableKvEngine::open_replicated(
            crate_namespace(&state(&root, node)).join("kv"),
            Epoch(1),
            "app",
        )
        .unwrap();
        assert_eq!(kv.get(b"client/key").unwrap().value, b"kv before failover");
        assert_eq!(kv.get(b"client/after").unwrap().value, b"new kv");
        assert!(kv.get(b"must-not-exist").is_none());
        assert_eq!(kv.changes_since(rsf_core::CommitVersion(0)).len(), 162);
        for index in 0..128 {
            assert_eq!(
                kv.get(format!("growing/{index}").as_bytes()).unwrap().value,
                format!("history-{index}").as_bytes()
            );
        }
        for (key, _, value, _) in &concurrent {
            assert_eq!(&kv.get(key.as_bytes()).unwrap().value, value);
        }
    }
    let acknowledged = concurrent
        .iter()
        .filter(|(_, _, _, result)| {
            result
                .as_ref()
                .is_ok_and(|response| response.starts_with("RSF/1 executed"))
        })
        .count();
    assert!(
        (4..32).contains(&acknowledged),
        "crash must interrupt requests after acknowledged writes"
    );
    println!(
        "{}",
        serde_json::json!({
            "workload":"automatic-client-takeover",
            "profile":if cfg!(debug_assertions) {"debug"} else {"release"},
            "coordinator_reuses_before_crash":reuses,"coordinator_recoveries_before_crash":recoveries,
        "concurrent_clients":4,"submitted_requests":32,"acknowledged_before_crash":acknowledged,
            "requests_without_acknowledgement":32-acknowledged,"retried_requests":32,
            "history_growth_ms":history_growth_ms,"verified_logical_kv_writes":162,"history_growth_writes":128,"surviving_replicas_checked":2,
            "primary_state_deleted":true,"replacement_restarts":1,"surface_count":4,
            "active_sql_session_resumed":true,"sql_session_original_results_replayed":true,
            "serializable_sql_phantom_rejected":true,
            "precrash_sql_read_dependency_recovered":true,"disjoint_sql_sessions_committed":true,
            "elapsed_ms":started.elapsed().as_millis(),
            "scope":"single host loopback TLS with fsync; automatic elections and client listeners; not independent-host capacity qualification"
        })
    );
    std::fs::remove_dir_all(root).unwrap();
}
fn crate_namespace(peer_state: &Path) -> PathBuf {
    use sha2::{Digest, Sha256};
    peer_state
        .join("application/namespaces")
        .join(hex(&Sha256::digest(b"app")))
}
