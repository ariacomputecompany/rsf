use super::*;
use std::path::Path;

fn exchange(listener: &DataPlaneListener, operation: &str, request: &str) -> String {
    let frame = WireCodec
        .encode(&rsf_wire::RawFrame::with_body(
            WireProtocol::Sql,
            "app-client",
            Some("app".into()),
            operation,
            Some(request.into()),
            Vec::new(),
        ))
        .unwrap();
    let address = listener.local_addr();
    let client = std::thread::spawn(move || {
        crate::test_support::send(address, &frame, crate::test_support::pki().client.clone())
            .unwrap()
    });
    crate::tests::wait_for_data_request(listener, &mut Vec::new(), "parked SQL application");
    client.join().unwrap()
}

fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

#[test]
fn idle_sql_writer_survives_polls_while_status_and_transport_checks_remain_fresh() {
    let state = crate::tests::test_path("parked-sql-status");
    crate::tests::bootstrap_one_node(state.to_str().unwrap());
    let listener = DataPlaneListener::bind("127.0.0.1:0".parse().unwrap(), &state).unwrap();
    assert!(
        exchange(
            &listener,
            "CREATE TABLE rows(id PRIMARY KEY, name)",
            "table"
        )
        .starts_with("RSF/1 executed")
    );
    let first = exchange(
        &listener,
        "INSERT INTO rows(id,name) VALUES (1,original)",
        "one",
    );
    assert!(first.starts_with("RSF/1 executed"), "{first}");
    for _ in 0..3 {
        assert_eq!(listener.serve_pending(&mut Vec::new()).unwrap(), 0);
        let snapshot = crate::daemon::load_snapshot(&state).unwrap();
        assert!(snapshot.ready);
        assert_eq!(snapshot.protocol_latest_version, 2);
    }
    let sql_root = crate::security::namespace_root(&state, "app").join("sql");
    assert_eq!(
        DurableSqlEngine::open_replicated(&sql_root, "app")
            .unwrap_err()
            .code,
        ErrorCode::QuorumUnavailable,
        "idle polls retain the same exclusive application writer"
    );
    let retry = exchange(
        &listener,
        "INSERT INTO rows(id,name) VALUES (1,original)",
        "one",
    );
    assert_eq!(first.replace("replayed=false", "replayed=true"), retry);
    assert!(
        exchange(
            &listener,
            "INSERT INTO rows(id,name) VALUES (2,changed)",
            "one"
        )
        .starts_with("RSF/1 rejected")
    );
    // A cached store cannot preserve an obsolete transport configuration.
    std::fs::write(
        state.join("replication.json"),
        b"invalid replication configuration",
    )
    .unwrap();
    let rejected = exchange(&listener, "INSERT INTO rows(id,name) VALUES (2,new)", "two");
    assert!(
        rejected.contains("invalid replication configuration"),
        "{rejected}"
    );
    std::fs::remove_file(state.join("replication.json")).unwrap();
    assert!(exchange(&listener, "SELECT * FROM rows", "read").contains("RSF/1 executed"));
    drop(listener);
    let listener = DataPlaneListener::bind("127.0.0.1:0".parse().unwrap(), &state).unwrap();
    assert_eq!(
        exchange(
            &listener,
            "INSERT INTO rows(id,name) VALUES (1,original)",
            "one"
        ),
        retry
    );
    drop(listener);
    let mut sql = DurableSqlEngine::open_replicated(&sql_root, "app").unwrap();
    let result = sql.read_snapshot("SELECT * FROM rows").unwrap();
    let SqlOutcome::Selected { rows } = result.outcome else {
        panic!("rows")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns["name"], "original");
    drop(sql);
    std::fs::remove_dir_all(state).unwrap();
}

#[test]
fn idle_application_rejects_same_version_divergent_protocol_history_repeatedly() {
    let state = crate::tests::test_path("parked-sql-divergence");
    let branch = crate::tests::test_path("parked-sql-branch");
    crate::tests::bootstrap_one_node(state.to_str().unwrap());
    let listener = DataPlaneListener::bind("127.0.0.1:0".parse().unwrap(), &state).unwrap();
    assert!(
        exchange(
            &listener,
            "CREATE TABLE rows(id PRIMARY KEY, name)",
            "table"
        )
        .starts_with("RSF/1 executed")
    );
    copy_tree(&state, &branch);
    assert!(
        exchange(
            &listener,
            "INSERT INTO rows(id,name) VALUES (1,original)",
            "one"
        )
        .starts_with("RSF/1 executed")
    );
    let other = DataPlaneListener::bind("127.0.0.1:0".parse().unwrap(), &branch).unwrap();
    assert!(
        exchange(
            &other,
            "INSERT INTO rows(id,name) VALUES (2,different)",
            "other"
        )
        .starts_with("RSF/1 executed")
    );
    drop(other);
    for name in ["protocol", "protocol.vsr"] {
        std::fs::remove_dir_all(state.join(name)).unwrap();
        copy_tree(&branch.join(name), &state.join(name));
    }
    for _ in 0..2 {
        let rejected = exchange(&listener, "SELECT * FROM rows", "read");
        assert!(rejected.starts_with("RSF/1 rejected"), "{rejected}");
        assert!(
            rejected.contains("retained application prefix"),
            "{rejected}"
        );
    }
    drop(listener);
    std::fs::remove_dir_all(state).unwrap();
    std::fs::remove_dir_all(branch).unwrap();
}
