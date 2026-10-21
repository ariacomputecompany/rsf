use super::*;
use rsf_wire::sql_stream::{SQL_STREAM_REQUEST, SqlStreamEvent, decode_sql_stream};

pub(super) fn rows(response: &str) -> Vec<BTreeMap<String, String>> {
    let mut rows = Vec::new();
    let mut current = Vec::new();
    let summary = decode_sql_stream(response.as_bytes(), |event| {
        match event {
            SqlStreamEvent::RowBegin { .. } => current.clear(),
            SqlStreamEvent::Chunk(bytes) => current.extend_from_slice(bytes),
            SqlStreamEvent::RowEnd { .. } => rows.push(serde_json::from_slice(&current).unwrap()),
        }
        Ok(())
    })
    .unwrap_or_else(|error| panic!("{}: {response}", error.message));
    assert_eq!(summary.rows as usize, rows.len());
    rows
}

#[test]
fn native_sql_streams_large_results_and_bounds_legacy_responses() {
    use rsf_wire::WireProtocol::Sql;
    let started = std::time::Instant::now();
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    let create = surface_request(
        primary.address,
        Sql,
        "CREATE TABLE streamed(id PRIMARY KEY, name)",
        "create-streamed",
        b"",
    );
    assert!(create.starts_with("RSF/1 executed"), "{create}");
    let value = "x".repeat(2048);
    for id in 0..128 {
        let response = surface_request(
            primary.address,
            Sql,
            &format!("INSERT INTO streamed (id, name) VALUES ('{id:03}', '{value}')"),
            &format!("stream-{id}"),
            b"",
        );
        assert!(response.starts_with("RSF/1 executed"), "{response}");
    }
    let buffered = surface_request(
        primary.address,
        Sql,
        "SELECT * FROM streamed",
        "buffer-limit",
        b"",
    );
    assert!(
        buffered.starts_with("RSF/1 rejected") && buffered.contains("RSF_SQL_STREAM_V1"),
        "{buffered}"
    );
    let stream = surface_request(
        primary.address,
        Sql,
        "SELECT * FROM streamed",
        "stream-all",
        SQL_STREAM_REQUEST,
    );
    let result = rows(&stream);
    assert_eq!(result.len(), 128);
    for (id, row) in result.iter().enumerate() {
        assert_eq!(row["id"], format!("{id:03}"));
        assert_eq!(row["name"], value);
    }
    let limited = surface_request(
        primary.address,
        Sql,
        "SELECT id FROM streamed LIMIT 2",
        "stream-limit",
        SQL_STREAM_REQUEST,
    );
    assert_eq!(
        rows(&limited),
        [
            BTreeMap::from([("id".into(), "000".into())]),
            BTreeMap::from([("id".into(), "001".into())])
        ]
    );
    let empty = surface_request(
        primary.address,
        Sql,
        "SELECT * FROM streamed WHERE id=absent",
        "stream-empty",
        SQL_STREAM_REQUEST,
    );
    assert!(rows(&empty).is_empty());
    let invalid = surface_request(
        primary.address,
        Sql,
        "INSERT INTO streamed (id, name) VALUES (forbidden, no)",
        "stream-write",
        SQL_STREAM_REQUEST,
    );
    assert!(invalid.starts_with("RSF/1 rejected"), "{invalid}");
    let missing = surface_request(
        primary.address,
        Sql,
        "SELECT * FROM missing",
        "stream-invalid",
        SQL_STREAM_REQUEST,
    );
    assert!(missing.starts_with("RSF/1 rejected"), "{missing}");
    drop(first);
    drop(second);
    let unavailable = surface_request(
        primary.address,
        Sql,
        "SELECT * FROM streamed",
        "stream-no-quorum",
        SQL_STREAM_REQUEST,
    );
    assert!(unavailable.starts_with("RSF/1 rejected"), "{unavailable}");
    assert!(!unavailable.contains("RSF/1 completed"));
    let summary = decode_sql_stream(stream.as_bytes(), |_| Ok(())).unwrap();
    println!(
        "{}",
        serde_json::json!({"workload":"native-sql-stream", "profile":if cfg!(debug_assertions) {"debug"} else {"release"}, "rows":summary.rows, "bytes":summary.bytes, "chunk_bound":rsf_wire::sql_stream::SQL_STREAM_CHUNK_BYTES,"buffered_overflow_rejected":true,"quorum_loss_rejected":true,"elapsed_ms":started.elapsed().as_millis(),"scope":"single-host loopback TLS with fsync; streaming delivery proof, not production capacity qualification"})
    );
}
