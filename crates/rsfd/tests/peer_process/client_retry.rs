//! Native client timeout/empty-response loss after a real SQL mutation commits.
//! The front connection drops responses; backend TLS, replication and SQL are real.
use super::*;
use rustls::{ServerConnection, StreamOwned};
use std::io::{Read, Write};

#[test]
fn automatic_client_takeover_transport_loss_preserves_exact_sql_mutation_after_restart() {
    use rsf_wire::WireProtocol::Sql;
    let root = root();
    let mut peers = provision(&root);
    let state = initialize_network_state(&root, &peers);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    assert!(
        surface_request(
            primary.address,
            Sql,
            "CREATE TABLE retry_rows(id PRIMARY KEY, name)",
            "retry-table",
            b""
        )
        .starts_with("RSF/1 executed")
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let backend = primary.address;
    let tls = tls::server_config(&state).unwrap();
    let worker = std::thread::spawn(move || {
        let mut requests = Vec::new();
        let mut responses = Vec::new();
        for attempt in 0..3 {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut front = StreamOwned::new(ServerConnection::new(tls.clone()).unwrap(), socket);
            let mut length = [0; 4];
            front.read_exact(&mut length).unwrap();
            let length = u32::from_le_bytes(length) as usize;
            assert!(length <= 1024 * 1024);
            let mut frame = vec![0; length];
            front.read_exact(&mut frame).unwrap();
            let response =
                test_support::send(backend, &frame, test_support::pki().client.clone()).unwrap();
            assert!(response.starts_with("RSF/1 executed"), "{response}");
            requests.push(frame);
            responses.push(response.clone());
            if attempt == 0 {
                // The backend has completed the write, but retain the client
                // connection beyond its unchanged three-second read deadline.
                std::thread::sleep(Duration::from_millis(3100));
            } else if attempt == 2 {
                front.write_all(response.as_bytes()).unwrap();
                front.flush().unwrap();
            }
            // The second connection closes cleanly with no application status.
            front.conn.send_close_notify();
            let _ = front.flush();
        }
        (requests, responses)
    });
    let statement = "INSERT INTO retry_rows (id, name) VALUES (1, exact)";
    let before = surface_transport_retries();
    let response = surface_request(address, Sql, statement, "lost-client-reply", b"");
    assert_eq!(surface_transport_retries() - before, 2);
    let (requests, responses) = worker.join().unwrap();
    assert_eq!(requests[0], requests[1]);
    assert_eq!(requests[1], requests[2]);
    assert_eq!(response, responses[2]);
    assert_eq!(
        responses[1],
        responses[0].replace("replayed=false", "replayed=true")
    );
    assert_eq!(responses[2], responses[1]);
    drop(primary);
    drop(first);
    drop(second);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    configure_primary(&root, &state, &peers);
    let primary = Process::primary(&state);
    assert_eq!(
        surface_request(primary.address, Sql, statement, "lost-client-reply", b""),
        response
    );
    let changed = surface_request(
        primary.address,
        Sql,
        "INSERT INTO retry_rows (id, name) VALUES (1, changed)",
        "lost-client-reply",
        b"",
    );
    assert!(changed.starts_with("RSF/1 rejected"), "{changed}");
    drop(primary);
    drop(first);
    drop(second);
    for node in [1, 2] {
        let mut sql = rsf_sql::DurableSqlEngine::open_replicated(
            client_takeover::crate_namespace(&root.join(format!("store-{node}"))).join("sql"),
            "app",
        )
        .unwrap();
        let result = sql.read_snapshot("SELECT * FROM retry_rows").unwrap();
        let rsf_sql::SqlOutcome::Selected { rows } = result.outcome else {
            panic!("SQL rows")
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns["name"], "exact");
        drop(sql);
        let cell = DurableVsrCell::open(
            root.join(format!("store-{node}/pg-1")),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        assert_eq!(
            cell.committed_prefix().unwrap().len(),
            2,
            "transport retry added a protocol commit"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
