use super::*;

#[test]
fn real_tls_begin_required_rejects_malformed_foreign_and_uncorrelated_responses() {
    let mut f = Fixture::new("control-begin-required-wire");
    let server = f.server.take().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let id = [43; 32];
    let worker = std::thread::spawn(move || {
        for case in 0..14 {
            let deadline = Instant::now() + Duration::from_millis(800);
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "status request {case} never arrived"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("status listener failed: {error}"),
                }
            };
            let mut stream = StreamOwned::new(
                ServerConnection::new(Arc::clone(&server.identity.server)).unwrap(),
                DeadlineStream::new(socket, deadline).unwrap(),
            );
            while stream.conn.is_handshaking() {
                stream.conn.complete_io(&mut stream.sock).unwrap();
            }
            let certificate = stream.conn.peer_certificates().unwrap().first().unwrap();
            let from = server.nodes[&fingerprint(certificate.as_ref())];
            assert_eq!(from, NodeId(0));
            let request = read_frame(&mut stream).unwrap();
            let (kind, request) = parse(&request, &server.cluster, from, server.local).unwrap();
            assert_eq!(&request[..40], &finish_bytes(id));
            let status_kind = if case == 10 {
                HISTORY_RECORD
            } else {
                HISTORY_FINISH
            };
            let error = HistoryBeginRequired::error(status_kind, CONTROL, id);
            let mut reply = crate::replication::history::status::from_error(&error)
                .unwrap()
                .encode();
            let mut response_kind = crate::replication::history::status::BEGIN_REQUIRED_RESPONSE;
            let mut cluster = server.cluster;
            let mut reporter = server.local;
            match case {
                0 | 10 => {
                    assert_eq!(kind, status_kind);
                } // exact positive roundtrip
                1 => {
                    reply.pop();
                }
                2 => reply.push(0),
                3 => reply[0] ^= 1,             // domain
                4 => reply[5] = b'2',           // unsupported version
                5 => reply[6] = HISTORY_RECORD, // request opcode
                6 => reply[7..15].copy_from_slice(&99u64.to_le_bytes()),
                7 => reply[15] ^= 1,                   // snapshot identity
                8 => assert_eq!(kind, HISTORY_BEGIN),  // status cannot answer BEGIN
                9 => assert_eq!(kind, HISTORY_RECORD), // FINISH status cannot answer RECORD
                11 => {
                    response_kind = 255;
                    reply = b"control history requires bounded BEGIN".to_vec();
                }
                12 => cluster[0] ^= 1,
                13 => reporter = NodeId(2),
                _ => unreachable!(),
            }
            write_frame(
                &mut stream,
                &frame(&cluster, reporter, from, response_kind, &reply).unwrap(),
            )
            .unwrap();
            stream.conn.send_close_notify();
            let _ = std::io::Write::flush(&mut stream);
        }
        server
    });
    let transport = f.transport(0, address);
    for case in 0..14 {
        let mut request = finish_bytes(id);
        let kind = match case {
            8 => HISTORY_BEGIN,
            9 | 10 => {
                request.extend_from_slice(&0u64.to_le_bytes());
                HISTORY_RECORD
            }
            _ => HISTORY_FINISH,
        };
        let result = transport.exchange_io(NodeId(1), kind, &request);
        if case == 0 || case == 10 {
            begin_required(result, case == 0, id);
            continue;
        }
        let error = result.unwrap_err();
        assert_eq!(
            error.kind(),
            io::ErrorKind::InvalidData,
            "case {case}: {error}"
        );
        assert!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<HistoryBeginRequired>())
                .is_none(),
            "uncorrelated case {case} became typed status"
        );
        if case == 11 {
            assert_eq!(
                error.to_string(),
                "peer rejected request: control history requires bounded BEGIN"
            );
        }
    }
    drop(worker.join().unwrap());
    f.cleanup();
}
