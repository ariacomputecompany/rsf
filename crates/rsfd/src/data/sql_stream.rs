//! Explicit native SQL streaming. Only a terminal completion record establishes
//! a complete result; partial rows or an EOF are never a successful query.
use super::*;
pub(super) use rsf_wire::sql_stream::SQL_STREAM_REQUEST as REQUEST;
const CHUNK_BYTES: usize = rsf_wire::sql_stream::SQL_STREAM_CHUNK_BYTES;
const BUFFERED_BYTES: usize = 64 * 1024;

pub(super) fn buffered(
    sql: &mut DurableSqlEngine,
    request: &RequestEnvelope,
) -> RsfResult<DataPlaneResponse> {
    let mut payload = vec![b'['];
    let count = sql.read_snapshot_into(&request.operation, |row| {
        let encoded = serde_json::to_vec(&row.columns).map_err(|err| invalid(err.to_string()))?;
        let separator = usize::from(payload.len() > 1);
        if payload.len().saturating_add(separator + encoded.len() + 1) > BUFFERED_BYTES {
            return Err(invalid(
                "SQL result exceeds buffered response limit; use RSF_SQL_STREAM_V1",
            ));
        }
        if separator != 0 {
            payload.push(b',');
        }
        payload.extend_from_slice(&encoded);
        Ok(())
    })?;
    payload.push(b']');
    Ok(DataPlaneResponse::Executed {
        protocol: request.protocol.as_str(),
        operation_kind: "read",
        surface: "sql",
        version: None,
        bytes: payload.len() as u64,
        payload,
        detail: format!("select rows={count} replayed=false"),
    })
}

pub(super) fn stream(
    sql: &mut DurableSqlEngine,
    request: &RequestEnvelope,
    version: u64,
    output: &mut dyn Write,
) -> RsfResult<DataPlaneResponse> {
    let mut started = false;
    let mut bytes = 0_u64;
    let mut ordinal = 0_usize;
    let mut start = |output: &mut dyn Write| -> std::io::Result<()> {
        if !started {
            writeln!(
                output,
                "RSF/1 sql-stream protocol={} surface=sql version={version}",
                request.protocol.as_str()
            )?;
            started = true;
        }
        Ok(())
    };
    let rows = sql.read_snapshot_into(&request.operation, |row| {
        start(output).map_err(io_error)?;
        writeln!(output, "RSF/1 row-begin ordinal={ordinal}").map_err(io_error)?;
        let mut chunks = RowChunks {
            output,
            buffer: Vec::with_capacity(CHUNK_BYTES),
            bytes: 0,
        };
        serde_json::to_writer(&mut chunks, &row.columns)
            .map_err(|err| invalid(format!("SQL stream row failed: {err}")))?;
        chunks.flush().map_err(io_error)?;
        let row_bytes = chunks.bytes;
        writeln!(
            chunks.output,
            "RSF/1 row-end ordinal={ordinal} bytes={row_bytes}"
        )
        .map_err(io_error)?;
        bytes = bytes
            .checked_add(row_bytes)
            .ok_or_else(|| invalid("SQL result byte count exhausted"))?;
        ordinal += 1;
        Ok(())
    })?;
    start(output).map_err(io_error)?;
    writeln!(
        output,
        "RSF/1 completed protocol={} surface=sql version={version} rows={rows} bytes={bytes}",
        request.protocol.as_str()
    )
    .map_err(io_error)?;
    Ok(DataPlaneResponse::Streamed {
        protocol: request.protocol.as_str(),
        rows,
        bytes,
        version,
    })
}
fn io_error(error: std::io::Error) -> RsfError {
    RsfError::new(
        ErrorCode::InvalidInput,
        format!("SQL stream write failed: {error}"),
    )
}
struct RowChunks<'a> {
    output: &'a mut dyn Write,
    buffer: Vec<u8>,
    bytes: u64,
}
impl RowChunks<'_> {
    fn emit(&mut self) -> std::io::Result<()> {
        if !self.buffer.is_empty() {
            writeln!(
                self.output,
                "RSF/1 row-chunk bytes={} payload_hex={}",
                self.buffer.len(),
                hex_bytes(&self.buffer)
            )?;
            self.bytes += self.buffer.len() as u64;
            self.buffer.clear();
        }
        Ok(())
    }
}
impl Write for RowChunks<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for part in bytes.chunks(CHUNK_BYTES) {
            let count = (CHUNK_BYTES - self.buffer.len()).min(part.len());
            self.buffer.extend_from_slice(&part[..count]);
            if self.buffer.len() == CHUNK_BYTES {
                self.emit()?;
            }
            self.buffer.extend_from_slice(&part[count..]);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.emit()?;
        self.output.flush()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn sql_stream_sink_failure_never_publishes_completion() {
        struct Closed {
            bytes: Vec<u8>,
            remaining: usize,
        }
        impl Write for Closed {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.remaining == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "closed client",
                    ));
                }
                let count = bytes.len().min(self.remaining);
                self.remaining -= count;
                self.bytes.extend_from_slice(&bytes[..count]);
                Ok(count)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let root = crate::tests::test_path("stream-closed-sink");
        let mut sql = DurableSqlEngine::open(&root).unwrap();
        let mut router = rsf_protocol::FabricRouter::with_groups([PlacementGroupId(1)]);
        for (statement, client) in [
            ("CREATE TABLE users(id PRIMARY KEY, name)", "create"),
            ("INSERT INTO users (id, name) VALUES (1, first)", "insert"),
        ] {
            sql.execute(&mut router, PlacementGroupId(1), statement, client)
                .unwrap();
        }
        let mut request = RequestEnvelope {
            protocol: WireProtocol::Sql,
            principal: "app-client".into(),
            namespace: Some("app".into()),
            operation: "SELECT * FROM users".into(),
            operation_kind: WireOperationKind::Read,
            idempotency_key: None,
            body_bytes: REQUEST.len() as u64,
            body: REQUEST.to_vec(),
        };
        let mut closed = Closed {
            bytes: Vec::new(),
            remaining: 100,
        };
        assert!(stream(&mut sql, &request, router.latest_version().0, &mut closed).is_err());
        assert!(
            !String::from_utf8(closed.bytes.clone())
                .unwrap()
                .contains("RSF/1 completed")
        );
        assert!(
            rsf_wire::sql_stream::decode_sql_stream(closed.bytes.as_slice(), |_| Ok(())).is_err()
        );
        request.operation = "SELECT * FROM missing".into();
        let mut output = Vec::new();
        assert!(stream(&mut sql, &request, router.latest_version().0, &mut output).is_err());
        assert!(output.is_empty());
    }

    use super::*;
    #[test]
    fn sql_row_chunks_bound_frames_and_preserve_escaped_json() {
        let columns = std::collections::BTreeMap::from([("name", "é\\\"\n".repeat(8000))]);
        let mut output = Vec::new();
        let mut chunks = RowChunks {
            output: &mut output,
            buffer: Vec::with_capacity(CHUNK_BYTES),
            bytes: 0,
        };
        serde_json::to_writer(&mut chunks, &columns).unwrap();
        chunks.flush().unwrap();
        let bytes = chunks.bytes;
        let expected = serde_json::to_vec(&columns).unwrap();
        assert_eq!(bytes as usize, expected.len());
        let body = String::from_utf8(output).unwrap();
        let mut actual = Vec::new();
        for line in body.lines() {
            assert!(line.len() < CHUNK_BYTES * 2 + 128);
            let hex = line.split("payload_hex=").nth(1).unwrap();
            assert!(hex.len() <= CHUNK_BYTES * 2);
            actual.extend(
                hex.as_bytes().chunks_exact(2).map(|pair| {
                    u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
                }),
            );
        }
        assert_eq!(actual, expected);
    }
}
