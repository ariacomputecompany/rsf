//! Native SQL result streaming. Consumers must discard partial results if this
//! decoder returns an error; only its successful summary proves completion.
use rsf_core::{ErrorCode, RsfError, RsfResult};
use std::io::{BufRead, Read};
pub const SQL_STREAM_REQUEST: &[u8] = b"RSF_SQL_STREAM_V1\n";
pub const SQL_STREAM_CHUNK_BYTES: usize = 4096;
const MAX_LINE: usize = SQL_STREAM_CHUNK_BYTES * 2 + 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlStreamSummary {
    pub version: u64,
    pub rows: u64,
    pub bytes: u64,
}
#[derive(Debug, Eq, PartialEq)]
pub enum SqlStreamEvent<'a> {
    RowBegin { ordinal: u64 },
    Chunk(&'a [u8]),
    RowEnd { ordinal: u64, bytes: u64 },
}
/// Decode one complete connection with bounded record/chunk memory. The sink
/// controls any result retention. Transport EOF before completion is an error.
pub fn decode_sql_stream(
    mut input: impl BufRead,
    mut sink: impl FnMut(SqlStreamEvent<'_>) -> RsfResult<()>,
) -> RsfResult<SqlStreamSummary> {
    let mut buffer = Vec::with_capacity(MAX_LINE);
    read_line(&mut input, &mut buffer)?;
    let header = fields(&buffer)?;
    if header.len() != 5
        || header[..4] != ["RSF/1", "sql-stream", "protocol=sql-wire", "surface=sql"]
    {
        return Err(invalid("invalid SQL stream header"));
    }
    let version = number(header[4], "version=")?;
    let (mut rows, mut bytes, mut active) = (0_u64, 0_u64, None);
    loop {
        read_line(&mut input, &mut buffer)?;
        let record = fields(&buffer)?;
        match record.as_slice() {
            ["RSF/1", "row-begin", ordinal] => {
                if active.is_some() || number(ordinal, "ordinal=")? != rows {
                    return Err(invalid("SQL row order mismatch"));
                }
                active = Some(0_u64);
                sink(SqlStreamEvent::RowBegin { ordinal: rows })?;
            }
            ["RSF/1", "row-chunk", length, hex] => {
                let length = number(length, "bytes=")?;
                if length == 0 || length > SQL_STREAM_CHUNK_BYTES as u64 {
                    return Err(invalid("SQL chunk size exceeds bound"));
                }
                let hex = hex
                    .strip_prefix("payload_hex=")
                    .ok_or_else(|| invalid("SQL chunk payload missing"))?;
                if hex.len() != length as usize * 2 {
                    return Err(invalid("SQL chunk length mismatch"));
                }
                let row_bytes = active
                    .as_mut()
                    .ok_or_else(|| invalid("SQL chunk outside row"))?;
                let decoded = hex
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        fn nibble(byte: u8) -> RsfResult<u8> {
                            match byte {
                                b'0'..=b'9' => Ok(byte - b'0'),
                                b'a'..=b'f' => Ok(byte - b'a' + 10),
                                _ => Err(invalid("noncanonical SQL chunk hex")),
                            }
                        }
                        Ok(nibble(pair[0])? * 16 + nibble(pair[1])?)
                    })
                    .collect::<RsfResult<Vec<_>>>()?;
                *row_bytes = row_bytes
                    .checked_add(length)
                    .ok_or_else(|| invalid("SQL row byte count overflow"))?;
                sink(SqlStreamEvent::Chunk(&decoded))?;
            }
            ["RSF/1", "row-end", ordinal, length] => {
                let row_bytes = active
                    .take()
                    .ok_or_else(|| invalid("SQL row end without row"))?;
                if number(ordinal, "ordinal=")? != rows || number(length, "bytes=")? != row_bytes {
                    return Err(invalid("SQL row end mismatch"));
                }
                sink(SqlStreamEvent::RowEnd {
                    ordinal: rows,
                    bytes: row_bytes,
                })?;
                rows = rows
                    .checked_add(1)
                    .ok_or_else(|| invalid("SQL row count overflow"))?;
                bytes = bytes
                    .checked_add(row_bytes)
                    .ok_or_else(|| invalid("SQL result byte count overflow"))?;
            }
            [
                "RSF/1",
                "completed",
                "protocol=sql-wire",
                "surface=sql",
                end_version,
                end_rows,
                end_bytes,
            ] => {
                if active.is_some()
                    || number(end_version, "version=")? != version
                    || number(end_rows, "rows=")? != rows
                    || number(end_bytes, "bytes=")? != bytes
                {
                    return Err(invalid("SQL completion mismatch"));
                }
                let mut trailing = [0];
                if input
                    .read(&mut trailing)
                    .map_err(|error| invalid(error.to_string()))?
                    != 0
                {
                    return Err(invalid("SQL data follows completion"));
                }
                return Ok(SqlStreamSummary {
                    version,
                    rows,
                    bytes,
                });
            }
            _ => return Err(invalid("invalid or rejected SQL stream record")),
        }
    }
}
fn read_line(input: &mut impl BufRead, output: &mut Vec<u8>) -> RsfResult<()> {
    output.clear();
    input
        .take((MAX_LINE + 1) as u64)
        .read_until(b'\n', output)
        .map_err(|error| invalid(error.to_string()))?;
    if output.is_empty() || output.len() > MAX_LINE || output.last() != Some(&b'\n') {
        return Err(invalid("SQL stream truncated or record exceeds bound"));
    }
    Ok(())
}
fn fields(line: &[u8]) -> RsfResult<Vec<&str>> {
    let text = std::str::from_utf8(line).map_err(|_| invalid("SQL stream record is not UTF-8"))?;
    if !text.is_ascii() {
        return Err(invalid("SQL stream record is not ASCII"));
    }
    Ok(text.trim_end_matches('\n').split(' ').collect())
}
fn number(field: &str, prefix: &str) -> RsfResult<u64> {
    let value = field
        .strip_prefix(prefix)
        .ok_or_else(|| invalid("SQL stream field missing"))?;
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(invalid("noncanonical SQL stream number"));
    }
    value
        .parse()
        .map_err(|_| invalid("SQL stream number overflow"))
}
fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    const VALID: &str = "RSF/1 sql-stream protocol=sql-wire surface=sql version=7\nRSF/1 row-begin ordinal=0\nRSF/1 row-chunk bytes=2 payload_hex=7b7d\nRSF/1 row-end ordinal=0 bytes=2\nRSF/1 completed protocol=sql-wire surface=sql version=7 rows=1 bytes=2\n";
    #[test]
    fn sql_stream_decoder_requires_complete_ordered_bounded_results() {
        let mut result = Vec::new();
        let summary = decode_sql_stream(VALID.as_bytes(), |event| {
            if let SqlStreamEvent::Chunk(bytes) = event {
                result.extend_from_slice(bytes);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(result, b"{}");
        assert_eq!(
            summary,
            SqlStreamSummary {
                version: 7,
                rows: 1,
                bytes: 2
            }
        );
        for length in 0..VALID.len() {
            assert!(
                decode_sql_stream(&VALID.as_bytes()[..length], |_| Ok(())).is_err(),
                "prefix {length}"
            );
        }
        for (from, to) in [
            ("rows=1", "rows=2"),
            ("ordinal=0", "ordinal=1"),
            ("bytes=2 payload", "bytes=3 payload"),
            ("7b7d", "7B7d"),
            ("version=7 rows", "version=8 rows"),
            ("row-end ordinal=0 bytes=2", "row-end ordinal=0 bytes=3"),
        ] {
            assert!(decode_sql_stream(VALID.replacen(from, to, 1).as_bytes(), |_| Ok(())).is_err());
        }
        assert!(decode_sql_stream(format!("{VALID}extra").as_bytes(), |_| Ok(())).is_err());
        let oversized = format!("RSF/1 {}\n", "x".repeat(MAX_LINE));
        assert!(decode_sql_stream(oversized.as_bytes(), |_| Ok(())).is_err());
        assert!(
            decode_sql_stream(VALID.as_bytes(), |_| Err(invalid("closed sink")))
                .unwrap_err()
                .message
                .contains("closed sink")
        );
    }
}
