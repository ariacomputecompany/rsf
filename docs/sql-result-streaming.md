# Native SQL result streaming

An authenticated native SQL SELECT can request streaming by setting its body to
exactly `RSF_SQL_STREAM_V1\n`. The operation remains a SELECT, with the usual
namespace-read authorization and current-primary quorum check. Standalone and
elected-peer client listeners use the same implementation. Streaming does not
create a durable session or change the SQL command/receipt format.

The server reconciles pending state before reading and serializes writes for the
entire visit. Rows come from one local materialized view established after quorum
confirmation. The `version` identifies the router version for that view. A new
connection establishes a new view; there is no resumable cursor or durable read
lease. Streaming can complete after leadership changes if its view was admitted
before the change. A new request without quorum is rejected before emitting rows.

## Wire contract

A result is a sequence of ASCII records, each terminated by one LF:

```text
RSF/1 sql-stream protocol=sql-wire surface=sql version=129
RSF/1 row-begin ordinal=0
RSF/1 row-chunk bytes=24 payload_hex=...
RSF/1 row-end ordinal=0 bytes=24
RSF/1 completed protocol=sql-wire surface=sql version=129 rows=1 bytes=24
```

The ellipsis above stands for the example's omitted hexadecimal bytes, not a
valid payload. Each row is one JSON object of projected SQL column/value strings.
A JSON object can span multiple `row-chunk` records. Chunk payloads contain at most
4,096 decoded bytes. Ordinals start at zero and advance once per complete row.
Row and completion byte counts cover decoded JSON bytes only, excluding framing.
Empty results contain a header and completion with zero rows/bytes.

Clients must require matching row ordinals, chunk/row/result byte counts, the
terminal completion record, and clean transport completion. EOF, an error record,
invalid framing, or a sink failure means an incomplete query. Partial rows/results
must not be treated as a successful complete response. No completion is published
when a row fails to decode, the result sink closes, or the socket budget expires.
An admission or planning failure can instead return the ordinary rejection line.

`rsf_wire::sql_stream::decode_sql_stream` implements this contract with bounded
record/chunk memory and a callback receiving `RowBegin`, borrowed `Chunk` bytes,
and `RowEnd`. It returns `SqlStreamSummary` only after validating completion and
EOF. Consumers choose whether to decode, store, or forward rows; retaining the
whole result in a callback is a client decision. The decoder rejects oversized
records, malformed hex/numbers, missing/reordered row boundaries, mismatched
counts/versions, truncated prefixes and trailing data. It validates transport
framing; consumers decoding JSON must also validate their row schema.

## Memory and compatibility

The query executor pulls one candidate at a time and stops at LIMIT or sink error.
Committed KV scans borrow their ordered prefix, and staged transaction mutations
merge in key order without cloning a result map. Fallback planning statistics count
encoded row bytes without decoding the table. Only the latest query report is
retained, with a cumulative report count. Invalid decoded rows fail the query
rather than silently disappearing from a streamed scan.

Native delivery retains a 4 KiB output chunk and one decoded row, plus schema,
query-plan and TLS state; it does not collect the candidate set, result rows or
whole-result JSON/hex. Its bound depends on the largest row and existing catalog
metadata. The listener retains the same cumulative two-second transport I/O budget
for handshake, request and response. Application CPU work does not consume that
budget; every blocking socket call during execution does. Slow readers cannot
renew it by receiving successive chunks. There is no query execution-time limit.

Legacy empty-body SELECT keeps its ordinary `executed` response for JSON payloads
up to 64 KiB. Larger results reject with an instruction to request streaming; they
are never silently truncated. Durable session reads retain their 24 KiB canonical
receipt bound, checked as rows are produced before adding them to the result.
They use session context bodies and cannot also request this autocommit stream.
Use a smaller LIMIT for a session read. Existing library collecting APIs remain
available; callers needing bounded result retention use
`DurableSqlEngine::read_snapshot_into` and establish read freshness themselves.

These changes bound result-path allocations. The resident database, retained MVCC
and receipts, cold recovery, and durable-session snapshot reconstruction still
need separate memory/retention/capacity work. Secondary query candidates currently
scan ordered rows while applying the typed predicate; incremental indexed planning
and production throughput qualification remain open.

## Qualification

The native TLS workload populates 128 rows, streams 264,960 decoded JSON bytes,
checks every value and row order, and covers projection/LIMIT, empty results,
buffered overflow, invalid operations and missing quorum. Both debug/release
profiles are required by the new SQL streaming Fozzy domain. Automatic takeover
also requires a streamed read from the replacement after old-primary data loss.
The workspace gate requires all nine pull/stream/deadline regressions. Current
full reports remain historical until the expanded unchanged-source gate passes.
