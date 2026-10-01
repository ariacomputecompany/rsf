# Network-replicated SQL

The native SQL data plane replicates supported autocommit mutations as complete,
namespace-bound VSR commands. It uses the authenticated network configuration
described in [KV replication](kv-network-replication.md). Independent peers apply
the schema, rows, indexes, tombstones and original request receipts before
acknowledging the committed command.

## Command and publication boundary

The versioned `RSFSC1` payload contains the exact statement, request client and
placement group. Its enclosing `RSFC1` command binds that payload to the SQL
surface, namespace, full relational operation, request identity, strict mode and
commit metadata. Supported mutations use the existing narrow grammar for CREATE
TABLE, CREATE INDEX, INSERT, UPDATE and DELETE. SELECT reads materialized state.
Transaction-control statements are rejected by this durable autocommit path.

Before recording a new local intent, SQL resolves the prospective command
metadata through `preview_application_request` and plans the statement on a
private engine. This preview is not commit evidence. Planning checks constraints,
the complete-command size and physical page capacity before an invalid request
can leave an intent or commit. Planning and submission are serialized; publication
requires the actual quorum certificate to match the preview exactly.

Both primary and peer reconstruct a canonical physical base from committed
logical state before applying a new statement. This prevents different live
page/tombstone histories from changing capacity decisions after restart. It is
currently full-state work; incremental persistence remains necessary for scale.

The checksummed `RSF_SQL_STATE_V4` checkpoint atomically stores namespace binding,
schema, committed MVCC rows, receipts and complete commands. SQL clears the local
pending intent only after checkpoint publication. A persistence failure fences
further operations until reopen. The writer uses the shared operating-system
exclusive file-lock guard, released on process death.

## Recovery and retry contract

`open_replicated` binds an empty store to one namespace. It rejects a different
namespace and nonempty legacy stores that need explicit migration. Legacy V3
checkpoints remain readable through the unbound library contract. Refusing
migration preserves the old data.

Peer stores reside at
`application/namespaces/<SHA-256-of-namespace>/sql` beneath peer state. Startup
applies certified complete commands before readiness. Daemon namespace startup
applies committed SQL history in version order, then resolves its local pending
statement. A command durably replicated before the local checkpoint can therefore
restore the original result without submitting a different operation.

Replay validates namespace, surface, request, placement, strict mode, relational
operation and ordering. Checkpoint recovery validates command/receipt agreement.
Exact duplicates return the original result. An old INSERT retry after DELETE
does not resurrect the row. Changed statements under an existing request identity
are rejected, and rolled-back protocol history cannot create replacement receipts.
Network retries recheck quorum even when a local receipt exists.

Native SELECT confirms primary quorum before reading. The library's
`read_snapshot` method inspects only local materialized state and provides no
leader-freshness guarantee; it is useful for offline follower recovery inspection.

## Verification and limits

SQL tests cover schema, index, row and tombstone replay, restart after every
command, old retries, malformed commands, invalid planning before persistence,
namespace isolation, V3 readability and migration refusal. Process interruption
after protocol commit and injected checkpoint-write failure exercise recovery of
the original pending statement and persistence-error fencing.

`native_sql_ingress_replicates_rows_indexes_and_tombstones_to_real_peers` runs a
primary and two authenticated peers with independent storage. It verifies writes
after peer loss, termination of the primary and current peer, offline follower
inspection, lagging-peer repair, original retry responses, no resurrection and
read/write-retry rejection without quorum. The SQL and daemon host-backed Fozzy
domains execute these tests and record, strictly verify, replay and CI-check their
traces.

The full verification gate for this implementation passed all 90 JSON results
across 15 domains; local evidence is in
`artifacts/sql-network-production.log` and `artifacts/verification.w5ElyB/`.
This establishes the tested paths, not overall production readiness.

Durable SQL sessions, multiple pages per table, bounded result streaming,
incremental checkpoints/replay and bounded receipt retention remain release work.
Native frames are limited to 64 KiB and complete commands to 1 MiB. Network
membership, automatic primary failover, historical topology validation, legacy
migration and machine-failure/performance measurements also remain unfinished.
