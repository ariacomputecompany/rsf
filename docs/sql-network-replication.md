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
currently full-state work; journal persistence does not remove that planning cost.

Namespace-bound stores use a checksummed `RSF_SQL_STATE_V5` checkpoint containing
schema, committed MVCC rows, complete commands, receipts and a journal index/hash.
Ordinary mutations append their complete command to `sql.journal` using the
storage hash chain and synced data, then atomically publish and sync
`sql.journal.head`. Only then may they clear the pending intent and acknowledge.
The separate checksummed head detects loss of an acknowledged record even if
storage recovery truncates its torn tail. A persistence failure fences further
operations until reopen. The shared operating-system exclusive writer lock is
released on process death.

After 64 commands or 1 MiB of encoded command payload since the checkpoint, the
writer publishes a full checkpoint by synced temporary file, rename and directory
sync. Only then does it compact the covered journal prefix. These are maintenance
thresholds, not capacity or latency guarantees. `checkpoint()` offers the same
ordering explicitly; `persistence_stats()` reports the local tail and checkpoint
positions and outstanding command/byte counts.

## Recovery and retry contract

`open_replicated` binds an empty store to one namespace. It rejects a different
namespace and nonempty legacy stores that need explicit migration. Legacy V3
checkpoints remain readable through the unbound library contract. Refusing
migration preserves the old data. Existing namespace-bound V4 stores upgrade
under the writer lock. A journal header binds namespace and the exact V4 base
checksum before publishing V5; interrupted header/head publication can resume.
Older readers reject the V5 format instead of silently ignoring its journal.
Unbound V3/V4 stores retain their existing full-checkpoint contract.

Recovery validates checkpoint and acknowledged-head hashes against retained
records or the compacted-prefix anchor, then replays only commands beyond the
checkpoint. Complete valid appends beyond the head are uncertain writes; recovery
materializes them and advances the head before serving. An incomplete append
beyond the head can be truncated. Missing acknowledged history, missing required
files, a changed migration base, or a stale checkpoint behind reclaimed history
fails closed on subsequent opens too. Reclamation bounds the command replay tail;
it does not bound checkpoint history, receipt memory or maintenance metadata.

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
after protocol commit exercises pending-statement recovery. Journal tests kill
writers after synced append, head publication, checkpoint publication and
compaction, and check original results/certificates after restart. Other tests
cover checkpoint and head-write failure fencing, acknowledged record loss across
repeated opens, torn unacknowledged tails, interrupted V4 upgrade, missing files,
foreign head hashes, stale snapshots and physical prefix reclamation.

`native_sql_ingress_replicates_rows_indexes_and_tombstones_to_real_peers` runs a
primary and two authenticated peers with independent storage. It verifies writes
after peer loss, termination of the primary and current peer, offline follower
inspection, lagging-peer repair, original retry responses, no resurrection and
read/write-retry rejection without quorum. The SQL and daemon host-backed Fozzy
domains execute these tests and record, strictly verify, replay and CI-check their
traces.

The journal milestone's [verification report](sql-journal-verification-2026-10-21.json)
records 66 passing SQL tests, passing native SQL network integration, strict host
trace verification/replay/CI and SQL Clippy. The full gate failed daemon,
automatic-client and debug-stress domains; an isolated automatic-client run also
failed election convergence after primary loss. The cause remains unresolved.
The gate was stopped during release compilation, so later domains and workspace
qualification are incomplete. The earlier whole-repository success in
[verification-2026-10-21.json](verification-2026-10-21.json) covers its recorded
older source hash, not this milestone. Current source is not production-qualified.

Durable SQL sessions, bounded result streaming, bounded receipt retention,
incremental physical planning and growing-table capacity qualification remain
release work. Multiple pages and atomic row relocation are implemented. Native
frames are limited to 64 KiB and complete commands to 1 MiB. Certified network
membership changes, independent-group routing, explicit legacy migration and
independent-machine fault/performance measurements remain unfinished. Opt-in
automatic takeover is implemented; its production qualification remains open.
