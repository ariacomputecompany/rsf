# Network-replicated SQL

The native SQL data plane replicates supported autocommit mutations as complete,
namespace-bound VSR commands. It uses the authenticated network configuration
described in [KV replication](kv-network-replication.md). Independent peers apply
the schema, rows, indexes, tombstones and original request receipts before
acknowledging the committed command.

## Command and publication boundary

New namespaces use the versioned `RSFSC3` payload, with an explicit command kind
and `KEY2` request encoding binding the relational key format. It contains the
exact statement, request client and placement group. Legacy namespaces retain
`RSFSC1` autocommit and `RSFSC2` session encodings. Its enclosing `RSFC1` command binds that payload to the SQL
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

New namespace-bound stores use a checksummed `RSF_SQL_STATE_V8` checkpoint containing
schema, committed MVCC rows, complete commands, receipts, active sessions and a journal index/hash.
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
checksum before publishing the current format; interrupted header/head publication can resume.
V5 checkpoints remain readable and upgrade on the next checkpoint. Existing
V3–V6 stores retain their legacy row-key encoding; journaled legacy stores continue
to write V6 checkpoints. New framed-key stores use V7 before journal binding and
V8 afterward. Older readers reject unsupported checkpoint and command versions.
Upgrade every voter before enabling new formats or sessions. Unbound legacy
stores retain their existing full-checkpoint contract.

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

## Relational key identity and legacy recovery

New stores encode row identities with a versioned binary domain, a length-prefixed
table name and the primary-key suffix. Constraint identities frame the table,
constraint name and each value separately. Table names and primary keys containing
slashes therefore cannot alias another table's rows or uniqueness identities.
Primary-key byte ordering within a table is preserved. Index rebuilds clear only
the selected table/index scope and validate all replacement mutations before
clearing any entries; generated index names can coincide across tables safely.

The checkpoint, pending intent and complete command bind one relational key
format. Recovery rejects mixed formats, unowned row keys and format downgrades.
An entirely blank derived peer store can learn the legacy format from its first
certified command, persisting that choice before appending the command. A nonempty
store cannot switch formats. Existing receipts and certificates retain their
original bytes and results.

Legacy delimited keys cannot represent every valid pair of table names without
ambiguity. Legacy stores reject new overlapping table prefixes before submission;
recovery rejects an already ambiguous catalog on every open without rewriting it.
An explicit data/receipt-preserving migration remains release work. These fences
do not migrate or reinterpret old data.

## Durable SQL sessions

Native requests with an empty body retain autocommit behavior. To use a session,
send `SqlSessionContext::encode()` as the SQL request body. Its canonical encoding
is `RSF_SQL_SESSION_V1\n` followed by JSON such as
`{"name":"import","transaction":null}` for BEGIN. Subsequent requests use the
64-character transaction token returned in BEGIN's `session=` result. The owner
comes from the authenticated principal, not the body. Session names are isolated
by namespace and principal. Transaction controls require namespace-write access;
SELECT requires namespace-read access. Current authorization is rechecked for
every native request, including retries.

BEGIN, staged DDL/DML, SAVEPOINT, ROLLBACK TO, COMMIT and ROLLBACK each carry a
complete `RSFSC3` session command through the strict quorum protocol in new
framed-key stores (`RSFSC2` in legacy stores). Their acknowledgements
make session state durable, while staged rows and schema remain invisible outside
the session. Session SELECT uses the BEGIN snapshot plus its own staged statements.
SELECT durably records its bounded result and row/predicate dependencies through
the strict quorum path before returning. Response `version` certifies that read
acknowledgement; `snapshot` is the older BEGIN data snapshot. Identical queries
against an unchanged snapshot and staged statement prefix reuse their original
read receipt, including after restart or failover.
COMMIT publishes all final row effects, schema changes and session closure at one
certified version. Transaction tokens prevent a stale request from committing a
new transaction after reuse of a name. Exact retries retain their original results,
including staged results after COMMIT; they never reopen the old transaction.
The pending intent contains the complete proposal, preserving an uncertain COMMIT
through process death. Recovery checks the transition chain, original results,
row effects, catalog history and active-session inventory.

Current native sessions use serializable row/predicate validation. Successful
mutations retain implicit row/existence dependencies, including inserts later
rewound by a savepoint. SELECT retains its supported equality predicate or table
scan. Dependencies survive ROLLBACK TO and process loss: forgetting observed reads
while rewinding writes would allow write skew. COMMIT rejects later changes to
dependent keys and changes whose old or new row matches an observed predicate,
including phantoms and tombstones. Catalog changes still invalidate a transaction.
Disjoint point reads/writes can commit, preserving the latest unrelated rows.
Predicate checks reuse SELECT's typed query planner/executor. They conservatively
cover the full predicate even with LIMIT or a narrower projection. ROLLBACK remains
available. Validation and planning still inspect retained history; throughput,
growing-history capacity and tighter conflict windows remain qualification work.

New decisions carry the `RSFSP3` read-tracking marker inside the complete command
and its operation digest. Canonical `RSFSP1` snapshot-only and `RSFSP2` namespace
snapshot commands remain readable; historical certificates and retry results are
unchanged. A new COMMIT on an older active session retains conservative
namespace-wide validation because earlier reads were not tracked. Reconciliation
checks an uncertified pre-upgrade COMMIT against namespace data changes before
resubmission and retains a conflicting intent for authoritative recovery. A
previously certified legacy COMMIT retains its original semantics. Upgrade every
voter and drain uncertain pre-upgrade writes before qualifying serializable
operation; mixed-version or unresolved legacy execution is not that guarantee.

The transaction is pinned to its original placement group. Limits per namespace
are 128 active sessions, 64 staged mutation statements per transaction, 32
savepoints, 128 unique row/query dependencies, 64 KiB of canonical session state,
24 KiB of canonical SELECT rows and 384 KiB of canonical transition payload.
SELECTs that exceed the result bound require a smaller LIMIT. Result admission
checks canonical bytes while rows are produced, before retaining the next row.
Session snapshot reconstruction and retained database/history memory remain
separate capacity work. Autocommit native reads support
[bounded result streaming](sql-result-streaming.md).
Limit failures occur before intent or
protocol submission and preserve the transaction for rollback. There is no
automatic expiry; clients must close abandoned transactions. Session reads and
planning reconstruct retained snapshot history and are not capacity-qualified.

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

The SQL session regressions cover transaction ownership, snapshot conflicts,
savepoint recovery, schema invalidation, active-session/statement/savepoint/byte
limits, checkpoint inventory and catalog validation, V5 compatibility, and a
real process kill after COMMIT durability before local publication. Both debug
and release automatic takeover profiles resume an open session after the old
primary's data is deleted, roll back to its savepoint, commit schema and rows,
and verify original responses after replacement restart on both surviving stores.
The full gate requires these named workspace tests and takeover metrics. See
[session verification](sql-session-verification-2026-10-21.json), including the
first attempt's missed proof-of-concept result consumer and its correction.
The [serializable verification](sql-serializable-verification-2026-10-21.json)
adds write-skew checks after restart/follower recovery, phantom inserts and
tombstones, legacy receipt/intent compatibility, checkpoint isolation validation,
and native phantom rejection in both automatic takeover profiles. All eleven
required session regressions ran in the full workspace host trace.
The [read-dependency verification](sql-read-dependencies-verification-2026-10-21.json)
adds disjoint transaction commits, dependency retention across savepoint rewind,
bounded read/result admission, real process-kill read recovery, original RSFSP2
receipt compatibility, and a pre-crash acknowledged read across native takeover.
Both profiles verify the resulting disjoint writes on both surviving stores.
All sixteen required session regressions ran in that full workspace trace.
The subsequent relational key qualification adds row/constraint identity, legacy
format fences, cold legacy peer recovery and table-scoped index rebuild checks.

The [key-identity verification](sql-key-identity-verification-2026-10-21.json)
records both original failing reproductions, all eight new required regressions,
legacy follower reconstruction after derived SQL directory deletion, and isolated
colliding table names through debug/release takeover. All 24 required SQL/key
regressions ran in that full workspace trace (1,129 passing tests).

The [current full verification](verification-2026-10-21.json) passes all 19
domains on unchanged source, including the full workspace suite, native SQL
network integration and both automatic takeover/stress profiles. The workspace
trace contains all named SQL journal and process-kill regressions. Earlier failed
attempts remain recorded in the scoped journal report; later transport and
installation fixes are covered by the current full source hash. This is local
loopback TLS/fsync evidence, not full production qualification.

Bounded receipt retention,
incremental physical planning and growing-table capacity qualification remain
release work. Multiple pages and atomic row relocation are implemented. Native request
frames are limited to 64 KiB and complete commands to 1 MiB. Certified network
membership changes, independent-group routing, explicit legacy migration and
independent-machine fault/performance measurements remain unfinished. Opt-in
automatic takeover is implemented; its production qualification remains open.

The [streaming qualification](sql-stream-verification-2026-10-21.json) adds
bounded native delivery, strict client completion checks, incremental session
result admission and post-takeover streaming. All ten named streaming regressions
ran in the current workspace trace, alongside prior session/key/atomicity checks.
Both debug/release native streaming workloads delivered all 128 rows and 264,960
JSON bytes; this is delivery/recovery evidence, not workload capacity qualification.

The [indexed-candidate qualification](sql-index-verification-2026-10-21.json) adds
actual bounded secondary lookup, canonical typed index values and maintained
committed planning sizes. All six required regressions appear in the current
workspace trace. Both debug/release native and takeover profiles verify indexed
reads. Growing-history write planning, cold recovery and workload capacity remain
release work.
