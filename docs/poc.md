# RSF End-To-End POC

Date: 2026-09-10

## Purpose

The POC is a simulated end-to-end run of the full RSF database architecture.

It is not the hard distributed database implementation. It is a runnable architectural proof that shows how the future modules fit together:

- bootstrap
- deployment package manifest
- rolling upgrade planning
- admission
- placement
- deterministic fault injection
- primary leases and fences
- tenancy and database ownership
- catalog metadata
- schema migration planning
- background job orchestration
- auth and authorization
- protocol wire adapters
- user API gateway
- idempotency and retry envelope
- database request routing
- consistency-mode policy
- quota admission
- internal service RPC envelopes
- node health scoring
- storage fabric
- consensus
- transaction visibility
- transaction coordination
- strict isolation validation
- ordered KV engine
- ordered KV watch delivery
- ordered range/SSTable layout
- SSTable compaction execution
- optimizer statistics publishing
- relational constraint validation
- relational write planning
- relational row materialization
- relational commit-group auditing
- relational page/redo adapter
- relational buffer pool
- SQL session binding
- relational index maintenance
- relational engine
- logical query planner/executor
- physical query access-path planning
- physical query operator execution
- composed SQL pipeline
- object store
- blob store
- cold-object erasure layout
- format-independent extent manifesting
- immutable chunk store
- local checkpoint compaction
- object/blob provider discovery
- local superblock boot validation
- database snapshot coordination
- backup/restore planning
- database recovery planning
- lifecycle/retention policy
- repair and scrub planning
- controlled rebalancing
- observability
- invariant checking

Each service emits logs so one run shows how the whole database architecture composes.

## Run

List demo profiles:

```text
cargo run -p rsfd -- demo list
```

Run one:

```text
cargo run -p rsfd -- demo run universal-vsr-rados
```

Run all:

```text
cargo run -p rsfd -- demo run all
```

Write logs to disk:

```text
cargo run -p rsfd -- demo run universal-vsr-rados --out demo-impls/universal-vsr-rados/runs/latest
cargo run -p rsfd -- demo run all --out runs/latest
```

Implementation package sources:

```text
demo-impls/universal-vsr-rados/manifest.rsf
demo-impls/foundationdb-extent/manifest.rsf
demo-impls/aurora-socrates/manifest.rsf
demo-impls/geo-local-wpaxos/manifest.rsf
demo-impls/dual-mode-cp-ap/manifest.rsf
```

Generated files:

```text
full.log
summary.txt
services/bootstrap.log
services/deployment.log
services/upgrade.log
services/admission.log
services/membership.log
services/placement.log
services/fault.log
services/lease.log
services/vsr.log
services/tenancy.log
services/catalog.log
services/schema.log
services/migration.log
services/jobs.log
services/auth.log
services/wire.log
services/api.log
services/idempotency.log
services/gateway.log
services/consistency.log
services/quota.log
services/rpc.log
services/health.log
services/fabric.log
services/consensus.log
services/transaction.log
services/txn.log
services/isolation.log
services/kv.log
services/watch.log
services/range.log
services/compaction.log
services/stats.log
services/constraints.log
services/deferred.log
services/rowcodec.log
services/tuple.log
services/vacuum.log
services/materialize.log
services/relcommit.log
services/write.log
services/page.log
services/wal.log
services/buffer.log
services/session.log
services/sessiontxn.log
services/prepared.log
services/portal.log
services/primary.log
services/index.log
services/relational.log
services/query.log
services/planner.log
services/executor.log
services/sqlpipeline.log
services/sqldml.log
services/objectmeta.log
services/object.log
services/blob.log
services/erasure.log
services/manifest.log
services/storage.log
services/chunkstore.log
services/checkpoint.log
services/provider.log
services/stream.log
services/cache.log
services/superblock.log
services/snapshot.log
services/backup.log
services/recovery.log
services/retention.log
services/repair.log
services/rebalance.log
services/observability.log
services/invariants.log
services/testability.log
services/requirements.log
```

`summary.txt` separates `committed_transactions`, which currently means recovered
storage-fabric placement-group commit tails, from `protocol_commits`, which means
database-personality commits issued through `rsf-protocol`.
`storage_committed_prefixes` counts recovered committed placement-group prefixes,
and `storage_manifest_covered` counts manifest entries proven covered by those
explicit prefixes.

`deployment_roles`, `deployment_endpoints`, `deployment_volumes`, and
`deployment_probes` come from the simulated implementation package manifest. The
POC assigns control-plane, gateway, metadata, storage, SQL, object, blob, and
background-worker roles, then emits service endpoints, durable database volumes,
and readiness probes before node admission starts.

`upgrade_steps`, `upgrade_checks`, `upgrade_nodes`, and `upgrade_rejections`
come from the simulated rolling upgrade planner. The POC preflights a compatible
one-node-at-a-time rollout, logs drain/install/probe/promote stages for each
node, and rejects an unsafe cross-major upgrade before capacity admission
continues.

`membership_epoch`, `membership_voters`, `membership_learners`,
`membership_promotions`, `membership_intersections`, and
`membership_rejections` come from the simulated membership cell. The POC starts
with three voters, adds an admitted node as a learner, marks it synced, promotes
it through an epoch change, records the old/new quorum sizes and minimum
intersection proof, removes one voter while preserving the three-voter floor,
and rejects the next unsafe voter removal. Real replicated reconfiguration,
catch-up streaming, and quorum-joint consensus remain simulated replacement
boundaries.

`vsr_view`, `vsr_prepares`, `vsr_acks`, `vsr_commits`,
`vsr_view_changes`, and `vsr_rejections` come from the simulated VSR metadata
cell. The POC prepares and commits one catalog record after quorum
acknowledgement, changes view to a new primary, rejects a stale-primary prepare,
then prepares and commits an object-manifest record in the new view. Durable
replicated logs, real network replication, checkpoint transfer, and full VSR
recovery remain simulated replacement boundaries.

`wire_frames`, `wire_protocols`, `wire_rejections`, and `wire_body_bytes` come
from the simulated wire adapter layer. The POC decodes SQL, KV, object, blob,
and admin protocol frames into normalized request envelopes, then rejects one
mutating object request that lacks an idempotency key before API classification.

`provider_records`, `provider_content`, `provider_selected`,
`provider_degraded`, and `provider_regions` come from the simulated provider
directory. The POC advertises object and blob chunk providers across regions,
selects nearest verified providers for a hot object, records one degraded blob
selection when the requested replica count cannot be met, and rejects a missing
content lookup before the stream planner schedules reads.

`catalog_items` is the number of database metadata objects registered in the
catalog service: namespace, table, bucket, and surface placement bindings.

`tenants`, `tenant_databases`, `tenant_namespaces`, and `tenant_surfaces` come
from the tenant control-plane catalog. The POC creates `acme.prod`, binds the
`app` namespace to that database, and validates that relational/object surfaces
are enabled before catalog publication.

`schema_steps`, `schema_backfills`, and `schema_rejected` come from the schema
migration planner. The POC plans an online additive column/index migration
against the catalog table descriptor, then rejects an unsafe primary-key column
drop before SQL execution.

`migration_begun`, `migration_backfilled_rows`, `migration_indexed_keys`,
`migration_resumed`, `migration_completed`, `migration_rolled_back`, and
`migration_rejected` come from the schema migration lifecycle service. The POC
executes one non-null column/index migration through validation, descriptor
publication, backfill, index catch-up, resume, read switch, and completion. It
also begins a second migration, rolls it back before read visibility changes,
and rejects a post-rollback advance.

`jobs_enqueued`, `jobs_leased`, `jobs_completed`, `jobs_retried`, and
`jobs_blocked` come from the background job queue. The POC enqueues schema,
index, statistics, compaction, and repair jobs, blocks an index build on an
incomplete schema job, completes the schema job, and requeues a transient stats
refresh failure.

`primary_inserts`, `primary_lookups`, `primary_updates`, `primary_deletes`,
`primary_rejections`, and `primary_entries` come from the primary-index
identity service. The POC validates primary-key metadata from the catalog,
encodes a deterministic primary key, records a row location/version, performs a
point lookup, updates the row version without moving identity, rejects a
duplicate primary key, deletes the entry, and logs a missing lookup.

`auth_grants`, `authz_allowed`, and `authz_denied` come from the simulated
control-plane policy loaded into `rsf-auth`. The POC issues one app token,
authorizes a query execution request, and denies a repair operation with the
same app token.

`api_accepted` and `api_rejected` come from the simulated user-facing gateway.
The POC classifies a SQL request and an admin join request, then enforces the
auth decision before those requests reach deeper service layers.

`idempotency_first_seen`, `idempotency_replays`, `idempotency_conflicts`, and
`idempotency_retries` come from the user-facing idempotency ledger. The POC
claims one SQL request envelope, stores a committed response digest, replays the
same key/fingerprint, rejects conflicting reuse of the same key, and schedules
one bounded retry for a retryable failure class.

`gateway_routes`, `gateway_rejections`, and `gateway_surfaces` come from the
database gateway router. The POC evaluates SQL, KV, object, blob, and admin
requests, routes the accepted database surfaces through catalog placement
bindings, labels strict versus availability-first consistency, and rejects
unauthorized admin work before it reaches the control plane.

`quota_allowed`, `quota_rejected`, `quota_policies`, and `quota_namespaces` come
from namespace/surface quota admission. The POC loads SQL, KV, object, and blob
policies for the `app` namespace, admits four reservations, and rejects one
oversized blob reservation without charging usage.

`rpc_calls`, `rpc_retries`, `rpc_rejections`, and `rpc_body_bytes` come from the
simulated internal RPC router. The POC routes service envelopes from gateway,
object, background, SQL, blob, and storage roles, records one retryable
idempotent internal call, rejects an over-deadline storage call, and rejects a
retried non-retryable blob read before fabric work starts.

`cache_hits`, `cache_misses`, `cache_evictions`, and `cache_hints` come from the
hot-read cache. The POC admits simulated SQL, KV, object, and blob cache keys,
records three version-valid hits, two misses, one bounded eviction, and two
read-ahead hints. Distributed invalidation and range/page server placement are
still simulated replacement boundaries.

`stream_write_chunks`, `stream_read_lanes`, `stream_window_rejections`, and
`stream_verified_parts` come from the streaming transfer layer. The POC plans
chunked object/blob writes, rejects one over-window transfer without charging
in-flight bytes, retries after a simulated release, and verifies a parallel
object read. Actual network transport, resumable streaming state, and remote
chunk scheduling are still simulated replacement boundaries.

`superblock_generation`, `superblock_checkpoint`, `superblock_roots`,
`superblock_advances`, and `superblock_rejections` come from the local
boot-record layer. The POC opens a simulated superblock, verifies catalog,
transaction-log, KV, SQL, object, and blob roots, advances the superblock
generation at the latest committed checkpoint, and rejects a malformed root
inventory missing the catalog root. Double-written pages, torn-superblock recovery, persisted root
catalogs, and production startup mount orchestration are still simulated
replacement boundaries.

`snapshot_roots`, `snapshot_barriers`, and `snapshot_validations` come from the
database snapshot coordinator. The POC freezes catalog, transaction-log, KV,
SQL, object, and blob roots at one checkpoint, creates copy-on-write barriers
for mutable roots, and validates required roots, root uniqueness, and checkpoint
versions.

`backup_items`, `restore_checks`, `restore_executed_items`,
`restore_executed_surfaces`, and `restore_executed_bytes` come from the
full-database backup planner. The POC creates a backup manifest at the latest
commit version, includes catalog, transaction-log, KV, SQL, object, and blob
items, validates a restore plan, and executes a simulated restore proof that
covers every manifest item and database surface before lifecycle or repair
services run.

`recovery_roots`, `recovery_replays`, `recovery_rejections`, and
`recovery_actions` come from the full-database recovery planner. The POC plans
a restart from catalog, transaction-log, KV, SQL, object, and blob roots,
replays lagging roots to the checkpoint, reuses checkpoint-matching roots, and
rejects a corrupted required catalog root. Durable startup orchestration,
in-doubt transaction resolution, and automatic repair handoff are still
simulated replacement boundaries.

`lease_grants` and `fence_rejections` come from the primary lease table. The
POC grants placement-group primary leases, simulates a view-change replacement
for one placement group, and rejects the old primary fence before protocol work
continues.

`rebalance_moves`, `rebalance_checks`, and `rebalance_policy_proofs` come from
controlled placement-group movement planning. The POC observes one hot storage
node, chooses a cold non-member target, records safety checks, and proves the
resulting acting set preserves the minimum replicated placement policy before
proposing the move.

`fault_events`, `dropped_messages`, `delayed_messages`,
`failure_drill_passed`, and `failure_drill_domains` come from deterministic
simulator controls and the failure-drill coverage contract. The POC schedules
node start/crash/restart events, evaluates delayed, partitioned, and healed
network deliveries, and then proves the full run covered event scheduling,
network drop/delay, recovery rejection, provider degradation, health demotion,
and rebalance policy-preservation domains.

`health_healthy`, `health_degraded`, `health_demote`, and `health_actions` come
from node health scoring. The POC evaluates disk headroom, latency, replica lag,
heartbeat, and corruption signals, then logs promotion, slow-replica demotion,
and corruption quarantine recommendations.

`range_partitions`, `range_non_overlaps`, `range_rejections`,
`sstable_manifests`, and `compaction_inputs` come from the ordered range layout
adapter. The POC creates a key-range descriptor, splits it across placement
groups, proves the split boundaries are non-overlapping, rejects an overlapping
descriptor, registers SSTable manifests, and plans compaction for the
relational/KV keyspace.

`compaction_outputs`, `compaction_live_keys`, `compaction_tombstones`, and
`compaction_reclaimed_bytes` come from the SSTable compaction executor. The POC
merges two simulated table runs, keeps the newest version of a key, suppresses a
tombstoned key, emits an output manifest, and records reclaimed bytes. Real
SSTable file IO, snapshot retention windows, throttling, and scheduler handoff
are still simulated replacement boundaries.

`stats_tables`, `stats_rows`, `stats_histogram_buckets`, and `stats_stale` come
from the optimizer statistics catalog. The POC publishes table row/byte
estimates and column histograms for `app.users`, then flags stale statistics
against a later catalog version. Real sampling jobs, sketching, index
selectivity, and execution-feedback refresh are still simulated replacement
boundaries.

`constraints_checked`, `constraints_accepted`, `constraints_rejected`, and
`constraints_unique_keys` come from the relational constraint validator. The POC
derives not-null checks from the `app.users` table descriptor, validates a
primary-key rule, probes a simulated unique-key set, accepts one row mutation,
and rejects duplicate unique-key and null required-column mutations. Durable
unique indexes, reference-index reads, and integration inside SQL commit
admission are still simulated replacement boundaries.

`deferred_transactions`, `deferred_obligations`,
`deferred_txn_references`, `deferred_committed_references`,
`deferred_commits`, and `deferred_rejections` come from the commit-time
deferred constraint checker. The POC stages an `orders.user_id -> users.id`
obligation before the referenced user exists, stages the referenced user key
later in the same transaction, and accepts the commit only after the deferred
check resolves. It also stages a second obligation whose reference never
appears and rejects that transaction at commit time. Durable reference indexes,
snapshot-aware reference reads, cascading actions, and cross-placement-group
reference validation remain simulated replacement boundaries.

`rowcodec_encoded`, `rowcodec_decoded`, `rowcodec_defaults`,
`rowcodec_generated`, `rowcodec_rejections`, and `rowcodec_bytes` come from the
relational row codec. The POC encodes a schema-versioned `users` row into typed
physical bytes, applies a default `active` value, materializes a generated
`name_len` value, decodes the bytes back into typed row values, and rejects one
missing-required row plus one type-mismatched row.

`tuple_versions`, `tuple_visible_reads`, `tuple_delete_markers`,
`tuple_rejections`, and `tuple_bytes` come from the relational tuple visibility
service. The POC stores encoded row bytes behind MVCC tuple headers, updates a
row by delete-marking the previous version and appending a new version, proves
old/new snapshot visibility, delete-marks the live tuple, proves delete
invisibility, and rejects duplicate-live insert plus missing-update cases.
Compact varint/null bitmap layout, compression/encryption hooks, page-format
integration, and executor row materialization from decoded pages remain
simulated replacement boundaries.

`vacuum_chains`, `vacuum_pruned_versions`, `vacuum_retained_versions`,
`vacuum_rejections`, and `vacuum_reclaimed_bytes` come from the relational
vacuum planner. The POC evaluates tuple chains against an oldest active
snapshot, prunes only versions whose delete marker is safe, keeps versions
needed by a younger snapshot, and rejects an empty tuple chain. Production
vacuum workers, page-slot pruning, oldest-snapshot distribution, and WAL-logged
page cleanup remain simulated replacement boundaries.

`materialized_rows`, `materialized_projected`, `materialized_columns`,
`materialized_rejections`, and `materialized_bytes` come from the relational row
materialization service. The POC decodes a visible encoded tuple payload into a
SQL-facing `QueryRow`, applies an explicit projection, and rejects table or
projection mismatches. Vectorized batches, page-slot tuple IDs, late expression
evaluation, and executor/page integration remain simulated replacement
boundaries.

`write_plans`, `write_steps`, `write_index_mutations`, and
`write_conflict_ranges` come from the relational write planner. The POC plans one
accepted insert by ordering constraint checks, row KV byte encoding,
secondary-index mutation planning, transaction conflict-range declaration, and
commit-intent construction. The plan is logged as architecture, not executed
against durable storage by the write service.

`relcommit_groups`, `relcommit_row_mutations`, `relcommit_index_mutations`,
`relcommit_conflict_ranges`, and `relcommit_rejections` come from relational
commit-group auditing. The POC audits insert, update, and delete plans to prove
the row mutation and secondary-index mutations are present together in one
transaction intent, verifies conflict-range evidence, and rejects a deliberately
partial commit group. Production lock scheduling, durable transaction records,
and cross-placement-group two-phase commit are still simulated replacement
boundaries.

`page_frames`, `page_redo`, `page_reads`, `page_rejections`, and
`page_checkpoint_bytes` come from the relational page/redo adapter. The POC
creates a native table page, applies insert/update redo records with LSN
validation, reads at a minimum LSN, rejects one stale redo record, and
checkpoints page bytes before SQL session binding. This is the structural
boundary for a future page server, buffer pool, and redo-log integration.

`wal_appends`, `wal_syncs`, `wal_flush_allows`, `wal_rejections`,
`wal_durable_lsn`, and `wal_bytes` come from the relational WAL durability gate.
The POC appends two redo records, rejects a page flush before the corresponding
redo is durable, syncs the group, advances the durable LSN, and then allows the
page flush. Device fsync, group-commit scheduling, checkpoint scheduling, and
real page-server flush RPCs remain simulated replacement boundaries.

`buffer_resident`, `buffer_hits`, `buffer_dirty`, `buffer_flushes`,
`buffer_evictions`, and `buffer_rejections` come from the relational buffer
pool. The POC pins a users page, records a repeated-page hit, marks it dirty,
rejects loading another page while every frame is pinned, then unpins, flushes,
and evicts the page before SQL session binding. This is separate from the
generic hot-read cache and models the RDBMS buffer-pool boundary.

`session_bound`, `session_authorized`, and `session_rejected` come from the SQL
session binder. The POC binds SQL text to catalog table metadata, required auth
capabilities, strict consistency, placement-group route, and transaction shape
before the relational engine executes. It also rejects an unknown table before
execution.

`index_mutations`, `index_entries`, and `index_rebuilds` come from the relational
index planner. The POC validates an index descriptor against the catalog table,
plans a rebuild, plans insert/update/delete index mutations, performs one
lookup, and leaves no live index entries after the simulated delete.

`planner_physical_plans`, `planner_index_choices`, `planner_range_scans`, and
`planner_stream_reads` come from the physical query planner. The POC lowers
logical query plans into primary-key lookup, secondary-index lookup, cache
lookup, range scan, and stream-read operators using statistics published by
`rsf-stats`. Real distributed placement costs, advanced cardinality estimation,
and physical execution are still simulated replacement boundaries.

`executor_plans`, `executor_stages`, `executor_rows`, and
`executor_remote_reads` come from the physical query executor. The POC consumes
one planned cache-plus-secondary-index query, records each execution stage, and
returns a simulated result report. Actual range cursors, cache fill/miss loops,
streaming row materialization, cancellation, and distributed retries are still
simulated replacement boundaries.

`sql_pipeline_bound`, `sql_pipeline_authorized`, `sql_pipeline_logical`,
`sql_pipeline_physical`, `sql_pipeline_stages`, `sql_pipeline_rows`,
`sql_pipeline_remote_reads`, and `sql_pipeline_rejections` come from the
composed SQL request pipeline. The POC binds a session, authorizes the token,
lowers a narrow SELECT into a logical plan, chooses cache/index physical
operators, executes the stages, materializes one row, and rejects an invalid
projection before physical execution. Full SQL parsing, cancellation, retry
envelopes, and multi-statement transactions are still simulated replacement
boundaries.

`prepared_statements`, `prepared_binds`, `prepared_invalidations`,
`prepared_deallocations`, `prepared_evictions`, and `prepared_rejections` come
from the prepared SQL cache. The POC prepares named statements, binds typed
parameters, rejects a type mismatch, invalidates a stale schema-version entry,
deallocates a statement, and evicts the least recently used entry in a tiny
cache. Binary codecs, planner-cache reuse, and distributed retry integration
remain simulated replacement boundaries.

`portal_parses`, `portal_binds`, `portal_describes`, `portal_executes`,
`portal_rows`, `portal_cancellations`, `portal_closes`,
`portal_statement_closes`, and `portal_rejections` come from the SQL portal
lifecycle. The POC parses prepared statements, binds named portals, describes a
portal, executes one portal under deadline, rejects one slow execution on
deadline, accepts a client cancellation, rejects execution after cancellation,
closes a portal, and closes a statement with bound-portal cleanup. Actual
PostgreSQL wire bytes, binary codecs, cursor fetch counts, socket-level
CancelRequest routing, and distributed cancellation propagation remain
simulated replacement boundaries.

`sql_dml_bound`, `sql_dml_authorized`, `sql_dml_planned`,
`sql_dml_prepared`, `sql_dml_committed`, `sql_dml_isolated`,
`sql_dml_kv_applied`, `sql_dml_session_staged`,
`sql_dml_overlay_verified`, and `sql_dml_rejections` come from the composed SQL
DML pipeline. The POC binds INSERT, UPDATE, and DELETE statements; authorizes
the namespace writes; validates constraints and index metadata; prepares and
commits the immediate mutations through the protocol; validates strict
isolation ranges; applies row and index KV mutations at each commit version;
stages an accepted INSERT into a strict session transaction overlay; reads the
staged row and index entries back before global visibility; commits that
overlay through KV/protocol; and rejects a duplicate unique-key write before
transaction commit. Retry envelopes, cancellation, deferred constraints, and
full multi-statement SQL transaction lifecycles remain simulated replacement
boundaries.

`sql_txn_began`, `sql_txn_dml_staged`, `sql_txn_overlay_reads`,
`sql_txn_commits`, `sql_txn_rollbacks`, `sql_txn_rollback_absent`,
`sql_txn_savepoints`, `sql_txn_savepoint_rewinds`,
`sql_txn_savepoint_absent`, and `sql_txn_rejections` come from the SQL
transaction script lifecycle. The POC runs a narrow `BEGIN`, staged INSERT,
transactional `SELECT ... WHERE id = ...`, and `COMMIT` sequence through
`rsf-sqltxn`; the staged SELECT reads from the session overlay before the row
is globally visible, and the commit then publishes row plus index mutations
through KV/protocol. A second script stages and reads another INSERT, rolls it
back, and verifies that row never became globally visible. A third script
stages one INSERT, creates a `SAVEPOINT`, stages and reads another INSERT,
rewinds with `ROLLBACK TO SAVEPOINT`, proves the later row is neither
overlay-visible nor globally visible, and commits the earlier row. A standalone
`COMMIT` is rejected to prove transaction-control admission. Full SQL
transaction grammar, deferred constraints, retry envelopes, and
cross-placement-group SQL transactions remain simulated replacement
boundaries.

`coordinator_transactions` is the number of transactions committed through the
transaction coordinator. The POC also logs a rejected stale overlapping
transaction to show conflict-range enforcement.

`sessiontxn_sessions`, `sessiontxn_pending_writes`,
`sessiontxn_overlay_reads`, `sessiontxn_snapshot_reads`,
`sessiontxn_commits`, `sessiontxn_savepoints`,
`sessiontxn_savepoint_rewinds`, and `sessiontxn_rejections` come from the
strict session transaction overlay. The POC begins one standalone session
transaction, stages a pending KV write, reads that pending value before global
commit, reads another key from the begin snapshot, commits through KV/protocol,
and rejects a read after the transaction is closed. The SQL DML service then
begins another session transaction, stages accepted relational row/index
mutations into the same overlay mechanism, verifies read-your-writes
visibility, and commits the overlay. The SQL transaction service uses that same
lower layer for BEGIN/DML/SELECT/COMMIT, BEGIN/DML/SELECT/ROLLBACK, and
SAVEPOINT/ROLLBACK TO SAVEPOINT scripts. Retry integration and
cross-placement-group session overlays remain simulated replacement
boundaries.

`isolation_commits`, `isolation_rejections`, `isolation_edges`, and
`isolation_ranges` come from the strict isolation validator. The POC commits
two serializable transactions, checks four accepted read/write ranges, and
rejects one stale read before the ordered KV and relational personalities run.
Distributed SSI storage and SQL planner integration are still simulated
replacement boundaries.

`query_rows` is the number of projected rows returned by the query executor.

`objectmeta_buckets`, `objectmeta_uploads`, `objectmeta_parts`,
`objectmeta_heads`, and `objectmeta_rejections` come from the strict object
metadata coordinator. The POC registers a bucket, starts a multipart upload,
accepts two manifest parts, commits one visible current head, reads that head,
and rejects an unknown-upload commit. Metadata version allocation now comes from
`rsf-protocol`; the protocol cell's real replicated quorum implementation is
still a simulated replacement boundary.

`repair_findings` is the number of scrub/health problems observed by the
repair planner. `repair_actions` is the number of copy, quarantine,
digest-recheck, or slow-replica demotion actions. Copy and quarantine actions
are executed in the POC and logged with their repaired or isolated replica
metadata.

`retention_kept` and `retention_actions` come from lifecycle policy evaluation.
The POC logs keep decisions plus planned compaction, expiration, and pin release
actions; it does not execute destructive lifecycle work.

`observability_metrics`, `observability_audits`, and `cluster_health` come from
the typed observability registry used by the final POC service.

`invariant_checks`, `invariant_passed`, and `invariant_failed` come from the
named invariant checker. The POC evaluates admission capacity, quorum metadata,
stale-primary rejection, multi-surface manifest coverage, required boot roots,
snapshot/recovery coverage, repair planning, large-payload verification,
immutable chunk-store verification, checkpoint compaction, SQL pipeline
coverage, prepared SQL lifecycle coverage, SQL portal lifecycle coverage,
object metadata head visibility, stale strict-read rejection, committed
storage-tail evidence, recovered committed-prefix evidence, and manifest
coverage proof over the final run summary. Trace-native invariants,
crash/restart shrinking, and machine-readable coverage reports are still
simulated replacement boundaries.

`requirement_checks`, `requirement_covered`, `requirement_simulated`, and
`requirement_design_targets` come from the product-requirement audit. The POC
groups the original product requirements into auditable families and labels each
family as structural evidence, simulated evidence, design-target evidence, or
missing evidence. Failure-model coverage is structural POC evidence only after
the failure-drill contract passes across all required domains. Low-latency
production shape is structural POC evidence only after the workload-shape
contract covers relational OLTP, ordered KV, object multipart, and blob
streaming paths with explicit budgets. This is still not a real benchmark claim.
Simulation-testability is structural POC evidence only after the testability
contract proves trace depth, service coverage, clean invariants, metrics
richness, audit presence, and replay readiness. The current run reports no
simulated or design-target requirement families, while the hard production
production implementations behind those structural contracts remain future work.

`consistency_strict`, `consistency_availability_first`, and
`consistency_rejections` come from the consistency-mode policy. The POC confirms
that SQL, KV, and object metadata route as strict-quorum work, blob payload
placement may route as availability-first work, and relational downgrade to
availability-first is rejected before quota/RPC/fabric work.

`erasure_data_shards`, `erasure_parity_shards`, `erasure_tolerated_loss`, and
`erasure_reconstructed_bytes` come from the cold-object layout adapter. The POC
stripes one cold object body into a 3+2 data/parity shard set, verifies shard
checksums, and reconstructs the original body with one missing data shard.

`manifest_entries`, `manifest_surfaces`, `manifest_bytes`, and
`manifest_rejections` come from the format-independent extent manifest. The POC
publishes relational page, relational WAL, ordered KV SSTable, object part, blob
chunk, and snapshot-root extents over placement groups, verifies each entry, and
rejects an overlapping KV byte range for the same logical object/version. Real
manifest pages, extent allocation, sparse corruption scanning, and manifest
garbage collection are still simulated replacement boundaries.

`chunkstore_chunks`, `chunkstore_pinned`, `chunkstore_bytes`,
`chunkstore_verified`, `chunkstore_deduped`, and `chunkstore_rejections` come
from the generic immutable chunk store. The POC writes format-tagged chunks for
SQL large values, object bodies, blob payloads, SSTable blocks, snapshot blocks,
and backup blocks; dedupes a repeated object body; verifies three chunk reads;
releases one pin; and rejects an empty payload. Durable chunk files, production
digests, zero-copy reads, refcount GC, compression, encryption, and real
provider publication are still simulated replacement boundaries.

`checkpoint_roots`, `checkpoint_segments`, `checkpoint_compacted`,
`checkpoint_reclaimable_bytes`, and `checkpoint_rejections` come from the local
checkpoint compaction planner. The POC retains catalog, transaction-log, KV,
SQL, object, blob, and chunk-index roots; keeps required-root and future-version
segments; compacts two old reclaimable segments; and rejects checkpoint
regression. Durable checkpoint catalog pages, physical segment rewrite, atomic
install, superblock advancement from checkpoint output, and crash-safe
compaction recovery are still simulated replacement boundaries.

`kv_changes` is the total number of KV mutations retained in the in-memory change
stream. The `kv/change-stream` log line may report a smaller number because it
queries changes after a specific read snapshot.

`watch_registered`, `watch_deliveries`, and `watch_resumes` come from the
ordered KV watch registry. The POC registers one prefix subscription after the
initial KV batch, delivers the later update and clear tombstone, acknowledges the
cursor, then resumes from that cursor with no pending replay.

## Implementations

- [Universal VSR-RADOS Fabric](../demo-impls/universal-vsr-rados/README.md)
- [FoundationDB Over Extent Fabric](../demo-impls/foundationdb-extent/README.md)
- [Aurora/Socrates Relational-First Fabric](../demo-impls/aurora-socrates/README.md)
- [Geo-Local WPaxos Fabric](../demo-impls/geo-local-wpaxos/README.md)
- [Dual-Mode CP/AP Fabric](../demo-impls/dual-mode-cp-ap/README.md)

## Simulation Boundary

The POC uses real Rust modules and real local WAL append/recovery behavior, but the distributed services are simulated.

Real today:

- crate boundaries
- service flow
- structured logs
- per-service log artifacts
- implementation selection
- deterministic simulator event scheduling and network-fault decisions through `rsf-sim`
- invite-token node admission through `rsf-admin`
- membership crate models learner sync, quorum-preserving promotion, epoch advancement, and voter-removal safety
- promoted storage nodes mapped into replicated placement groups
- health crate scores admitted nodes and produces promotion, demotion, and quarantine recommendations
- lease crate grants primary fences and rejects stale writers after a simulated view change
- VSR crate models prepare, prepare_ok, quorum commit, view change, and stale-primary rejection
- tenancy crate creates tenant/database descriptors, binds namespaces to one owner, and validates enabled database surfaces
- protocol router crate used by database personalities
- protocol crate tags commit consistency mode and demonstrates idempotent client request replay
- catalog crate registers namespaces, table descriptors, bucket descriptors, and surface placement bindings through protocol commits
- schema crate plans descriptor publication, index build, read switch, backfill steps, and unsafe migration rejection
- migration crate advances schema lifecycle state through validation, descriptor publication, backfill, index catch-up, resume, completion, rollback, and rejected post-rollback advance
- primary-index crate owns row identity, deterministic primary-key encoding, point lookup, version replacement, delete, duplicate-key rejection, and missing-key lookup evidence
- jobs crate enqueues, leases, dependency-gates, completes, and retries database maintenance jobs
- auth crate registers principals, stores scoped grants, issues expiring tokens, and evaluates allow/deny decisions
- API crate classifies SQL, KV, object, blob, and admin requests and turns auth decisions into accepted/rejected gateway decisions
- idempotency crate classifies first-seen requests, replayed responses, conflicting key reuse, expiry, and bounded retry delays
- gateway crate validates API decisions against original requests, applies budgets, selects handlers, labels consistency, and resolves catalog placement bindings
- consistency crate classifies strict-quorum and availability-first work and rejects attempts to disguise async availability as strict mutable truth
- quota crate reserves request, row, and byte budgets per namespace/surface and rejects over-limit work before execution
- RPC crate validates internal service envelopes, deadline policy, retry class, attempts, trace IDs, and body sizes before fabric work starts
- cache crate admits hot SQL, KV, object, and blob read entries, enforces visible-version freshness, evicts within capacity, and plans read-ahead hints
- transaction crate captures read versions, prepares conflict ranges, commits through protocol, and rejects stale overlapping transactions
- isolation crate validates strict serializable read/write ranges, rejects stale reads and write/write conflicts, and detects write-skew cycles
- SQL portal crate proves Parse/Bind/Describe/Execute/Close-shaped client lifecycle, deadline rejection, cancellation, and cleanup around prepared statements
- KV crate executes atomic batches, snapshot prefix scans, live scans, tombstones, and change streams
- watch crate registers prefix subscriptions, filters KV changes, propagates tombstones, advances cursors, and resumes delivery
- session transaction crate provides read-your-writes overlays, pinned snapshot reads, SQL DML staging, KV/protocol commit handoff, and closed-transaction rejection
- range crate validates ordered range descriptors, split boundaries, SSTable manifests, and compaction inputs
- compaction crate merges planned SSTable runs, drops tombstoned keys, and emits output manifests with reclaimed-byte accounting
- checkpoint crate retains full database roots, classifies local segments, plans reclaimable compaction, and rejects checkpoint regression
- stats crate publishes table estimates and column histograms, detects stale optimizer stats, and feeds physical planning inputs
- constraints crate validates not-null, primary-key, unique-key, and reference-key write admission decisions
- deferred constraint crate validates transaction-local and committed references at commit time, including one accepted and one rejected deferred foreign-key obligation
- row codec crate materializes schema-versioned typed row bytes, decodes them, applies default/generated columns, and rejects missing-required/type-mismatched physical rows
- tuple crate records MVCC tuple headers, update/delete markers, snapshot-visible versions, and rejection paths over encoded row bytes
- vacuum crate plans oldest-snapshot-safe tuple pruning, retained versions, reclaimed bytes, and rejection paths
- materialize crate converts visible encoded tuple payloads into `QueryRow` output, applies projections, and rejects table/projection mismatches
- write crate plans accepted relational writes across constraint checks, row KV bytes, index mutations, transaction conflict ranges, and commit intent
- relcommit crate audits insert/update/delete row-plus-index commit groups and rejects partial transaction intents before commit
- SQL DML crate composes INSERT, UPDATE, and DELETE through session binding, auth, relational write planning, transaction prepare/commit, strict isolation validation, atomic KV materialization, and strict session-transaction overlay staging
- SQL transaction crate composes BEGIN, staged DML, overlay SELECT, COMMIT, and transaction-control rejection above the session transaction overlay
- page crate applies relational page redo with LSN validation, read-version checks, stale redo rejection, and checkpoint summaries
- WAL crate enforces redo append, fsync, durable-LSN accounting, and WAL-before-page-flush admission
- buffer crate handles relational page residency, pins, dirty tracking, flushes, evictions, and full-pinned-pool rejection
- session crate binds SQL text to catalog metadata, auth intent, strict consistency, placement-group route, and transaction shape
- index crate validates index descriptors, encodes index keys, plans rebuilds, and plans insert/update/delete index mutations
- invariants crate evaluates named architecture and safety checks over final POC summaries
- SQL crate executes narrow SQL text for create table, create index, insert, update, delete, and equality select over ordered KV/protocol commits
- query crate validates query plans against catalog descriptors and executes projection/filter/limit over row batches
- planner crate lowers logical query plans into physical access-path operators using catalog descriptors, index descriptors, and deterministic simulated stats
- executor crate consumes physical plans and reports cache, index, range, and stream execution stages
- SQL pipeline crate composes session binding, token authorization, logical planning, physical planning, physical execution, materialization, and invalid-query rejection
- object metadata crate tracks strict bucket state, multipart draft parts, current visible heads, and metadata rejection paths before body storage runs
- object crate materializes object manifests over blob chunks/protocol commits
- object crate creates buckets, accepts multipart parts, delays visibility until manifest commit, and verifies readback
- blob crate stores deterministic content IDs, dedupes duplicate payloads, verifies reads, and tracks retention pins
- erasure crate creates cold-object data/parity shard manifests, verifies shard checksums, and reconstructs after one simulated missing shard
- manifest crate validates format-independent relational, KV, object, blob, and snapshot extent records with byte-range and checksum checks
- provider crate records object/blob content advertisements, filters unhealthy or unverified providers, selects locality-aware providers, and reports degraded provider quorums
- stream crate chunks object/blob transfers, plans parallel read lanes, enforces flow-control windows, and verifies expected chunk hashes
- superblock crate validates cluster boot records, root inventories, committed checkpoint generations, and malformed required-root cases
- snapshot crate validates database snapshot roots and copy-on-write barriers across catalog, transaction-log, KV, SQL, object, and blob records
- backup crate validates backup manifests and restore plans across catalog, transaction-log, KV, SQL, object, and blob records
- recovery crate plans full-database restart replay/reuse/quarantine actions and rejects corrupted required roots
- retention crate evaluates retain-until floors, legal holds, current version protection, compaction, expiration, and blob pin release
- repair crate plans missing-replica copy, corrupt-replica digest recheck, slow-replica demotion, and under-replication findings
- testability crate proves trace depth, service coverage, invariant cleanliness, metrics richness, audit presence, and replay readiness before requirements are audited
- requirements crate audits the final POC summary against the product requirements without claiming simulated or design-target evidence is production-real
- rebalance crate plans controlled replica movement with per-move placement-policy proofs
- observability crate records metrics, audit events, service health, and aggregate cluster health
- placement-group WAL append/recovery
- per-placement-group tails
- committed-tail detection from commit records

Simulated today:

- consensus decisions
- quorum-backed lease acquisition, safe-read tokens, view-change replication, node identity attestation, production health scoring, sync catch-up, and production trust checks
- rolling health windows, persisted health history, failure-domain aggregate scores, and direct admission/repair/rebalance hooks
- durable auth policy storage, external identity providers, key rotation, mTLS binding, and row/object ACL inheritance
- production byte-level protocol adapters for SQL wire, KV API, S3-compatible object API, blob streaming API, and admin RPC
- gateway deadline propagation, durable idempotency-key tables, overload shedding, tenant-aware routing policy, streaming request bodies, service RPC transports, and mTLS-authenticated service identity
- durable idempotency storage, body canonicalization, transport-aware retry classification, expiry compaction, and audit trails
- durable tenant catalog integration, hierarchical quotas, rate windows, burst credits, overload shedding hooks, storage-capacity reservations, and usage export
- distributed cache invalidation, adaptive admission, page/range-server cache placement, streaming object/blob cache fills, stale-read safety tokens, and cache warmup
- production prepared SQL binary codecs, transaction retry envelopes, nested/released savepoint grammar, and production-grade multi-statement SQL transaction lifecycle
- production buffer pool replacement policy, dirty-page writeback, checkpoint throttling, WAL-before-page flush enforcement, and page prefetch
- distributed SSI storage, lock-free conflict-range index, read-your-writes overlays, retry envelopes, and SQL/object metadata isolation adapters
- placement optimization
- catalog recovery from replicated metadata cells
- durable schema descriptor mutation, worker-driven backfill execution, online index catch-up checkpoints, replicated visibility barriers, crash-restart migration resume, and rollback publication
- durable job catalog, worker heartbeats, lease stealing, priority/fairness scheduling, progress checkpoints, cancellation, and audit trails
- distributed cross-placement-group commit protocol and retry loops
- broad SQL parsing, production prepared statement/portal binary codecs, distributed retry envelopes, nested/released savepoint grammar, and cross-placement-group SQL transactions
- durable primary-index storage/routing, durable unique constraints, deferrable/reference constraints, online index catch-up, backfill execution, compaction, production histogram/cardinality collection, distributed cost model, and index-aware KV-backed query operators
- real write-service execution, durable secondary unique probes, SQL commit admission, retry envelopes, and cross-placement-group write orchestration
- durable watch subscriber catalog, API transport fanout, slow-consumer backpressure, and compaction-safe change retention windows
- real SSTable persistence, bloom filters, compaction execution, range-server RPC, and split/merge orchestration
- object-store protocol
- production Reed-Solomon erasure codec, placement-group shard writes, parallel shard reads, degraded reads, and cold/hot object policy transitions
- persisted manifest pages, extent allocation and free-space catalogs, manifest compaction, sparse corruption scans, and checksum upgrades
- durable chunk files, production content digests, zero-copy chunk reads, refcount GC, compression/encryption policy hooks, and provider publication from the chunk index
- durable checkpoint catalog pages, physical segment rewrite, atomic checkpoint install, superblock advancement, and crash-safe compaction recovery
- production provider indexes, provider liveness gossip, anti-entropy republishing, network-backed blob/object transfer, resumable stream state, zero-copy local chunk IO, sparse object reads, and remote chunk fetch scheduling
- double-written superblock pages, torn-superblock recovery, persisted boot-root catalogs, checkpoint garbage collection, and production mount orchestration
- durable snapshot catalog, per-placement-group snapshot markers, copy-on-write manifest persistence, partial snapshot transfer resume, real backup export/import, remote repositories, and production restore IO
- durable recovery root catalog, per-placement-group replay cursors, in-doubt transaction resolution, automatic repair handoff, checkpoint GC, and production startup orchestration
- background lifecycle execution
- full network transport behavior beyond deterministic delay/drop/deliver decisions
- scrub inventory collection, repair execution, and rebalancing
- actual replica copy execution, lease transfer during movement, and backpressure-aware throttling
- production metrics exporters, traces, alerting, and health endpoints
- trace-native invariant inputs, crash/restart invariant evaluation, Fozzy shrinking hints, and machine-readable invariant coverage reports

The point is to preserve the architecture while leaving clean replacement boundaries for real implementations.
