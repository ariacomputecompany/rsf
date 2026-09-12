# RSF Implementation Roadmap

Date: 2026-09-10

## Phase 0: Deterministic Simulator

Build this before trusting any database code.

Deliverables:

- deterministic event loop: started with `rsf-sim` logical-time event ordering
- node model
- network model: started with `rsf-sim` link partitions, delays, and delivery decisions
- disk model
- timers
- random seed input
- trace recording
- trace replay
- failure injection: started with `rsf-sim` crash/restart events and message delay/drop decisions
- invariant checker: started with `rsf-invariants` named checks for admission capacity, quorum metadata, stale-primary rejection, multi-surface layout, boot roots, snapshot/recovery coverage, repair planning, payload verification, immutable chunkstore verification, checkpoint compaction, SQL pipeline coverage, ordered range non-overlap, isolation rejection, and committed storage tails

Scenarios:

- crash/restart: started as deterministic simulator event types
- dropped messages
- duplicated messages
- reordered messages
- asymmetric partitions: started as deterministic partition/drop decisions
- slow disk
- torn write
- corrupted segment
- partial snapshot
- node join/promotion

## Phase 1: Local Engine

Deliverables:

- superblock: started with `rsf-superblock` boot-record validation, required root inventory, generation advancement at a committed checkpoint, and malformed-root rejection
- segment file format: started in `crates/rsf-storage`
- WAL records: started as append-only segment records
- checksums: started with per-record checksum
- hash chaining: started with WAL previous-hash and record-hash verification
- placement-group WAL semantics: started with per-PG tails and monotonic indexes
- object/extent manifest: started with `rsf-manifest` format-independent records for relational pages/WAL, KV SSTables, object parts, blob chunks, snapshot roots, byte ranges, placement groups, checksums, and overlap rejection
- immutable chunk store: started with `rsf-chunkstore` content-addressed writes, format class tags, dedupe/refcount accounting, retention pins, verified reads, corruption quarantine decisions, and malformed payload rejection
- startup recovery: started with ordered local recovery plus torn-tail truncation and `rsf-recovery` full-database root replay planning
- local checkpoint compaction: started with `rsf-checkpoint` database root retention, segment inventory validation, required/future segment retention, reclaimable-byte planning, and checkpoint-regression rejection

Invariant:

- local recovery must never invent, reorder, or silently drop acknowledged durable state.

## Phase 2: Placement Groups

Deliverables:

- placement group ID
- acting set
- primary/secondary roles
- primary leases/fences: started with `rsf-lease` active leases, renewal, expiry, stale-epoch rejection, and stale-fence rejection
- deterministic object-to-PG mapping
- PG log
- peering
- scrub metadata: started with typed replica health observations in `crates/rsf-repair`
- basic repair: started with missing, corrupt, slow, and under-replicated placement-group planning

Invariant:

- each placement group has exactly one valid primary per epoch. Started structurally with `rsf-lease` fence validation.

## Phase 3: VSR Metadata Cell

Deliverables:

- request protocol
- prepare/prepare_ok: started with `rsf-vsr` prepare and quorum acknowledgement decisions
- commit: started structurally through `rsf-protocol`
- catalog metadata operations: started with `rsf-catalog` namespace, table, bucket, and surface binding commits
- view change: started with `rsf-vsr` view advancement and stale-primary prepare rejection
- client idempotency table: started with request ID replay cache
- learner sync: started with `rsf-membership` learner sync, promotion gating, and old/new quorum-intersection proof
- snapshot transfer
- snapshot coordination: started with `rsf-snapshot` database roots, checkpoint validation, and copy-on-write barriers
- membership epoch changes: started with `rsf-membership` epoch advancement, quorum-preserving learner promotion, and unsafe-removal rejection

Invariant:

- no acknowledged write is lost after minority failure.

## Phase 4: Blob Personality

Deliverables:

- chunk write: started in `crates/rsf-blob`
- chunk read: started with verified reads by content ID
- content ID: implemented with BLAKE3-backed typed content identities
- dedupe index: started with duplicate content collapsing to one stored chunk
- retention pin: started with protocol-committed blob pins and lifecycle release decisions in `rsf-retention`
- provider discovery: started with `rsf-provider` content advertisements, locality-aware provider selection, degraded quorum reporting, and missing-content rejection
- parallel fetch: started with `rsf-stream` read lanes over chunked object/blob transfer plans
- hash verification: started with read-time digest checks

Invariant:

- a chunk returned by ID must match its digest.

## Phase 5: Object Personality

Deliverables:

- namespace/bucket metadata: started with bucket metadata records and `rsf-objectmeta` bucket registration
- object key metadata
- object version record: started with visible `ObjectVersion` and `rsf-objectmeta` current-head commits
- multipart upload state: started with begin/upload/complete flow and strict metadata draft sessions
- manifest records: started with part offsets, lengths, and chunk hashes
- lifecycle metadata: started with `rsf-retention` policy evaluation
- replicated hot metadata: started structurally through `rsf-protocol` metadata commits
- replicated or erasure-coded bodies

Invariant:

- object version visibility changes only through a strict metadata commit.

## Phase 6: Ordered KV Personality

Deliverables:

- ordered keys
- MVCC values: started in `crates/rsf-kv`
- range reads: started with prefix scans, snapshot prefix scans, `rsf-range` descriptors, split non-overlap proof, and overlap rejection evidence
- atomic batches: started with one protocol commit per KV batch
- transaction conflict ranges: started with per-key stale-write conflict checks in `rsf-kv` and range conflicts in `rsf-txn`
- watches/change stream: started with changes-since-version events

Invariant:

- strict sessions observe read-your-writes: started with `rsf-sessiontxn` pending-write overlays, snapshot reads, savepoint snapshots, rollback-to-savepoint rewinds, KV/protocol commit handoff, and closed-transaction rejection.

## Phase 7: Relational Prototype

Deliverables:

- deployable package envelope: started with `rsf-deployment` package/profile validation, rollout lifecycle recovery, role assignments, service endpoints, durable volumes, and readiness probes
- catalog: started with database-wide table descriptors in `crates/rsf-catalog`
- schema records: started with protocol-committed table/index changes
- schema migration lifecycle: started with `rsf-migration` durable migration progress through validation, descriptor publication, backfill, index catch-up, resume, completion, rollback, and rejected post-rollback advance
- background jobs: started with `rsf-jobs` enqueue, dependency blocking, leasing, completion, and retry accounting for database maintenance work
- table creation: started through tiny SQL text executor
- prepared SQL statements: started with `rsf-prepared` named statement preparation, typed parameter binding, schema-version invalidation, explicit deallocation, LRU eviction, and rejection evidence
- SQL portal lifecycle: started with `rsf-portal` Parse/Bind/Describe/Execute/Close-shaped flow, deadline rejection, cancellation, and bound-portal cleanup evidence
- primary indexes: started with `rsf-primary` primary-key descriptor validation, deterministic key encoding, point lookup, update version replacement, delete, duplicate insert rejection, and missing-key lookup evidence
- secondary indexes: started with single-column index metadata and equality lookup in `rsf-sql`, plus mutation/rebuild planning in `rsf-index`
- insert: started with primary-key row materialization through one relational protocol commit
- update: started with indexed value maintenance through one relational protocol commit
- delete: started with row removal, index cleanup, and physical KV tombstone through one relational protocol commit
- relational constraints: started with `rsf-constraints` not-null, primary-key, unique-key, and reference-key write admission
- deferred constraints: started with `rsf-deferred` transaction-local deferred foreign-key obligations, commit-time validation, transaction-local reference satisfaction, and missing-reference rejection
- relational row encoding: started with `rsf-rowcodec` schema-versioned typed row bytes, default column materialization, generated column materialization, decode round-trip, and malformed-row rejection evidence
- relational tuple visibility: started with `rsf-tuple` MVCC tuple headers, update/delete version chains, snapshot visibility, and duplicate/missing tuple rejection evidence
- relational vacuum planning: started with `rsf-vacuum` oldest-snapshot-safe tuple pruning, retained-version accounting, reclaimed-byte accounting, and empty-chain rejection evidence
- relational row materialization: started with `rsf-materialize` conversion from encoded visible tuple payloads into SQL-facing query rows, explicit projection output, and table/projection rejection evidence
- relational write planning: started with `rsf-write` constraint decisions, row KV encoding, index mutation planning, transaction conflict ranges, and ordered commit steps
- relational commit-group atomicity: started with `rsf-relcommit` audits that accept insert/update/delete row-plus-index mutation groups and reject partial commit intents
- relational page/redo adapter: started with `rsf-page` page frames, LSN-checked redo application, minimum-LSN reads, stale redo rejection, and checkpoint summaries
- point select: started through `SELECT * FROM <table> WHERE id = <value>`
- optimizer statistics: started with `rsf-stats` table estimates, column histograms, average row bytes, and stale-stat detection
- logical query planner: started with `rsf-query` projection, predicate, and limit validation
- physical query planner: started with `rsf-planner` primary-key, secondary-index, range-scan, cache-lookup, and stream-read access-path selection from durable optimizer statistics
- logical query executor: started with `rsf-query` row filtering and projection
- physical query executor: started with `rsf-executor` cache, index, range, and stream stage reports from planned operators
- composed SQL request pipeline: started with `rsf-sqlpipeline` binding a session, authorizing the principal, lowering SELECT text, planning logical/physical execution, running physical stages, materializing rows, and rejecting invalid SQL before execution
- composed SQL DML path: started with `rsf-sqldml` binding INSERT, UPDATE, and DELETE text; authorizing namespace writes; planning row/index mutations; preparing and committing immediate writes through `rsf-txn`; validating strict isolation through `rsf-isolation`; applying KV mutations at one commit version; staging accepted DML into `rsf-sessiontxn` read-your-writes overlays; and rejecting duplicate unique-key writes before commit
- SQL transaction lifecycle: started with `rsf-sqltxn` driving BEGIN, staged DML, read-your-writes SELECT, SAVEPOINT, rollback-to-savepoint absence verification, COMMIT, ROLLBACK with rollback-absence verification, and transaction-control rejection over `rsf-sessiontxn` and `rsf-sqldml`
- range scan: started structurally with ordered range descriptors, split non-overlap proof, overlap rejection evidence, and SSTable manifests in `rsf-range`
- compaction execution: started with `rsf-compaction` newest-version merge, tombstone suppression, output manifest creation, and reclaimed-byte reporting
- basic SQL parser or API shim: started with narrow create/index/insert/update/delete/select grammar
- user-facing idempotency: started with `rsf-idempotency` request keys, request fingerprints, replayed responses, conflict detection, expiry, and bounded retry policy
- transaction boundary: started by sharing one commit version for logical row and physical KV bytes
- database transaction coordinator: started with `rsf-txn` read versions, conflict-range prepare, and commit-version allocation
- strict serializable isolation validation: started with `rsf-isolation` read/write ranges, stale-read rejection, write/write conflict rejection, and write-skew cycle detection

Invariant:

- table data and index data commit atomically.

## Phase 8: Distribution Hardening

Deliverables:

- node admission: started in `crates/rsf-admin`
- automatic capacity assignment: started with promoted storage nodes becoming placement candidates
- placement scoring: started with deterministic round-robin replica placement
- rebalancing: started with `rsf-rebalance` load observations, bounded movement plans, replica-safety checks, and per-move placement-policy proofs
- slow-replica demotion: started as a planned repair action in `crates/rsf-repair`
- user-facing API gateway: started with `crates/rsf-api` request classification and auth-gated accept/reject decisions
- database gateway routing: started with `crates/rsf-gateway` handler selection, request budgets, consistency labels, and catalog placement bindings
- consistency-mode policy: started with `rsf-consistency` strict-quorum versus availability-first classification and downgrade rejection
- wire protocol adapters: started with `crates/rsf-wire` SQL, KV, object, blob, and admin frame normalization before API classification, plus `crates/rsf-portal` SQL portal lifecycle evidence
- namespace quota admission: started with `crates/rsf-quota` request, row, and byte reservations per namespace/surface
- internal service RPC envelopes: started with `crates/rsf-rpc` deadline, retry-class, attempt, trace ID, and body-size admission before fabric work
- relational WAL durability gate: started with `crates/rsf-wal` append, fsync, durable-LSN, and WAL-before-page-flush decisions
- auth policy: started with `crates/rsf-auth` principals, grants, expiring tokens, durable recovery, and allow/deny checks
- observability: started with `rsf-observability` metrics, audit events, service health, and aggregate health reports
- rolling upgrade protocol: started with `rsf-upgrade` version compatibility checks, one-node-at-a-time rollout stages, and unsafe cross-major rejection
- backup/restore: started with `rsf-snapshot` database snapshot coordination and `rsf-backup` backup manifests, restore-plan validation, and durable full-surface restore lifecycle recovery
- database restart recovery: started with `rsf-recovery` checkpoint root inventory, replay/reuse actions, and required-root corruption rejection

Invariant:

- rebalancing cannot reduce the committed replication guarantees for any placement group. Started with `rsf-rebalance` per-move placement-policy proofs, durable rebalance recovery, and invariant coverage.

## Phase 9: Performance Engineering

Deliverables:

- batched fsync
- pipelined replication
- direct I/O evaluation
- cache design: started with `rsf-cache` version-aware hot-read admission, deterministic eviction, pinned-entry rejection, and read-ahead plans
- hot/cold separation
- value separation
- read-ahead
- parallel object reads: started with `rsf-stream` parallel read-lane planning and verified chunk reads
- compaction scheduling
- workload-shape budgets: started with `rsf-workload` structural budget coverage for relational OLTP, ordered KV, object multipart, and blob streaming paths
- trace/testability gates: started with `rsf-testability` trace-depth, service-coverage, invariant-clean, metrics-richness, audit-presence, and replay-ready checks
- benchmark suite

Invariant:

- workload-shape budgets are structural planning evidence, not measured production benchmark claims.
- testability gates must be fed by actual run output and deterministic trace/replay artifacts, not hand-written acceptance.
- performance optimizations must preserve deterministic replay semantics in the simulator.

## Phase 10: WAN And Optional Decentralization

Deliverables:

- locality-aware placement
- async geo replication
- strict multi-region mode
- DHT/provider routing for immutable chunks
- public/untrusted node research branch
- Byzantine verification research branch

Invariant:

- async replicas must never be accepted as proof of strict commit.

## Current Concrete Operator Path

The current runnable path is the production daemon CLI:

```text
rsfd init --state <dir> --token <token> --node-key <key> --node-nonce <nonce> --mtls-trust-domain <domain> --tls-cert-sha256 <hex> --region <id> --zone <id>
rsfd join --state <dir> --token <token> --node-key <key> --node-nonce <nonce> --mtls-trust-domain <domain> --tls-cert-sha256 <hex> --region <id> --zone <id>
rsfd policy plan --state <dir>
rsfd policy apply --state <dir>
rsfd serve --state <dir> --listen <ip:port> --data-listen <ip:port> --profile <standalone|regional|multi-region> --max-consecutive-errors <n>
```

It should support:

- durable one-node bootstrap and token-verified node admission
- learner sync and quorum-safe promotion
- local segment files and durable policy/admin/protocol stores
- VSR-backed metadata commits and recovered certificates
- blob/object/SQL/KV surfaces routed through protocol-backed state
- state-backed status, audit, TCP health/metrics, canonical `rsf-wire` data-plane ingress admission, topology-profile readiness gates, bounded runtime supervision, and stdout progress
- ordered KV point writes/reads

The early SQL surface is intentionally narrow. Broader SQL only expands after the storage, metadata, ordered KV, planner/executor, and SQL-pipeline invariants are passing.
