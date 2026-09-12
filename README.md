# RSF

RSF is a full distributed database system with an internal replicated storage-fabric protocol.

The storage fabric is not the product boundary. It is the durability, placement, replication, repair, and verification core that lets the database expose multiple first-class surfaces:

- relational database
- ordered key-value database
- object store
- blob store
- append log
- block/file-compatible storage layers later

The project standard is not “interesting distributed storage.” The standard is a database that can be deployed as a single binary on new servers, absorb those servers into the cluster, and provide production-grade latency, durability, consistency, and operational behavior.

The developer experience standard is equally high: scaling from one server to redundant or multi-region topology should be expressed as simple database policy commands while RSF handles admission, replication, rebalancing, ownership, repair, routing, and consistency enforcement.

## Current Docs

- [Protocol Architecture Research](./docs/protocol-architecture-research.md)
- [Full Implementation Plan](./PLAN.md)
- [Product Requirements](./docs/requirements.md)
- [Full Database Architecture](./docs/full-database-architecture.md)
- [Implementation Roadmap](./docs/implementation-roadmap.md)
- [Implementation Map](./docs/implementation-map.md)
- [Storage Engine Design](./docs/storage-engine-design.md)
- [Verification Strategy](./docs/verification-strategy.md)
- [Decision Record](./docs/decision-record.md)
- [End-To-End POC](./docs/poc.md)

## Current Code

The Rust workspace is the production source path for the custom RSF protocol and database surfaces:

- `crates/rsf-admin`: cluster invite-token admission, node promotion, and placement mapping.
- `crates/rsf-api`: user-facing API request classification and auth-gated admission.
- `crates/rsf-auth`: principals, capability grants, scoped resources, and token authorization.
- `crates/rsf-backup`: snapshot manifests, restore-plan validation, and durable restore lifecycle recovery.
- `crates/rsf-blob`: immutable chunk/blob personality with retention pins.
- `crates/rsf-buffer`: relational buffer-pool residency, dirty-page, flush, and eviction contracts.
- `crates/rsf-cache`: version-aware hot-read cache admission, eviction, misses, and read-ahead plans.
- `crates/rsf-catalog`: versioned database metadata for namespaces, tables, buckets, and placement bindings.
- `crates/rsf-checkpoint`: local checkpoint-root retention and segment-compaction planning for the full database root set.
- `crates/rsf-chunkstore`: generic immutable content-addressed chunk store for SQL large values, object bodies, blob payloads, snapshots, SSTable blocks, and backup blocks.
- `crates/rsf-compaction`: SSTable compaction execution with newest-value merge, tombstone suppression, and output manifests.
- `crates/rsf-constraints`: relational write-admission checks for not-null, primary-key, unique, and reference constraints.
- `crates/rsf-consistency`: consistency-mode policy that separates strict quorum, availability-first, bounded-stale, and immutable-verified work.
- `crates/rsf-core`: shared database, storage, placement, transaction, relational, object, and blob types.
- `crates/rsf-deployment`: deployment package manifests, profile validation, rollout lifecycle recovery, roles, endpoints, volumes, and readiness probes.
- `crates/rsf-deferred`: commit-time deferred reference checks with transaction-local and committed referenced-key evidence.
- `crates/rsf-erasure`: cold-object erasure shard planning, checksums, and reconstruction contracts.
- `crates/rsf-executor`: physical query operator execution reports for cache, index, range, and stream stages.
- `crates/rsf-gateway`: database request routing across SQL, KV, object, blob, and admin surfaces.
- `crates/rsf-health`: node health scoring, promotion, demotion, and quarantine recommendations.
- `crates/rsf-idempotency`: user-facing idempotency ledger and retry policy before gateway routing.
- `crates/rsf-index`: relational index descriptors, mutation planning, rebuilds, and lookup state.
- `crates/rsf-invariants`: named architecture and safety invariant checks over full-run evidence summaries.
- `crates/rsf-isolation`: strict serializable read/write range validation and stale-read rejection.
- `crates/rsf-jobs`: background job orchestration for schema, index, stats, compaction, backup, retention, and repair work.
- `crates/rsf-kv`: ordered KV personality with MVCC reads, batches, tombstones, and change streams.
- `crates/rsf-lease`: primary lease and fence-token validation for stale-writer rejection.
- `crates/rsf-manifest`: format-independent extent manifests for relational, KV, object, blob, and snapshot storage records.
- `crates/rsf-materialize`: executor row materialization from encoded MVCC tuple payloads into SQL-facing query rows with projection and rejection paths.
- `crates/rsf-membership`: epoch-based membership changes, learner sync, quorum-preserving promotion, and voter-removal safety.
- `crates/rsf-migration`: durable schema-migration lifecycle for validation, descriptor publication, backfill, index catch-up, resume, completion, and rollback.
- `crates/rsf-object`: object-store personality over blob chunks and strict manifests.
- `crates/rsf-objectmeta`: strict object metadata coordinator for buckets, multipart drafts, visible version heads, and metadata rejection paths.
- `crates/rsf-observability`: typed metrics, audit events, and service health reports.
- `crates/rsf-page`: relational page/redo adapter with LSN validation and checkpoint summaries.
- `crates/rsf-planner`: physical query access-path planning over logical plans, indexes, ranges, cache, and streams.
- `crates/rsf-poc`: simulated end-to-end architecture runner.
- `crates/rsf-prepared`: prepared SQL statement cache with typed parameter binding, schema-version invalidation, deallocation, eviction, and rejection paths.
- `crates/rsf-portal`: SQL portal lifecycle over prepared statements with parse, bind, describe, execute, cancellation, deadline rejection, and close paths.
- `crates/rsf-primary`: primary-key index identity, point lookup, update, delete, and duplicate-key rejection.
- `crates/rsf-provider`: content-provider discovery and locality-aware provider selection for object/blob chunks.
- `crates/rsf-protocol`: durable VSR-backed protocol boundary for placement-group commits, placement maps, policy gates, membership changes, and recovered certificates.
- `crates/rsf-query`: relational logical query validation and row-batch execution contracts.
- `crates/rsf-quota`: namespace and surface quota policies with deterministic request reservations.
- `crates/rsf-range`: ordered range descriptors, split non-overlap proof, SSTable manifests, and compaction planning.
- `crates/rsf-rebalance`: controlled placement-group movement planning with replica-safety checks and placement-policy proofs.
- `crates/rsf-recovery`: database restart root inventory, replay/reuse actions, and corruption rejection.
- `crates/rsf-relcommit`: relational commit-group auditing that proves row and secondary-index mutations travel together with conflict ranges.
- `crates/rsf-repair`: scrub finding and repair-action planning over placement groups.
- `crates/rsf-requirements`: product-requirement coverage audit for production-readiness evidence and explicit remaining gaps.
- `crates/rsf-retention`: lifecycle and retention policy evaluation for object/blob records.
- `crates/rsf-rpc`: internal service envelopes, deadline policy, retry classification, and route admission.
- `crates/rsf-rowcodec`: schema-versioned relational row encoding and decoding with default/generated column materialization and rejection paths.
- `crates/rsf-schema`: schema migration planning, compatibility checks, and rollout steps.
- `crates/rsf-session`: SQL session binding, authorization intent, consistency, and transaction-shape planning.
- `crates/rsf-sessiontxn`: strict session transaction overlay for read-your-writes, snapshot reads, savepoints, SQL DML staging, partial rewind, and KV/protocol commit handoff.
- `crates/rsf-sim`: deterministic simulator with event scheduling, fault-injection controls, and full-run failure-drill coverage reports.
- `crates/rsf-snapshot`: database snapshot roots, checkpoint validation, and copy-on-write barriers.
- `crates/rsf-sql`: relational personality over ordered KV/protocol commits.
- `crates/rsf-sqldml`: composed SQL DML path for INSERT, UPDATE, and DELETE through session binding, auth, write planning, immediate transaction commit, strict isolation validation, atomic KV materialization, and session-transaction overlay staging.
- `crates/rsf-sqltxn`: SQL transaction script lifecycle for BEGIN, staged DML, read-your-writes SELECT, SAVEPOINT, rollback-to-savepoint, COMMIT, ROLLBACK, absence verification, and transaction-control rejection.
- `crates/rsf-sqlpipeline`: composed SQL request path for session binding, auth, logical planning, physical planning, physical execution, materialization, and rejection.
- `crates/rsf-stats`: table estimates, column histograms, and optimizer-stat freshness checks.
- `crates/rsf-storage`: local append-only segment/WAL recovery layer.
- `crates/rsf-stream`: chunked object/blob transfer plans, flow-control windows, parallel read lanes, and verification.
- `crates/rsf-superblock`: local boot-record validation for cluster identity, layout epoch, checkpoint, and required database roots.
- `crates/rsf-tenancy`: tenant, database, namespace ownership, and enabled database-surface descriptors.
- `crates/rsf-testability`: trace-depth, service-coverage, invariant, metrics, and audit gates for deterministic replay-ready evidence.
- `crates/rsf-txn`: transaction coordinator for read/write conflict ranges and commit-version allocation.
- `crates/rsf-tuple`: MVCC tuple headers, update/delete version chains, snapshot visibility, and rejection paths over encoded relational rows.
- `crates/rsf-upgrade`: rolling upgrade compatibility checks and one-node-at-a-time rollout plans.
- `crates/rsf-vacuum`: oldest-snapshot-safe tuple-chain pruning plans with retained-version and reclaimed-byte evidence.
- `crates/rsf-vsr`: VSR-style replicated metadata cell with prepare, quorum ack, commit, and view-change decisions.
- `crates/rsf-watch`: ordered KV watch subscriptions, prefix filtering, cursors, and resumable delivery.
- `crates/rsf-wal`: relational redo durability gate with durable-LSN and WAL-before-page-flush decisions.
- `crates/rsf-wire`: typed SQL, KV, object, blob, and admin wire-frame normalization with payload-bearing frames before API classification.
- `crates/rsf-workload`: workload-shape budgets for relational, KV, object, and blob paths without claiming production benchmarks.
- `crates/rsf-write`: relational write planning across constraints, row KV bytes, index mutations, conflict ranges, and commit intent.
- `crates/rsfd`: production daemon CLI for durable init, join, status, policy reconciliation, audit, upgrade planning, and serve-once/live daemon operation with health and payload-backed data-plane KV ingress listeners.

Run:

```text
cargo test
cargo run -p rsfd
cargo run -p rsfd -- init --state ./run/rsf --token local-dev-token --node-key node-1 --node-nonce nonce-1 --mtls-trust-domain rsf.local --tls-cert-sha256 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef --region iad --zone iad-a
cargo run -p rsfd -- status --state ./run/rsf
cargo run -p rsfd -- policy status --state ./run/rsf
cargo run -p rsfd -- policy plan --state ./run/rsf
cargo run -p rsfd -- serve --state ./run/rsf --listen 127.0.0.1:7000 --data-listen 127.0.0.1:7001 --profile standalone --max-consecutive-errors 3 --once
```

## Working Thesis

RSF should be built as a full database whose lower substrate combines:

- RADOS-style placement groups for physical storage and rebalancing
- VSR-style strict replicated cells for mutable truth
- content-addressed immutable chunks for large values, snapshots, and object/blob payloads
- FoundationDB-style layering for data models and APIs
- Aurora/Socrates-style page/log/WAL adapters for relational performance

The first implementation should prove a small, deterministic, crash-fault-tolerant database core before attempting global scale, Byzantine trust, or a public decentralized capacity market.
