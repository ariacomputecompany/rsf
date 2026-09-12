# RSF Implementation Map

Date: 2026-09-10

This file maps the full database architecture to concrete code modules.

## Workspace

```text
crates/rsf-admin
crates/rsf-api
crates/rsf-auth
crates/rsf-backup
crates/rsf-blob
crates/rsf-buffer
crates/rsf-cache
crates/rsf-catalog
crates/rsf-chunkstore
crates/rsf-checkpoint
crates/rsf-compaction
crates/rsf-constraints
crates/rsf-consistency
crates/rsf-core
crates/rsf-deployment
crates/rsf-deferred
crates/rsf-erasure
crates/rsf-executor
crates/rsf-gateway
crates/rsf-health
crates/rsf-idempotency
crates/rsf-index
crates/rsf-invariants
crates/rsf-isolation
crates/rsf-jobs
crates/rsf-kv
crates/rsf-lease
crates/rsf-manifest
crates/rsf-materialize
crates/rsf-migration
crates/rsf-membership
crates/rsf-relcommit
crates/rsf-object
crates/rsf-objectmeta
crates/rsf-observability
crates/rsf-page
crates/rsf-planner
crates/rsf-poc
crates/rsf-prepared
crates/rsf-portal
crates/rsf-primary
crates/rsf-provider
crates/rsf-protocol
crates/rsf-query
crates/rsf-quota
crates/rsf-range
crates/rsf-rebalance
crates/rsf-recovery
crates/rsf-repair
crates/rsf-requirements
crates/rsf-retention
crates/rsf-rpc
crates/rsf-rowcodec
crates/rsf-schema
crates/rsf-session
crates/rsf-sessiontxn
crates/rsf-sim
crates/rsf-snapshot
crates/rsf-sql
crates/rsf-sqldml
crates/rsf-sqltxn
crates/rsf-sqlpipeline
crates/rsf-stats
crates/rsf-storage
crates/rsf-stream
crates/rsf-superblock
crates/rsf-tenancy
crates/rsf-testability
crates/rsf-txn
crates/rsf-tuple
crates/rsf-upgrade
crates/rsf-vacuum
crates/rsf-vsr
crates/rsf-watch
crates/rsf-wal
crates/rsf-wire
crates/rsf-workload
crates/rsf-write
crates/rsfd
```

## Current Crates

### rsf-core

Owns shared database types and protocol/domain boundaries.

Modules:

- `cluster`: node identity, role assignment, cluster map.
- `placement`: placement groups, acting sets, replication/erasure policies.
- `storage`: WAL records, extents, layout kinds.
- `transaction`: commit intents, mutations, conflict ranges.
- `catalog`: relational schema descriptors.
- `relational`: relational plans and write representation.
- `object`: bucket/object/version/manifest structures.
- `blob`: content IDs and immutable chunks.

### rsf-deployment

Owns the simulated implementation package envelope.

Current scope:

- implementation-local manifest files under `demo-impls/*`
- implementation profiles parsed from package manifests
- role assignments for control-plane, gateway, metadata, storage, SQL, object, blob, and background-worker processes
- per-role service endpoints
- per-node durable database volume specs
- per-role readiness probe specs
- POC integration immediately after bootstrap and before node admission

Next scope:

- Kubernetes, systemd, Nomad, and bare-metal renderer adapters
- TLS identity and bootstrap secret material
- resource sizing policy
- upgrade compatibility gates
- per-role failure-domain and anti-affinity rules

### rsf-upgrade

Owns rolling upgrade compatibility and rollout planning.

Current scope:

- semantic software versions
- per-node version inventory
- same-major wire compatibility checks
- one-node-at-a-time drain, install, readiness probe, and promote stages
- compatibility check accounting
- unsafe cross-major rollout rejection
- POC integration after deployment manifests and before node admission

Next scope:

- persisted cluster version catalog
- mixed-version feature gates
- binary artifact identity and signature verification
- lease transfer and placement drain integration
- rolling downgrade and rollback plans
- canary waves and automatic pause criteria
- operator approval and audit records

### rsf-sim

Owns deterministic simulation.

Current scope:

- logical time
- seed
- scheduled events
- node/disk/message/timer event types
- node crash/restart event types
- deterministic link partitions
- deterministic per-link delays
- delivery decisions for deliver, delay, and drop
- full-run failure-drill reports for event scheduling, network, recovery, provider, health, and rebalance domains
- POC integration for crash, restart, delayed message, partition drop, and healed delivery

Next scope:

- deterministic disk fault execution
- production crash/restart process orchestration
- trace recording
- invariant runner

### rsf-erasure

Owns cold-object erasure layout contracts.

Current scope:

- data/parity erasure policy validation
- deterministic shard manifests
- node placement for erasure shards
- per-shard checksums
- read threshold and tolerated-loss reporting
- one-missing-data-shard reconstruction for the simulated XOR codec
- POC integration for cold object body layout after object/blob writes

Next scope:

- production Reed-Solomon codec
- placement-group backed shard writes
- parallel shard reads
- degraded reads
- repair scheduling for missing or corrupt shards
- cold/hot object policy transitions
- integration with object manifests and lifecycle policy

### rsf-health

Owns node health scoring and control-plane action recommendations.

Current scope:

- deterministic node health observations
- disk-free, p99-latency, replica-lag, heartbeat, and corruption signals
- policy-based score thresholds
- healthy, degraded, and demote classifications
- learner promotion recommendations
- slow-replica demotion recommendations
- corruption quarantine recommendations
- POC integration before repair and rebalancing services

Next scope:

- rolling health windows
- persisted health history
- failure-domain aggregate scores
- integration with admission promotion gates
- lease-transfer coordination for demotion
- direct repair/rebalance suppression hooks
- production metrics and alert export

### rsf-admin

Owns the first cluster-management structure.

Current scope:

- invite-token join requests
- capability validation
- learner-to-promoted admission records
- cluster-map epoch bumps
- role assignment for storage, metadata, gateway, and repair responsibilities
- deterministic replicated placement-group mapping over promoted storage nodes

Next scope:

- bootstrap credential storage
- health scoring
- slow-replica demotion
- placement movement plans
- rebalancing state machines
- rolling upgrade compatibility checks

### rsf-wire

Owns user-facing protocol frame normalization before API classification.

Current scope:

- SQL wire-frame envelopes
- ordered KV HTTP-style envelopes
- object/S3-style envelopes
- blob streaming envelopes
- admin RPC envelopes
- principal, namespace, operation, idempotency-key, and body-size extraction
- mutating protocol idempotency-key rejection
- POC integration after auth token issuance and before API classification

Next scope:

- PostgreSQL/MySQL wire adapters
- KV API adapter
- S3-compatible object gateway
- resumable blob streaming transport
- admin RPC transport
- request deadlines and body streaming backpressure
- protocol-specific canonicalization and body digests

### rsf-provider

Owns content-provider discovery for immutable object/blob chunks.

Current scope:

- content advertisements keyed by object/blob content ID
- object versus blob chunk surface labeling
- provider health, verification, region, node, and latency metadata
- locality-aware provider selection
- degraded decisions when healthy providers exist but the requested replica count cannot be met
- missing-content and no-healthy-provider rejection paths
- POC integration after object/blob/erasure materialization and before streaming transfer

Next scope:

- provider indexes persisted in the metadata plane
- provider liveness gossip and republishing windows
- anti-entropy reconciliation for stale advertisements
- placement-policy integration for replicated versus erasure-coded chunk reads
- sparse object read scheduling
- remote transport adapter integration with `rsf-stream`

### rsf-api

Owns user-facing request classification before requests enter database
personalities.

Current scope:

- SQL request classification
- ordered KV write request classification
- object put request classification
- blob put request classification
- admin join request classification
- auth-gated accepted/rejected decisions
- POC integration for one accepted SQL request and one rejected admin request

Next scope:

- request budgets, deadlines, and idempotency keys

### rsf-gateway

Owns database request routing after API classification and authorization.

Current scope:

- original API request plus authorization decision validation
- request budgets with deadline, row, and byte limits
- SQL requests routed to the session layer and relational placement binding
- ordered KV requests routed to the KV engine placement binding
- object requests routed to the object-store placement binding
- blob requests routed to the blob-store placement binding
- admin requests kept at the control-plane handler boundary
- strict versus availability-first consistency labeling
- POC integration after API classification and before storage/protocol work

Next scope:

- wire protocol adapters
- request deadline propagation
- idempotency-key tables
- admission control and overload shedding
- tenant-aware routing policy
- streaming object/blob request bodies
- gateway-to-service RPC envelopes

### rsf-idempotency

Owns user-facing idempotency and retry envelopes before routed requests reach
database personalities.

Current scope:

- validates non-empty idempotency keys and request fingerprints
- claims first-seen request envelopes with commit-version TTLs
- stores committed or rejected response digests for replay
- returns replayed responses for same-key/same-fingerprint requests
- rejects same-key/different-fingerprint reuse as conflicts
- classifies expired request identities
- computes bounded exponential retry delays for retryable failures
- POC integration after API classification and before database gateway routing

Next scope:

- durable idempotency-key table
- namespace/tenant scoping and request auth binding
- body-digest and parameter canonicalization
- response-body storage policy
- retry classification from transport, overload, and transaction errors
- expiry compaction and audit trails
- integration with SQL, KV, object, blob, and admin wire protocols

### rsf-quota

Owns namespace and surface quota admission.

Current scope:

- namespace/surface quota keys
- request-count limits
- optional row limits
- optional byte limits
- deterministic reservation accounting
- missing-policy rejection
- over-limit rejection without charging usage
- namespace and policy counting
- POC integration after gateway routing and before deeper execution

Next scope:

- durable tenant catalog integration
- hierarchical tenant/database/namespace quotas
- rate-window accounting
- burst credits
- overload shedding hooks
- storage-capacity reservations
- billing and usage export

### rsf-rpc

Owns internal service call envelopes after user-facing admission.

Current scope:

- source and destination service roles
- trace IDs
- operation names
- deadline policy
- retry classes
- attempt limits
- body-size policy
- accepted, retryable, and rejected route decisions
- POC integration after quota admission and before storage-fabric work

Next scope:

- real HTTP/2, QUIC, or custom transport adapters
- mTLS-authenticated service identity
- backpressure and circuit-breaker integration
- structured cancellation propagation
- hedged read execution
- overload-aware retry budgets
- distributed tracing export

### rsf-cache

Owns hot-read cache contracts for SQL, KV, object, and blob surfaces.

Current scope:

- multi-surface cache keys
- version-aware reads against minimum visible commit versions
- hit and miss reporting
- bounded entry admission
- deterministic unpinned eviction
- pinned-entry rejection when capacity cannot be reclaimed
- read-ahead plans scoped to the same namespace and surface
- POC integration after database personalities produce data and before snapshot coordination

Next scope:

- distributed invalidation from transaction commits and watch streams
- range/page server cache placement
- object/blob streaming cache chunks
- adaptive admission policy
- tenant-aware memory budgets
- stale-read safety tokens
- cache warmup from snapshots and backup restore plans

### rsf-stream

Owns chunked transfer planning for large database payloads.

Current scope:

- stream targets for SQL pages, KV ranges, object bodies, and blob chunks
- bounded chunk write plans
- deterministic lane assignment for parallel reads
- contiguous read-plan validation
- flow-control window reservations
- over-window rejection without charging in-flight bytes
- per-chunk verification reports
- corruption detection through expected chunk hashes
- POC integration after object/blob/erasure data is materialized and before cache/snapshot work

Next scope:

- resumable upload/download state
- transport adapters for streaming request bodies
- zero-copy local chunk IO
- remote chunk fetch scheduling
- retry and backpressure policy
- object manifest integration for sparse reads
- SQL page and KV range streaming execution adapters

### rsf-workload

Owns structural workload-shape budget contracts for the full database POC.

Current scope:

- workload classes for relational OLTP, ordered KV, object multipart, and blob streaming
- explicit p99 budget placeholders for each workload class
- deterministic observed-path costs derived from the simulated service path
- evidence checks for commits, cache hits, provider choices, stream lanes, and regions
- structural-shape report that keeps `benchmark_claim=false`
- POC integration after provider discovery, streaming, and cache evidence

Next scope:

- measured benchmark harness
- workload trace capture and replay
- production latency histograms
- benchmark gates separated by single-node, single-AZ, multi-AZ, and WAN profiles
- regression budgets tied to real page/log/KV/object/blob implementations

### rsf-testability

Owns structural trace/testability contracts for the full database POC.

Current scope:

- trace-depth gates over the complete simulated run
- service-coverage gates so architectural slices cannot silently disappear
- invariant-clean gate over the final summary
- observability metric and audit richness gates
- replay-ready report consumed by the product-requirement audit
- POC integration after invariants and before requirements

Next scope:

- direct Fozzy trace metadata ingestion
- shrink/replay result summaries attached to the run output
- per-requirement traceability links into service logs
- deterministic replay coverage split by implementation profile
- machine-readable evidence bundles for CI promotion

### rsf-auth

Owns identity and authorization contracts.

Current scope:

- user, service, and node principals
- capability grants
- cluster, namespace, and database-surface resources
- expiring version-scoped tokens
- allow/deny authorization decisions
- namespace grants that apply to surfaces within that namespace
- POC integration for app query access and denied repair operation

Next scope:

- durable auth catalog integration
- bootstrap credential storage
- key rotation
- external identity providers
- mTLS node identity binding
- row, object, and bucket ACL inheritance
- audit trails for policy changes

### rsf-tenancy

Owns tenant, database, namespace, and enabled database-surface descriptors.

Current scope:

- creates tenant descriptors with default regions
- creates tenant-scoped database descriptors
- requires each database to enable at least one surface
- binds catalog namespaces to a single tenant database
- rejects namespace rebinding to a different database
- validates enabled relational, KV, object, and blob surfaces
- POC integration after node health evaluation and before catalog publication

Next scope:

- durable tenant catalog roots
- tenant-scoped auth, quota, billing, and audit integration
- database lifecycle states
- region/failure-domain placement policy per database
- namespace rename and transfer workflows
- per-surface feature gates
- catalog publication through strict metadata cells

### rsf-backup

Owns full-database snapshot, restore planning, and durable restore lifecycle contracts.

Current scope:

- backup manifests
- checkpoint version validation
- catalog-root backup items
- transaction-log backup items
- KV, SQL, object-manifest, and blob-chunk backup items
- checksum and byte-count validation
- restore-plan validation requiring catalog and transaction-log roots
- restore execution evidence covering catalog, transaction-log, KV, SQL, object, and blob items
- durable restore operation replay for planned, started, completed, and failed lifecycle states

Next scope:

- incremental backups
- remote backup repositories
- encrypted manifests
- production restore IO
- crash-resume state machines
- point-in-time restore
- backup scheduling and retention

### rsf-snapshot

Owns database snapshot coordination before backup/export.

Current scope:

- snapshot IDs and namespace scope
- checkpoint commit-version validation
- catalog root tracking
- transaction-log root tracking
- KV, SQL, object, and blob root tracking
- duplicate root rejection
- future-version rejection
- copy-on-write barrier planning for mutable roots
- POC integration before backup manifest creation

Next scope:

- durable snapshot catalog
- per-placement-group snapshot markers
- partial snapshot transfer resume
- copy-on-write manifest persistence
- incremental snapshot deltas
- snapshot expiry policy
- restore checkpoint selection

### rsf-recovery

Owns full-database restart planning after local segment recovery and snapshot/backup validation.

Current scope:

- recovery roots for catalog, transaction-log, KV, SQL, object, and blob state
- required catalog and transaction-log root validation
- checkpoint version validation
- duplicate-root rejection
- replay actions for lagging durable roots
- reuse actions for checkpoint-matching roots
- optional-root quarantine
- required-root corruption rejection
- POC integration after snapshot and backup planning

Next scope:

- durable root catalog
- per-placement-group replay cursors
- partial snapshot transfer resume
- automatic root repair handoff
- restart-safe schema migration resume
- transaction recovery and in-doubt commit resolution
- checkpoint garbage collection
- production startup orchestration

### rsf-superblock

Owns local database boot-record validation before snapshot, backup, and recovery services trust root state.

Current scope:

- cluster identity in the boot record
- generation and layout epoch validation
- checkpoint commit-version anchoring
- required catalog and transaction-log roots
- KV, SQL, object, and blob root inventory
- duplicate, empty, future-version, and empty-checksum rejection
- generation/checkpoint advancement
- POC integration before snapshot, backup, and database recovery planning

Next scope:

- double-written superblock pages
- torn-superblock recovery
- generation quorum or majority selection across devices
- persisted durable root catalog
- checkpoint garbage collection
- startup mount integration

### rsf-manifest

Owns format-independent extent manifests for database records before they are placed into the storage fabric.

Current scope:

- manifest kinds for relational pages, relational WAL, ordered KV SSTables, object parts, blob chunks, and snapshot roots
- namespace, object name, commit version, placement group, byte range, extent id, and checksum fields
- surface counting across relational, KV, object, blob, and snapshot data
- total-byte accounting for mixed database manifests
- duplicate extent-id rejection
- overlapping byte-range rejection for one logical object/version
- empty namespace, object, range, extent-id, and checksum rejection
- POC integration after object/blob/erasure layout and before provider discovery, streaming, superblock, and snapshot summaries

Next scope:

- persisted manifest pages
- extent allocation and free-space catalogs
- immutable chunk store integration
- manifest compaction and garbage collection
- per-placement-group extent prefix comparison
- sparse corruption scan
- checksum upgrade path

### rsf-chunkstore

Owns the generic immutable content-addressed chunk layer below SQL large values, object bodies, blob payloads, snapshot blocks, SSTable blocks, and backup blocks.

Current scope:

- format class tags for relational large values, object bodies, blob payloads, snapshots, SSTables, and backup blocks
- namespace and non-empty payload validation
- scaffold content digest for deterministic POC runs
- immutable put semantics with dedupe/refcount accounting
- retention pin and pin-release accounting
- verified reads
- corruption quarantine decisions
- POC integration after manifest publication and before provider discovery/streaming

Next scope:

- durable chunk files
- real BLAKE3 or SHA-256 digest implementation
- zero-copy local reads
- extent allocation and manifest integration
- refcount garbage collection
- sparse verification scans
- replica/provider publication
- compression and encryption policy hooks

### rsf-checkpoint

Owns local checkpoint-root retention and segment-compaction planning for the full database root set.

Current scope:

- checkpoint roots for catalog, transaction log, KV, SQL, object, blob, and chunk index state
- required-root validation for catalog, transaction log, and chunk index roots
- namespace, name, version, byte-size, checksum, duplicate-root, and future-root validation
- local segment inventory validation
- retention of segments containing required checkpoint roots
- retention of segments with records newer than the checkpoint
- compaction decisions for old segments with reclaimable bytes
- checkpoint-regression rejection
- POC integration after chunkstore writes and before provider discovery

Next scope:

- durable checkpoint catalog pages
- physical segment rewrite and atomic install
- superblock advancement from checkpoint output
- manifest root rewrites after compaction
- chunk-index persistence and GC handoff
- crash-safe compaction recovery
- background scheduling and IO throttling

### rsf-invariants

Owns named architecture and safety checks over POC run summaries.

Current scope:

- admission capacity and placement-group coverage check
- quorum metadata path check
- stale-primary and stale-fence rejection check
- format-independent multi-surface manifest coverage check
- required boot-root validation check
- snapshot/recovery root coverage check
- failure-drill coverage check
- workload-budget shape check
- repair-action planning check
- rebalance placement-policy preservation check
- stream and erasure payload verification check
- immutable chunk-store verification check
- checkpoint compaction check
- SQL pipeline coverage check
- consistency-mode separation check
- strict isolation stale-read rejection check
- committed storage-tail and protocol-commit coverage check
- POC integration after observability summarizes the full run

Next scope:

- trace-native invariant inputs from deterministic simulator events
- per-placement-group prefix stability checks
- crash/restart invariant evaluation
- invariant shrinking hints for Fozzy failures
- safety/liveness separation
- machine-readable invariant coverage reports

### rsf-requirements

Owns product-requirement coverage audits over complete POC run summaries.

Current scope:

- groups the product requirements into 15 auditable families
- labels evidence as structural, simulated, design-target, or missing
- verifies full-database surfaces across relational, KV, object, and blob paths
- verifies one-binary join/promotion, deployment envelope, quorum mutable truth, native relational path, SQL execution, ordered KV, object/blob, format-independent layout, large-payload repairability, structural failure-drill coverage, recovery/backup roots, simulation testability, structural workload shape, and explicit mode boundaries
- records workload-shape performance evidence as structural without claiming real benchmark results
- records simulation-testability evidence as structural only after `rsf-testability` proves trace depth, service coverage, invariant cleanliness, metrics richness, audit presence, and replay readiness
- POC integration after invariants so requirement coverage audits the final run evidence

Next scope:

- generated traceability matrix from `docs/requirements.md`
- per-requirement links to implementation crates, tests, Fozzy traces, and manual artifacts
- regression gates for requirements that must stay structural
- separate production-readiness audits once simulated components are replaced

### rsf-consistency

Owns consistency-mode policy before routed database work reaches storage or execution services.

Current scope:

- classifies relational, KV, object-metadata, blob-payload, and admin work
- requires strict quorum for mutable truth and metadata
- allows availability-first only for immutable blob payload placement after hash verification
- rejects attempts to route relational mutable truth through availability-first semantics
- POC integration beside gateway routing, before quota/RPC/fabric work

Next scope:

- bounded-stale safe-read token validation
- immutable-verified provider proofs
- per-namespace consistency policy overrides
- WAN strict-write admission that makes RTT cost explicit
- API-visible consistency contracts

### rsf-protocol

Owns the protocol boundary between database personalities and the storage fabric.

Current scope:

- placement-group protocol router
- deterministic commit records
- per-placement-group monotonic log indexes
- database-global commit versions
- explicit strict vs availability-first consistency mode tags
- client request IDs with replayed-result idempotency
- fabric operation taxonomy for catalog, SQL, KV, object, and blob work

This crate is deliberately small today. Its public interface is the replacement seam for real VSR/Raft-style replicated cells, reconfiguration, and idempotent client requests. Primary lease and fence-token contracts have started in `rsf-lease`.

### rsf-vsr

Owns VSR-style replicated metadata-cell decisions.

Current scope:

- replica-set validation
- current-primary prepare admission
- prepare_ok quorum acknowledgement tracking
- quorum commit decisions
- view change with carried committed-prefix count
- stale-primary rejection after view change
- POC integration after lease fencing and before catalog/protocol work starts

Next scope:

- durable replicated prepare log
- normal-case pipelining
- checkpoint and state transfer
- recovery after primary crash
- full view-change transcript validation
- membership integration and learner catch-up

### rsf-catalog

Owns database-wide metadata.

Current scope:

- namespaces
- table descriptors
- object bucket descriptors
- logical surface to placement-group bindings
- catalog snapshots at protocol commit versions
- idempotent create semantics for existing metadata
- metadata commits through `rsf-protocol`

Next scope:

- schema migrations
- retention policies
- durable auth policy roots
- tenant/database descriptors
- metadata watches
- catalog recovery from replicated metadata cells

### rsf-kv

Owns the ordered key-value personality.

Current scope:

- in-memory ordered keys
- versioned values
- MVCC read snapshots
- MVCC clear tombstones
- stale write conflict detection
- atomic batch commits
- prefix scans
- change streams by commit version
- writes through `rsf-protocol`

Next scope:

- conflict ranges
- range splits: started structurally in `rsf-range` with non-overlap proof and overlap rejection
- durable stream retention policies

### rsf-watch

Owns ordered KV watch subscriptions and resumable delivery.

Current scope:

- watch subscription IDs
- client identity validation
- prefix-based subscriptions
- cursor-based delivery over `rsf-kv` changes
- tombstone propagation
- acknowledgement-driven cursor advancement
- resume from durable cursor
- POC integration after ordered KV writes and before range/session layers

Next scope:

- durable subscriber catalog
- fanout workers
- transport adapters for SQL/KV APIs
- backpressure and slow-consumer policy
- delivery leases and retry envelopes
- compaction-safe change retention windows

### rsf-jobs

Owns background job orchestration for database maintenance work.

Current scope:

- typed jobs for schema backfill, index build, stats refresh, SSTable compaction, backup export, retention sweep, and repair copy
- validates job namespace, target, and attempt budget
- enqueues idempotent job specs
- leases ready jobs to workers with commit-version TTLs
- blocks jobs whose dependencies are incomplete
- completes leased jobs with versioned completion records
- requeues retryable leased failures until attempt budget is spent
- POC integration after schema planning and before auth/API traffic

Next scope:

- durable job catalog
- worker heartbeats and lease stealing
- dependency graph persistence
- priority queues and per-tenant fairness
- job progress checkpoints and resume
- integration with schema backfill, index build, stats, compaction, backup, retention, and repair services
- operator cancellation and audit trails

### rsf-lease

Owns primary lease and fence-token contracts for strict placement-group writes.

Current scope:

- placement-group lease keys
- active primary leases
- monotonically issued fence tokens
- stale-epoch acquisition rejection
- lease renewal
- expired-lease write rejection
- stale-fence write rejection after a newer epoch replaces the primary
- POC integration before catalog/protocol work starts

Next scope:

- quorum-backed lease acquisition
- safe-read tokens
- lease replication through VSR/Raft cells
- clock-drift bounds or logical lease alternatives
- automatic lease transfer during rebalancing
- integration inside `rsf-protocol` commit admission

### rsf-membership

Owns epoch-based cluster membership changes.

Current scope:

- initial voter-set validation
- learner addition
- learner sync gating
- learner promotion with epoch advancement
- old/new quorum-intersection proof for learner promotion
- voter removal with minimum-voter safety
- unsafe reconfiguration rejection
- POC integration after admission and before placement

Next scope:

- joint-consensus or VSR reconfiguration protocol
- replicated membership log
- learner catch-up streaming
- snapshot transfer to learners
- failure-domain-aware voter changes
- placement and lease integration

### rsf-query

Owns relational logical query planning and row-batch execution contracts.

Current scope:

- table-scan query plans
- projection validation against catalog table descriptors
- equality predicates
- conjunctive predicates
- limit enforcement
- row filtering and projection execution

Next scope:

- range scans over `rsf-kv`
- join planning
- integration as the SELECT path inside `rsf-sql`

### rsf-planner

Owns physical query access-path planning after logical query validation.

Current scope:

- consumes `rsf-query` logical plans
- consumes table row/byte estimates published by `rsf-stats`
- validates table stats against catalog table descriptors
- validates available `rsf-index` descriptors
- selects primary-key lookup for primary-key equality predicates
- selects secondary-index lookup for indexed equality predicates
- falls back to range scan for unindexed predicates and unconstrained scans
- prepends cache lookup for cacheable reads
- appends stream read for large estimated results
- POC integration after logical query execution and before object/blob work

Next scope:

- real cardinality and byte cost model
- multi-column and covering-index selection
- range-bound derivation for ordered KV scans
- distributed placement and locality-aware operator routing
- physical execution pipeline contracts
- feedback from cache, range, and stream services

### rsf-stats

Owns optimizer statistics for relational and ordered-KV planning.

Current scope:

- publishes table row and byte estimates against catalog table descriptors
- computes average row bytes
- builds deterministic column histograms from value samples
- counts distinct values and null samples
- tracks statistics versions
- classifies fresh versus stale stats by catalog-version lag
- POC integration after compaction execution and before SQL session binding

Next scope:

- incremental stats collection from real range/table samples
- histogram compaction and sketching
- index selectivity estimates
- correlation and multi-column statistics
- write-path invalidation and asynchronous refresh jobs
- feedback from physical execution reports
- catalog persistence and lease/fence protection for published stats

### rsf-constraints

Owns relational constraint validation before write commit.

Current scope:

- derives not-null checks from catalog table descriptors
- validates primary-key values are present
- validates unique constraints against simulated key state
- validates foreign-key-style references against simulated referenced keys
- returns accepted/rejected decisions rather than hiding constraint failures as internal errors
- rejects malformed constraint metadata as `InvalidInput`
- POC integration after optimizer statistics and before SQL session binding

Next scope:

- integration into `rsf-sql` insert/update/delete commit admission
- durable unique indexes and reference indexes
- snapshot-aware reference checks
- cascading actions
- conflict-range declaration for serializable constraint probes
- online validation during schema migration

### rsf-deferred

Owns commit-time deferred relational reference checks for multi-statement transactions.

Current scope:

- begins a transaction-local deferred constraint context
- stages foreign-key obligations without requiring the referenced row to exist immediately
- stages referenced keys produced inside the same transaction
- validates obligations at commit against transaction-local and committed referenced-key evidence
- accepts commit-time checks when a referenced key appears later in the same transaction
- rejects commit-time checks when the referenced key remains missing
- POC integration after immediate relational constraints and before relational write planning

Next scope:

- direct integration with SQL transaction commit admission
- snapshot-aware reference-index reads
- durable deferred-obligation storage
- cascading action planning
- cross-placement-group reference validation

### rsf-write

Owns relational write planning before commit.

Current scope:

- consumes `rsf-constraints` accepted/rejected decisions
- encodes physical row KV keys and row value bytes for insert/update/delete planning
- invokes `rsf-index` to plan secondary-index mutations
- builds a transaction `CommitIntent` with row and index mutations
- declares row and primary-key conflict ranges for the transaction layer
- returns ordered write steps for constraint checks, row write, index update, conflict declaration, and commit
- POC integration after relational constraints and before SQL session binding

Next scope:

- generated columns and default values
- durable secondary unique constraint probes
- direct `rsf-deferred` integration for deferrable constraints inside multi-statement transactions
- SQL engine integration for insert/update/delete admission
- retry envelopes and idempotency-key table integration
- cross-placement-group write orchestration

### rsf-relcommit

Owns relational commit-group auditing between write planning and transaction commit.

Current scope:

- consumes `rsf-write` plans
- verifies exactly one table-row mutation is present in the transaction intent
- verifies planned secondary-index mutations are also present in the same intent
- verifies row/identity conflict-range evidence is declared before commit
- accepts insert, update, and delete row-plus-index commit groups
- rejects partial commit groups that lost the row or index mutation
- POC integration after relational write planning and before page/WAL/buffer simulation

Next scope:

- cross-placement-group atomic commit grouping
- durable transaction-record materialization
- lock/latch scheduling hooks
- commit-retry and idempotency envelope integration
- direct SQL DML admission over audited groups

### rsf-executor

Owns physical query operator execution after access-path planning.

Current scope:

- consumes `rsf-planner` physical plans
- maps cache lookup, primary index, secondary index, range, and stream operators into execution stages
- reports output rows, bytes, stage count, and remote-read count
- rejects empty physical plans
- validates stream-read operator thresholds
- POC integration after physical query planning and before object/blob work

Next scope:

- real async operator pipeline
- range and index cursor clients
- cache miss/fill feedback
- bounded memory and backpressure accounting
- streaming row/page materialization
- distributed retry and partial-result cancellation

### rsf-primary

Owns relational primary-key identity and point lookup.

Current scope:

- primary-key descriptor validation from catalog table metadata
- deterministic encoded primary-key bytes
- insert path that records row key, placement group, and commit version
- point lookup path returning row location evidence
- update path that replaces row version without changing row identity
- delete path that removes row identity
- duplicate primary-key insert rejection
- missing primary-key lookup evidence
- POC integration before secondary index maintenance

Next scope:

- durable primary-index pages or ordered-KV ranges
- primary-key participation in SQL write commit admission
- cross-placement-group primary-key ownership and lookup routing
- snapshot-aware primary-index reads
- page-server and range-server cache integration

### rsf-prepared

Owns prepared SQL statement lifecycle boundaries.

Current scope:

- named prepared statement cache with deterministic capacity
- SQL text validation for empty statements and positional placeholder count
- typed parameter descriptors and typed bind values
- bind-time SQL rendering for POC evidence
- schema-version invalidation before bind reuse
- explicit statement deallocation
- least-recently-used eviction evidence
- POC integration after session transactions and before primary-index work

Next scope:

- binary/text parameter codecs and result formats
- dependency graph invalidation across table, column, index, and function changes
- prepared DML and query-plan reuse through planner/executor caches
- per-session memory quotas and statement timeout/deadline propagation
- cancellation and distributed retry integration

### rsf-portal

Owns SQL portal lifecycle boundaries above prepared statements.

Current scope:

- Parse-shaped statement preparation through `rsf-prepared`
- Bind-shaped named portal creation with typed parameters
- Describe-shaped portal metadata reporting
- Execute-shaped row-count and elapsed-time reporting
- deadline rejection before execution completes
- client cancellation marking and rejected execution after cancellation
- explicit portal close and statement close cleanup
- POC integration after prepared SQL cache work and before primary-index work

Next scope:

- PostgreSQL wire protocol message encoding/decoding
- binary/text parameter codecs and result formats
- unnamed statement and unnamed portal replacement rules
- backend key data and asynchronous CancelRequest handling
- planner/executor cursor integration with partial fetch counts
- transaction retry envelopes and distributed cancellation propagation

### rsf-index

Owns relational index maintenance contracts.

Current scope:

- secondary index descriptors
- descriptor validation against catalog table columns
- deterministic index-key encoding
- insert mutation planning
- update delete/put mutation planning
- delete mutation planning
- one-index rebuild planning
- in-memory lookup state for simulated execution
- POC integration between SQL session binding and relational execution

Next scope:

- multi-column and unique constraint enforcement
- durable index metadata roots
- online catch-up from schema migration barriers
- background backfill execution
- index compaction and storage layout
- optimizer integration through `rsf-planner`

### rsf-range

Owns ordered range and SSTable layout contracts.

Current scope:

- key-range descriptors
- overlap rejection
- range split validation
- non-overlapping split-boundary proof
- placement-group assignment per range
- immutable SSTable manifests
- SSTable bounds, byte-count, and checksum validation
- compaction planning over multiple input tables
- POC integration between ordered KV and relational/query services

Next scope:

- range merges
- real SSTable persistence
- bloom filters and block indexes
- range-server read RPC
- split/merge orchestration through consensus
- integration with `rsf-kv` storage paths

### rsf-compaction

Owns SSTable compaction execution after ordered range compaction planning.

Current scope:

- consumes `rsf-range` compaction plans and SSTable manifests
- validates that each planned input has a matching run
- validates entry keys against manifest bounds
- merges sorted run entries by key
- keeps the newest version per key
- drops tombstoned keys from live output
- emits a new output SSTable manifest
- reports input bytes, output bytes, tombstone count, and reclaimed bytes
- POC integration after range layout planning and before SQL session binding

Next scope:

- streaming multi-run merge iterators
- snapshot-aware tombstone retention windows
- bloom-filter and block-index generation
- write amplification accounting
- output file writing and fsync policy
- compaction scheduling and throttling
- handoff to repair when checksums fail

### rsf-session

Owns the SQL session/request boundary before relational execution.

Current scope:

- session context with principal and namespace
- SQL statement classification into query, DDL, and DML
- catalog binding for relational table targets
- relational surface placement-group lookup
- required capability selection for read and write statements
- strict consistency tagging for SQL session work
- transaction-shape selection for read-only, metadata-write, and single-partition write statements
- authorization evaluation through `rsf-auth`
- POC integration before `rsf-sql` execution

Next scope:

- prepared statement cache
- request budgets and deadlines
- SQL wire protocol session state
- transaction retry envelopes
- multi-statement transaction lifecycle
- row-level and object-level policy hooks
- binding to physical query operators

### rsf-schema

Owns relational schema migration planning.

Current scope:

- schema version transition records
- additive column validation
- index-build step planning
- online compatibility flags for old readers and writers
- non-nullable column backfill step planning
- unsafe column-drop rejection unless explicitly allowed
- primary-key drop rejection
- POC integration after catalog descriptor creation

Next scope:

- durable catalog descriptor updates
- background backfill workers
- online index catch-up
- tenant-scoped migration queues

### rsf-migration

Owns durable schema migration lifecycle orchestration.

Current scope:

- begins migration execution from an `rsf-schema` plan
- records durable validation and descriptor-publication barriers
- advances non-null column backfill work
- advances online index catch-up work
- reports resumable progress after partial execution
- completes after read visibility switches
- rolls back a not-yet-visible migration with an operator reason
- rejects attempts to advance a rolled-back migration
- POC integration after schema planning and before background jobs

Next scope:

- persistent migration journal records through replicated metadata cells
- schema leases and catalog visibility barriers
- worker checkpointing for chunked backfill and index catch-up
- crash/restart resume from durable migration cursors
- tenant-scoped migration queues and cancellation

### rsf-rebalance

Owns controlled placement-group movement planning.

Current scope:

- node load observations
- high/low utilization policy
- minimum replica floor
- bounded movement plans
- replica movement actions
- safety checks for storage capacity, replica count, and target selection
- per-move placement-policy preservation proofs
- POC integration after repair planning

Next scope:

- executable movement state machines
- lease transfer during movement
- repair-aware movement suppression
- failure-domain scoring
- backpressure and throttling
- progress tracking and rollback

### rsf-repair

Owns scrub findings and repair-action planning.

Current scope:

- replica health observations by placement group
- missing replica findings
- corrupt replica findings
- slow replica findings
- under-replicated placement-group findings
- repair actions for replica copy, digest recheck, and slow-replica demotion
- refusal to plan unsafe copy repair when no healthy source exists

Next scope:

- scrub inventory collection from storage nodes
- executable repair workers
- rebalancing plans
- repair progress tracking
- foreground I/O throttling

### rsf-retention

Owns lifecycle and retention policy evaluation.

Current scope:

- retain-until commit-version floors
- legal hold overrides
- current object version protection
- noncurrent object compaction decisions
- expired object decisions
- unreferenced blob pin release decisions

Next scope:

- durable policy catalog integration
- background lifecycle workers
- object-delete markers
- compaction execution
- audit trails for destructive lifecycle actions

### rsf-sql

Owns the relational personality.

Current scope:

- table descriptors
- tiny SQL text executor for `CREATE TABLE`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`, and equality `SELECT`
- catalog changes committed through `rsf-protocol`
- primary-key row materialization
- secondary index metadata and lookup
- writes encoded into ordered KV keys
- row, index, and physical KV bytes share relational protocol commit versions
- deletes leave physical KV tombstones
- snapshot access to encoded physical rows

Next scope:

- catalog transactions
- broader SQL parser
- planner/executor operators beyond equality lookup
- MVCC read timestamps

### rsf-sqlpipeline

Owns the composed SQL request lifecycle across session, auth, planning, and execution crates.

Current scope:

- binds SQL text to catalog metadata through `rsf-session`
- authorizes the request token and principal before planning work
- lowers narrow `SELECT <columns> FROM <table> [WHERE column = literal] [LIMIT n]` text into `rsf-query`
- validates projection, predicate, and limit against the catalog table descriptor
- chooses physical access paths through `rsf-planner`
- runs planned cache, index, range, and stream stages through `rsf-executor`
- materializes projected rows through the logical query executor
- rejects DML and invalid projections before physical execution
- POC integration after standalone physical query execution and before object/blob work

Next scope:

- full SQL parser
- prepared statements
- distributed retries and cancellation
- row encoding/decoding integration with the relational storage path
- optimizer feedback loops
- multi-statement transactions

### rsf-sqldml

Owns the composed SQL write lifecycle across session, auth, write planning, transaction coordination, strict isolation, and physical KV materialization.

Current scope:

- binds DML SQL text to catalog metadata through `rsf-session`
- authorizes namespace writes before planning or committing
- lowers narrow `INSERT INTO <table> (...) VALUES (...)`, `UPDATE <table> SET ... WHERE id = ...`, and `DELETE FROM <table> WHERE id = ...` text into `rsf-write`
- validates constraints and index metadata before transaction prepare
- prepares and commits through `rsf-txn` with one protocol commit version
- validates strict serializable read/write ranges through `rsf-isolation`
- applies row and index KV mutations at the transaction commit version
- stages accepted DML mutations into `rsf-sessiontxn` overlays and proves read-your-writes visibility before session commit
- rejects duplicate unique-key writes before transaction commit
- POC integration after SELECT pipeline execution and before object/blob work

Next scope:

- prepared DML statements
- retry envelopes and cancellation
- deferred constraints over the session overlay

### rsf-rowcodec

Owns schema-versioned relational row bytes.

Current scope:

- typed row values for int64, text, bool, bytes, and null
- schema-version tag embedded in encoded row payloads
- descriptor-ordered physical row encoding
- physical row decode back into typed values
- default column materialization before encoding
- generated text-length column materialization before encoding
- missing required-column rejection
- type-mismatch rejection
- descriptor mismatch and trailing-byte rejection during decode
- POC integration before relational write planning

Next scope:

- nullable bitmap and compact varint layout
- generated/default expression catalog integration
- row compression and encryption hooks
- page-format integration with `rsf-page`
- vectorized batch decode and late expression-evaluation hooks

### rsf-tuple

Owns relational MVCC tuple metadata over encoded row bytes.

Current scope:

- tuple keys by table and primary key bytes
- tuple headers with create version, optional delete version, schema version, and encoded row byte length
- insert of a new physical row version
- update as previous-version delete marker plus appended current version
- delete as live-version delete marker
- snapshot-visible version selection
- duplicate-live insert rejection
- missing update/delete rejection
- POC integration between row encoding and relational write planning

Next scope:

- durable page-slot tuple identifiers
- HOT/update-chain pruning decisions
- integration with page and WAL redo records

### rsf-vacuum

Owns relational tuple-chain pruning safety.

Current scope:

- validates oldest-snapshot vacuum policies
- plans tuple-version pruning only after delete versions are older than the oldest active snapshot
- keeps versions still needed by younger snapshots
- enforces at least one retained tuple version
- accounts pruned versions, retained versions, and reclaimed bytes
- rejects empty tuple chains
- POC integration between tuple visibility and relational write planning

Next scope:

- distributed oldest-active-snapshot collection
- WAL-logged page-slot pruning records
- vacuum worker scheduling and throttling
- index cleanup handoff
- visibility-map and free-space-map integration

### rsf-materialize

Owns executor-facing row materialization from encoded MVCC tuple payloads.

Current scope:

- decodes tuple row bytes through `rsf-rowcodec`
- validates tuple table identity against the descriptor
- validates tuple schema version against the descriptor
- emits SQL-facing `QueryRow` values for full-row reads
- applies explicit column projections
- rejects table mismatch, schema mismatch, malformed payloads, empty projections, unknown columns, and projected null values
- POC integration between tuple visibility/vacuum and relational write planning

Next scope:

- vectorized/batched materialization
- page-slot tuple identifiers from `rsf-page`
- executor expression projection and late materialization
- zero-copy value views over page buffers
- integration with physical executor pipelines and WAL redo records

### rsf-sqltxn

Owns SQL-facing transaction-control orchestration above `rsf-sessiontxn` and `rsf-sqldml`.

Current scope:

- parses narrow `BEGIN`, `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, DML statements, and `SELECT ... FROM <table> WHERE id = ...`
- rejects DML and SELECT outside an active SQL transaction
- begins strict session transactions at a KV snapshot
- stages DML through `rsf-sqldml` into the session overlay
- reads staged row data through SQL-shaped SELECT before global commit
- commits staged row and index mutations through `rsf-sessiontxn`, `rsf-kv`, and `rsf-protocol`
- rolls back staged INSERT mutations and verifies the inserted row never became globally visible
- rewinds to a savepoint, verifies later staged INSERT rows are no longer overlay-visible or globally visible, and then commits earlier staged writes
- POC integration after composed SQL DML and before object/blob work

Next scope:

- full SQL transaction grammar
- deferred constraint validation at COMMIT
- retry envelopes and cancellation
- cross-placement-group SQL transaction execution

### rsf-page

Owns the relational page/redo adapter boundary.

Current scope:

- table page IDs
- page frames with LSN, row count, and byte count
- redo records for insert, update, delete, and split operations
- stale-page-LSN rejection
- minimum-LSN read validation
- checkpoint summaries with high LSN and page bytes
- POC integration between relational write planning and SQL session binding

Next scope:

- page server storage backed by placement groups
- buffer-pool eviction and dirty-page flush policy
- redo log replay into pages
- page split/merge algorithms
- relation-to-page mapping
- MVCC visibility on page reads
- integration with physical query execution

### rsf-txn

Owns the first database transaction coordinator structure.

Current scope:

- read versions from the protocol router
- commit intents with read conflict ranges, write conflict ranges, and mutations
- range validation
- optimistic conflict detection against newer committed write ranges
- transaction commits through `rsf-protocol`
- committed transaction records for replayable audit state

Next scope:

- cross-placement-group commit protocol
- lock-free conflict-range storage over ordered KV
- retry envelopes
- integration adapters for SQL and object metadata transactions

### rsf-sessiontxn

Owns strict per-session transaction overlays above KV and below higher-level SQL/object transaction adapters.

Current scope:

- session transaction begin at a KV snapshot
- pending set and clear overlay entries
- read-your-writes overlay reads before global commit
- non-overlay reads pinned to the begin snapshot
- named savepoints over pending overlay state
- rollback-to-savepoint partial rewinds that discard later pending writes while preserving earlier writes
- commit handoff through `rsf-kv` and `rsf-protocol`
- SQL DML staging and overlay readback through `rsf-sqldml`
- SQL transaction-control orchestration through `rsf-sqltxn`
- rejection accounting for duplicate or closed session transactions
- POC integration after ordered KV/watch and before later storage-layout services

Next scope:

- retry envelopes
- object metadata transaction adapters
- cross-placement-group session overlays

### rsf-isolation

Owns strict-layer transaction isolation validation.

Current scope:

- transaction read versions
- declared read and write ranges
- strict serializable stale-read rejection
- write/write conflict rejection
- write-skew cycle detection from read/write dependencies
- serialization dependency edge tracking
- committed isolation records for replayable audit state
- POC integration after transaction commit-version allocation and before KV/SQL/object work

Next scope:

- production serializable snapshot isolation
- distributed conflict-range storage
- integration with `rsf-sessiontxn` read-your-writes overlays
- safe retry envelopes
- cross-placement-group isolation adapters
- lock-free range index
- integration with SQL planner and object metadata transactions

### rsf-object

Owns the object-store personality.

Current scope:

- object versions
- manifests
- bucket metadata creation
- multipart upload state
- object visibility delayed until manifest commit
- object bodies stored through `rsf-blob`
- manifest visibility committed through `rsf-protocol`

Next scope:

- metadata indexes
- retention and lifecycle policy: started in `rsf-retention`
- S3-compatible gateway adapter

### rsf-objectmeta

Owns strict object metadata truth separate from object body persistence.

Current scope:

- bucket registration
- multipart upload session state
- one-based unique part references
- visible object head commits after non-empty multipart manifests
- current-head reads
- strict metadata commits through `rsf-protocol`
- rejection accounting for invalid metadata requests
- POC integration before the object/blob body path

Simulated boundaries:

- quorum replication is represented by the current structural protocol cell and should be replaced by the real metadata consensus path
- manifest digests are deterministic inputs, not cryptographic manifest encodings

### rsf-observability

Owns operational telemetry contracts.

Current scope:

- counters
- gauges
- service health states
- audit events with version, node, and placement-group context
- aggregate health reports
- POC integration for summary metrics and degraded repair health

Next scope:

- structured log ingestion
- metrics exporters
- tracing spans
- alert rules
- operator-facing health endpoints

### rsf-blob

Owns the immutable blob/chunk personality.

Current scope:

- deterministic demo content IDs
- chunk storage
- duplicate content dedupe
- verified reads by content ID
- retention pins
- pin commits through `rsf-protocol`

Next scope:

- real cryptographic hashing
- chunking
- streaming read/write
- dedupe indexes
- repair-by-digest

### rsf-buffer

Owns relational page residency and dirty-frame accounting.

Current scope:

- fixed-size buffer pools
- resident page frames
- pin and unpin accounting
- repeated-page hit detection
- dirty-page marking
- page flush decisions
- unpinned-frame eviction
- full-pinned-pool rejection
- POC integration after page redo and before SQL session binding

Next scope:

- clock or LRU-K replacement policy
- dirty-page writeback scheduling
- checkpoint throttling
- page prefetch and scan resistance
- NUMA and direct-IO integration
- page-server backed fetch/flush RPCs

### rsf-wal

Owns relational redo durability admission.

Current scope:

- WAL policy validation
- redo append decisions
- monotonic LSN enforcement
- group sync decisions
- durable-LSN accounting
- WAL-before-page-flush admission
- POC integration after page redo and before buffer-pool flush behavior

Next scope:

- group-commit scheduling
- storage-backed fsync and torn-write recovery
- checkpoint integration
- log truncation and archival
- page-server redo subscription
- replication and quorum durability coupling

### rsf-poc

Owns the simulated end-to-end architecture proof.

Current scope:

- implementation catalog
- reusable simulated service pipeline
- structured trace logs
- full database path through bootstrap, deployment package manifest, rolling upgrade planning, admission, membership changes, placement, deterministic faults, lease fencing, tenancy, catalog, schema planning, migration lifecycle execution, jobs, auth, wire-frame normalization, API classification, idempotency, database gateway routing, consistency-mode policy, quota admission, internal RPC admission, fabric, consensus, transaction visibility, transaction coordination, strict isolation validation, ordered KV, watch delivery, range layout, compaction execution, optimizer statistics, relational constraints, primary indexing, secondary indexing, relational write planning, relational commit-group auditing, page redo, WAL durability gating, buffer pool residency, relational execution, logical query execution, physical query planning, physical query execution, composed SQL pipeline, object, blob, erasure layout, format-independent extent manifesting, immutable chunk storage, local checkpoint compaction, provider discovery, streaming transfer, cache admission/read serving, workload-shape budgeting, superblock boot validation, snapshot coordination, backup, database recovery, retention, repair, rebalancing, observability, invariant checking, testability auditing, and product-requirement coverage auditing
- fault service schedules simulator events, logs delay/drop/deliver decisions, and emits a failure-drill coverage proof
- health service scores node signals and emits promotion, demotion, and quarantine recommendations
- deployment service materializes a simulated implementation package into roles, endpoints, volumes, and probes
- upgrade service preflights version compatibility, stages rolling drain/install/probe/promote work, and rejects unsafe major-version rollout
- membership service adds and syncs a learner, proves old/new quorum intersection during promotion, promotes it through an epoch change, removes one voter safely, and rejects unsafe voter removal
- lease service grants primary fences and rejects a stale writer after simulated view change
- VSR service prepares quorum-backed metadata records, commits after prepare_ok quorum, changes view, and rejects a stale primary
- tenancy service creates the tenant/database ownership envelope and validates enabled database surfaces before catalog publication
- catalog service registers namespace, SQL table, object bucket, and surface placement bindings
- schema service plans online descriptor/index rollout and rejects unsafe drops before SQL execution
- migration service executes schema lifecycle decisions, including descriptor publication, backfill, index catch-up, resume, completion, rollback, and rejected post-rollback advance
- jobs service enqueues, leases, blocks, completes, and retries background database maintenance work
- auth service registers principals, grants scoped capabilities, issues a token, and logs allow/deny decisions
- wire service normalizes SQL, KV, object, blob, and admin frames and rejects a mutating frame without idempotency
- API service classifies user-facing requests and logs accepted/rejected gateway decisions
- gateway service routes accepted SQL, KV, object, and blob requests through catalog placement bindings and rejects unauthorized admin work
- consistency service confirms strict-quorum labels for SQL, KV, and object metadata, allows availability-first blob payload work, and rejects relational downgrade attempts
- quota service admits namespace SQL, KV, object, and blob reservations and rejects an oversized blob reservation
- RPC service validates internal service calls across gateway, SQL, object, blob, storage, and background roles before fabric work starts
- transaction coordinator service demonstrates read-version capture, prepare, commit-version allocation, and stale overlapping conflict rejection
- isolation service demonstrates strict serializable read/write range validation and rejects a stale read
- watch service registers an ordered prefix subscription, delivers update/delete changes, acknowledges the cursor, and resumes without replay
- range service splits the ordered keyspace and plans SSTable compaction
- compaction service merges overlapping table runs, drops tombstoned keys, and emits an output manifest
- stats service publishes table estimates and histograms used by the physical planner
- constraints service validates relational write admission before SQL session binding
- row codec service encodes and decodes schema-versioned relational row bytes, applies default/generated columns, and rejects malformed physical rows before write planning
- relcommit service audits insert/update/delete row-plus-index commit groups and rejects a partial transaction intent before page/WAL work
- WAL service rejects page flush before redo durability and allows it after sync advances the durable LSN
- buffer service pins relational pages, marks dirty frames, rejects full pinned pools, flushes, and evicts unpinned pages
- session service binds SQL text to catalog metadata, auth capability, strict consistency, and transaction shape before execution
- prepared service demonstrates named statement preparation, typed binding, schema invalidation, deallocation, eviction, and rejection paths
- portal service demonstrates Parse/Bind/Describe/Execute/Close-shaped client lifecycle, deadline rejection, cancellation, and cleanup around prepared statements
- primary service demonstrates primary-key insert, point lookup, update, duplicate rejection, delete, and missing lookup before secondary indexes run
- index service plans secondary-index rebuild, insert, update, delete, and lookup work before SQL execution
- query service demonstrates schema-validated projection, filtering, limit, and row execution
- planner service lowers logical query plans into primary-key, secondary-index, range-scan, cache-lookup, and stream-read physical operators
- SQL pipeline service composes session binding, token authorization, logical planning, physical planning, physical execution, row materialization, and invalid projection rejection
- relational/object/blob services call real database-personality crates over `rsf-protocol`
- erasure service maps a cold object body into data/parity shards and verifies degraded reconstruction
- manifest service publishes relational page, relational WAL, KV SSTable, object part, blob chunk, and snapshot-root extent records, then rejects overlapping ranges
- provider service advertises object/blob chunk locations, selects locality-aware healthy providers, reports degraded provider quorum, and rejects unknown content
- stream service chunks object/blob transfer plans, exercises flow-control backpressure, assigns parallel read lanes, and verifies chunk hashes
- cache service admits hot SQL, KV, object, and blob keys, records version-aware hits/misses, evicts bounded unpinned entries, and emits read-ahead hints
- superblock service validates a local boot record, verifies required database roots, advances generation at the latest committed checkpoint, and rejects malformed root inventory
- snapshot service freezes catalog, transaction-log, KV, SQL, object, and blob roots at one checkpoint before backup
- backup service validates a full-database snapshot manifest and restore plan
- recovery service plans database restart actions across catalog, transaction-log, KV, SQL, object, and blob roots, then rejects corrupted required-root recovery
- repair service calls real repair-planner logic over simulated scrub inventory
- rebalance service plans controlled replica movement with safety checks and per-move placement-policy proofs
- retention service evaluates lifecycle policy over simulated object/blob inventory
- observability service summarizes typed metrics, audit events, and service health
- invariant service evaluates named safety and architecture checks over the final run summary
- requirements service audits the final run against product requirements and labels each family as structural, simulated, design-target, or missing evidence
- five runnable architecture packages

Reference:

- [End-To-End POC](./poc.md)

### rsf-storage

Owns local durable storage primitives.

Current scope:

- append-only segment records
- magic header
- payload length
- record checksum
- fsync on append
- recovery in order
- torn-tail truncation
- checksum-corruption rejection
- typed WAL records with placement-group ID, epoch, log index, kind, hash chain, payload hash, and record hash
- per-placement-group recovery logs and tail summaries
- distinct commit records and recovered committed-prefix extraction
- cheap placement-group prefix comparison without payload scans
- manifest extent coverage proof against explicit required placement-group prefixes

Reference:

- [Storage Engine Design](./storage-engine-design.md)

### rsfd

Owns the first daemon binary.

Current scope:

- boots a dev scaffold
- wires `rsf-core` and `rsf-sim`

Next scope:

- parse `rsfd --dev --node <id> --cluster <name>`
- start simulated three-node dev cluster
- expose a small admin/status command

## Deferred Crates

Add these only when the implementation forces them:

- `rsf-net`: QUIC/runtime transport.

Do not split crates before the module boundaries become real. The first job is correctness, not a pretty crate graph.
