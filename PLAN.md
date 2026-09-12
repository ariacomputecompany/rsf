# RSF Full Implementation Plan

Date: 2026-09-11

## Commitment

RSF is not a demo, not a storage-fabric side project, and not a staged toy. It is a full database implementation from the beginning.

The target is one full distributed database implementation built correctly from top to bottom:

- relational database
- ordered KV database
- object store
- blob store
- append/log surface
- future block/file-compatible surfaces
- strict metadata and transaction core
- repairable, verifiable, content-addressed large-data plane
- complete recovery, backup, observability, and deterministic verification

We are not choosing scope reduction as an architecture strategy. We are choosing a cohesive design that absorbs the best ideas from all five POC implementations without letting their trade-offs leak into the wrong layer.

## Final Architecture Choice

The custom RSF implementation uses **Universal VSR-RADOS** as the spine, then incorporates the best ideas from the other four profiles.

```text
Universal VSR-RADOS placement and metadata spine
+ FoundationDB-style transaction/KV discipline
+ Aurora/Socrates page/WAL relational path
+ Dual-mode CP/AP consistency boundaries
+ Geo-local WPaxos ownership/locality strategy
```

This is one architecture, not five modes.

The central split is:

- **Mutable truth:** strict replicated metadata, catalog, schema, transactions, object heads, manifests, placement maps, recovery roots.
- **Immutable data:** content-addressed chunks, extents, object bodies, blob payloads, large SQL values, backups, snapshots, SSTables, erasure shards.

Mutable truth is CP by default. Immutable/verifiable data can use availability-first placement, caching, and repair policies without corrupting database state.

The developer-facing promise is just as important as the internals:

```text
install binary
join server
apply policy
RSF absorbs the distributed-systems machinery
```

Scaling from one server to many servers or many regions must feel like changing database policy, not hand-building a distributed system.

## Distillation From The Five POCs

### Universal VSR-RADOS: The Spine

Keep:

- RADOS-style placement groups as the physical distribution and repair unit.
- VSR-style strict metadata cells for mutable truth.
- Content-addressed immutable chunks for large payloads.
- Ordered MVCC KV as a central strict substrate.
- Object manifests and blob chunks as first-class database personalities.
- One-binary deployment, cluster admission, role assignment, and durable volumes.

Solve its risks with the other profiles:

- Risk: generic placement groups could become too low-level for database semantics.
- Solution: FoundationDB-style transaction layers and Aurora/Socrates relational adapters sit above the fabric.
- Risk: local quorum behavior could leave global latency on the table.
- Solution: WPaxos-style locality and movable ownership are built into placement policy.
- Risk: strict consensus everywhere could punish immutable payload traffic.
- Solution: Dual-mode consistency rules allow AP behavior only where verification makes it safe.

### FoundationDB Over Extent Fabric: Transaction Discipline

Keep:

- Ordered transactional KV as an authoritative internal layer.
- Tuple-like key encoding for catalog, SQL rows, indexes, object metadata, and system state.
- Sequencer/proxy/resolver/storage separation as an internal role model.
- Conflict-range validation for strict serializability.
- Immutable extents below transactionally committed metadata.

Solve its risks with the other profiles:

- Risk: becoming only a distributed KV with layers.
- Solution: Universal VSR-RADOS owns placement, repair, chunks, object/blob, and cluster operations as native concerns.
- Risk: SQL over KV alone may not satisfy long-term RDBMS compatibility.
- Solution: Aurora/Socrates page/WAL path is a full parallel relational physical layout.
- Risk: large object/blob bodies do not belong in the transactional hot path.
- Solution: content-addressed chunk and manifest layers keep bodies out of mutable truth.

### Aurora/Socrates Relational-First Fabric: Native Relational Physics

Keep:

- Page/WAL-native relational storage path.
- WAL-before-page-flush gate.
- LSN validation, redo replay, checkpointing, buffer residency, dirty frame accounting.
- Page/log separation for relational workloads that want RDBMS-style physical behavior.
- Protection-group thinking for relational pages and logs.

Solve its risks with the other profiles:

- Risk: SQL-first architecture could swallow object/blob/KV into second-class status.
- Solution: Universal VSR-RADOS and FoundationDB-style KV remain equal substrate pieces.
- Risk: page/WAL path can be more complex than needed for simple distributed SQL.
- Solution: ordered KV relational layout also exists, and the catalog can choose physical layout per table/profile.
- Risk: object and blob APIs become awkward on page infrastructure.
- Solution: object/blob stay manifest/chunk-native.

### Dual-Mode CP/AP Fabric: Consistency Honesty

Keep:

- CP-only catalog, schema, transactions, relational state, object heads, manifests, placement maps, and recovery roots.
- AP-friendly immutable payload placement, edge caching, and replicated blob/object bodies.
- Consistency labels at wire, gateway, RPC, manifest, and storage boundaries.
- Hard rejection of attempts to route mutable truth through availability-first paths.

Solve its risks with the other profiles:

- Risk: CP/AP split can become user-visible confusion.
- Solution: consistency mode is a policy contract, not a casual client toggle.
- Risk: AP reconciliation can corrupt semantics if applied too broadly.
- Solution: AP is restricted to immutable, digest-verified, reconstructable data.
- Risk: edge behavior can hide stale state.
- Solution: strict object heads and catalog roots remain CP; caches are invalidated or version-checked.

### Geo-Local WPaxos Fabric: Avant-Garde Locality

Keep:

- Movable ownership toward active writers.
- Region-aware leader and owner placement.
- Geo-local ordered ranges.
- Object metadata heads owned near write hot spots.
- Global immutable chunk cache.
- WAN-aware routing policy.

Solve its risks with the other profiles:

- Risk: locality-adaptive consensus is complex and easy to get wrong.
- Solution: ownership transfer must preserve the same strict metadata-cell and transaction invariants.
- Risk: WAN optimizations can obscure correctness.
- Solution: every ownership change is epoch/versioned, observable, replayable, and backed by Fozzy scenarios.
- Risk: global behavior can bloat the local implementation.
- Solution: locality is a placement and ownership policy over the same core contracts, not a separate product.

## Unified System Model

### Client Surfaces

- SQL wire protocol and SQL sessions.
- Ordered KV API.
- Object API with buckets, multipart upload, versions, manifests, and strict visible heads.
- Blob API with content-addressed immutable payloads.
- Admin API for admission, placement, repair, backup, restore, and observability.
- Append/log API where it naturally falls out of the protocol.

### Request Boundary

Every request passes through:

- wire normalization
- API classification
- auth
- quota
- idempotency
- gateway routing
- consistency classification
- internal RPC envelope
- trace context

No surface bypasses these controls.

### Developer Control Plane

The control plane must make hard topology changes feel like simple database settings.

Developers should be able to express intent with compact commands:

```text
rsfd init --cluster myapp
rsfd join --token <token> --region fsn1
rsf policy set replication.factor=3
rsf policy add-region ash --mode read-replica
rsf policy set region.ash.writes=enabled --ownership movable
rsf policy set objects.payloads=availability-first
rsf policy set sql.consistency=strict
rsf status
```

Those commands are not cosmetic wrappers. They are the product surface for distributed systems.

The developer makes explicit product choices:

- add capacity
- add redundancy
- add a region
- enable local reads
- enable region-local writes
- choose strict global writes
- mark immutable payloads as availability-first
- choose table/object/blob placement profiles
- change replication and erasure policies

RSF translates those choices into:

- node admission
- identity verification
- learner sync
- metadata-cell reconfiguration
- placement-map updates
- replica movement
- range ownership movement
- object-head ownership movement
- chunk copy and repair jobs
- routing-table updates
- cache invalidation
- snapshot/root protection
- consistency-mode enforcement
- operator-visible progress

The rule is: explicit choice, automatic execution.

No developer should manually calculate quorum membership, copy chunks, rebalance placement groups, rewire app connection strings, script WAL shipping, coordinate object metadata, or write repair jobs to scale the database.

### Policy Model

Policy is durable database metadata, not ad hoc config.

Policies cover:

- cluster topology
- regions
- failure domains
- replication factor
- erasure profile
- placement class
- consistency mode
- ownership movement
- table physical layout
- object metadata locality
- blob/chunk placement
- cache behavior
- backup and retention
- rolling upgrade behavior

Policy changes are transactions over strict metadata. A policy change either commits and is reconciled by RSF, or it fails with a typed reason.

Every policy has:

- validation
- safety preflight
- dry-run/explain output
- rollout plan
- progress events
- rollback or compensating action where possible
- audit record

Policy should be declarative:

```text
desired state: 3 replicas across fsn1, nbg1, hel1
actual state: 2 clean replicas, 1 learner syncing
next action: copy placement group pg-42 to node-7
```

The app keeps talking to the same database endpoint while RSF reconciles actual state toward desired state.

### Database Services

The implementation includes real services for:

- tenancy
- catalog
- schema
- migrations
- background jobs
- sessions
- prepared statements
- portals
- transactions
- isolation validation
- constraints
- indexing
- query planning
- query execution
- object metadata
- multipart coordination
- retention
- backup
- restore
- recovery
- repair
- rebalance
- observability

### Data Personalities

RSF has multiple personalities over one substrate:

- relational
- ordered KV
- object
- blob
- append log

They share placement, recovery, authentication, observability, and verification machinery. They do not collapse into one physical shape.

### Physical Layouts

The implementation supports multiple physical layouts as first-class citizens:

- ordered KV ranges and SSTables
- relational row/index KV layout
- relational page/WAL layout
- object multipart manifests and parts
- blob chunks
- SQL large-value chunks
- snapshot blocks
- backup blocks
- erasure-coded cold-object shards

Every physical record has:

- surface kind
- layout kind
- namespace/database identity
- object/range/page/chunk identity
- placement-group identity
- epoch/version/LSN where applicable
- checksum or digest
- recovery-root relationship

### Storage Fabric

The storage fabric includes:

- placement groups
- VSR metadata cells
- durable segment logs
- manifest records
- content-addressed chunk store
- checkpoint roots
- replica scrub
- repair
- rebalance
- provider discovery
- erasure coding
- hot-read cache
- locality-aware placement

The storage fabric is not exposed as the product. It is the internal durability and placement core for the database.

## Full Workstreams

These are not demo phases. They are implementation workstreams that converge into one complete database. Work can proceed in parallel where contracts are stable, but every workstream is required for the correct overall system.

### Core Types And Contracts

Owns:

- node IDs, epochs, views, terms, op numbers
- commit versions and timestamps
- placement-group IDs
- namespace, database, table, bucket, object, chunk identities
- range descriptors
- consistency labels
- layout kinds
- error taxonomy
- trace IDs and operation IDs

Acceptance:

- Every crate uses authoritative domain types from `rsf-core`.
- No service invents local stringly typed identities.
- Error codes carry enough context for logs, retries, and operator action.

### Durable Storage And WAL

Owns:

- segment file format
- record framing
- checksums
- fsync policy
- partial-write detection
- replay scanning
- truncation
- corruption quarantine
- checkpoint compaction

Borrowed value:

- Universal VSR-RADOS placement-group tails.
- Aurora/Socrates WAL discipline.
- FoundationDB log/storage separation.

Acceptance:

- Crash/restart survives partial records, torn writes, and unclean shutdown.
- Replay can distinguish committed, prepared, and garbage suffixes.
- Corruption is localized and never silently accepted.

### VSR Metadata Cells

Owns:

- primary/backup roles
- view changes
- prepare and prepare-ok
- commit progression
- client request idempotency
- stale primary rejection
- durable metadata-cell logs
- recovery from segment state

Borrowed value:

- Universal VSR-RADOS strict metadata cells.
- TigerBeetle-style VSR rigor from the research base.
- Dual-mode CP rule: truth is strict.

Acceptance:

- Metadata records commit through quorum.
- View changes preserve safety.
- Restarted replicas recover committed state.
- Stale primaries cannot mutate truth.

### Membership, Admission, Placement

Owns:

- one-binary node admission
- invite/bootstrap identity
- learner sync
- promotion
- voter removal
- quorum intersection proofs
- placement maps
- failure-domain policy
- anti-affinity
- region/locality hints

Borrowed value:

- Universal VSR-RADOS one-binary join and placement groups.
- RADOS/Ceph repair and placement thinking.
- WPaxos locality annotations.

Acceptance:

- New nodes join safely and become capacity.
- Unsafe membership changes are rejected.
- Placement maps are versioned, durable, and recoverable.
- Locality hints never override quorum safety.

### Ordered MVCC KV

Owns:

- ordered key encoding
- MVCC versions
- snapshots
- point reads
- range scans
- atomic batches
- tombstones
- compaction inputs
- watch/change streams
- SSTable/range layout

Borrowed value:

- FoundationDB tuple and transaction layer discipline.
- Universal VSR-RADOS physical placement below ranges.
- Dual-mode strict consistency labeling.

Acceptance:

- KV supports snapshot reads, range scans, batch writes, tombstones, watches, and recovery.
- KV writes commit through the strict protocol path.
- Range descriptors and compaction metadata are recoverable.

### Transaction Coordinator And Isolation

Owns:

- commit-version allocation
- read conflict ranges
- write conflict ranges
- serializable validation
- transaction retries
- idempotent commit handling
- session overlays
- savepoints
- rollback and rollback-to-savepoint
- prepared transaction hooks where needed

Borrowed value:

- FoundationDB resolver/sequencer model.
- POC transaction, isolation, session transaction, and SQL transaction contracts.
- Dual-mode rejection of AP mutable writes.

Acceptance:

- Strict serializable anomalies are rejected.
- Non-conflicting transactions can proceed concurrently.
- Transaction outcomes are idempotent under retries.
- Session read-your-writes is preserved.

### Relational Engine

Owns:

- catalog-backed schemas
- row codecs
- tuple visibility
- primary indexes
- secondary indexes
- constraints
- deferred constraints
- write planning
- relational commit groups
- materialization
- logical query planning
- physical query planning
- physical execution
- SQL DML
- SQL transactions
- prepared statements
- portals
- vacuum
- stats

Borrowed value:

- FoundationDB SQL-over-ordered-KV shape.
- Aurora/Socrates page/WAL physical path.
- Universal VSR-RADOS placement and recovery roots.

Acceptance:

- Tables, rows, indexes, constraints, reads, writes, updates, deletes, and transactions are real.
- Row and index mutations commit atomically.
- Relational data can use ordered-KV layout or page/WAL layout according to table/profile policy.
- Vacuum and tuple visibility preserve snapshot correctness.

### Page/WAL Relational Path

Owns:

- page IDs
- LSNs
- redo records
- durable-LSN gate
- WAL-before-page-flush enforcement
- buffer residency
- dirty frame accounting
- page replay
- checkpoint summaries

Borrowed value:

- Aurora/Socrates relational-first architecture.
- Current POC `rsf-page`, `rsf-wal`, `rsf-buffer`.

Acceptance:

- Dirty pages cannot flush ahead of durable redo.
- Page replay is deterministic.
- Page/WAL tables can coexist with ordered-KV tables.
- Relational physical layout choice is explicit, not accidental.

### Object Metadata And Object Store

Owns:

- bucket catalog
- object keys
- multipart draft state
- part manifests
- visible object heads
- versioning
- strict manifest commit
- delete markers
- retention hooks
- object reads over provider/chunk layout

Borrowed value:

- Universal VSR-RADOS strict metadata plus multipart manifests.
- FoundationDB ordered metadata keys.
- WPaxos writer-local object-head ownership.
- Dual-mode AP-safe immutable bodies.

Acceptance:

- Object bodies are invisible until strict metadata commit.
- Object heads are CP.
- Object bodies can be cached, repaired, and replicated through digest-verified immutable chunks.

### Blob And Chunk Store

Owns:

- durable chunk files
- real digest implementation
- dedupe
- pinning
- verification reads
- quarantine
- streaming write/read
- provider discovery
- erasure-coded cold layout
- SQL large-value storage
- snapshot and backup blocks

Borrowed value:

- Universal VSR-RADOS content-addressed chunks.
- FoundationDB immutable extents.
- Dual-mode AP payload placement.
- WPaxos global immutable chunk cache.

Acceptance:

- Every chunk verifies by digest.
- Duplicate content dedupes safely.
- Missing/corrupt chunks produce repair actions.
- Large SQL values, object bodies, blob payloads, snapshots, and backups share the chunk infrastructure without losing surface identity.

### Manifest, Snapshot, Backup, Recovery

Owns:

- format-independent extent manifests
- checkpoint roots
- superblock roots
- snapshot barriers
- backup manifests
- restore validation
- restart replay
- required-root verification
- corruption rejection

Borrowed value:

- Universal VSR-RADOS manifest coverage.
- Aurora/Socrates checkpoint/redo discipline.
- FoundationDB log/storage recovery split.

Acceptance:

- The whole database can restart from roots.
- Backups cover catalog, transaction log, KV, SQL, object, blob, chunk index, and manifests.
- Restore refuses incomplete or corrupt roots.
- Snapshot semantics are explicit and testable.

### Repair, Rebalance, Erasure, Cache

Owns:

- replica scrub
- digest comparison
- missing replica copy
- corrupt replica quarantine
- under-replication repair
- placement-group movement
- rebalance policy proofs
- erasure coding
- degraded reconstruction
- hot-read cache
- version-aware freshness
- read-ahead

Borrowed value:

- RADOS/Ceph-style operational repair.
- Universal VSR-RADOS placement groups.
- Dual-mode immutable payload availability.
- WPaxos locality-aware provider selection.

Acceptance:

- Repair can act on every physical layout.
- Rebalance preserves safety.
- Cache never serves stale strict mutable truth.
- Erasure reconstruction is verified by digest and manifest.

### Consistency, Locality, CP/AP

Owns:

- consistency labels
- strict quorum paths
- availability-first payload paths
- AP-safe cache and replication rules
- ownership movement
- region-aware routing
- WAN-aware placement
- locality policy

Borrowed value:

- Dual-mode CP/AP consistency boundaries.
- WPaxos locality-adaptive ownership.
- Universal VSR-RADOS strict cells.

Acceptance:

- Mutable truth is always CP.
- AP mode only applies to immutable, verifiable, reconstructable data.
- Ownership movement is epoch/versioned and replayable.
- Locality improves latency without changing semantics.

### Deployment, Operations, Observability

Owns:

- package manifest
- one-binary role assignment
- developer-facing policy commands
- durable policy catalog
- policy validation and dry-run
- policy reconciliation engine
- config
- durable volume mapping
- readiness probes
- rolling upgrade
- mixed-version compatibility gates
- health scoring
- demotion/quarantine
- metrics
- audit events
- trace rendering
- operator summaries

Borrowed value:

- Universal VSR-RADOS one-binary deployment.
- POC deployment, upgrade, health, observability services.
- Dual-mode explicit mode reporting.

Acceptance:

- Operators can see what role every node plays.
- Developers can scale topology with policy commands rather than manual distributed-systems procedures.
- Policy changes are transactional, audited, explainable, and reconciled automatically.
- Upgrades are compatibility-gated.
- Health actions are explainable.
- Every important state transition is traceable.

### Verification Runtime

Owns:

- deterministic simulator
- network partitions
- message delay/drop
- crash/restart
- disk faults
- Fozzy scenarios
- trace recording
- trace verification
- replay
- shrinking
- invariant checks
- requirement checks
- testability reports

Borrowed value:

- Existing POC proof surfaces.
- Fozzy deterministic workflow.
- Failure-drill and testability contracts.

Acceptance:

- Every workstream has deterministic failure scenarios.
- Every safety invariant has a machine-checkable contract.
- Every production feature has replay evidence.
- No “it logged nicely once” evidence is accepted as correctness.

## Integrated Trade-Off Strategy

We are deliberately trying to have our cake and eat it too, but only by putting each benefit behind the right contract.

| Tension | We Want | Risk | Contract That Makes It Safe |
| --- | --- | --- | --- |
| Unified substrate vs workload-specific physics | One system for SQL, KV, object, blob | One bad generic layout | Manifest records surface and layout kind explicitly |
| Ordered KV vs page/WAL | Clean transactions and RDBMS-native physical behavior | Two relational paths diverge | Catalog-owned layout policy and shared transaction/recovery roots |
| CP truth vs AP latency | Strict correctness and low-latency payloads | AP corrupts mutable state | AP only for immutable digest-verified payload data |
| Local quorum vs global writes | Simple local safety and geo-local latency | WAN ownership bugs | Epoch/versioned ownership movement with replayed invariants |
| Content addressing vs database identity | Dedupe and repair without losing semantics | Chunks become anonymous garbage | Every chunk is reachable through typed manifests and roots |
| Aggressive caching vs strict freshness | Fast reads without stale truth | Cache lies | Version-aware cache admission and invalidation |
| Full feature scope vs correctness | Complete database from the beginning | Shallow fake surfaces | Every surface owns real state, recovery roots, and failure tests |
| Simple DX vs distributed-systems honesty | Add nodes/regions with flags | Flags hide dangerous topology changes | Policy preflight, explicit modes, automatic reconciliation, visible progress |

## Non-Negotiable Invariants

- Mutable database truth is strict by default.
- Availability-first behavior is never implicit.
- AP paths only handle immutable, verifiable, reconstructable data.
- Every committed user-visible object has a recovery root.
- Every large payload has digest verification.
- Every physical record has a surface kind and layout kind.
- Every node membership change preserves quorum safety.
- Every stale writer has a fence rejection path.
- Every transaction declares enough conflict information to validate isolation.
- Every relational row/index mutation commits atomically.
- Every object body is invisible until its metadata head commits.
- Every snapshot and backup root can be verified before restore.
- Every ownership transfer is epoch/versioned and replayable.
- Every repair action is traceable to manifest/checksum/root evidence.
- Every topology change is expressed as durable policy and reconciled by the system.
- Every policy change has preflight validation and audit evidence.
- Every major state machine has deterministic replay tests.

## Build Policy

This is not a sequence of demos. The implementation can be scheduled in work packages, but every package must be production-shaped.

Rules:

- No throwaway adapters.
- No fake repositories.
- No placeholder consensus paths.
- No mocked business behavior in production crates.
- No temporary storage formats.
- No untyped IDs crossing service boundaries.
- No AP behavior for mutable truth.
- No manual topology surgery as the normal developer experience.
- No policy command may hide a consistency or latency trade-off.
- No feature is considered real until recovery and failure behavior exist.
- No surface is considered real until it has observability and invariant coverage.

When an implementation has to be incomplete during active development, it must fail closed with a typed error. It must not pretend to work.

## Full-System Acceptance Bar

The implementation is not real until it can do all of this in one integrated cluster:

```text
admit nodes
assign roles
accept policy changes
validate policy safety
reconcile desired topology
form strict metadata cells
write durable logs
commit placement metadata
serve ordered MVCC KV
commit strict serializable transactions
serve SQL tables and indexes
serve object metadata and object bodies
serve blob chunks
verify manifests
stream payloads
cache hot reads safely
repair missing/corrupt replicas
rebalance placement groups
snapshot the whole database
backup the whole database
restore the whole database
restart after crash
reject stale writers
reject unsafe membership changes
reject AP mutable writes
explain policy trade-offs
show rollout progress
trace every major action
replay deterministic failure scenarios
```

## Verification Bar

Every workstream must ship with:

- unit tests for local state machines
- deterministic integration scenarios
- Fozzy strict deterministic run
- host-backed trace recording
- trace verification
- replay
- CI report
- invariant checks
- failure-path tests
- recovery tests
- observability evidence

Standard command shape:

```text
cargo fmt --all --check
cargo test --quiet
fozzy doctor --deep --scenario <scenario> --runs 5 --seed <seed> --json
fozzy test --det --strict-verify <scenario> --json
fozzy run <scenario> --det --host-backends --record <trace> --json
fozzy trace verify <trace> --strict --json
fozzy replay <trace> --json
fozzy ci <trace> --json
```

## Operating Principle

We do not merge five architectures into a soup. We distill them into one system with hard ownership boundaries:

- Universal VSR-RADOS gives the spine.
- FoundationDB gives transaction/KV discipline.
- Aurora/Socrates gives relational physical completeness.
- Dual-mode CP/AP gives consistency honesty.
- Geo-local WPaxos gives the locality and avant-garde edge.

The goal is a database that feels inevitable after you see it: strict where truth requires strictness, flexible where data is immutable, relational where SQL needs native behavior, content-addressed where large payloads need repairability, and deterministic enough that we can trust it under failure.
