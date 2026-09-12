# RSF Full Database Architecture

Date: 2026-09-10

## Core Correction

RSF is a database. The storage fabric is the protocol core inside the database.

The architecture should be layered like this:

```text
L8  User APIs
    protocol wire adapters, SQL sessions, KV, object/blob API, admin API

L7  Database gateway
    auth decision validation, idempotency/retry envelope, request budgets, routing, consistency labels, placement bindings

L6  Database personalities
    relational engine, ordered KV engine, object store, blob store, append log

L5  Logical services
    tenancy, catalog, schema migration, background jobs, indexes, transactions, snapshots, recovery, quotas, object metadata, retention, auth

L4  Execution services
    SQL session binder, SQL planner/executor, KV transaction coordinator, object multipart coordinator, streaming transfer, hot-read cache

L3  Consistency services
    VSR cells, transaction timestamp/version service, serializable isolation validator, conflict resolver, leases/fences

L2  Layout adapters
    relational pages/WAL, KV ranges/SSTables, object manifests/parts, blob chunks, format-independent extent manifests

L1  Storage fabric protocol
    placement groups, acting sets, replicated log, snapshots, repair, scrubbing

L0  Local engine
    superblocks, segment files, WAL records, checksums, hash chains, content chunks, indexes

Runtime
    QUIC transport, io_uring/kqueue/epoll abstraction, deterministic simulator, metrics
```

## Database Personalities

### Relational Engine

The relational engine should not be bolted onto an object store. It needs a native path.

Initial architecture:

- catalog stored in strict metadata cells
- membership cells advance epochs through learner sync, learner promotion, and safe voter removal before placement changes become authoritative
- VSR metadata cells prepare records, collect quorum acknowledgements, commit the prefix, and reject stale primaries after view change
- tenant/database descriptors own namespaces and enabled SQL, KV, object, and blob surfaces before catalog publication
- database gateway validates API decisions against original requests, applies budgets, selects handler boundaries, labels consistency, and routes through catalog placement bindings
- wire adapters normalize SQL, KV, object, blob, and admin protocol frames before API classification
- idempotency and retry envelopes classify first-seen requests, replay committed responses, reject conflicting key reuse, and schedule bounded retries before work reaches database personalities
- background jobs lease, retry, dependency-gate, and account for schema, index, statistics, compaction, backup, retention, and repair work
- quota admission reserves namespace/surface request, row, and byte budgets before deeper execution
- internal service RPC envelopes carry deadlines, retry class, trace IDs, body sizes, and service-role routing before work reaches storage fabric
- hot-read cache admits SQL, KV, object, and blob keys with version-aware freshness checks before falling back to deeper service execution
- SQL sessions bind text statements to catalog metadata, capabilities, consistency mode, and transaction shape before execution
- prepared SQL caches bind named statements to typed parameters and schema versions before execution, with invalidation, explicit deallocation, eviction, and rejection boundaries
- SQL portal lifecycles expose Parse/Bind/Describe/Execute/Close-shaped request flow with cancellation and deadline rejection before execution reaches planner/storage work
- relational constraint enforcement validates not-null, primary-key, unique, and reference rules before row mutations commit
- relational row codecs convert typed rows into schema-versioned physical bytes, apply default/generated column materialization, decode for read paths, and reject malformed physical rows before write planning
- relational tuple visibility owns MVCC tuple headers, update/delete version chains, and snapshot-visible physical row versions over encoded row bytes before durable page or KV storage work
- relational vacuum planning prunes obsolete tuple versions only after the oldest active snapshot no longer needs them, while retaining live/current versions and surfacing unsafe prune rejection paths
- relational row materialization decodes visible encoded tuple payloads into SQL-facing query rows, applies projections, and rejects table/projection mismatches before executor output leaves the relational engine
- deferred relational constraint enforcement records commit-time reference obligations and resolves them against transaction-local or committed referenced keys
- relational write planning orders constraint checks, row KV mutation, secondary-index maintenance, conflict-range declaration, and commit intent construction before execution
- relational commit-group auditing proves table-row mutations and secondary-index mutations are present together in one transaction intent, with row/identity conflict ranges and partial-group rejection
- relational page/redo adapters keep RDBMS pages on a native LSN-checked path instead of routing page work through object/blob semantics
- relational WAL gates reject dirty-page flushes until corresponding redo has reached a durable LSN
- relational buffer pools own page residency, pins, dirty tracking, flushes, and eviction separate from the generic hot-read cache
- schema planning publishes compatible descriptor/index/backfill rollout steps and rejects unsafe changes before visibility
- schema migration lifecycle execution records durable validation, descriptor barriers, backfill progress, index catch-up, resume points, completion, rollback, and rejected post-rollback advances
- primary indexes own row identity, deterministic primary-key encoding, point lookup, version replacement, delete, and duplicate-key rejection before secondary indexes are considered
- index maintenance plans primary/secondary index updates, rebuilds, lookup keys, and future online catch-up work as part of relational commits
- physical query planning lowers logical SELECT work into primary-key lookup, secondary-index lookup, range scan, cache lookup, and stream-read operator choices
- physical query execution consumes those operators through explicit cache, index, range, and stream stages before returning rows to SQL
- statistics publishing feeds optimizer table estimates, column histograms, and stale-stat refresh signals into physical planning
- provider discovery resolves immutable object/blob content IDs to healthy locality-aware providers before reads are scheduled
- streaming transfer splits object/blob payloads into bounded chunks, applies flow-control, assigns read lanes, and preserves hash-verification boundaries
- table/index data stored as ordered ranges or page extents
- compaction execution merges ordered table runs, preserves newest visible values, suppresses tombstoned keys, and emits verified output manifests
- format-independent extent manifests map relational pages/WAL, KV SSTables, object parts, blob chunks, and snapshot roots onto placement groups without collapsing those data models into one physical API
- WAL records replicated through quorum storage
- local superblocks anchor cluster identity, layout epoch, checkpoint version, and root inventory before recovery trusts local state
- MVCC versions mapped to commit versions
- strict serializable layers validate declared read/write ranges and reject stale reads rather than silently returning snapshot-isolation anomalies
- page servers or range servers cache hot data near compute
- query engine starts simple: logical validation, point lookup, insert, update, delete, primary index scan, physical access-path selection, and physical operator execution

The relational layer can evolve along two possible physical paths:

- **Page/WAL path:** closer to Aurora/Socrates, good for compatibility with traditional RDBMS engines.
- **Ordered KV path:** closer to FoundationDB/Cockroach/TiKV, good for clean distributed transactions and layered SQL.

The first RSF prototype should choose the ordered KV path for simplicity, while keeping page/WAL docs alive because RDBMS compatibility may become strategically important.

### Ordered KV Engine

The ordered KV engine is the minimal strict database substrate.

Responsibilities:

- encode logical records into ordered keys
- provide range reads
- maintain MVCC versions
- support atomic batches
- expose conflict ranges
- publish ordered change streams with resumable watch cursors
- feed SQL indexes and object metadata

### Object Store

The object store is a database personality over RSF, not the core itself.

Responsibilities:

- bucket metadata
- object keys
- multipart upload state
- object version pointers
- manifests from object key to chunk IDs
- lifecycle rules
- optional S3-compatible API later

Hot object metadata must be strict. Object bodies may be immutable chunks placed by policy.

### Blob Store

The blob store is the immutable data plane.

Responsibilities:

- chunking
- hash-addressing
- dedupe
- parallel read/write
- retention pins
- repair by digest
- optional DHT provider advertisements later

Blob chunks are ideal for:

- object bodies
- erasure data/parity shards for cold object bodies
- relational large values
- backups
- snapshots
- SSTables
- log archives

## Core Storage Fabric

The fabric has four key abstractions:

### Placement Group

A placement group is the unit of ownership, peering, repair, and rebalancing.

It contains:

- placement group ID
- epoch
- acting set
- primary node
- replication or erasure policy
- log tail
- object/extent set
- scrub state

### Extent

An extent is the physical unit used by layout adapters.

It may represent:

- a range segment
- a relational page group
- an object part
- a blob chunk
- a WAL segment
- a snapshot block
- a recovery replay span

### Mutable Head

A mutable head points to the current version of a namespace, object, range, table, manifest, or transaction state.

Mutable heads must be quorum-protected.

### Immutable Chunk

An immutable chunk is content-addressed and self-verifying.

Immutable chunks can be replicated, erasure-coded, cached, exported, or advertised through DHT/provider routing.

The current source path has a durable content-addressed chunk store for SQL large values, object bodies, blob payloads, snapshot blocks, SSTable blocks, and backup blocks. Chunk IDs are BLAKE3-backed, duplicate payloads collapse to the same content identity, pins survive restart, verified reads recheck on-disk bytes, and corruption is quarantined instead of returned as data.

### Local Checkpoint Compaction

A checkpoint is the durable root set from which the full database can restart: catalog, transaction log, KV, SQL, object, blob, and chunk-index state. Local compaction may reclaim old segment bytes only after required roots are retained and newer-than-checkpoint records remain protected.

The current source path validates checkpoint roots and compaction decisions before they can feed boot evidence. Checkpoint certificates bind root inventories, segment retention decisions, high-watermarks, and counter fields; superblock and recovery crates consume those roots through durable WAL-backed catalogs and fail closed on malformed or regressing histories.

## Node Roles

One binary can host many roles:

- storage daemon
- placement-group primary
- placement-group secondary
- VSR metadata replica
- learner
- witness
- range server
- page server
- blob provider
- query gateway
- object gateway
- transaction coordinator
- repair worker
- control-plane member

Small clusters can run all roles everywhere. Large clusters specialize roles by capacity and latency.

## Write Path

Relational write:

```text
client -> SQL/KV coordinator -> transaction check -> layout adapter -> placement group primary -> replica quorum -> commit version -> client
```

Object write:

```text
client -> multipart coordinator -> chunk store -> manifest write -> metadata quorum -> object version visible
```

Cold object layout:

```text
object body -> erasure planner -> data/parity shard manifest -> placement groups -> recover by threshold
```

Blob write:

```text
client -> chunker -> placement groups/provider set -> hash verification -> retention pin
```

## Read Path

Relational read:

```text
client -> planner/executor -> range/page server -> MVCC version -> rows
```

Object read:

```text
client -> metadata lookup -> manifest -> parallel chunk fetch -> hash verify -> stream
```

Blob read:

```text
client -> CID/chunk ID -> nearest providers -> hash verify -> stream
```

## Control Plane

The control plane owns:

- implementation package manifests
- node admission
- health scoring and demotion recommendations
- cluster map
- placement policies
- membership epochs
- schema/catalog root
- failure-domain inventory
- software-version compatibility
- rolling upgrade rollout plans
- feature gates
- per-role endpoints, volumes, and readiness probes

The control plane must be replicated with strict consensus. Data-plane reads and writes should not flow through it during normal operation.

## Safety Principle

Every layer must be explicit about whether it is:

- strict and quorum-protected
- bounded-stale
- async replicated
- eventually consistent
- immutable and verifiable

The system is allowed to offer different modes. It is not allowed to make those modes ambiguous.
