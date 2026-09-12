# RSF Product Requirements

Date: 2026-09-10

## Product Definition

RSF is a full distributed database. It is not only a storage fabric, object store, consensus library, or replication protocol.

The internal storage fabric must support the full database, but the delivered system must include structural implementations for:

- SQL/relational data
- relational page and redo-log path
- relational WAL durability gate before page flush
- relational buffer-pool residency and dirty-page writeback
- local superblock and boot-root inventory
- format-independent extent manifests for relational, KV, object, blob, and snapshot records
- transactional metadata
- ordered key-value storage
- object storage
- blob storage
- VSR-style quorum replication and view changes for mutable truth
- epoch-based membership with learner promotion and safe voter removal
- deployment package manifests
- rolling upgrade compatibility and rollout planning
- protocol wire adapters
- object/blob provider discovery
- internal service RPC envelopes
- query execution
- recovery
- repair
- observability
- named invariant checking against the full-database POC run
- deterministic testing

The POC also emits a product-requirement audit. Each audited requirement family
is labeled as structural evidence, simulated evidence, design-target evidence,
or missing evidence, so the project does not quietly confuse a storage substrate
with the full database product.

## Non-Negotiable Requirements

1. A server can join by running one binary with bootstrap credentials or an invite token.
2. A joined server automatically becomes usable cluster capacity after validation, sync, and promotion.
3. The system supports low-latency production database workloads.
4. The system supports relational, object, and blob workloads without forcing them through the wrong physical layout.
5. The system exposes strict consistency where promised and never disguises async replication as strict global consistency.
6. No acknowledged write may be lost under the declared crash-fault model.
7. Disk corruption, partial writes, stale primaries, slow replicas, and network partitions are first-class design cases.
8. Large payloads are chunked, checksummed, content-addressed, and independently repairable.
9. Hot metadata and relational write paths use quorum replication, not eventual reconciliation.
10. The implementation must be simulation-testable before it is trusted.

## Required User-Facing Surfaces

### Relational

- SQL parser and wire/API surface eventually
- catalog
- tables
- indexes
- transactions
- MVCC snapshots
- query planner
- execution engine
- page/WAL or range/SSTable physical adapter

### Ordered KV

- ordered byte keys
- range scans
- transactional writes
- watches/change streams
- versioned reads

### Object Store

- buckets/namespaces
- object metadata
- multipart uploads
- object versioning
- strict mutable object-head commits before visibility
- metadata rejection paths for unknown uploads and malformed manifests
- lifecycle/retention policy
- erasure-coded cold objects
- replicated hot metadata

### Blob Store

- immutable chunks
- content IDs
- dedupe
- streaming reads/writes
- repair by hash
- retention pins

## Internal System Requirements

### Storage Fabric

- placement groups
- deterministic object/extent mapping
- format-independent extent manifests
- primary/secondary acting sets
- replicated WAL
- content-addressed chunk store for SQL large values, object bodies, blob payloads, snapshots, SSTables, and backups
- local superblock boot records
- local checkpoint compaction for full database roots and segment reclamation
- checksums and hash chains
- snapshots
- copy-on-write manifests
- scrubbing and repair
- invariant checks for commit tails, stale primaries, root coverage, and multi-surface layout

### Consensus and Transactions

- VSR/Raft-style replicated cells for mutable truth
- epoch-based membership
- learner promotion
- idempotent client requests
- monotonic commit versions
- serializable transactions for strict layers
- strict session read-your-writes overlays before commit
- composed SQL DML writes must bind, authorize, validate constraints, commit through the transaction coordinator, pass strict isolation, and apply row/index physical mutations atomically
- clear AP mode boundaries for optional availability-first layers
- consistency-mode policy that rejects attempts to route strict mutable truth through availability-first semantics

### Cluster Management

- node identity
- capability-based admission
- failure-domain awareness
- health scoring
- automatic rebalancing
- slow replica demotion
- controlled shard/placement-group movement

## Performance Requirements

These are first structural workload-shape targets in the POC, not yet measured
production benchmark claims:

- Single-region strict writes should complete in one quorum replication path plus local fsync batching.
- Read paths must avoid central routers for hot data.
- Relational page reads must not route through S3/object semantics.
- Large object/blob reads must stripe and fetch in parallel.
- Small hot objects must use replication rather than erasure coding by default.
- Cold large objects may use erasure coding.
- Global strict writes are opt-in because WAN RTT is physically unavoidable.

## Trust Model

Initial implementation should assume crash faults plus disk corruption detection. Byzantine consensus, public untrusted capacity, token incentives, and adversarial proofs are later layers.

The first correctness bar is already high enough:

- crash
- restart
- torn write
- stale view
- minority partition
- asymmetric partition
- slow disk
- corrupted segment
- duplicate client request
- partial snapshot transfer
- node join during reconfiguration

## Out Of Scope For The First Prototype

- public decentralized storage market
- token economics
- Byzantine consensus
- full SQL optimizer
- multi-region external consistency
- arbitrary cross-shard serializable transactions
- POSIX filesystem semantics

These are not rejected. They are sequenced after the correctness core exists.
