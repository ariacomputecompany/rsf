# RSF Protocol And Database Architecture Research

Date: 2026-09-10

## Reality

RSF is a full database system. Its storage fabric is the internal protocol substrate that lets the database expose relational, ordered KV, object, blob, log, block, and file-compatible layers.

The physical constraint is not “distributed databases are too hard.” The physical constraint is that every durable write must be witnessed by enough independent failure domains to satisfy the promised durability and consistency level. If those witnesses are far away, write latency includes WAN RTT. If they are nearby, global survivability either requires async replication or a larger quorum path.

The viable target is therefore not one protocol mode. It is a storage binary with several explicit placement, layout, and consistency modes:

- local quorum for hot low-latency writes
- regional or multi-AZ quorum for production durability
- multi-region quorum when external consistency matters more than latency
- async global replication for low-latency local writes plus global survivability
- content-addressed immutable storage for cheap distribution
- layout profiles for relational pages/WAL, ordered KV ranges, object parts, append logs, and cold blobs

Evidence: FoundationDB separates transaction processing, logging, and storage so reads and writes scale independently ([FoundationDB paper](https://www.foundationdb.org/files/fdb-paper.pdf), [FDB architecture docs](https://apple.github.io/foundationdb/architecture.html)). FoundationDB also explicitly decouples storage technology from data model/API through layers ([FDB layer concept](https://apple.github.io/foundationdb/layer-concept.html)). Ceph proves that one distributed object substrate can expose object, block, file, and custom interfaces through layers while storing the lower-level representation as RADOS objects ([Ceph architecture](https://docs.ceph.com/en/reef/architecture/)). Dynamo chose availability by using consistent hashing, object versioning, sloppy quorum, hinted handoff, and anti-entropy ([Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)). Spanner gets global external consistency with synchronous replication and TrueTime, but that guarantee is not free of clock and coordination costs ([Spanner paper](https://research.google.com/archive/spanner-osdi2012.pdf), [Cloud Spanner TrueTime docs](https://docs.cloud.google.com/spanner/docs/true-time-external-consistency)). IPFS/libp2p show how peer discovery and content routing can be decentralized, but provider liveness requires republishing and churn management ([libp2p DHT docs](https://libp2p.io/docs/dht/), [IPFS Kademlia DHT spec](https://specs.ipfs.tech/routing/kad-dht/), [IPFS CID docs](https://docs.ipfs.tech/concepts/content-addressing/)).

## Interpretation

The right invention is probably not “a blockchain database.” Blockchain gives decentralized ordering under adversarial trust, but pays in throughput, latency, and operational complexity. The target is a self-forming, high-performance distributed database whose internal fabric has optional decentralized membership, optional verifiability, and hard production-grade durability.

The minimum useful primitive is:

`binary + identity + discovery + placement + replicated log + local engine + repair + deterministic tests`

The product promise is:

Deploy one binary on a server, give it bootstrap identity or invite token, and it becomes capacity in a storage fabric. The system then assigns it roles: storage cell, log replica, page server, range server, object-part server, read replica, witness, repair peer, DHT router, cold blob provider, or transaction service.

For relational/object/blob independence, the core must not expose SQL, S3, filesystem, or block semantics directly. It should expose durable primitives that can be shaped into those frontends:

- append-only replicated logs
- immutable chunks/extents
- mutable quorum heads
- ordered metadata keys
- range scans
- compare-and-swap / transaction fences
- snapshots and cloneable manifests
- placement groups and failure-domain policy

## Contradictions

- “Low latency everywhere” conflicts with strong global writes. If a client in Tokyo must commit synchronously with replicas in Virginia and Frankfurt, the speed of light is on the critical path.
- “Fully decentralized” conflicts with predictable p99 latency unless the system controls admission, routing quality, stake/reputation, or failure-domain placement.
- “Any server can join” conflicts with correctness unless membership is controlled by a consensus/configuration layer or cryptographic capability system.
- “Storage receptacle” sounds passive, but production storage nodes must actively repair, compact, shed load, prove liveness, detect corruption, and participate in rebalancing.
- “Blockchain-like” verifiability is valuable for immutable history and untrusted providers, but global consensus for every database write is the wrong default.
- “Format independent with no performance dropoff” conflicts with fixed internal layout. The attainable version is one substrate with profile-specific physical layouts: row/page/WAL paths for RDBMS, range/SSTable paths for ordered KV, striped multipart paths for object storage, content-addressed extents for blob storage.

## What The Lamp Reveals

The lamp is your original frustration: managed databases feel like surrender because they bundle three separate things:

- durable storage
- correctness under failure
- operational automation

The hidden abstraction is to separate those. You do not need to build “a database” first. You need to build a deterministic storage fabric that can host database layers.

## Better Abstraction

Call the internal protocol a **programmable replicated storage fabric**. Call RSF itself a **full distributed database**.

Core object:

```text
namespace / pool / placement-group / shard / epoch / log-index / extent-id / object-id / blob-cid
```

Core planes:

- **Data plane:** append, read, range-scan, blob-put, blob-get, watch, repair.
- **Consensus plane:** shard-local replicated logs, view/term changes, membership epochs.
- **Layout plane:** maps high-level personalities to physical shapes: database pages, log segments, KV ranges, object stripes, immutable blobs.
- **Placement plane:** maps shards/extents/objects to replica or erasure-coded sets using latency, failure domain, disk class, trust class, and load.
- **Discovery plane:** bootstrap nodes plus optional libp2p Kademlia-style peer and provider routing.
- **Verification plane:** hash-chained logs, checksummed immutable blocks, Merkle manifests, audit proofs.
- **Execution plane:** optional database/query layers over the storage fabric.

## What Is Proven

- Unbundling works. FoundationDB’s architecture proves that separating transaction management, logs, and storage can scale reads and writes independently.
- Layering works. FoundationDB’s layer concept and Ceph’s RADOS architecture both show that data model/API can sit above a smaller distributed storage core.
- Unified storage works when semantics are pushed up. Ceph stores data as RADOS objects and lets clients/layers provide object, block, file, or custom semantics.
- Relational-on-disaggregated-storage works. Aurora and Socrates show that RDBMS engines can scale around log/page-aware storage services rather than local disks only ([Aurora SIGMOD 2018 paper](https://pages.cs.wisc.edu/~yxy/cs839-s20/papers/aurora-sigmod-18.pdf), [Socrates paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2019/05/socrates.pdf)).
- Leadered consensus works for correctness. Raft/Viewstamped Replication/Multi-Paxos style systems are the sane base for strict durability under crash faults.
- Flexible quorums are real leverage. TigerBeetle uses Viewstamped Replication, batching, pipelining, tail-latency tolerance, and flexible quorums for safety/performance ([TigerBeetle safety docs](https://docs.tigerbeetle.com/concepts/safety/), [TigerBeetle VSR internals](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vsr.md), [Flexible Paxos](https://arxiv.org/abs/1608.06696)).
- Leaderless/tunable quorum systems work for high availability, but push conflict handling into data models or applications. Dynamo is the canonical proof.
- Decentralized discovery works for finding peers/content, not for magically making mutable strongly consistent state cheap. libp2p/IPFS solve routing and content addressing, not OLTP by themselves.
- Storage-engine physics matter. LSM compaction creates read/write amplification; value separation can reduce large-value write amplification ([CockroachDB storage layer](https://docs.cockroachlabs.com/docs/v26.3/architecture/storage-layer), [BVLSM paper](https://arxiv.org/abs/2506.04678)).

## What Is Still Unknown

- Whether the first product layer should be relational-page/WAL storage, ordered KV, object/blob storage, or append-only event log.
- Whether the trust model is crash-fault-only, Byzantine-aware, or hybrid with Byzantine verification but crash-fault consensus.
- Whether the system optimizes for single-tenant deployment first or public decentralized capacity from day one.
- Whether the routing layer should be pure libp2p, custom QUIC gossip, or a dual path: managed bootstrap for production clusters and DHT for open meshes.
- How much automatic admission can be allowed before noisy, slow, or malicious nodes ruin p99.
- Which physical layout profiles can share one binary without sharing one hot path.

## Candidate Protocol Architectures

### 1. VSR Cell Fabric

Confidence: 0.91

Thesis: The safest core is shard-local Viewstamped Replication cells with deterministic state machines, hash-chained immutable WAL entries, repair-first storage, and an explicit placement plane.

How it works:

- Every shard is owned by a small replica cell, usually 3, 5, or 6 nodes.
- A primary assigns log indexes and timestamps inside the shard.
- Backups durably append prepares before quorum commit.
- Reads go to the primary for linearizable reads or to followers with safe-read tokens.
- New servers join as learners/witnesses before being promoted.
- Placement continuously moves shards toward low-latency clients and healthy disks.

Why it hits the boxes:

- One binary can join, advertise capacity, and become a learner automatically.
- Latency is excellent when clients hit their nearest primary/cell.
- Correctness model is narrow enough to implement and test.
- TigerBeetle is strong evidence that VSR plus batching/pipelining can be production-grade.

Hard limits:

- Not fully decentralized unless admission and placement are opened.
- Cross-region writes still pay quorum distance.

Best first build:

- Ordered KV with single-shard transactions.
- Multi-shard transactions only after the log, recovery, membership, and repair protocols are boring.

### 2. FoundationDB-Style Unbundled Fabric

Confidence: 0.90

Thesis: Split the system into sequencers/proxies/resolvers/log servers/storage servers from the beginning. This is the highest ceiling for a general database substrate.

How it works:

- Storage servers own key ranges and serve MVCC reads.
- Log servers persist committed mutations.
- Commit proxies accept client transactions.
- Resolvers perform optimistic conflict detection.
- A sequencer assigns versions.
- A small control plane owns membership and data distribution.

Why it hits the boxes:

- Very strong general-purpose database substrate.
- Reads scale horizontally because clients read directly from sharded storage servers.
- Writes scale by adding commit proxies, resolvers, and log servers.
- Clean platform for SQL, document, vector, queue, and event layers.

Hard limits:

- More moving parts than VSR Cell Fabric.
- “Deploy one binary” is possible operationally, but internally that binary must run role-specific actors.
- Needs a highly reliable control plane; decentralization is mostly at the data plane, not governance.

Best first build:

- Implement a single process that can host all roles.
- Then allow roles to split across processes as the cluster grows.

### 3. WPaxos Locality-Adaptive WAN KV

Confidence: 0.87

Thesis: For “distributed anywhere” with genuinely low latency, the system should move object ownership toward demand rather than forcing all writes through one fixed leader region.

How it works:

- Keyspace is partitioned into objects or ranges.
- Multiple region-local leaders own different partitions.
- Ownership can be stolen/reassigned using Paxos phase 1.
- Normal writes commit against phase-2 quorums chosen near the current leader.
- Placement adapts when client locality shifts.

Why it hits the boxes:

- It directly targets low-latency WAN consensus.
- It is semi-decentralized: no single global write leader.
- It can keep hot objects near the users writing them.
- WPaxos is explicit about flexible quorums and locality-aware ownership ([WPaxos](https://arxiv.org/abs/1703.08905)).

Hard limits:

- Harder than Raft/VSR.
- Hot objects with writers in many regions will thrash ownership or pay WAN latency.
- Needs careful starvation and fairness design.

Best first build:

- Use this as v2 for geo-distributed namespaces after VSR Cell Fabric works.

### 4. Dynamo-CRDT Availability Mesh

Confidence: 0.89 for availability-first storage; 0.72 for general production database semantics

Thesis: If the main box is “always accepts writes,” use a Dynamo-descended design with consistent hashing, N/R/W tunable quorums, hinted handoff, Merkle anti-entropy, vector/dotted version vectors, and CRDT-native data types.

How it works:

- Keys map onto a consistent-hash ring with virtual nodes.
- Each item is replicated to N owners.
- Coordinators accept writes after W acknowledgements and reads after R responses.
- Failures use sloppy quorum and hinted handoff.
- Replicas reconcile with Merkle anti-entropy.
- Values are either immutable versions, app-resolved siblings, or CRDTs.

Why it hits the boxes:

- New binary joins ring, receives virtual-node assignments, and becomes capacity.
- Excellent write availability and operational simplicity.
- Decentralized shape is natural.
- Great for sessions, carts, counters, presence, telemetry, caches, and eventually consistent app state.

Hard limits:

- It does not give strict serializable database behavior.
- Conflict semantics become the product surface.
- Secondary indexes and cross-key invariants are painful.

Best first build:

- Use this as the AP mode of the storage fabric, not the only mode.

### 5. Content-Addressed Blob Plane With Mutable Quorum Heads

Confidence: 0.88

Thesis: Separate immutable data from mutable truth. Store bytes/content as CIDs in a decentralized provider mesh, then update small mutable heads through VSR/Raft/WPaxos.

How it works:

- Large values, files, SSTables, snapshots, and segment blocks are content addressed.
- Nodes advertise provided blocks through a DHT/provider-routing layer.
- Mutable namespace heads point to Merkle roots or log tips.
- Heads are replicated through strict quorum consensus.
- Garbage collection follows namespace reachability and retention policy.

Why it hits the boxes:

- New nodes can immediately add blob capacity.
- Immutable content is cheap to replicate, cache, verify, and repair.
- Mutable consensus payloads stay small, improving latency.
- Blockchain-like verifiability appears where it helps: hash-linked data and auditable manifests.

Hard limits:

- Content routing is not enough for database semantics.
- Provider records expire and require republishing under churn.
- Hot mutable records still need consensus.

Best first build:

- Make the storage engine value-separated from day one: WAL/log entries reference immutable blob chunks for large payloads.

### 6. Aurora-Style Quorum Log Storage

Confidence: 0.89 for RDBMS/page-log workloads; 0.80 as the only universal substrate

Thesis: Build the fabric as a distributed log-structured storage layer beneath database engines: the protocol replicates redo/log records to storage nodes using quorum rules, while compute/query nodes remain stateless-ish.

How it works:

- Compute nodes emit redo records or logical mutation batches.
- Storage protection groups keep multiple copies across failure domains.
- Writes complete after quorum acknowledgement.
- Reads reconstruct pages/objects from materialized state plus log tail.
- Repair uses log sequence numbers, checksums, and segment manifests.

Why it hits the boxes:

- Excellent fit for “storage receptacle” nodes.
- Strong separation of compute from storage.
- Good route to supporting multiple database frontends.
- AWS Aurora is evidence that quorum log storage can be a high-performance database substrate; AWS describes six-way quorum across three AZs with write quorum four and read quorum three ([AWS Aurora quorum post](https://aws.amazon.com/blogs/database/amazon-aurora-under-the-hood-quorum-and-correlated-failure/)).
- Socrates is further evidence for a page/log-oriented relational architecture: primary compute, secondaries, log service, page servers, and long-term storage are autonomous mini-services, with page servers consuming only the log records relevant to their partitions.

Hard limits:

- Strongest when the database engine above it has a clear log/page model.
- General KV semantics require another layer.
- Recovery logic becomes the heart of the system.

Best first build:

- Use this as the storage substrate under VSR/FDB-style metadata and transaction layers.

### 7. RADOS-Style Universal Object Substrate

Confidence: 0.92 for format-independent storage; 0.78 for ultra-low-latency OLTP without an additional transaction/log layer

Thesis: The core persistent unit should be an object/extent in a placement group, with clients and higher services translating relational pages, S3-style objects, block devices, files, SSTables, and blobs into those lower-level objects.

How it works:

- Nodes run storage daemons that own placement groups.
- A cluster map plus deterministic placement function maps each object to an acting set.
- Clients compute locations directly from the map and placement function.
- Each placement group has a primary that coordinates writes and replication.
- Frontends convert their native abstractions into substrate objects:
  - RDBMS: pages, WAL segments, undo/redo fragments, table/index extents.
  - Object store: multipart object parts, metadata manifests, version records.
  - Blob store: immutable chunks, content-addressed extents, retention manifests.
  - Block store: virtual block image stripes.
  - File store: file data stripes plus separate metadata service.
- Replication and erasure coding are pool/layout policies, not separate products.

Why it hits the boxes:

- It is the cleanest proof that one substrate can expose many storage personalities.
- Ceph shows the specific shape: RADOS below, object/block/file/custom APIs above.
- Client-side placement avoids a centralized data lookup bottleneck.
- Placement groups give a manageable unit for peering, repair, scrubbing, rebalancing, and erasure coding.
- New servers can join as capacity and receive placement-group movement as the map changes.

Hard limits:

- The object substrate alone is not a relational database.
- RDBMS latency needs a log/page adapter with caching, read-ahead, and transaction semantics.
- Metadata services become critical for rich APIs.
- Erasure coding helps capacity and durability but can hurt small-write latency.

Best first build:

- Use this as the physical substrate, then implement VSR/FDB/Aurora-style strict layers above it.

## Format-Independent Requirement

The target should be **format independent at the protocol boundary**, not format indifferent in the physical engine.

Bad version:

```text
One internal representation handles SQL rows, database pages, S3 objects, videos, embeddings, event logs, and tiny counters with identical performance.
```

That version is fantasy because access patterns are physically different.

Good version:

```text
One fabric exposes common durability, placement, replication, repair, identity, discovery, and verification.
Each frontend supplies a layout adapter optimized for its workload.
```

Required layout adapters:

- **Relational adapter:** page server API, WAL/redo stream, MVCC snapshot metadata, transaction fences, hot-page cache, range prefetch.
- **Ordered KV adapter:** sorted keyspace, range partitions, conflict ranges, MVCC versions, compaction-aware SSTable/object layout.
- **Object-store adapter:** bucket/object metadata, multipart manifests, versioning, lifecycle policy, erasure-coded cold parts, replicated hot metadata.
- **Blob adapter:** immutable chunks, content IDs, dedupe, retention pins, provider records, streaming reads.
- **Append-log adapter:** topic/partition logs, segment manifests, consumer offsets, tiered storage.
- **Block adapter:** virtual volume stripes, snapshots, copy-on-write clones, direct I/O alignment.

No-dropoff performance policy:

- Tiny strongly consistent metadata uses replicated quorum cells.
- Hot relational pages use cache-close page servers and log shipping.
- Large immutable payloads use content-addressed chunks and parallel fetch.
- Cold large objects use erasure coding.
- Hot small objects use replication, not erasure coding.
- Cross-region strict writes are opt-in and priced in latency.
- Async geo replication must never be mislabeled as strict global consistency.

## Five All-Box Implementation Packages

These are the architectural implementation templates I am at least 85% confident can hit the broadened target if built with the stated constraints. Each one supports relational, object, and blob use cases by making format independence a layer property rather than a single physical layout.

### A. Universal VSR-RADOS Fabric

All-box confidence: 0.91

Core: RADOS-style placement groups for physical storage, VSR cells for strict mutable metadata, content-addressed chunks for large values, and layout adapters for SQL pages/WAL, object manifests, blobs, logs, and block stripes.

Why it qualifies:

- One binary joins as an OSD-like storage daemon and can later host consensus/page/log roles.
- Object/blob/block/file semantics map naturally to placement-group objects.
- Relational support comes through a page/WAL adapter, not by pretending object storage is a database.
- VSR gives a tractable correctness core for strict metadata and hot mutable state.

Primary risk: building placement, peering, repair, and VSR together is a lot. The first prototype must be ruthlessly scoped.

### B. FoundationDB-Over-Extent Fabric

All-box confidence: 0.90

Core: FDB-style sequencer/proxy/resolver/log/storage services backed by a lower extent/blob substrate. The ordered KV layer becomes the universal metadata and transaction substrate; SQL/object/blob layers sit above it.

Why it qualifies:

- Strongest route to relational semantics because ACID transactions and ordered keys compose into indexes, catalogs, object metadata, manifests, and queues.
- One binary can run all roles in small deployments and specialize in larger clusters.
- Object/blob payloads can live in immutable extents while metadata lives in ordered transactional keys.
- FoundationDB’s published layer model directly supports the “many formats, one core” idea.

Primary risk: the transaction fabric is harder to get right than a single replicated cell.

### C. Aurora/Socrates Relational-First Fabric

All-box confidence: 0.88

Core: relational page servers, quorum log segments, async log dissemination, and long-term object/blob storage. Object/blob APIs are added as manifest/chunk layers over the same protection-group substrate.

Why it qualifies:

- Best if RDBMS support is the flagship requirement.
- Preserves native database mental models: WAL, LSN, pages, checkpoints, replicas, hot page caches.
- Blob/object support is natural because large values and backups are already chunk/manifest shaped.
- Aurora and Socrates both validate compute/storage separation for production relational systems.

Primary risk: the core becomes too RDBMS-shaped unless object/blob adapters are designed from day one.

### D. Geo-Local WPaxos Fabric

All-box confidence: 0.86

Core: placement-group extents plus locality-adaptive ownership for mutable heads/ranges. Object ownership migrates toward active writers; large immutable data uses content-addressed chunks.

Why it qualifies:

- Best answer to “distributed anywhere” without forcing every write through one home region.
- Relational partitions, object metadata, and blob manifests can each be owned near their write locality.
- Flexible quorums can keep steady-state commits close to the active leader.
- Still compatible with strict cells for data that must not move frequently.

Primary risk: ownership migration is complex and hot multi-region records can thrash.

### E. Dual-Mode CP/AP Storage Fabric

All-box confidence: 0.87

Core: CP mode for relational metadata, object manifests, and transactional state; AP Dynamo/CRDT mode for availability-first records, telemetry, sessions, counters, and edge writes. Both modes share identity, placement, chunks, repair, and observability.

Why it qualifies:

- Real products need both strict and available modes.
- Relational state stays CP.
- Blob/object payloads can be immutable and globally cached.
- Edge-friendly AP data can accept local writes and reconcile later without corrupting strict relational invariants.

Primary risk: the API must make consistency mode impossible to misunderstand. Silent mixing of CP and AP semantics would be fatal.

## Recommended Architecture

The best architecture is not one of the seven alone. It is a stack:

```text
L6: User-facing personalities
    relational SQL, ordered KV, document, S3/object, blob, block, file, append log, vector sidecar

L5: Layout adapters
    pages/WAL, ranges/SSTables, object manifests/parts, chunks/extents, volume stripes

L4: Mutable truth
    VSR cells first; FDB-style transaction fabric second; WPaxos ownership later for WAN-local writes

L3: Physical substrate
    RADOS-style placement groups plus content-addressed chunks, snapshots, SSTables, large values

L2: Local storage engine
    append-only WAL, value separation, checksums, Merkle manifests, bounded compaction

L1: Membership and placement
    bootstrap identities, capability admission, health scoring, failure-domain maps

L0: Transport
    QUIC, direct cluster gossip, optional libp2p/Kademlia provider routing
```

The initial protocol should be **RADOS-style physical substrate + VSR Cell Fabric + Content-Addressed Blob Plane**.

That is the version I am more than 85% confident can satisfy the real goal without lying:

- one binary can join and become storage
- hot data can stay low-latency
- correctness can be strict inside cells
- relational/object/blob/file/block can all be layers over the same fabric
- profile-specific layouts prevent the “universal format” from becoming universally mediocre
- large immutable data can be decentralized and verifiable
- the design can later grow FDB transaction services, AP Dynamo mode, and WAN-local WPaxos mode
- the core is small enough to actually implement with discipline

## Protocol Sketch

Node identity:

```text
node_id = hash(public_key)
node_cert = signed(capacity, region, az, disk_class, trust_class, endpoint, version)
```

Join:

```text
1. Node starts with bootstrap address or invite token.
2. Node proves identity and reports capacity.
3. Placement plane admits it as observer.
4. Node receives shard snapshots or blob assignments.
5. Node serves reads only after verified sync.
6. Node becomes voting replica only through an epoch change.
```

Write:

```text
1. Frontend translates request into layout-specific operations.
2. Client or gateway routes each object/extent/log record to its placement group.
3. Primary appends request to deterministic log.
4. Replicas fsync/hash-chain/checksum the prepare.
5. Primary commits after quorum.
6. Large values are stored as content-addressed chunks; metadata/log stores CIDs.
7. Client receives commit version, object version, or idempotency result.
```

Read:

```text
1. Linearizable read: primary or quorum read fence.
2. Bounded-stale read: nearest replica with safe version.
3. Blob read: fetch by CID from nearest provider; verify hash.
```

Repair:

```text
1. Every segment has checksum and Merkle linkage.
2. Replicas exchange log headers and missing ranges.
3. Corruption is repaired from quorum-good peers.
4. Slow/corrupt nodes are demoted automatically.
```

## Next Experiments

1. Build a deterministic simulator before the database.
2. Model VSR cell membership, log append, view change, crash/restart, disk corruption, and network partitions.
3. Implement the storage file format next: fixed-size segments, checksums, hash chain, superblock, WAL records, snapshots, object/extent manifests.
4. Implement placement groups with deterministic object mapping and primary/secondary acting sets.
5. Implement the first strict metadata cell with VSR.
6. Implement three layout adapters in this order: blob chunks, object manifests, relational WAL/page fragments.
7. Add new-node join as learner, snapshot transfer, placement-group movement, and promotion.
8. Add placement scoring and shard movement.
9. Add ordered KV and relational transaction semantics.
10. Only then add multi-shard transactions or WAN-local ownership.

Acceptance tests:

- no acknowledged write is lost after any minority crash pattern
- no stale primary can commit after view change
- no corrupted WAL entry is accepted silently
- replay is deterministic byte-for-byte
- learner promotion cannot reduce quorum safety
- p99 write latency remains bounded under one slow replica
- read-your-writes holds for strict sessions
- async geo replicas never masquerade as strict replicas
- object, blob, and relational layout adapters share the same durability core
- relational page reads do not route through object-store HTTP semantics
- large object reads stripe across placement groups and verify chunk hashes
- erasure-coded pools reject small-write hot-path placement unless explicitly configured

## Honest Thesis

Yes, this can be done, but the winning move is to refuse the false binary between “managed cloud database” and “blockchain.” The serious version is a self-forming storage fabric whose core is boringly correct and whose decentralization is applied where it helps: identity, discovery, immutable content distribution, auditability, and optional public capacity.

Do not start with SQL as the core. Do not start with S3 as the core. Do not start with Byzantine consensus. Do not start with a token. Start with a replicated, deterministic, crash-fault-tolerant placement-group object/extent substrate that never loses an acknowledged write. Then add the VSR metadata cell, DHT/blob plane, object manifests, and relational page/WAL adapter. Then add locality-adaptive ownership. That path has the best chance of becoming a real system instead of a beautiful pile of distributed-systems pain.
