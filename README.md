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

RSF is still under development. The daemon supports durable SQL autocommit,
KV, object, and blob paths, with authenticated peer replication when configured.
Standalone initialization provides one durable copy. Some admin/policy paths
still use local transport. Certified network membership installation, learner
promotion, primary failover, legacy-state migration, durable SQL sessions,
bounded storage lifecycle, and measured production release gates remain open
in [WORK.md](./WORK.md).

`rsfd init` selects a standalone policy with one durable copy. That node can
serve writes and recover them after restart, but cannot survive loss of its
storage. A redundant policy requires at least three replicas. Voter admission
advances one node at a time; the two-voter transition requires both votes.
Local join/restart coverage does not establish independent replica durability.
Data listeners require mutual TLS and explicit namespace/capability grants;
see [client setup](./docs/client-access.md).

- [Protocol Architecture Research](./docs/protocol-architecture-research.md)
- [Full Implementation Plan](./PLAN.md)
- [Product Requirements](./docs/requirements.md)
- [Full Database Architecture](./docs/full-database-architecture.md)
- [Implementation Roadmap](./docs/implementation-roadmap.md)
- [Implementation Map](./docs/implementation-map.md)
- [Storage Engine Design](./docs/storage-engine-design.md)
- [Verification Strategy](./docs/verification-strategy.md)
- [Data-plane Client Access](./docs/client-access.md)
- [Decision Record](./docs/decision-record.md)
- [End-To-End POC](./docs/poc.md)

## How It Works

Most databases make scale and durability an infrastructure project. The operator has to choose separate systems for SQL, objects, blobs, replication, placement, failover, repair, backup, and capacity growth, then keep the contracts between those systems correct under load and failure.

RSF moves that work into one database binary and one storage-fabric control plane.

The operating model is direct:

1. Stand up a server with local durable storage.
2. Put the RSF binary on it.
3. Initialize or join the node with the cluster admission token and node identity.
4. Set the database policy: replication factor, placement topology, consistency mode, durability target, region/zone awareness, object/blob retention, and workload surface.
5. Start the daemon.

From that point, RSF treats the server as database capacity. The node is admitted into the cluster, assigned ownership, given placement-group responsibilities, and brought into the current layout epoch. The operator does not wire together a separate metadata store, object store, replication service, SQL coordinator, and repair system. Those are internal database responsibilities.

When policy changes, RSF computes the resulting placement plan and reconciles the cluster toward it. Adding servers increases the available placement surface. Increasing the replication factor creates additional durable copies. Moving from a single-zone profile to a region- or zone-aware profile changes where replicas are allowed to live. The policy is the interface; admission, rebalancing, repair, routing, and consistency enforcement are the implementation.

The same fabric supports multiple data surfaces. Small ordered data can flow through the KV and relational paths. Large values, objects, blobs, snapshots, and backup blocks can flow through immutable chunk storage. Mutable truth is protected by strict replicated cells. Physical data movement is handled through placement groups and content-addressed records. The user sees database APIs; the system keeps the storage layout correct underneath them.

The problem RSF solves is operational compression. Instead of deploying a database beside a storage system beside a replication system beside an object system, RSF makes the database own the full durability and placement stack. The expected production motion is simple: add machines, declare policy, and let the cluster converge while preserving the database contracts exposed at the SQL, KV, object, blob, and log surfaces.

## Working Thesis

RSF should be built as a full database whose lower substrate combines:

- RADOS-style placement groups for physical storage and rebalancing
- VSR-style strict replicated cells for mutable truth
- content-addressed immutable chunks for large values, snapshots, and object/blob payloads
- FoundationDB-style layering for data models and APIs
- Aurora/Socrates-style page/log/WAL adapters for relational performance

The first implementation should prove a small, deterministic, crash-fault-tolerant database core before attempting global scale, Byzantine trust, or a public decentralized capacity market.
