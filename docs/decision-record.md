# RSF Decision Record

Date: 2026-09-10

## Current Architectural Decision

RSF is a full distributed database. Its internal storage fabric is a protocol substrate, not the whole product.

The current recommended implementation path is:

```text
RADOS-style placement groups
+ VSR strict metadata cells
+ content-addressed immutable chunks
+ ordered KV transaction layer
+ relational engine
+ object/blob personalities
```

## Why This Decision

This path satisfies the original requirements without pretending physics is optional:

- one binary can join a cluster and become capacity
- the database can distribute data across many servers
- relational/object/blob surfaces are first-class database personalities
- each workload gets an appropriate physical layout
- strict consistency is available for database metadata and transactions
- immutable large data can be decentralized, cached, verified, and repaired
- low-latency writes are possible when quorum placement is local
- global strict writes are explicit rather than hidden

## Rejected Starting Points

### Start With SQL First

Rejected for now because SQL would force premature decisions before the storage/consensus/recovery substrate is correct.

### Start With S3/Object Store First

Rejected because object semantics are not enough for relational transactions, indexes, or low-latency mutable metadata.

### Start With Blockchain Consensus

Rejected because the first target is production database performance under controlled membership, not adversarial public consensus.

### Start With Pure Distributed KV

Rejected as the whole product because RSF must become a full database. Accepted as one internal layer.

## Next Decision Needed

Choose the first physical implementation path for relational data:

- **Ordered KV relational path:** tables/indexes encoded into ordered MVCC keys. Better for first implementation.
- **Page/WAL relational path:** database pages and redo records over distributed log/page servers. Better for long-term RDBMS compatibility.

Current recommendation: start with ordered KV, but keep the page/WAL adapter in the architecture because it may become necessary for serious RDBMS compatibility.

