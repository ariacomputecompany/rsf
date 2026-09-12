# RSF Storage Engine Design

Date: 2026-09-10

## Purpose

The local storage engine is the first physical reality layer under RSF. It is responsible for turning protocol decisions into durable bytes and for refusing to accept corrupted or ambiguous bytes during recovery.

This layer is intentionally below SQL, object storage, blob storage, and even placement-group replication. If this layer lies, every database personality above it lies.

## Current Implementation

Code:

- [rsf-storage](../crates/rsf-storage/src/lib.rs)
- [rsf-manifest](../crates/rsf-manifest/src/lib.rs)
- [rsf-chunkstore](../crates/rsf-chunkstore/src/lib.rs)
- [rsf-checkpoint](../crates/rsf-checkpoint/src/lib.rs)

Implemented:

- append-only segment file
- record magic header
- payload length
- per-record checksum
- typed WAL payloads
- placement group ID, epoch, log index, and record kind for WAL records
- previous record hash
- payload hash
- record hash
- durable immutable chunk store with BLAKE3 content-addressed put, dedupe, restart-safe retention pins, verified reads, and malformed-payload rejection
- local checkpoint-root retention and segment-compaction planning for catalog, transaction log, KV, SQL, object, blob, and chunk index roots
- hash-chain verification during recovery
- independent WAL tails per placement group
- monotonic WAL indexes per placement group
- fsync on append
- ordered recovery
- torn-tail truncation
- checksum-corruption rejection
- format-independent extent manifests for relational pages/WAL, ordered KV SSTables, object parts, blob chunks, and snapshot roots
- manifest byte-range, placement-group, extent-id, and checksum validation
- per-placement-group recovery APIs
- committed-prefix extraction from distinct commit records
- cheap placement-group prefix comparison without payload scans
- manifest coverage proofs against explicit recovered committed prefixes

Current record shape:

```text
magic:    4 bytes  "RSF1"
len:      8 bytes  little-endian u64
checksum: 8 bytes  little-endian u64
payload:  len bytes
```

Segment envelopes carry an explicit checksum policy. New records use BLAKE3-derived checksums, while legacy FNV records are accepted only for recovery compatibility and are never the default write policy. WAL payload hashes and record hashes are full BLAKE3 digests that bind payload bytes, placement group, epoch, log index, record kind, and the previous hash-chain link.

Current WAL payload shape:

```text
wal_magic:       4 bytes  "WAL1"
pg:              8 bytes  little-endian u64
epoch:           8 bytes  little-endian u64
index:           8 bytes  little-endian u64
kind:            1 byte
previous_hash:  32 bytes
payload_hash:   32 bytes
record_hash:    32 bytes
payload_len:     8 bytes  little-endian u64
payload:         payload_len bytes
```

The record hash commits to:

```text
pg || epoch || index || kind || previous_hash || payload_hash || payload
```

## Recovery Semantics

Recovery reads records from the start of a segment:

1. If it reads a complete valid record, the record is returned.
2. If EOF occurs while reading the next header or payload, recovery truncates the file to the last known-good offset.
3. If magic is invalid, recovery fails with corruption.
4. If checksum does not match payload, recovery fails with corruption.
5. If the payload is a WAL record, recovery verifies the WAL payload hash and record hash.
6. If the WAL previous hash does not match the recovered placement-group log tail, recovery fails with corruption.
7. If the WAL index is not monotonic within its placement group, recovery fails with corruption.

This distinction matters:

- A torn tail can happen during a crash and is recoverable.
- A corrupted committed record is not silently recoverable and must trigger repair from replicas.

## Placement-Group Recovery API

The current implementation exposes placement-group recovery APIs and commit semantics tied to the manifest layer:

```text
pg_log = recover(pg_id)
committed_prefix = pg_id || epoch || committed_log_index || record_hash
coverage = manifest_entries + required_pg_prefixes + recovered_committed_prefixes
```

Implemented behavior:

- callers can recover only the WAL records for one placement group
- placement-group logs can expose tail summaries for replica prefix comparison
- repair can compare log prefixes cheaply without scanning payloads
- snapshots can name the exact placement-group log prefix they cover
- commit markers are distinct from prepare records
- committed prefix can be computed independently from appended records
- recovered placement-group prefixes can prove which manifest extents are covered

## Later Requirements

- superblock
- segment ID and generation
- client request ID
- snapshot marker
- direct I/O investigation
- batched fsync
- preallocation
- sparse corruption scan
- persisted manifest pages for immutable chunks/extents
- durable chunk files, production digests, zero-copy reads, refcount GC, compression, and encryption
- durable checkpoint catalog pages, physical segment rewrite, atomic install, superblock advancement, and crash-safe compaction recovery

## Current Tests

Rust:

- append and recover records in order
- truncate torn tail
- reject checksum corruption
- append and recover typed hash-chained WAL records
- reject wrong previous hash during append
- reject WAL chain corruption during recovery
- keep independent hash tails for interleaved placement groups
- allow index one for a new placement group after other placement groups have records
- reject non-monotonic indexes within a placement group
- filter recovered records by placement group and report tail summaries
- compute committed prefixes from commit records
- compare placement-group prefixes without payload scans
- prove manifest entries are covered by recovered committed prefixes
- reject manifest coverage beyond the committed prefix
- publish mixed database extent manifests
- reject duplicate manifest extent IDs
- reject overlapping manifest byte ranges
- reject manifest entries with empty checksums

Fozzy:

- strict deterministic storage test scenario
- host-backend storage test scenario
- trace verification
- replay
- CI
