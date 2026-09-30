# Network-replicated immutable blobs

Native blob PUT now submits the exact bytes, namespace, content-digest operation
and caller's request identity as a complete VSR command. It uses the authenticated
network configuration described in [KV replication](kv-network-replication.md).
A successful PUT requires a quorum to persist the command and its commit evidence;
peer services also materialize the verified bytes before acknowledging COMMIT or
prefix transfer. Local staging by itself cannot produce a successful PUT response.

## Storage and recovery

`DurableBlobEngine::open_replicated` binds an empty store to a namespace in a
checksummed `rsf-storage` journal under `replication/`. The primary stores blobs in
`namespaces/<SHA-256-of-namespace>/blob`; peers use
`application/namespaces/<SHA-256-of-namespace>/blob` beneath their peer-state root.
The existing chunk store supplies verified content addressing, durable immutable
bytes, exclusive writer ownership and idempotent pinning.

The surface payload is `RSFB1` followed by nonempty bytes. Its enclosing `RSFC1`
command contains the namespace, request, Blob surface tag, BlobPin digest,
placement group, version, index and availability-first mode. The materializer
checks the digest against the bytes, persists the chunk, then journals the full
command as its receipt. Repeated application repairs bytes without incrementing
pins or writing duplicate receipts. A journal persistence error fences requests
and reads until reopen.

The VSR command is authoritative if primary receipt persistence is interrupted.
Daemon startup/request recovery applies certified commands in version order;
peer startup does so before exposing its listener. A peer's missing prefix is
transferred and applied before a new commit is acknowledged. Blob history cannot
be applied out of order, and an old receipt cannot be replaced by a new command.

## Request and consistency behavior

The daemon now binds the supplied idempotency key to the original blob bytes,
namespace and placement group. Reusing that key with different bytes is rejected.
An exact retry retains its original certificate and checks the network quorum,
even if a local receipt exists. A local receipt requires matching recovered
protocol history; it cannot manufacture a replacement command after rollback.
Different request identities may reference the same immutable bytes without
adding physical pins. The `deduped` field describes local chunk reuse, so it may
become true on a retry; the content ID and commit evidence remain stable.

GET verifies the requested content ID locally and does not require a read quorum.
This preserves the immutable-verified read contract during peer loss. It does not
claim that an absent blob exists, nor certify a mutable object head. A PUT still
uses the current VSR quorum durability path; availability-first classification
does not substitute local-only acknowledgement for that contract.

Plain `open` recovers any existing namespace binding. A store cannot silently
revert to metadata-only pin commits or change namespace. Nonempty legacy blob
stores require explicit migration; refusal leaves their bytes intact. Existing
unbound library callers, including internal object pinning, retain
their original metadata-only pin API. [Object replication](object-network-replication.md)
uses a complete Object command to carry both the pin evidence and its bytes before
acknowledging an object head. Network SQL execution remains disabled until its
complete command/materializer path is implemented.

Native frames remain bounded to 64 KiB and complete VSR commands to 1 MiB. Request
validation rejects oversized commands before staging bytes. Large payload chunk
transfer, receipt retention, incremental replay, network membership/failover and
live-quorum operator readiness remain release work.

## Evidence

Blob tests cover complete-byte replay, corrupt body rejection before staging,
stable receipt/pin recovery, namespace conflicts, changed retries, oversized
requests, protocol rollback, legacy migration refusal, failed receipt-write fencing,
corrupt journal rejection and reconstruction from
an authoritative command without primary application state.

`native_blob_ingress_preserves_payload_on_peers_after_primary_loss` runs the actual
daemon and two authenticated peers with independent directories. It kills a peer,
writes with the remaining quorum, terminates the primary and live peer, verifies
the followers' own stored bytes, repairs the lagging peer through an exact retry,
and checks quorum-loss write rejection plus verified-read availability. The
object/blob and daemon Fozzy domains execute these tests on the host, record their
traces, and run strict verification, replay and CI checks. These tests establish
process-recovery evidence; machine power-loss and promoted-primary failover remain
unverified.
