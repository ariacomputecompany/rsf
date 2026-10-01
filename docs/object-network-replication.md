# Network-replicated objects

The native object data plane replicates bucket creation and object PUT as complete
namespace-bound VSR commands. It uses the same authenticated network configuration
as [KV](kv-network-replication.md). Peer services apply buckets, object history and
the bytes behind each head before acknowledging the committed command. See [SQL replication](sql-network-replication.md) for the supported autocommit
statement path.

## Command and durability boundary

The `RSFOC1` surface payload contains the exact original request intent. Bucket
commands carry the bucket name and placement group. PUT commands also carry the
key, content ID, byte count, original deduplication result, blob placement group,
pin evidence, pin count and complete immutable bytes. These are enclosed by the
versioned `RSFC1` protocol command, which binds namespace, Object surface, request,
full operation, strict mode, placement, index and global commit version.

Before persisting an intent, the primary validates both object and internal pin
placement/policy, existing pin request identity and complete command size. It
stages verified bytes, journals the intent, obtains the content-addressed pin,
replicates the complete object command, then atomically journals that command as
the application receipt before publishing the head. The internal pin is a
metadata-only protocol record; it alone never acknowledges object durability.
The complete object command carries the bytes needed by independent peers.

Each object store retains its existing nested `blob/` directory. A peer's store
lives at `application/namespaces/<SHA-256-of-namespace>/object` beneath its peer
state. Replay verifies the namespace, request, operation, mode, placement, content
digest, payload length, pin evidence and version relationships before storing
bytes and publishing a bucket or head. It rejects missing bucket dependencies,
conflicting history and out-of-order new commands.

## Recovery and retries

`open_replicated` persists a namespace binding in the existing object journal.
Plain `open` recovers that binding. Existing nonempty metadata-only stores require
explicit migration; rejection preserves their original data. Bound stores reject
the metadata-only application APIs. Legacy unbound library stores remain readable
through their original contract.

An unresolved local intent can be reconciled after interruption at either the pin
or object commit boundary. The complete VSR command can also reconstruct buckets,
heads, history, receipts and payloads without primary application state. Daemon
recovery applies certified commands in version order and reconciles the local
intent before serving the namespace. Peer startup replays committed commands
before listener readiness.

The atomic application record includes the complete command and deterministically
derives its original receipt during recovery. An exact retry returns the original
receipt and rechecks network quorum. Older write retries do not replace a newer
head. A repeated bucket creation under a new request identity has a replicated
command of its own but returns the original bucket's creation evidence; this
preserves bucket identity while making the additional request durable on peers.
Different bytes, keys, buckets or placement under an existing request are rejected.
A rolled-back protocol history cannot manufacture a replacement receipt.

Receipt persistence failure fences object reads and writes until reopen. A peer
application failure rejects the RPC even if the VSR command is already durable;
retry/startup can reapply that command without duplicating object history.

Object GET confirms a primary quorum before reading the mutable head. This is
different from a [blob GET](blob-network-replication.md), which verifies immutable
bytes addressed by their content ID. Configured static-primary, historical
membership and readiness limits from the KV network contract still apply.

## Limits and verification

Native frames remain limited to 64 KiB and complete VSR commands to 1 MiB. Larger
object streaming and independently replicated chunk references, incremental
replay, bounded receipts, namespace migration, network membership/failover and
machine-failure durability measurements remain release work.

Object tests cover namespace isolation, bucket aliases, overwrite history, exact
old retries, corrupt commands, oversized requests, invalid pin placement before
intent persistence, protocol rollback and legacy migration refusal. Process tests
kill a writer after pin or head commit and recover the original bytes and receipt.
Receipt-write fault injection verifies hidden unpublished heads, failure fencing
and reconciliation after reopening.

`native_object_ingress_recovers_heads_and_bytes_from_independent_peers` runs the
actual primary and two authenticated peer daemons with independent state. It
checks bucket aliases, writes after peer loss, simultaneous primary/live-peer
termination, follower-only recovery, lagging-peer prefix repair, exact response
replay, no resurrection on old retries, and read/write-retry rejection after
quorum loss. The object/blob and daemon Fozzy domains execute these host tests and
verify, replay and CI-check the recorded traces. These tests do not establish
machine power-loss safety or automatic primary promotion.
