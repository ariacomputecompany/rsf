# Durable KV apply and request recovery

`DurableKvEngine` persists a full mutation intent before requesting a protocol
commit. After the protocol decision, it writes one WAL record containing the
values, original request identity, and commit evidence. Only then does it
publish the values in memory. A protocol certificate alone is insufficient
evidence that the KV values were applied.

This ordering covers crashes before the protocol decision, after that decision
but before local apply, and after local apply but before a client receives its
response. Callers that omit a client request receive an internal random request
identity persisted in the intent, allowing recovery to finish that attempt
without creating another protocol commit. A later new call without an explicit
request identity remains a new operation.

## Recovery and reads

Opening a store recovers its locally applied values and any unresolved intent.
Call `reconcile(&mut router)` before serving application reads or accepting
later commands. Reconciliation may finish an earlier uncertain write; it
preserves that intent's original bytes. It fails if the router cannot establish
the required commit, instead of treating the missing values as successfully
applied. The daemon performs this reconciliation when opening a namespace's KV
runtime, including for GET requests. Low-level inspection of an opened engine
without reconciliation sees only its locally applied prefix.

Writes also reconcile older intents before proceeding. Reusing a request ID
with a different placement group or mutation batch is rejected. A completed
retry returns its historical version and never reapplies stale values, even
after later updates or compaction. Batches require unique keys; each new apply
must advance the materialized commit version. Out-of-order replicated batches
must be ordered by the replication coordinator before applying them.

A failed journal or snapshot write fences further mutations until the engine
is reopened. Failed appends do not expose unpersisted values. Compaction builds
and persists its replacement state before publishing it. An operating-system
lock prevents concurrent writers over the same KV store and releases ownership
when the process dies.

## Applying replicated commands

`apply_committed_batch(request, evidence, mutations)` persists an already
committed batch and its local receipt without requesting another protocol
commit. It checks request, position, version, operation, and mutation consistency
against existing durable evidence. Repeating the same command is idempotent;
conflicting commands at an existing position are rejected.

The caller must validate the quorum certificate and decode the mutations from
its complete replicated command. This method is a materialization boundary,
not a certificate authenticator. Wiring it into the daemon's peer-driven
application state machine remains required.

## Compatibility and retention

Existing KV mutation and snapshot records remain readable. Older binaries do
not understand the new intent/receipt records; stop them before upgrading a
store and do not reopen an upgraded store with an older binary. A legacy retry can
be associated with a new receipt only when the local WAL proves the original
complete mutation and commit evidence. If a legacy crash left only a protocol
certificate and no durable values or intent, the engine fails closed; arbitrary
retry bytes cannot establish the lost original value.

Receipts remain available across logical KV compaction, so historical retries
cannot resurrect collected versions. The current implementation retains their
WAL history and mutation evidence. A bounded receipt-retention policy requires
an explicit client retry horizon and remains part of production retention work.

## Verification

The KV suite forcibly kills a child process after the protocol commit and
before the value/receipt append, then reopens both stores and recovers the
original value exactly once. It also covers failed receipt writes, stale and
conflicting retries, compaction, missing legacy values, malformed payloads,
exclusive ownership, and direct committed-batch materialization. The Fozzy KV
and workspace host scenarios execute these tests and record their results.
