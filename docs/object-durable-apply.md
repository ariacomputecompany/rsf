# Durable object writes and replay

Object writes use immutable chunk payloads plus a checksummed, append-only object
journal. A PUT persists and verifies its payload first, records its full intent,
commits the content-addressed blob pin and object manifest, then persists one
receipt before publishing the new head in memory. The intent includes the client
request, both placement groups, bucket, key, content digest, byte length, and the
original deduplication result. The receipt preserves the complete original
response and both commit evidence records.

`DurableObjectStore::reconcile` resolves an interrupted intent through the same
protocol request IDs and the original staged payload. Call it before serving
reads after opening a store; the daemon does so for every object runtime. A router
behind the materialized object version is rejected. An uncertain journal write
fences the object runtime until it is reopened. Object and chunk directories each
permit one writer through an operating-system lock, released on process exit.

A repeated request must match its original operation and bytes. It returns the
original receipt, including a historical head, without making that head current
again or adding another history entry. Reusing the request for different bytes,
keys, buckets, placement groups, or operation types fails. Bucket creation through
the daemon now uses its authenticated, namespace-scoped client request identity.
Internal bucket request IDs also include a persisted random store scope.

Blob PUT ensures a durable pin by content ID. Retrying the same immutable content
does not increment its chunk reference or pin counts. Existing legacy counts are
preserved. An explicit release can be followed by another PUT to restore one pin.
The blob protocol certificate must match the content ID, placement group, and
availability-first mode. A staging write can leave an unreferenced pinned chunk
if the subsequent object operation fails; safe reclamation requires reachability
and receipt-retention rules and is not implemented by this journal.

## Applying authoritative commands

`apply_committed_bucket` and `apply_committed_put` let a materializer install an
authoritative receipt idempotently. The caller must authenticate the complete
replicated command and validate its quorum certificate. These APIs check local
operation/evidence consistency, reject conflicting receipts, and persist exact
object bytes before publishing the receipt. A historical replay preserves the
current head. These local APIs do not themselves replicate commands or provide
network quorum validation; integrating them into the daemon peer path remains a
release blocker.

## Upgrading existing data

On first open without a journal, a valid legacy `objects.index` becomes the
checksummed bootstrap record. Subsequent opens recover exclusively from the
journal; changes to the obsolete index cannot roll back object history. Existing
chunk payload files remain usable. Stop writers and preserve a backup of the
entire state directory before upgrading; older binaries cannot recover new
journal-only writes. Do not remove the journal after migration.

A legacy request certificate can be replayed only when its original durable head
and verified payload still exist. If the old implementation committed a protocol
operation but lost its payload/head intent, recovery fails closed instead of
binding the certificate to arbitrary retry bytes. Legacy receipts cannot recover
the originally returned deduplication flag; migration reports such a verified
payload as deduplicated.

## Verification and remaining limits

`tests/object-blob-host.pass.fozzy.json` runs real object, blob, object-metadata,
and chunk-store tests under the Fozzy host backend. Tests terminate a real writer
process after either blob-pin or manifest commit, reopen its state, and reconcile
the original bytes exactly once. They also cover historical retries, changed
payload rejection, receipt-persistence failure fencing, writer exclusion,
idempotent pinning, legacy bootstrap migration, invalid journal records, and
applying authoritative receipts to an independent local store.

The full verification script runs strict scenario checks and five-run doctor
audits, captures host traces, and verifies, replays, and checks those traces in CI.
These are process-failure checks, not machine-power-loss or end-to-end distributed
application durability proofs. The journal retains all receipts and history;
bounded retention with an explicit retry horizon remains required for production.
