# Complete application commands on VSR

`VsrBackedFabricRouter::commit_application` persists one versioned command in the
VSR prepare before requesting quorum acknowledgement. The command contains the
complete `FabricOperation`, placement group, log index, global commit version,
consistency mode, client request identity, tenant namespace, surface tag, and
surface-owned binary application payload. It uses the `RSFC1` encoding inside a
`VsrOperation::ReplicatedCommand`.

The payload must contain the values needed for deterministic application replay
or references to independently durable immutable content. The router does not
interpret surface payloads, authorize namespaces, or transfer referenced blobs.
Those responsibilities belong to the authenticated ingress and application
materializer. The daemon KV, SQL, object and blob paths call this API and peer services apply their commands durably;
see [KV network replication](kv-network-replication.md) and
[blob network replication](blob-network-replication.md), and
[object network replication](object-network-replication.md), and
[SQL network replication](sql-network-replication.md). The default router
constructor still selects `LocalVsrTransport`; use `open_with_transport` with the
authenticated `NetworkVsrTransport` for actual remote disk acknowledgement.

## Ordering and recovery

`preview_application_request` resolves earlier uncertain commands and returns the
exact prospective command metadata for local planning. It does not prepare a new
entry or supply commit evidence. Callers must serialize planning and submission,
then verify that the actual certificate matches the preview before publishing
planned state. SQL uses this to reject invalid physical transitions before commit.

The VSR command is authoritative. The protocol WAL receipt is a derived lookup
record published only after the VSR commit has durable quorum acknowledgement.
Because the request identity and commit version are inside the prepare, recovery
does not depend on a later, primary-local VSR client-table record.

Opening a router recovers complete commands from its VSR prepares and verifies
that existing protocol receipts agree with their committed VSR evidence.
`reconcile_application_commands` finishes an interrupted prepare/commit and
publishes its original protocol receipt. It can contact peers and fail when a
quorum is unavailable. Call it before serving application reads on recovery.
All router commit paths reconcile pending application commands before allocating
another global version, including writes to a different placement group. An
uncertain protocol-receipt persistence error fences further writes until reopen.
Placement updates cannot remove a group containing application history until
certified migration is implemented; otherwise restart could hide its commands.

`committed_application_commands` returns the locally certified commands in commit
version order for a materializer. Materializers must apply their own durable
receipt boundaries before exposing results. This API is not a distributed
read-index or a substitute for validating leadership and read consistency.

## Retries and compatibility

A retry must match the original namespace, surface, payload bytes, full operation,
placement group and consistency mode. It retains its original index and commit
version. Even a retry with an existing local protocol receipt reconciles the exact
command against the configured peer quorum before returning success. The legacy
metadata-only request API rejects retries of a complete application command.

Existing metadata-only VSR entries remain readable and writable through their
original API. They cannot be promoted into complete application commands by
supplying arbitrary retry values; explicit migration or original durable
application intent evidence is required. Complete commands use a new payload
format and must not be opened by old binaries that cannot interpret it.

The complete encoded command, including metadata and operation fields, must fit
within the VSR 1 MiB limit. Oversized commands fail before writing a prepare.
Namespaces and client identities are bounded at 4096 bytes; decoding checks
lengths, tags, consistency, operation identity, truncation and trailing bytes.
Large object/blob data still requires independent chunk transfer and repair.
The command inventory currently retains its history and needs the shared
receipt-retention and checkpoint policy before production-scale operation.

## Evidence

Protocol tests exercise binary codec round trips for every operation variant,
malformed/oversized input rejection, exact retries, namespace conflicts,
cross-placement-group version reservation, receipt persistence failure fencing,
and a real writer process killed after VSR commit but before a protocol receipt.
The killed writer's original values, version and request recover without a VSR
client-table record.

The peer RPC tests run complete protocol commands through the authenticated
network transport into two independent peer processes. After forcibly stopping
those peers, they reopen each peer's own WAL and verify its complete application
payload and certificate. A retry fails without a quorum and succeeds with its
original certificate after one peer restarts. These tests prove command-log
replication; application materialization, distributed view changes and daemon
failover remain separate requirements.
