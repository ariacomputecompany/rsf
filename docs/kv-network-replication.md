# Network-replicated KV execution

The native KV data plane now submits namespace-bound complete commands through
`VsrBackedFabricRouter::commit_application`. An acknowledged write has its exact
request identity, namespace, keys, values/tombstones, and commit version in VSR.
The primary persists a local intent before committing and an atomic value/receipt
record before replying. On reopen it reconciles protocol commands, applies the
certified KV command history, and resolves the local intent before serving data.

A one-voter data plane uses an explicit standalone transport that cannot
acknowledge a remote replica. Multiple voters require authenticated network
configuration. The daemon data plane no longer constructs `LocalVsrTransport`.
Admin/policy orchestration still has local transport paths; automated network
membership changes and failover remain release blockers.

## Primary configuration

Place `replication.json` in the daemon state directory:

```json
{
  "cluster": "my-cluster",
  "node": 0,
  "tls": "peer-tls",
  "timeout_ms": 1000,
  "peers": [
    {"node": 0, "address": "127.0.0.1:7200", "server_name": "node-0", "fingerprint": "<64 hexadecimal SHA-256 digits>"},
    {"node": 1, "address": "127.0.0.1:7201", "server_name": "node-1", "fingerprint": "<64 hexadecimal SHA-256 digits>"},
    {"node": 2, "address": "127.0.0.1:7202", "server_name": "node-2", "fingerprint": "<64 hexadecimal SHA-256 digits>"}
  ]
}
```

Replace the example addresses, names and fingerprints with actual peer identities.
Relative TLS paths resolve against the state directory. The TLS directory contains
`certificate.pem`, `key.pem` and `ca.pem`, as described in
[peer replication](peer-replication.md). Every configured fingerprint must equal
the admitted node's SHA-256 certificate identity. All voters must be mapped, the
local node must be the selected primary, and duplicate/unknown fields are rejected.
The cluster/node binding is persisted under `replication-identity/IDENTITY.json`;
changing configuration cannot silently move an existing state to another cluster.

Start the other nodes with `rsfd peer serve --config <peer.json>`. Their cell views,
primary and voter sets must match the primary's persisted VSR membership; the
`VsrBackedFabricRouter::membership` API exposes those values. Editing bootstrap
configuration does not perform a membership change. New peers cannot catch up
entries certified under older voter configurations until historical-membership
transfer is implemented; changing the bootstrap view cannot bypass that check. Automatic provisioning and
network view exchange are still required before this becomes the final join UX.
Then start the primary's ordinary `rsfd serve` data listener with its existing
client TLS, authorization and tenancy configuration.

## Follower application state

Peer services apply supported complete commands before acknowledging COMMIT or
committed-prefix transfer. Startup replays committed commands before readiness.
KV data lives beneath the peer state's
`application/namespaces/<SHA-256-of-namespace>/kv` directory, using the same durable
KV value/receipt format as the primary. Only authenticated, VSR-validated entries
reach the materializer. It checks surface, namespace, request identity, exact
operation and payload before applying. Replaying an old receipt cannot resurrect
a newer value or tombstone. Application errors reject the RPC; retry/restart
replays the already durable command.

Network KV GET confirms a current primary quorum before reading local values.
The caller serializes reads with writes. The configured primary must still match
each cell's persisted primary. Requests fail when quorum is unavailable; local
receipts alone do not make a network retry successful. This does not implement
leader election or distributed view reconciliation.

## Compatibility and current limits

KV stores opened for replication persist a namespace binding. They cannot reopen
under a different namespace or silently revert to metadata-only commits. A
nonempty legacy store requires explicit migration; refusing the new mode leaves
its old data intact. Do not delete the namespace binding or receipt history.

Network SQL and object execution currently rejects requests because their
complete surface codecs/materializers are unfinished. Blob replication is described in
[the blob command contract](blob-network-replication.md). SQL and object remain available in
standalone mode. Raw legacy VSR operations remain log-only and are not presented
as materialized application commands. The primary's native frame limit remains
64 KiB, and complete VSR commands must fit the 1 MiB command bound. KV validates
command bounds before persisting an intent, so invalid input cannot strand it.

Cross-group application scheduling, incremental replay/checkpoint cursors,
bounded request-history retention, migrations, automated membership/failover,
and network-aware operator readiness are still release work. Existing health
snapshots report the admin/policy inventory and do not establish live network
quorum availability. Do not use those snapshots alone as a production-readiness
proof.

## Verification

`native_kv_ingress_replicates_and_materializes_on_real_daemon_peers` runs the actual
`rsfd` binary for a primary and two peers with separate directories and TLS
identities. It checks rejection without network configuration, writes with both
peers, writes after one peer is killed, simultaneous primary/live-follower
termination, independent recovery of follower values, missing-prefix repair on a
lagging peer, original-receipt retry, changed-value rejection, and read/retry
rejection after quorum loss. Unit tests also cover tombstones, historical reads,
namespace mismatch, legacy migration refusal and oversized-input rejection before
intent persistence. The daemon and KV Fozzy domains execute these tests through
real host processes, then verify, replay and CI-check their recorded traces.

These checks cover process failure, not machine power loss or complete multi-surface
production failover.
