# Authenticated VSR peer replication

`rsf-rpc::replication::NetworkVsrTransport` sends complete prepares, commit
certificates, committed-tail queries, current-primary probes, and committed-prefix entries over TLS
1.3. Each voter owns a separate `DurableVsrCell` store. The peer process is:

```sh
cargo run -p rsfd -- peer serve --config peer-1.json
```

The router can replicate complete versioned application commands through this
service using `commit_application`; see [the command contract](replicated-application-commands.md).
The native KV, SQL, object and blob data planes now use this path with namespace-bound complete commands;
peer services materialize those commands durably before acknowledging them. See
[KV network execution](kv-network-replication.md) for primary configuration and
recovery behavior, and [blob network replication](blob-network-replication.md) for
immutable payloads. [Object replication](object-network-replication.md) covers
buckets and mutable heads, and [SQL replication](sql-network-replication.md) covers
autocommit statements. Opt-in peer elections and client takeover are implemented. Certified membership changes and deployment qualification remain unfinished. Admin/policy paths still use the in-process transport in places;
`LocalVsrTransport` itself remains a simulation with no remote disk durability.

Peer services retain up to 64 recently used namespace/surface application
writers. A hot store keeps its recovered state and exclusive writer ownership
across commits. Eviction releases that ownership before opening another store;
reopening and process restart use normal durable recovery. Application failures
are still returned before any successful acknowledgement, and a resident writer
that has fenced itself after a durability failure remains fenced.

The native primary reuses one namespace/surface runtime within a batch of at
most 64 accepted data requests. It releases the runtime before health sampling
or an idle return. Every request still checks current authorization; reuse also
rechecks admitted membership, coordination epoch, control metadata and current
replication configuration. A switch of namespace/surface or an execution error
requires recovery again. This bounds ownership and prevents an indefinitely
busy data queue from starving health sampling, but does not provide incremental
recovery of arbitrarily large histories.

## Configuration

The JSON configuration rejects unknown fields. Relative `state` and `tls` paths
resolve against the configuration file's directory.

| Field | Required value |
| --- | --- |
| `cluster` | Stable cluster identity shared by all peers |
| `node` | This process's unsigned numeric node ID |
| `state` | This node's exclusive durable peer-state directory |
| `tls` | Directory containing `certificate.pem`, `key.pem`, and `ca.pem` |
| `listen` | Numeric IP and port for this peer listener |
| `timeout_ms` | Absolute RPC deadline, 1 through 30000 milliseconds |
| `peers` | Array of node endpoint objects, including this node |
| `cells` | Nonempty array of placement-group bootstrap configurations |
| `elections` | Optional peer election timing object; omitted by default |
| `client` | Optional application-client listener with provisioned admission state |

Each `peers` object requires `node`, `address` (numeric IP and port),
`server_name` (the TLS certificate's DNS name or IP), and `fingerprint` (64
hexadecimal digits encoding SHA-256 of the DER leaf certificate). Node IDs and
certificate fingerprints must be unique. The local fingerprint must match the
configured certificate. A certificate must permit both client and server
authentication; the private key must match it. `ca.pem` is the peer CA trust
bundle, separate from application-client authorization.

Each `cells` object requires `pg`, positive `view`, `primary`, and `voters`.
Groups and voters must be unique. Every voter must have a peer mapping, and
the primary must be a voter. The local node is a voter unless the cell explicitly
sets `learner_source`; see [learner history transfer](learner-history-transfer.md)
for non-voting configuration, resumable catch-up, and role restrictions.
At most 4096 voters are allowed
per cell. Existing WAL membership is authoritative after initialization;
editing bootstrap fields does not install a new view or change membership.
Authenticated quorum view installation is implemented. Quorum-certified
topology changes remain unfinished.

To enable automatic peer elections, add for example:

```json
"elections": {"heartbeat_ms": 100, "timeout_ms": 1500, "rank_delay_ms": 1000}
```

This example uses a top-level RPC `timeout_ms` of 150 in a three-voter
group. Election timeout must cover twice the sum of remote RPC deadlines plus
three heartbeat intervals. Rank delay must lie between heartbeat and election
timeout. Heartbeat is 1–60000 ms; election timeout is at most one hour. Each
hosted voting group must satisfy the timing bounds before stores are opened;
a learner-only service cannot enable elections. The controller acts only on
voting groups. The `elections` object enables peer consensus and materialization. Add `client`
to enable the replacement application-client listener. `peer.election` JSON
events report changed controller observations, not a client readiness lease.
See [primary failover](primary-failover.md) for recovery and remaining integration.

The service persists `IDENTITY.json` before opening cells and refuses to reuse
the directory under a different cluster or node. Unbound directories containing
existing data require explicit migration. A process ownership lock
prevents simultaneous servers over the same state. TLS identities and mappings
are loaded at startup; changes require restarting the service. Readiness and
accepted/rejected RPC events are emitted as JSON. Private keys and command
payloads are not logged.

## Commit and recovery contract

The primary first repairs a follower's missing committed prefix, then sends
its prepare. A follower verifies the authenticated primary and active view,
validates the payload, and syncs its WAL before acknowledging. The primary
collects a quorum, persists the certificate, and distributes it. The router
publishes a protocol certificate only after a quorum confirms durable commit
evidence. A timeout after the local commit is an uncertain outcome: retrying
the same request reconciles peers and publishes the existing commit without
creating a second operation.

Every frame binds protocol version, cluster digest, authenticated sender,
destination, and request type. The client validates the server CA/name and
exact node fingerprint before sending a command. The server requires a client
certificate and resolves its fingerprint to a node before dispatch. Frames,
commands, prefix entries, and response bodies are bounded. Each connection has
one absolute deadline covering connect, handshake, request, and response.
Storage fsync itself is not cancellable by a socket deadline.

Prefix repair transfers one certified entry at a time and rejects gaps or
conflicting committed history. Matching log heights alone never satisfy a
retry: the common committed entry and the exact retried command are checked.
Peers must already have the necessary view and
membership installed. [Historical certificate validation](vsr-configuration-history.md)
uses each entry's original installed primary, voters, quorum and committed
boundary, including after restart. Missing configuration history is rejected.
This implementation assumes non-Byzantine cluster
members; signer sets are quorum evidence maintained by the authenticated
primary, not independent cryptographic signatures from each voter.

See [network readiness](network-readiness.md) for the current-view probe and
operator health contract.

## Verification

`cargo test -p rsf-rpc` starts independent peer processes and checks full binary
payload recovery, one-follower loss, quorum loss, restart catch-up, wrong
cluster/node/primary rejection, deadlines, and lost commit-response recovery.
`cargo test -p rsfd --test peer_process` starts the actual `rsfd` executable,
replicates through two peer services, forcibly terminates them, reopens their
independent stores, and verifies persistent cluster/node binding.

`scripts/verify-fozzy.sh` includes a dedicated peer RPC domain and the daemon CLI
integration test through its daemon/workspace domains. These are host process
and network tests; machine power-loss and complete application-state failover
still need separate verification.

## Automatic client service

An optional `client` object serves the existing canonical RSF wire protocol for
SQL/KV/object/blob requests from the peer's actual installed cells:

```json
"client": {
  "state": "/srv/rsf/control",
  "listen": "10.0.0.1:7300",
  "endpoints": [
    {"node": 0, "address": "10.0.0.1:7300"},
    {"node": 1, "address": "10.0.0.2:7300"},
    {"node": 2, "address": "10.0.0.3:7300"}
  ]
}
```

`state` resolves relative to the peer configuration. It must already contain
application TLS (`tls/server.pem`, `server-key.pem`, `client-ca.pem`), the admitted
node catalog under `admin`, and explicitly provisioned `auth` and `tenancy` data.
The service creates no application grants. Admitted node fingerprints must match
the authenticated peer mapping, and admitted voters must match installed
membership. Each process needs the current admission policy; network distribution
of those control catalogs remains separate release work.

Client endpoints require unique configured node IDs, nonzero ports, coverage of
all peer mappings, and a local endpoint equal to `listen`. Automatic elections
must be enabled, and the process cannot host learner cells. With client service,
the minimum election timeout also includes the two-second absolute client socket
deadline. A three-voter example uses top-level `timeout_ms: 500` and elections
`heartbeat_ms: 200`, `timeout_ms: 5000`, `rank_delay_ms: 1500`. Socket deadlines do
not preempt fsync or recovery work; growing-history latency still needs qualification.

Application data stays in the peer's `application/namespaces` stores. Derived
coordination receipts live under its `client-protocol` directory. Peer control and
one accepted client connection per tick share exclusive cell custody and the same
resident application writers. A replacement reconciles installed complete
commands and confirms a fresh quorum before serving. Every request rechecks
current authorization. Existing legacy `serve` listeners do not acquire this
behavior automatically.

An authorized request sent to a follower receives:

```text
RSF/1 redirect protocol=kv-http mutation=not-attempted primary=1 address=10.0.0.2:7300
```

Clients should authenticate the destination and resend the identical request
identity and payload. Redirects are advisory; the destination repeats authority
and quorum checks. A fenced view or different primaries across hosted placement
groups returns a rejection. Independently led groups require further scheduling
and routing work. A local primary's mutation error remains `mutation=unknown`
when durable admission may already have occurred; retrying the same identity
resolves the outcome.

## Resident client coordination

The opt-in client endpoint retains derived coordination between requests while
the peer keeps exclusive ownership of every voting cell. Cache entries contain
no cells while idle. Reuse requires the exact live handles, unchanged cell
mutation tokens, matching local node/coordination epoch/path, and an unchanged
validated protocol-store revision. Reopen, a durable promise, an installation,
a peer-side receipt/write, or changed derived metadata invalidates that entry.
Callback errors and unwinding return all cells and discard derived state.

All protocol groups share one physical SegmentStore writer. A logical receipt
cursor follows the validated physical tail, so appending another receipt does
not replay unchanged history. Changed files/layouts invalidate the storage
cursor and undergo recovery/checksum validation. An observer epoch distinguishes
own writes from another handle's changes; a change detected during coordination
returns an uncertain error and discards the cache. This observation is not a
persistent checkpoint or authorization lease.

Pending commands and certified application versions have separate ordered
indexes. Healthy requests reconcile outstanding work and apply only payloads
newer than the materializer cursor. Successful requests mark the complete local
prefix materialized before returning custody, avoiding a redundant full-prefix
application on the next heartbeat. Failures retain the pending-materialization
fence. Every request still rechecks authorization, admitted identities, installed
authority, and fresh quorum.

On cold takeover, installed committed payloads may lack local derived receipts.
The coordinator repairs/confirms the committed prefix, reconstructs only matching
receipts in global version order, and confirms the prefix/quorum again. Accepted
but uncommitted commands use ordinary fresh replication. Failure of either
confirmation prevents success. This avoids separate network round trips for
every already-committed historical receipt.

`peer.client_cache` events report full recovery attempts and retained uses.
Required debug/release automatic-client workloads grow 128 acknowledged writes
before crashing the primary, require at least 128 retained uses, then resolve
32 concurrent identities and inspect all 162 logical KV writes on both survivors.
They also exercise SQL/object/blob recovery, authorization revocation, and restart.
These are single-host correctness/work-count measurements. Cold history loading,
full-history elections, retained payload memory, segment metadata checks, SQL
checkpoint cost, receipt retention, and independent-host capacity still need
checkpointing/retention work and separate qualification.

A global payload cursor does not establish the completeness of a newly opened
application writer. Reopened/evicted writers replay their own namespace and
surface from certified commands before execution, even when the coordinator
remains resident. A regression deletes an evicted derived KV store and requires
the next read to return its committed value, rather than a false miss. This cold
namespace replay remains proportional to its retained history; durable checkpoints
are still required to bound reconstruction and memory at large scale.
