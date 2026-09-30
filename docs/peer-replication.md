# Authenticated VSR peer replication

`rsf-rpc::replication::NetworkVsrTransport` sends complete prepares, commit
certificates, committed-tail queries, and committed-prefix entries over TLS
1.3. Each voter owns a separate `DurableVsrCell` store. The peer process is:

```sh
cargo run -p rsfd -- peer serve --config peer-1.json
```

The router can replicate complete versioned application commands through this
service using `commit_application`; see [the command contract](replicated-application-commands.md).
The native KV data plane now uses this path with namespace-bound complete commands;
peer services materialize those commands durably before acknowledging them. See
[KV network execution](kv-network-replication.md) for primary configuration and
recovery behavior. SQL/object/blob replay, automatic membership and failover are
still unfinished. Admin/policy paths still use the in-process transport in places;
`LocalVsrTransport` itself remains a simulation with no remote disk durability.

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

Each `peers` object requires `node`, `address` (numeric IP and port),
`server_name` (the TLS certificate's DNS name or IP), and `fingerprint` (64
hexadecimal digits encoding SHA-256 of the DER leaf certificate). Node IDs and
certificate fingerprints must be unique. The local fingerprint must match the
configured certificate. A certificate must permit both client and server
authentication; the private key must match it. `ca.pem` is the peer CA trust
bundle, separate from application-client authorization.

Each `cells` object requires `pg`, positive `view`, `primary`, and `voters`.
Groups and voters must be unique. Every voter must have a peer mapping, and
both the local node and primary must be voters. At most 4096 voters are allowed
per cell. Existing WAL membership is authoritative after initialization;
editing bootstrap fields does not install a new view or change membership.
Quorum-certified view exchange and topology changes over this network service
remain unfinished.

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
membership installed. This implementation assumes non-Byzantine cluster
members; signer sets are quorum evidence maintained by the authenticated
primary, not independent cryptographic signatures from each voter.

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
