# Network quorum readiness

Daemon health snapshots and `rsfd status` use the same authenticated replication
configuration as native data requests. A multi-voter inventory without a valid
`replication.json` is degraded. A single-voter store can run in standalone mode;
its transport never acknowledges a remote replica.

Readiness requires the existing profile, role, policy and membership checks plus
a current primary quorum in every placement group. The configured replica floor
and placement-group count must also be satisfied; a working smaller cluster does
not override an unfulfilled desired policy. Each remote vote is an
authenticated `PRIMARY_PROBE` RPC with a canonical `RSFV1` payload binding:

- placement group, current view and primary;
- the complete sorted voter set;
- committed log index and BLAKE3 digest of the encoded committed prepare.

For an empty log the digest covers the empty byte string. The peer compares the
entire payload with its own durable state and checks that the TLS-authenticated
sender is its current primary. A matching height alone cannot establish quorum.
Unknown probe RPCs fail closed, so older peers cannot supply readiness evidence.
Transports without the probe contract reject the operation by default.

The probe does not append commands, repair peers or change membership. Native
mutable reads first reconcile and repair through their existing path, then run
this same current-view check before exposing local data. Readiness is an
observation, not a lease; every read or write must still enforce its own contract.

## Operator behavior

`/healthz` adds `replication` (`standalone`, `network` or `unavailable`),
`quorum_available` and `replication_detail`. `/readyz`, `/healthz` and `/metrics`
return 503 while degraded. `/livez` remains 200 while the process is serving.
Metrics include `rsf_daemon_quorum_available` and one gauge for each replication
mode. `rsfd status` uses live quorum evidence for its placement-health report.

Unavailable peers, malformed or missing replication configuration, and mismatched
network identity are readiness failures. They do not count as consecutive daemon
runtime errors that shut down the health listener. The daemon retries observation
on later ticks and can become ready again after the peers or configuration recover.
Local storage corruption or unrecoverable inventory errors retain the existing
supervision behavior.

Each RPC has the configured absolute timeout. Probes currently visit peers and
groups sequentially; total observation time can therefore exceed one RPC timeout.
Concurrent bounded probing and incremental runtime recovery remain performance
work. Readiness neither performs automatic failover nor installs new membership.
Admin/policy mutations still have separate unfinished network integration.

## Verification contract

The peer RPC tests cover empty and nonempty logs, changed view, primary, voters,
and committed-tip digest, malformed probes, and rejection of a stale view after
peer restart without changing log height. The daemon process test covers live
health endpoints, one-peer loss, quorum loss, restart recovery, configuration
failure/recovery, unchanged certificate counts and CLI status. Fozzy runs these
through the peer RPC and daemon domains with real processes and recorded traces.

The full gate passed all 90 verification results across 15 domains. Local evidence
is in `artifacts/quorum-readiness-production.log`,
`artifacts/quorum-readiness-verification-summary.json` and
`artifacts/verification.IICf3O/`. These checks establish the tested readiness and
read contracts, not complete production failover or machine-failure durability.
