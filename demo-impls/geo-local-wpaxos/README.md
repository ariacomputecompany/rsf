# Geo-Local WPaxos Fabric

Run:

```text
cargo run -p rsfd -- demo run geo-local-wpaxos
cargo run -p rsfd -- demo run geo-local-wpaxos --out demo-impls/geo-local-wpaxos/runs/latest
```

Package manifest: `manifest.rsf`

This simulated implementation demonstrates:

- locality-adaptive ownership
- movable relational partitions
- object metadata heads owned near writers
- global immutable blob cache

Simulation note: ownership transfer is described in logs and topology metadata. The full WPaxos protocol is not implemented yet.

The generated `services/deployment.log` is the simulated package manifest: role assignments, service endpoints, durable volumes, and readiness probes for this architecture.
