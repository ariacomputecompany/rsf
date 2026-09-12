# Universal VSR-RADOS Fabric

Run:

```text
cargo run -p rsfd -- demo run universal-vsr-rados
cargo run -p rsfd -- demo run universal-vsr-rados --out demo-impls/universal-vsr-rados/runs/latest
```

Package manifest: `manifest.rsf`

This simulated implementation demonstrates the recommended core path:

- RADOS-style placement groups
- VSR strict metadata cells
- content-addressed chunks
- ordered KV-backed relational tables
- object manifests
- blob chunks

Simulation note: the services return deterministic outcomes and emit logs. Consensus, network, and disk behavior are represented by service boundaries and WAL records, not by full protocol execution yet.

The generated `services/deployment.log` is the simulated package manifest: role assignments, service endpoints, durable volumes, and readiness probes for this architecture.
