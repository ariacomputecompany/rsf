# Aurora/Socrates Relational-First Fabric

Run:

```text
cargo run -p rsfd -- demo run aurora-socrates
cargo run -p rsfd -- demo run aurora-socrates --out demo-impls/aurora-socrates/runs/latest
```

Package manifest: `manifest.rsf`

This simulated implementation demonstrates:

- page/WAL-native relational storage
- quorum log durability
- page-server replay
- object/blob APIs layered over protection-group manifests and chunks

Simulation note: page servers, log propagation, and protection groups are represented by structured logs and WAL records.

The generated `services/deployment.log` is the simulated package manifest: role assignments, service endpoints, durable volumes, and readiness probes for this architecture.
