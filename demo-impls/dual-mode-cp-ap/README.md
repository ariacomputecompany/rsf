# Dual-Mode CP/AP Fabric

Run:

```text
cargo run -p rsfd -- demo run dual-mode-cp-ap
cargo run -p rsfd -- demo run dual-mode-cp-ap --out demo-impls/dual-mode-cp-ap/runs/latest
```

Package manifest: `manifest.rsf`

This simulated implementation demonstrates:

- CP relational catalog and transactions
- strict object manifests
- immutable blob replication
- AP edge namespaces for data that can reconcile later

Simulation note: consistency-mode boundaries are logged explicitly. The AP reconciliation engine is not implemented yet.

The generated `services/deployment.log` is the simulated package manifest: role assignments, service endpoints, durable volumes, and readiness probes for this architecture.
