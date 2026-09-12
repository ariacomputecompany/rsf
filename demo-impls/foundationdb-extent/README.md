# FoundationDB Over Extent Fabric

Run:

```text
cargo run -p rsfd -- demo run foundationdb-extent
cargo run -p rsfd -- demo run foundationdb-extent --out demo-impls/foundationdb-extent/runs/latest
```

Package manifest: `manifest.rsf`

This simulated implementation demonstrates:

- sequencer/proxy/resolver/storage-style roles
- ordered transactional KV as the universal metadata layer
- immutable extents for object/blob bodies
- SQL layered over ordered tuple-like keys

Simulation note: the run logs show the intended role boundaries and data flow. The transaction resolver and sequencer are simulated services, not complete implementations.

The generated `services/deployment.log` is the simulated package manifest: role assignments, service endpoints, durable volumes, and readiness probes for this architecture.
