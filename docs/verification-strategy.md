# RSF Verification Strategy

Date: 2026-09-10

## Principle

Testing is not enough for RSF. The database must be designed so its failure cases can be simulated, traced, replayed, shrunk, and audited.

## Running the release checks

Use the Fozzy determinism engine. `fz` is the FozzyLang compiler, and some
machines also install that compiler under the name `fozzy`. The verification
script checks the CLI identity and accepts an explicit engine path:

```sh
FOZZY_BIN=/path/to/fozzy FOZZY_SEED=20260930 sh scripts/verify-fozzy.sh
```

Each invocation creates a fresh `artifacts/verification.*` directory containing
host scenarios and recorded traces for storage, VSR, membership, protocol, KV
transactions, SQL, authentication, tenancy, the daemon, objects/blobs, recovery, policy, the POC, and the
complete workspace test suite.
It first runs five seeded doctor checks and strict deterministic tests. Those
checks use scripted process responses and verify scenario orchestration; they
do not execute Rust tests. The script then removes `proc_when` mock responses
from a copy of each scenario and runs its actual process steps with host
backends. Cargo must exit successfully. Host stdout and stderr are recorded
without asserting that variable test timing or scheduling matches empty mock
output. Every host trace must pass strict integrity verification, replay, and
CI. Replay verifies recorded observations; it does not constitute another fresh
execution of the Rust tests.

The script requires Python 3 to prepare host scenarios and Cargo to execute
them. A pass from a compiler shim or a scripted-only scenario is insufficient
release evidence. The historical run IDs below do not replace a fresh host
run against the current source.

SQL durability coverage includes transaction-control rejection through the
same parser used for execution, checkpoint byte corruption and truncation,
exclusive writer ownership, persistence-failure fencing, repeated reopen with
insert/index/update/delete, and recovered tuple history. Daemon TCP tests
assert returned SQL rows and KV/object/blob payloads after reopening storage.
SQL retry tests reopen both SQL state and the VSR router at the intent/commit/
checkpoint boundaries, assert a single commit per request, replay historical
results after update/delete, reject key conflicts, and recover from failed
checkpoint publication. These are local durable recovery tests; independent
network peers and machine-failure tests remain release requirements.

Standalone coverage writes and reads through the TCP listener, reopens state,
grows membership from one to two to three voters, and resumes a join interrupted
between protocol reconfiguration and admin promotion. VSR recovery tests replay
the original one-voter commits after growth using the persisted bootstrap
configuration. Exhaustive five-node configuration checks reject transitions
whose old and new majorities can be disjoint. These checks cover local state
and quorum rules; the default transport still acknowledges peers locally.
Pending joins make readiness degraded and reject data execution until the
protocol and admin voter sets agree. Uncommitted prepares cannot reuse votes
across a view change. Legacy logs without a bootstrap record still require
their original initial configuration; an explicit migration remains required
before upgrading such state and changing its topology.

Data-plane tests use real TLS 1.3 handshakes with generated test certificates.
They reject plaintext, missing/expired/untrusted certificates, unregistered
fingerprints, principal spoofing, missing grants, and disabled surfaces before
creating namespace storage. Tests revoke certificates and grants while the
listener remains running. SQL, KV, object, and blob tests use identical names
in separate tenant namespaces and verify independent data after reopen.
Local security command tests verify durable grants, revocation, and audit
records. Legacy shared data roots fail closed pending explicit migration.

## Required Verification Layers

### Deterministic Simulation

Every distributed protocol must run inside the simulator before it runs on real sockets.

Required controls:

- seed
- schedule
- message delay/drop/duplicate/reorder
- deterministic message delay/drop controls: started in `rsf-sim`, protocol transport tests, and fault logs
- disk write success/failure/torn write
- clock/timer progression
- process crash/restart
- trace output
- trace replay

### Invariant Checking

Core invariants:

- no acknowledged write is lost
- committed storage-tail evidence: started with `rsf-invariants` checks over recovered placement-group commit tails, committed prefixes, and manifest coverage proofs
- no stale primary commits
- stale primary rejection evidence: started with `rsf-invariants` checks over VSR stale-primary and lease fence rejections
- stale primary fence rejection: started in `rsf-lease` unit tests, durable lease recovery tests, and protocol stale-primary tests
- no two primaries commit conflicting entries for the same epoch/index
- committed log prefix is stable
- recovery never rolls back below a committed quorum point
- object manifest visibility is atomic
- chunk hash always verifies: started with `rsf-chunkstore` verified reads and `rsf-invariants` checks over chunkstore verification, dedupe, and rejection evidence
- relational table and index updates commit together: started with `rsf-relcommit` row/index commit-group audits, partial-group rejection, SQL DML protocol commits, and `rsf-invariants` relational commit-group atomicity coverage
- deferred relational constraints resolve at commit time: started with `rsf-deferred` transaction-local foreign-key obligations, transaction-local reference satisfaction, missing-reference rejection, and `rsf-invariants` deferred constraint coverage
- schema migrations advance through a durable lifecycle: started with `rsf-migration` validation, descriptor publication, backfill, index catch-up, resume, completion, rollback, rejected post-rollback advance, and `rsf-invariants` schema migration lifecycle coverage
- SQL requests validate before physical execution: started with `rsf-sqlpipeline` composed bind/auth/logical/physical/execute reports and `rsf-invariants` SQL pipeline coverage checks
- SQL DML writes commit atomically: started with `rsf-sqldml` composed INSERT/UPDATE/DELETE bind/auth/write-plan/transaction/isolation/KV-apply reports, SQL DML session-overlay staging/readback reports, and `rsf-invariants` SQL DML atomic mutation coverage
- SQL transaction control preserves read-your-writes, rollback invisibility, and savepoint rewind invisibility: started with `rsf-sqltxn` BEGIN/staged-DML/overlay-SELECT/SAVEPOINT/ROLLBACK TO SAVEPOINT/COMMIT/ROLLBACK/absence-proof/rejection reports and `rsf-invariants` SQL transaction lifecycle coverage
- consistency modes remain explicit: started with `rsf-consistency` strict-quorum/availability-first classification and `rsf-invariants` downgrade-rejection coverage
- object metadata heads become visible only after valid multipart metadata: started with `rsf-objectmeta` bucket/upload/part/head/rejection checks and `rsf-invariants` object metadata head visibility coverage
- strict sessions observe read-your-writes: started with `rsf-sessiontxn` overlay/snapshot/savepoint/partial-rewind/commit/rejection checks, SQL DML overlay staging integration, and `rsf-invariants` strict session read-your-writes coverage
- prepared SQL statements preserve typed bind and invalidation boundaries: started with `rsf-prepared` named statement preparation, typed parameter binding, schema-version invalidation, explicit deallocation, LRU eviction, rejection paths, and `rsf-invariants` prepared SQL lifecycle coverage
- SQL portals preserve client-facing request lifecycle boundaries: started with `rsf-portal` parse, bind, describe, execute, deadline rejection, cancellation, portal close, statement close, and `rsf-invariants` SQL portal lifecycle coverage
- relational row encoding round-trips typed physical rows: started with `rsf-rowcodec` schema-versioned encode/decode, default/generated column materialization, missing-required/type-mismatch rejection, and `rsf-invariants` relational row encoding coverage
- relational tuple visibility preserves MVCC version selection: started with `rsf-tuple` insert/update/delete tuple headers, old/new snapshot visibility, delete invisibility, duplicate-live/missing-update rejection, and `rsf-invariants` relational tuple visibility coverage
- relational vacuum preserves oldest-snapshot safety: started with `rsf-vacuum` prune/retain planning over tuple chains, reclaimed-byte accounting, empty-chain rejection, and `rsf-invariants` relational vacuum safety coverage
- relational row materialization preserves executor handoff: started with `rsf-materialize` full-row materialization, explicit projection, table/projection rejection, and `rsf-invariants` relational row materialization coverage
- primary indexes preserve row identity: started with `rsf-primary` insert, lookup, update, delete, duplicate-key rejection, missing-key lookup, and `rsf-invariants` primary-index identity coverage
- ordered range split boundaries remain non-overlapping: started with `rsf-range` split non-overlap checks, overlap rejection evidence, durable range recovery, requirement coverage, and `rsf-invariants` ordered range non-overlap coverage
- multi-surface extent layout remains format-independent: started with `rsf-invariants` checks over manifest coverage and overlap rejection
- local checkpoint compaction preserves required roots: started with `rsf-checkpoint` root/segment planning and `rsf-invariants` checks over retained roots, compacted segments, reclaimable bytes, and regression rejection
- snapshot and recovery root inventories match: started with `rsf-invariants` checks over durable snapshot, superblock, backup, and recovery roots
- learner promotion preserves quorum intersection: started with `rsf-membership` old/new quorum intersection proofs, durable admin membership recovery, requirement coverage, and `rsf-invariants` learner-promotion quorum-intersection coverage
- rebalancing preserves placement policy: started with `rsf-rebalance` per-move placement-policy proofs, durable rebalance recovery, requirement coverage, and `rsf-invariants` rebalance placement-policy coverage

### Fault Matrix

Minimum fault matrix:

- primary crash before prepare
- primary crash after prepare before commit
- primary crash after reply
- backup crash before fsync
- backup crash after fsync
- minority partition
- asymmetric partition
- network heal
- duplicate client request
- stale client view
- corrupted WAL record
- corrupted chunk
- slow replica
- partial snapshot transfer
- node rejoins with old epoch
- node joins during rebalancing

### Trace Requirements

Every shipped protocol change should produce:

- recorded passing trace
- replayed passing trace
- shrunk failing trace when a bug is found
- invariant coverage report

Per the local project instructions, Fozzy should be preferred for deterministic scenario execution where applicable:

```text
fozzy doctor --deep --scenario <scenario> --runs 5 --seed <seed> --json
fozzy test --det --strict-verify <scenarios...> --json
fozzy run ... --det --record <trace.fozzy> --json
fozzy trace verify <trace.fozzy> --strict --json
fozzy replay <trace.fozzy> --json
fozzy ci <trace.fozzy> --json
```

For Fozzy's built-in distributed scenario files, use `explore`:

```text
fozzy validate tests/rsf-bootstrap.pass.fozzy.json --json
fozzy explore tests/rsf-bootstrap.pass.fozzy.json --json
```

Current first scenario:

- [rsf-bootstrap.pass.fozzy.json](../tests/rsf-bootstrap.pass.fozzy.json)
- Latest verified command: `fozzy explore tests/rsf-bootstrap.pass.fozzy.json --json`
- Result: pass, run id `cff4d5c3-7991-44b7-bbee-491ef645c963`

Current host-backed storage scenario:

- [storage-host.pass.fozzy.json](../tests/storage-host.pass.fozzy.json)
- Command: `fozzy run tests/storage-host.pass.fozzy.json --det --record artifacts/storage-host.trace.fozzy --json`
- Strict deterministic result: pass, run id `1ed8e21b-71d0-4ed1-93c6-39cf63c83a5a`
- Strict doctor command: `fozzy doctor --deep --scenario tests/storage-host.pass.fozzy.json --runs 5 --seed 238 --strict --host-backends --json`
- Strict doctor result: pass, deterministic signature `a2027c754cec540c71ac6ab1b618a6b4c21505adbdbfb436e11ad634d4a77353`
- Strict test command: `fozzy test tests/storage-host.pass.fozzy.json --det --strict-verify --json`
- Strict test result: pass, run id `1818d546-fb4e-492b-a67b-ce438017b22f`
- Host-backend command: `fozzy run tests/storage-host.pass.fozzy.json --det --host-backends --record artifacts/storage-host-20260911-scan-truncate.trace.fozzy --json`
- Host-backend result: pass, run id `a8a6bee8-1538-4de6-a793-06fae99b3e41`
- Host-backed replay result: pass, run id `386d4055-2cc2-479a-80e3-6e4338067b4a`
- Host-backend trace verification, replay, and CI result: pass

Current host-backed protocol scenario:

- [protocol-host.pass.fozzy.json](../tests/protocol-host.pass.fozzy.json)
- Strict doctor command: `fozzy doctor --deep --scenario tests/protocol-host.pass.fozzy.json --runs 5 --seed 242 --json`
- Strict doctor result: pass, deterministic signature `f39aa441d4ad591cee5a1d650420ae70b068314d3d7b9953db8a8a07c961a15e`
- Strict test command: `fozzy test tests/protocol-host.pass.fozzy.json --det --strict-verify --json`
- Strict test result: pass, run id `b0980623-2368-4c14-8746-1681c5a27c37`
- Command: `fozzy run tests/protocol-host.pass.fozzy.json --det --record artifacts/protocol-host-20260911-segment-store-metadata-truncate.trace.fozzy --proc-backend host --fs-backend host --http-backend host --json`
- Host-backend result: pass, run id `a44d2e59-1d3b-45a6-937e-ad8de8e3e453`, trace `artifacts/protocol-host-20260911-segment-store-metadata-truncate.trace.fozzy`
- Host-backed replay result: pass, run id `66c17e10-eca6-4486-a395-2ab8d90b8119`
- Host-backend trace verification, replay, and CI result: pass

Current host-backed VSR scenario:

- [vsr-host.pass.fozzy.json](../tests/vsr-host.pass.fozzy.json)
- Strict doctor command: `fozzy doctor --deep --scenario tests/vsr-host.pass.fozzy.json --runs 5 --seed 240 --strict --host-backends --json`
- Strict doctor result: pass, deterministic signature `4100078a169474ca38b3edeb75f19160af8f1d1ac7827b6af280b119f028710c`
- Strict test command: `fozzy test --det --strict-verify tests/vsr-host.pass.fozzy.json --json`
- Strict test result: pass, run id `81afc0f5-74c5-4b86-95e9-846c47cdd93a`
- Command: `fozzy run tests/vsr-host.pass.fozzy.json --det --host-backends --record artifacts/vsr-host-20260911-rotation-compaction-safety.trace.fozzy --json`
- Host-backend result: pass, run id `bc104f9d-adc2-4bfc-87e6-b52a30329066`, trace `artifacts/vsr-host-20260911-rotation-compaction-safety.trace.fozzy`
- Host-backed replay result: pass, run id `2e3d49d7-0395-4e8d-affa-17b1ea7f6c41`
- Host-backend trace verification, replay, and CI result: pass

Current host-backed legacy POC scenario:

- [poc-host.pass.fozzy.json](../tests/poc-host.pass.fozzy.json)
- Command: `fozzy run tests/poc-host.pass.fozzy.json --det --host-backends --record artifacts/poc-host-20260910.trace.fozzy --json`
- Host-backend result: pass, run id `4ac34123-8fb0-4922-b50b-9bae07f0aa5a`
- Host-backend trace verification, replay, and CI result: pass

Current host-backed legacy POC artifact scenario:

- [poc-output-host.pass.fozzy.json](../tests/poc-output-host.pass.fozzy.json)
- Strict doctor command: `fozzy doctor --deep --scenario tests/poc-output-host.pass.fozzy.json --runs 5 --seed 245 --json`
- Strict doctor result: pass, deterministic signature `0047ce30480711a472dcc13b9ec9dfb58952626f4ce46a1774fcb4b9afe0ed47`
- Strict test command: `fozzy test --det --strict-verify tests/poc-output-host.pass.fozzy.json --json`
- Strict test result: pass, run id `0f0f03be-45ff-4b71-9109-71896a7ab0fd`
- Command: `fozzy run tests/poc-output-host.pass.fozzy.json --det --record artifacts/poc-output-host-20260911-blake3-content-id.trace.fozzy --proc-backend host --fs-backend host --http-backend host --json`
- Host-backend result: pass, run id `d0a3a80b-c6f9-4bda-aef0-9c5b5496e68d`, trace `artifacts/poc-output-host-20260911-blake3-content-id.trace.fozzy`
- Host-backed replay result: pass, run id `cfa04dc7-663a-4cb2-ab80-8dfd935dec43`
- Host-backend trace verification, replay, and CI result: pass

## Model Checking

Use a small formal/model-checking spec for:

- VSR cell normal operation
- view change
- membership reconfiguration
- learner promotion
- object manifest visibility
- placement-group rebalancing

The model should prove safety first. Liveness comes after the safety model is stable.

## Performance Verification

Performance tests must be tied to semantics:

- strict single-region write latency
- follower/nearest read latency
- object multipart throughput
- blob chunk throughput
- relational point lookup latency
- range scan throughput
- recovery time after primary failure
- rebalancing impact on p99
- scrub impact on foreground reads/writes

## Release Gate

No RSF protocol or storage-engine release is acceptable unless:

- deterministic simulator passes
- trace replay passes
- corruption tests pass
- crash/restart tests pass
- at least one real host-backed run passes
- performance regressions are explained
- consistency mode behavior is documented
