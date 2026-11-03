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

By default, each invocation creates a fresh `artifacts/verification.*` directory containing
host scenarios and recorded traces for storage, VSR, membership, protocol, authenticated peer RPC, KV
transactions, SQL, authentication, tenancy, the daemon, objects/blobs, recovery, policy, the POC, and the
complete workspace test suite, plus automatic client takeover in debug and release.
Use `--artifact-dir artifacts/verification.example` to choose a new directory.
The runner saves each phase's raw outputs and exit codes alongside an atomic
run checkpoint. Run `python3 -B scripts/test_verify_fozzy.py` to check the bookkeeping;
those isolated fake-engine tests do not qualify RSF itself.

An existing run can be resumed with `--resume <artifact-directory>`. Completed
domains are reused only after checking their complete six-phase evidence,
source, engine, toolchain, Cargo configuration and test-concurrency identity.
Recorded failures stay failed. An observed identity change invalidates that
run permanently; use a fresh directory after fixing the source.

If the checkpoint has an active owner, inspect the runner and all relevant
Fozzy/Cargo/daemon descendants before resuming. A released lock or missing tool
session does not prove those processes have stopped. The `--no-live-processes`
option requires a JSON attestation with `version: 1`, the absolute
`artifact_root`, current `checkpoint_sha256`, `no_live_processes: true`, a
nonempty `checked_by` description, and the absolute `evidence` path and
`evidence_sha256` for the saved process inspection. The runner retains that
attestation and any interrupted attempt before creating a new attempt. It does
not import older uncheckpointed runs or overwrite their failures.

It also runs two real-process KV stress workloads:

| Profile | Clients | Acknowledged logical writes | Forced process shutdowns | Uncertain writes resolved |
| --- | --- | --- | --- | --- |
| Debug | 4 | 198 | 18 | 3 |
| Optimized release | 4 | 774 | 18 | 3 |

Both workloads use actual TLS connections and independent daemon state
directories. They grow history across three rounds, remove a minority and then
the remaining quorum, kill/restart all processes, retry original request IDs,
and inspect durable values and receipt counts on both followers. A write
without a successful acknowledgement may still commit during recovery; the
test resolves that uncertainty with the same request identity and requires
exactly one applied version. Client I/O timeout limits stay enabled.

The script continues independent domains after a failure and writes
`summary.json` with their individual results, workload counts, build profiles,
latency/throughput measurements and source fingerprints. Overall success
requires all 25 domains, the expected real workload evidence, and unchanged
Rust/test/script inputs throughout the run. A filter accidentally running zero
stress tests, or edits made during verification, cannot produce a successful
release summary. Hardware/WAN capacity targets and the complete machine-failure
matrix still require separate qualification; these single-host measurements
are not a production capacity guarantee.

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

SQL page tests cover 160 physical pages with a 128-frame buffer, growing-row
relocation, physical snapshot replay and canonical KV recovery followed by
writes. Atomic relocation redo is tested with either, both, or neither page
already flushed, duplicate records, reversed input ordering across two moves,
conflicting records, and every truncated prefix of a move record. Replicated
SQL tests reopen the follower after each command, then check grown values,
deletions, original receipt retries, and post-restart writes. Oversized rows
must fail before protocol commit. These tests establish functional behavior;
they do not qualify SQL throughput or replace incremental persistence.

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

View-change tests persist a mandatory promise record before exporting recovery
input. They check all majority subsets of three and five voters after reopen,
delayed old-view certificates, forbidden post-fence WAL writes, truncation,
conflicting candidates, and frozen snapshot identity over real TLS process
restarts. Selection tests replay the reporting quorum, reject forged or forked input,
and preserve commit evidence and client identities. Local installation tests
exercise successive generations, abrupt process exit at publication boundaries,
corrupt pointers/seals, idempotent retries after later commits, and generation
reclamation. A TLS process test installs the selected view on a running remote
survivor and resumes replication from the replacement primary. Another kills
the recipient mid-upload and restarts the candidate after local publication;
both resume the same durable input. Tests reject changed/gapped uploads and
wrong candidates, and require materialization to succeed before a finish
acknowledgement or normal traffic, including after restart. A minority installation
cannot report quorum success. These do not establish automatic primary takeover:
election scheduling, application recovery integration, and daemon/client
orchestration remain required. Mixed-view tests select the latest normal view,
merge late older certificates without reopening stale traffic, reject committed
forks, and recover after a candidate alone activated before restart. Legacy
installation-marker replay and equivalent retry across codec versions are also
covered. See
`primary-failover.md` for the implemented boundary.

Installed-cell coordinator recovery is tested after killing the original daemon
and deleting its entire state directory. The replacement reconstructs metadata
and application receipts from survivor logs, retries original certificates, and
commits new SQL/KV/object/blob writes through a running TLS peer. Both stores are
reopened to inspect values and exact histories; strict read confirmation fails
without quorum and succeeds after the peer restarts. Unit checks cover receipt
persistence failure, exclusive ownership handoff, conflicting inventories and
receipts, protocol codec bounds, and request collisions. The test invokes the
recovery API directly; it does not establish automatic daemon takeover.
Adoption rejects ordinary SQL/KV/object/blob protocol records without their
surface payloads. Internal object pins use an explicit immutable-dependency
encoding; tests cover orphan recovery, exact identity retries, wrong digests,
purpose confusion, routed user-blob retries, and preflight. Object process-kill
tests preserve the dependency tag across both pin and object-commit boundaries.

Data-plane tests use real TLS 1.3 handshakes with generated test certificates.
They reject plaintext, missing/expired/untrusted certificates, unregistered
fingerprints, principal spoofing, missing grants, and disabled surfaces before
creating namespace storage. Tests revoke certificates and grants while the
listener remains running. SQL, KV, object, and blob tests use identical names
in separate tenant namespaces and verify independent data after reopen.
Local security command tests verify durable grants, revocation, and audit
records. Legacy shared data roots fail closed pending explicit migration.

VSR follower tests persist complete binary commands before acknowledging a
prepare and persist certificates before advancing the committed prefix. They
reject gaps, conflicting duplicates, stale views, wrong sender identities,
minority certificates, and malformed bounded wire encodings. A child process
is forcibly killed after each durable boundary; reopening must recover the
exact payload and expected commit state. A separate persistence-error test
checks that an unpublished commit stays invisible and further writes require
reopening. Reproposal after a locally installed view preserves the command and
requires new votes. These APIs are prerequisites: the daemon still needs
data-plane integration with the peer transport, quorum-selected view reconciliation, and state-machine replay.

Storage ownership uses operating-system file locks, retained by a reentrant
in-process registry for recovery. Lock files remain on disk; file existence
alone does not indicate a live owner. Process termination releases ownership.
The registry explicitly unlocks its final handle so a concurrent process spawn
cannot temporarily retain ownership through an inherited descriptor. All old
sentinel-lock binaries must be stopped before upgrading an existing store;
they do not participate in the new operating-system lock protocol.

Independent peer-process and actual daemon peer-command tests are described in
[peer-replication.md](peer-replication.md). They include real TLS traffic, forced
process termination, quorum loss, prefix repair, and lost commit replies.

KV apply-boundary crash tests and receipt semantics are documented in
[kv-durable-apply.md](kv-durable-apply.md). The forced-kill test stops after a
protocol commit but before the value/receipt append, then recovers the original
mutation through the durable intent.

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

Automatic peer control is also covered by real daemon tests: primary loss and
stale-primary rejoin preserve the acknowledged payload, minority observations
cannot establish a new normal view, and a candidate killed after local-only
publication resumes its retained input on daemon restart. Authenticated status
queries do not refresh failure timers; failed materialization blocks heartbeat
success. A separate required automatic-client domain now exercises actual replacement
listeners, all four application surfaces, primary state deletion, four concurrent
clients and 32 identity-preserving retries, replacement restart, certificate
revocation, redirects, and quorum loss/restoration. It validates actual debug
and release workload output; zero-test filters cannot satisfy the gate. These
loopback process tests do not establish machine-failure or WAN capacity readiness.

Authority-fence regressions distinguish simulated local changes from network
configuration. They require unchanged cell WAL/state after rejected shortcuts,
reopen and rejection again, rejection of validly checksummed unchecked authority
records after installation, and a successful subsequent quorum-selected view.
The real TLS router test exercises the same rejection with quorum present and
absent, then restores quorum and commits in the original configuration. This
coverage is included in the VSR/RPC host traces and full workspace suite.

The automatic-client domain now requires 128 acknowledged history-growth writes
before the crash, at least 128 observed retained coordinator uses, and fewer
full recoveries than retained uses. Each profile then resolves 32 concurrent
request identities and verifies exactly 162 logical KV writes plus all four
surface recovery paths. Actual JSON workload output is required in both debug
and optimized-release traces. Reported history-growth time is a loopback work
measurement, not an independent-host capacity or latency guarantee.

Resident-coordinator regressions verify shared multi-group WAL writers, cached
logical tail invalidation, checksum failure after changed disk state, receipt/
policy/generation/reopen invalidation, and ownership restoration after errors
and unwinding. Installed-prefix recovery is tested with quorum lost before or
after receipt reconstruction and with an accepted-only suffix that still needs
fresh prepare acknowledgements. Both cold and retained execution continue to
require fresh quorum before client admission.

The growing workload also checks that object-internal tagged pins remain absent
from the user blob surface. A deliberately rejected blob read invalidates its
coordinator entry, so this profile can include an additional cold reconstruction
before the history-growth phase. Post-execution materialization visits every new
certified payload command before advancing the cursor; protocol-only dependencies
are excluded from that index. Workload cache counters describe observed use, not
an entitlement to serve without fresh authority/quorum checks.

Cold materializer coverage evicts a namespace writer, removes its derived files,
and verifies reconstruction before the next read. The regression first reproduced
an incorrect empty read, then passed with per-writer cold replay. Election-disabled
seed processes bind port zero atomically and advertise their actual addresses;
automatic-election phases retain their configured fixed endpoints. This removes
the seed fixture's reserve/exec gap without relaxing RPC deadlines or crash checks.

## Pinned Fozzy builds

`scripts/fozzy-engine.json` pins the public engine source commit, tree, lockfile
and Rust 1.92.0. With that named toolchain installed and Python 3.11 or newer,
build from a fresh checkout outside any ancestor Cargo workspace:

```sh
RUSTUP_TOOLCHAIN=1.92.0 python3 -B scripts/bootstrap-fozzy.py /absolute/clean-engine-checkout /absolute/new-engine-prefix
python3 -B scripts/test_bootstrap_fozzy.py
```

The bootstrap checks every tracked source file and executable mode, rejects
additional or aliased files and compiler redirection, builds locked dependencies,
embeds the actual engine commit, and writes provenance beside the new binary.
It requires a new prefix and does not replace the installed engine. Source,
compiler and configuration identities do not imply bit-for-bit reproducibility
across different host images. An installed engine's unembedded version metadata
can report the calling repository's commit; that is not engine provenance.

The 28 Python tests use fake subprocesses and validate bootstrap rejection and
bookkeeping only. A real pinned release build, 447 actual engine tests and an
11-test RSF control/TLS smoke run are separately recorded in
`docs/fozzy-engine-qualification-2026-11-03.json`. The smoke run is not a final
RSF baseline, proof of equivalence to the installed binary, or hosted CI.
Full qualification must use an explicit `FOZZY_BIN` and retain the complete
source-frozen gate evidence for the selected engine. No hosted workflow or
required check is enabled by these bootstrap scripts.
