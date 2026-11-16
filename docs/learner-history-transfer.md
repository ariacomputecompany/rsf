# Authenticated non-voting learner history transfer

A peer can receive a complete placement-group history without becoming a voter.
`NetworkVsrTransport::sync_learner` holds the source cell's exclusive writer
ownership while transferring its original bootstrap, prepares, commit
certificates, view/membership changes, and client request receipts. Certificates
are replayed against their original voting configurations. This allows a new
learner to recover entries from before and after configuration changes.

This implements catch-up only. Quorum-certified configuration installation,
automatic promotion, primary failover, and admin/policy orchestration remain
release blockers. The receiver never supplies prepare, commit, tail-query,
prefix-repair, or current-primary quorum acknowledgements for a learner group.
Editing a learner's configuration into a voter configuration is rejected.

## Configuration and operation

Use the [peer configuration](peer-replication.md) with an explicit
`learner_source` on the receiving cell. For example, node 2 can receive from
primary 0 while the current voters are 0 and 1:

```json
{"pg":1,"view":2,"primary":0,"voters":[0,1],"learner_source":0}
```

The local node must be absent from `voters`. The source must be the configured
primary, differ from the local node, and have an authenticated peer mapping.
Both voters and learners use the same cluster-bound TLS identity and bounded
RPC framing. A learner group cannot replace an existing `pg-<id>` voting store;
its separate storage is `learner-<id>`. Its source and local identity are
persistently bound to that directory.

Start the receiver normally:

```sh
rsfd peer serve --config learner-peer.json
```

Quiesce the source placement group's writers, then run:

```sh
rsfd peer sync-learner --config source-peer.json --pg 1 --target 2
```

The source configuration uses the ordinary peer configuration schema. It must
identify the current primary and point `state` to the existing parent of
`pg-1`. For the native daemon's stores, that parent is
`<daemon-state>/protocol.vsr`. Source TLS keys and current endpoint mappings
must match the admitted primary. The CLI acquires exclusive cell ownership and
rejects missing source stores, conflicting ownership, learner sources, and
voting targets. It does not create a source store or change the source's
membership.

For daemon application sources, the CLI retains the existing sibling
`<daemon-state>/replication-identity` binding. It must match the source cluster
and node. Native identity, pending or peer-lock markers at `protocol.vsr`
select strict native ownership; a missing or damaged native identity never
falls back to the application binding.

Older daemon versions wrote a V1 `replication-identity/IDENTITY.json` without
`.peer.lock`. Stop that legacy daemon and its administrative writers, then run
this explicit application-only migration before restarting or exporting:

```sh
rsfd peer migrate-application-identity --state <daemon-state> --cluster <cluster> --node <node>
```

The command requires the existing exact application layout and V1 identity.
It retains original directory/file ownership, creates and syncs the missing
exclusive lock, and preserves identity and source bytes. Repeating it renews
the barriers without replacing the lock. It refuses unknown or pending binding
entries, managed/control identity formats, aliases, mismatches and missing
source state. It cannot repair a native peer's missing lock. Normal startup and
history transfer never perform this migration implicitly. Keep legacy writers
stopped throughout the operation; old binaries did not honor this lock.

The source verifies a live quorum of matching current-primary probes before
transfer and again before publication. A matching probe binds placement group,
view, primary, voter set, committed index, and committed-command digest.
Unavailable or stale voters cannot be replaced by the learner's response.
Followers need the current configuration and committed tip installed already;
this command does not repair or reconfigure them.

Success emits `peer.learner-synced`, the placement group, target, committed tail,
and `role: learner`. A timeout or failure can have an uncertain publication
outcome. Retry the identical command: matching records and publication are
idempotent, and interrupted transfers resume from their recovered WAL boundary.

## Durable transfer contract

The descriptor identifies the current primary probe, total record count, and a
BLAKE3 rolling digest over canonical history records. Its own content digest
names the staging directory. Each record RPC carries a snapshot ID, ordinal,
record kind, epoch, and bounded payload; the wire does not contain an unbounded
whole-snapshot frame. Uncommitted prepares remain uncommitted after recovery.

The receiver accepts only the configured authenticated source. It rejects
missing ordinals, conflicting duplicate records, extra bootstrap authority,
foreign placement groups, malformed encodings, and oversized records. Every
accepted record is durably appended before its progress acknowledgement.

Publication requires all of the following:

- The recovered record count and digest match the descriptor.
- Normal VSR recovery validates bootstrap authority, configuration transitions,
  historical commit certificates, and client receipts.
- The recovered current-primary probe exactly matches the descriptor.
- Previously published bootstrap, configurations, commits, and receipts remain
  intact. A lower view or committed tail cannot replace accepted history.

The receiver syncs an atomic `ACTIVE` pointer only after these checks. Until
then, its previous publication remains authoritative. Superseded and abandoned
snapshot directories are reclaimed while preserving the active and current
staged snapshots. A publication I/O failure fences the receiver until restart.

A finish acknowledgement also requires successful application materialization.
Published entries are replayed through the existing idempotent KV, SQL, object,
and blob materializers. A materialization failure or process crash leaves the
published WAL available for retry or startup replay. A receipt for successful
catch-up is not a membership certificate.

Legacy source logs without persisted bootstrap authority require the explicit
[bootstrap migration](vsr-bootstrap-migration.md) before export. Export
normalizes migrated logs to their original bootstrap without altering the
source WAL or receipts.

## Verification and current limits

The VSR tests cover restart resume, torn staged tails, digest and certificate
rejection, preservation of historical configurations and receipts, publication
replacement, snapshot reclamation, identity binding, and exclusive ownership.
The RPC tests transfer over real TLS, resume after receiver restart, reject a
wrong source, and prove a caught-up learner cannot satisfy voting requests or
replace a lost voter quorum. The daemon process test transfers historical
configuration records using the actual CLI and verifies an application KV value
and idempotent recovery after terminating and restarting the learner.

These tests run through the VSR, peer-RPC, daemon, and workspace domains in
`scripts/verify-fozzy.sh`, including host execution, trace verification, replay,
and CI checks.

Network records are bounded, but source export and VSR recovery currently
materialize the full history in memory. Incremental replay/checkpoint cursors,
large-history performance, cross-group application scheduling, automatic
membership installation, and the complete machine-failure matrix remain
unfinished. Current membership records are trusted crash-fault protocol state;
this transfer does not add Byzantine or independent voter-signature guarantees.
