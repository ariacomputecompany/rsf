# Primary failover

Status: durable election promises, authenticated recovery-input collection,
mixed-installed-view quorum selection, and local atomic generation
installation, and authenticated resumable remote view installation are implemented.
A real TLS process test publishes the selected view to the running survivor and
resumes replication from the replacement primary. Another kills the recipient
mid-upload, publishes only the candidate, restarts both endpoints, and completes
the same installation without recollecting old-view reports.

Opt-in daemon peer election scheduling, quorum installation, and lagging voter
adoption are implemented. Opt-in client coordinator ownership, shared application writers, and primary
redirects are implemented through the peer's `client` configuration. Certified
membership changes, independent-group routing, incremental recovery, and
independent-machine qualification remain release work. Peer election events
do not establish client-service readiness.

## Automatic peer control

An optional `elections` object enables the serialized peer controller; see
[peer configuration](peer-replication.md). Without it the service retains its
RPC-only behavior. The controller runs between bounded incoming RPC batches,
round-robins placement groups, sends authenticated heartbeats, and staggers
candidates after missed contact. Timing validation accounts for sequential peer
RPC deadlines. Timers only suspect failure; durable promises and a completed
quorum installation establish authority.

Read-only replica status includes the installed normal view and any durable
promise, including while ordinary probes are fenced. Status queries never
refresh primary contact. Heartbeats require the installed authenticated primary,
an unfenced view, and completed local application. Only a matching successful
heartbeat counts toward the controller's current quorum observation. That
observation is neither a lease nor authorization to admit client requests.

Candidates retain installation input before publication and resume it after
restart. A candidate restarted after local-only publication must complete the
quorum installation before confirming its view. A higher promise fences obsolete
work. A healthy primary repairs reachable stale voters from the retained input,
then transfers later committed entries. Failed materialization suppresses
successful heartbeats until the committed prefix has been applied.

Real-daemon tests kill the elected primary, preserve acknowledged KV data through
successor elections, rejoin a stale primary, fence isolated minorities, and
restore quorum. A separate subprocess is killed after only local publication;
its restarted daemon completes the retained installation. These tests use
loopback TLS and independent state directories on one machine.

## Durable election preparation

`VsrViewChangeRequest` binds a placement group, minimum admitted normal view
(`previous_view`), target view, candidate, and the complete canonical voter set.
Each report retains the replica's actual installed normal view; it must be at
least the request floor and below the target. `begin_view_change` accepts it
only from that authenticated candidate, for a locally hosted voter. It persists
and syncs the promise before returning a report. An identical request is
idempotent. A higher target can supersede the promise; a lower target or another
candidate in the same target view is rejected.

A promised replica retains its last installed view for recovery. It rejects
prepares, prepare acknowledgements, commits, prefix transfer, reprepare, client
receipt writes, primary probes, normal learner-history export, and local
view/membership changes. A promise has no expiry. Restart does not restore the
old primary's authority. A failed election can leave the group unavailable;
only a verified future installation may resume normal operation.

Promises have their own mandatory WAL kind, `ViewPromise` (tag 39). Older
storage readers reject this unknown kind rather than silently ignoring an
unrecognized membership payload and resuming the fenced view. Tail truncation
preserves the promise and all accepted history before it. Recovery rejects
ordinary mutations after a promise and conflicting/regressing promise records.

## Recovery input over TLS

`NetworkVsrTransport::collect_view_change` first promises the candidate's local
replica, then obtains reports and frozen WAL records from authenticated remote
voters. It requires a majority of distinct configured voters. The request and
reporting node must match each authenticated response. TLS frame types 9 and 10
carry reports and individual records; records use the existing bounded history
codec. Each record query is bound to the exact report and current promise.
Restart can reconstruct the same frozen snapshot without another begin call.
A higher promise invalidates queries for the previous snapshot.

Reports include the last installed view, accepted log height, committed tip,
and a record count/digest covering bootstrap authority, historical
configurations, complete prepares, commit certificates, client receipts, and
the final promise. Accepted operations are retained even when no commit was
observed. Integrity validation checks the count, hash chain, and terminal
promise. These reports are authenticated recovery inputs, not signed election
certificates; integrity validation does not select or semantically reconcile a
new log. Collection currently retains full histories in memory. Incremental
recovery and checkpointing remain separate requirements.

## Quorum selection and local installation

`VsrViewInstallation::select` semantically replays every frozen history using the
same implementation as durable WAL recovery. It checks request and voter
identity, accepted height, committed tip, historical authority, and client-table
mappings. A majority of distinct configured voters is required. It selects the highest last-normal view, then the longest accepted log and a
canonical voter tie-break. Within that newest normal view, accepted operation
prefixes must agree; reprepare headers may differ. An older normal view's longer
uncommitted suffix cannot override the selected log. Every reported committed
operation must still match the chosen operation at the same index. Shared
historical views retain the same primary and voter authority. Commit evidence
and durable client mappings are merged from the quorum. Valid alternative
signer sets are normalized deterministically. Accepted operations remain
uncommitted until the new primary reprepares them and obtains fresh votes.

`prepare_view_installation` collects these inputs over authenticated TLS before
constructing a plan. Plans built directly through the Rust selection API require
the caller to authenticate the reporting nodes. Persisted certificate summaries
use the existing crash-fault trust model; node IDs are not digital signatures.
No external write endpoint accepts an arbitrary caller-supplied report vector.

`DurableVsrCell::install_view` requires the exact active promise, preserves local
committed operations and receiver-local receipts, and rejects superseded plans.
It writes a separate immutable SegmentStore generation, replays the exact
persisted form, and syncs the files, seal, and directory before publishing a
checksummed `VSR_ACTIVE` pointer by rename and parent-directory sync. New WAL
kind `ViewInstallation` (tag 40) carries bounded commit/client deltas and a
compact quorum activation marker. Version 2 markers retain each reporting voter's
actual normal view and primary; version 1 markers and commit deltas remain
readable. Installation retry compares the decision rather than its codec bytes,
so a codec upgrade cannot cause rollback or reject an equivalent completed
installation. Replay checks the source history digest,
delta digest/count, quorum, selected source, and committed boundary before
installing the new view. Incomplete delta sequences cannot become active.

The root writer lock spans generation changes. Publication errors after rename
fence the handle until reopen. A generation seal binds its installation prefix;
missing generations/seals, corrupt pointers, lost prefixes, and live pointer
changes fail closed. Retry of an already installed plan preserves later writes.
Inactive owned generations are reclaimed only after pointer publication is
durable. The original root WAL retains its downgrade fence. Full-history replay
and the retained root prefix still require future checkpoint/retention work.

## Resumable remote view installation

`VsrInstallationInput` encodes compact canonical quorum reports and the identity
of the semantically selected plan. `stage_installation_input` validates the full
snapshots and durably stages them on the candidate before any publication.
`resume_view_installation` can recover this input after candidate restart, even
when the candidate already published the target view. It publishes locally,
invokes the caller's idempotent materializer, uploads the missing records to
other voters, and returns success only after a majority including the candidate
has installed and materialized the selected prefix.

TLS frame types 11–13 carry the bounded descriptor, individual history records,
and finish request. Only the authenticated candidate can upload or finish an
installation on a hosted voter. Learners cannot receive voting installations.
The receiver durably promises before accepting an upload. Record acknowledgements
report the fsynced prefix; exact duplicates are idempotent, while gaps and changed
records are rejected. Newer promises fence old uploads and finish requests.

Each cell retains one selected input under `vsr-install-input/`, separate from
active generations. A synced descriptor and checksummed SegmentStore records
precede atomic publication of `VSR_INSTALL_INPUT`. Records are flattened in
canonical reporting-voter order; their outer WAL epoch is the target view, while
the bounded inner encoding preserves each original kind and epoch. On restart,
the descriptor identity, WAL envelopes, counts, and record encodings are checked.
A selected input whose record store disappeared fails closed. A higher input
reclaims only owned inactive input directories after its pointer is durable.

Finish independently replays every quorum history, checks the resulting plan
identity, and uses the existing atomic generation publication. Partial uploads
cannot activate a view. The peer applies all committed entries before returning
the finish acknowledgement. Publication followed by a materializer error leaves
the group pending; normal peer traffic cannot succeed until prefix application
succeeds. An installed generation starts pending after server restart as well.
Materializers must remain idempotent and the candidate controller must keep
client service gated until the quorum installation call succeeds. The daemon
peer controller enforces installation/materialization completion. The opt-in
client endpoint independently reconciles recovery and confirms a current quorum
before executing each request.

The transfer and selection currently hold full histories in memory. Incremental
checkpoint recovery remains required for growing histories. These APIs install
views with the existing voter set; they do not promote learners or certify a
new configuration.

## Client coordinator recovery from installed cells

`VsrBackedFabricRouter::open_with_installed_cells` takes exclusive ownership of
all mapped, already-installed voting cells. It checks each group's identity,
active view, and local primary against persisted authority. A promised cell or
an incomplete placement inventory cannot become a client coordinator. It does
not create replacement VSR logs from bootstrap settings. `into_cells` returns
the same live cells while retaining their writer locks, for integration with a
peer service that serializes peer and client work.

New protocol-only operations carry the complete `ReplicatedProtocolCommand`
(`RSFG1`) in the VSR log, including the exact operation, group/index, global
version, consistency mode, and optional client identity. Application commands
retain their `RSFC1` encoding and complete surface payload. Startup rebuilds
both inventories, rejects reused versions/requests and conflicting local
receipts, and reconciles missing protocol receipts in original version order.
Protocol receipt persistence errors fence the coordinator until reopen. An
uncertain metadata command retried without a client ID keeps its original
index/version. Previously written metadata-only VSR entries are kept immutable
and can still recover through the original local protocol WAL; importing them
without that WAL requires explicit migration.

Ordinary protocol-only records for data-bearing SQL/KV/object/blob operations
are insufficient to recover values. Installed-cell adoption rejects them before
creating a coordination store; new mutations on an adopted coordinator must use
the complete surface command path. Cached or routed request retries cannot
turn an internal certificate into a user payload acknowledgement. Local legacy
API histories still require explicit migration.

An object's internal blob pin is a separate immutable dependency (`RSFI1`),
explicitly tagged by `ProtocolCommandPurpose::ImmutableDependency`. It must be
an idempotent `BlobPin` in immutable availability-first mode. The containing
object command (`RSFC1`) replicates the payload before object success. Losing
the primary between the internal pin and object command can leave an orphaned
dependency. Recovery preserves that coordination receipt without inventing an
application value. The original pin identity can retry; another digest, purpose,
or ordinary user-blob request cannot reuse it. Policy and identity preflight run
before an object intent is persisted. Ports that do not implement typed immutable
dependencies fail closed. Unmarked historical pins are not automatically upgraded
to dependencies; their migration must establish the original purpose and payload
history explicitly.

An unresolved lower-version command currently blocks fresh allocation across
groups. A healthy group's own membership/quorum remains independent, but a
new command cannot reuse a version already carried by another group's accepted
command. Cross-group closed-version scheduling remains release work; this
ordering barrier is not a claim of independent-group availability under an
uncertain global version.

The constructor recovers coordination inventory only. The controller must
reconcile commands, materialize their surface payloads, and confirm the installed
primary quorum before admitting client traffic. A real-daemon integration test
writes all four surfaces through TLS ingress, kills the original primary,
deletes its state, installs a new view using surviving peer histories, rebuilds
protocol receipts on the replacement, retries original application commands,
and issues new SQL/KV/object/blob writes through the surviving TLS peer. Both
survivors are reopened and checked for exact values and history counts. Loss
of the remaining quorum rejects strict reads, and restarting it restores quorum
confirmation. This older test drives the recovery API directly. A separate real-daemon test now
exercises automatic elections and client listeners without an external recovery
coordinator, including concurrent requests during primary loss.

## Remaining takeover work

Mixed normal views after interrupted publication are reconciled, including
restarting a candidate that activated alone, selecting a newer normal view over
a longer stale suffix, and preserving commit certificates learned from older
voters. A late certificate can refer to an operation beyond a previously sealed
view boundary. A completed installation records an exact prepare digest for
that certificate after validating its historical voter quorum and equality with
the selected operation. This is not a general reopening of the old view:
ordinary stale-view prepare/commit traffic remains fenced, and another payload
or index cannot use the authorization. The authorization is replayed with the
installation and survives subsequent views.

The peer's optional `client` endpoint now serializes request execution with peer
traffic. `VsrBackedFabricRouter::with_installed_cells` lends exact live cells for
one scoped operation and returns them on callback failure, recovery rejection,
and unwinding. The peer marks materialization pending before the handoff, so
ordinary acknowledgements cannot bypass a failed application operation. The
client coordinator recovers receipts from those cells, reconciles complete
commands, applies their payloads through the peer's resident writers, and
confirms the installed quorum before execution. It does not reopen a separate
bootstrap log or assume the smallest voter is primary.

Every connection uses application mTLS. Each request rechecks certificate
binding, namespace grants, tenancy, admitted voter identities, and installed
membership. An unfenced follower returns an explicit primary endpoint without
attempting the request; clients must retry with the same request identity.
Admission remains closed during a promise, before installation, on conflicting
primary ownership across hosted groups, or when quorum confirmation fails.

The real-daemon client test writes all four surfaces, starts four concurrent
KV clients, kills the primary after acknowledged writes, deletes its state,
and resolves all 32 concurrent request identities on the elected replacement.
It checks original certificates, exact applied values/counts on both survivors,
new writes on all surfaces, replacement restart, a usable redirect, certificate
revocation, and strict-read rejection/restoration with quorum loss. Debug and
optimized-release executions are required by the automatic-client Fozzy domain.

Remaining work includes certified membership changes, independently led placement
groups and cross-group scheduling, incremental recovery/checkpoints, control
metadata distribution, and deployment qualification. The legacy standalone
`serve` entry point retains its existing static primary configuration; automatic
service takeover requires the explicit peer `client` configuration.
Independent-machine partitions and crash/power-loss qualification remain
required. Process exit tests establish software publication ordering; they do
not simulate loss of filesystem caches or prove hardware fsync behavior.

The protocol basis is the view-change and recovery safety discussion in
[Liskov and Cowling, Viewstamped Replication Revisited](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf),
sections 4.2 and 8.1–8.2. This implementation persists promises before reporting,
so recovery cannot forget a previously issued vote.

## Current tests

- Promise persistence, retry identity, stale/conflicting/spoofed requests, and
  higher-target supersession.
- Rejection of every ordinary mutation path and delayed valid certificates
  after a promise; replay rejects post-fence writes.
- Every majority subset of three- and five-voter groups, with each store
  reopened before attempting old-primary acknowledgements.
- WAL truncation preserving the fence and accepted history.
- Report/request truncation and history-digest failures.
- Actual TLS peer processes, primary loss, accepted-but-uncommitted commands,
  stale-primary rejection, snapshot-bound record queries, peer kill/restart,
  reconstructed frozen snapshots, and loss of the remaining quorum.
- Shared semantic replay rejects forged accepted heights, uninstalled prepare
  views, forked accepted prefixes, duplicate voters, and corrupted histories.
- Local publication merges lagging commit evidence and receipts, preserves
  receiver-local receipts outside the reporting quorum, and recovers successive
  installed views with original request identities intact.
- Abrupt process exit at staged, renamed, and published boundaries; pointer,
  seal and directory corruption; permanent I/O-failure fencing; idempotent
  installation after later commits; and inactive-generation reclamation.
- History transfer through a non-voting learner after view installation.
- Real TLS process replication by a replacement primary after remote publication
  to a running survivor, including reprepare of a previously accepted operation
  and rejection of the stale original primary.
- Interrupted upload followed by recipient kill and candidate restart after
  local publication; incomplete installation cannot report a successful quorum.
- Changed/gapped records, wrong candidates, missing selected input stores,
  superseded uploads, input reclamation, and materializer failure/retry/restart.
- Mixed-normal-view selection after partial publication, newest-view priority
  over a longer stale suffix, same-view reprepare-header differences, late older
  certificates after a newer-view commit, and rejection of committed forks.
- Exact-operation learned-certificate authorization, stale-index/payload
  rejection, legacy marker replay, and equivalent retry across codec versions.
- TLS collection from mixed normal views after candidate restart, followed by
  local publication on the survivor and successful new-view replication.
- Maximum-size replicated commands learn commit evidence without duplicating
  their payload inside a transfer record. Decoder regressions reject insufficient
  signer quorums, payload changes, skipped indexes, uninstalled views, wrong
  primaries and altered byte counts before publishing any log mutation.
