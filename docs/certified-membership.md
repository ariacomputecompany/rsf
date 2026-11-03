# Certified membership installation

Status: old-quorum approval, new-quorum input choice, sealed staging, certified
publication and owned learner promotion are implemented in the kernel and
peer protocol. Native peers can recover an existing chosen transition through
bounded background work and cold role recovery. Native clients remain closed
when managed policy or replayed membership ancestry requires replicated control
admission. Autonomous pre-choice recovery, repeated transition lifecycles and
replicated control/catalog admission remain unfinished. Local configuration and
reachability probes do not install network membership; learner catch-up alone
does not grant a vote. Installed histories reject local authority shortcuts;
[historical authority](vsr-configuration-history.md) documents that fence.

The current checkpoint has an unresolved staging RPC timeout in its broader
qualification run. See [current verification](native-membership-verification-2026-11-02.json)
and [the work ledger](../WORK.md). Verification counts in earlier sections describe
historical milestones, not current release approval.

## Required authority contract

A transition must bind the placement group, exact historical old configuration,
complete proposed voter set, primary/candidate identity, durable transition
identity, and the accepted/committed prefix being carried forward. Operator input
cannot replace historical authority. Election views and membership generations
must have an unambiguous ordering during interrupted transitions; the current
same-voter view-change request cannot simply be relabeled as a voter change.

Old-voter approval must durably prevent the old configuration from continuing
independently. A successful primary probe establishes neither proposal approval
nor this fence. Before acknowledging completion, a verified new voting quorum
must have installed the selected complete history and materialized committed
application payloads. Accepted-but-uncommitted operations and durable request
receipts must survive; new voters cannot authorize older certificates.

Every installation must replay the old authority chain and its transition
proof through the same validator used on restart. New nodes receive that chain;
a new bootstrap configuration alone is insufficient. Configuration proofs follow
the existing authenticated crash-fault trust model. Node IDs in Rust values or
WAL signer sets are not independently verifiable signatures.

## Durable recovery states

The implementation must distinguish an unapproved proposal, an old quorum's
fence/approval, partially staged recovery input, partially published membership,
and a completed new-quorum installation. A local publication alone must not
admit clients or count as completion. Transition input and progress must survive
process death; the surviving configuration needs a way to resume if the initiating
candidate is permanently lost.

Recovery must resolve a partly published configuration without permitting old
and new majorities to acknowledge competing histories. It cannot blindly raise
the election view while discarding the configuration transition. A previously
removed node must learn current authority before resuming service. Learners
must remain non-voting for application operations until verified installation,
regardless of their log height. Proposed voters may separately participate in
transition-only plan consensus after verified old-quorum approval.

The current installation protocol authenticates the original candidate as the
uploader. Generalizing membership therefore also requires a verified handoff or
recovery rule for a different surviving candidate, rather than relying on that
original process returning. The current request also requires the reported voter
set to match the installed set; mixed membership cannot be accepted merely by
loosening that comparison.

## Control metadata and client admission

The admitted node catalog and application policy must agree with the certified
configuration before client execution. Admission must remain closed if only one
of those durable updates completed. Existing local `join` orchestration does not
provide this network protocol. A learner's authenticated identity, catch-up state,
configuration proof, and voting promotion are separate facts.

For a multi-group transition, recovery must retain progress for every group and
prevent a local admin update from presenting an unfinished cluster-wide change
as complete. Independently led groups also need the outstanding cross-group
scheduling and routing work. Neither problem is solved by changing an endpoint
file or selecting the lowest node ID.

## Required verification

Tests must crash at each durable boundary: before/after old-quorum fencing,
during input upload, after publication on one voter, after new-quorum publication,
and before/after client acknowledgement. They must lose the initiating candidate,
restart each participant independently, rejoin removed voters, and retry conflicting
proposals. Repeated configuration changes must preserve older commit certificates,
all application values, and original request receipts.

Coverage must include learner promotion, voter growth/shrink, changed primaries,
concurrent traffic, quorum loss, partitions across old/new sets, and independent
host failures. Exhaustive small-set quorum checks supplement these executions;
they cannot stand in for persisted transitions, authenticated transfer, or restart
behavior. The normal production gate must require actual workload evidence and
recorded host traces before this blocker can be marked complete.

## Implemented old-quorum approval phase

`VsrMembershipProposal` binds the exact old view, primary and sorted voter set,
target view/primary and complete proposed voter set. Canonical encoding and a
BLAKE3 identity distinguish proposals. The target primary must belong to both
sets. The current old primary alone can initiate a new proposal; a stored exact
retry does not append another vote. Old voters persist a `MembershipPromise`
record (WAL tag 41, transferred-history tag 6) before acknowledging.

Before initiating an unfenced proposal, `validate_publication_capacity()` bounds
the complete old-input, recovered decision, chosen, staged-quorum and activation
proofs. It counts every distinct configured reporter, so any subset fits; the
individual 4,096-voter codec limit alone is insufficient. An oversized proposal
fails before a local or remote old voter is fenced. Normal old-view operations
remain available. Existing acknowledged fences keep their original decoding and
exact retry behavior; this check does not make an older oversized fence
publishable. Native membership publication and client admission remain separate.

The same authoritative replay used on open restores the membership fence. Normal
prepares, commits, primary probes, local reconfiguration, ordinary election
promises and ordinary view installations cannot discard it. The old cell's voter
set remains unchanged; the proposal does not promote any node. Malformed fence
envelopes fail repeated opens without rewriting the retained WAL.

`VsrMembershipSnapshot` binds the fence to its entire immutable accepted and
committed history and client mappings. `VsrMembershipApproval` requires distinct
old voters forming a majority, independently replayed histories, matching
proposal/old authority/bootstrap, compatible accepted prefixes and client maps.
Authenticate every report's node and cluster before using this library evidence;
node IDs and WAL records are not independent signatures.

Authenticated peer RPCs expose BEGIN (18), existing SNAPSHOT (19) and bounded
history BATCH (20). The current old primary initiates BEGIN. Any authenticated
old voter can recover an existing proposal and full reports from surviving old
voters after loss of the initiator. Configured learners cannot use these voting
recovery RPCs. Exact immutable recovery exchanges may retry one lost reply;
normal voting traffic retains its existing deadline/retry contract.

The approval API returns old-quorum input, never an installed epoch or readiness.
It is not exposed as an operator membership-change command. A surviving old voter may now finish the same proposal using a durable report,
including adopting a directly authenticated peer report before relaying its own.
Pending fences do not yet have an abort or competing-proposal resolution protocol.
Recovered approval does not activate the new quorum or resume application
service. Those lifecycle states, sealed membership installation, voter/learner
role changes and atomic control-metadata admission remain required work under
the contract above.

Targeted evidence covers codec bounds and identity, exact retries, restart fences,
accepted/committed prefixes and original request mapping, rejection of conflicting
or tampered reports, ordinary-installation shortcut rejection, malformed WAL
recovery, a real process kill after synced fence before acknowledgement, and
old-quorum recovery from real TLS peers after deleting the initiator's store.
The unchanged-source full gate passes all 19 domains and 1,154 workspace
tests with zero failures, including these six named regressions. Strict lint
checks pass for the changed production packages and daemon. The report is
[membership approval verification](membership-approval-verification-2026-10-21.json).
This phase is not complete production membership qualification.

## Surviving minority continuation

`resume_membership` accepts an existing `VsrMembershipReport` directly from its
reporting authenticated old voter. It checks the canonical report, exact old
configuration and sender/reporter identity before persisting the same fence.
It cannot replace an ordinary election promise or a different membership choice.
The report is an authenticated crash-fault assertion of an existing synced vote;
it is not an independently verifiable signature. This step copies no application
history and grants no new voting authority. Full history replay and prefix/client
mapping agreement remain required to construct old-quorum approval.

Peer RESUME (21) binds the report's voter to the TLS sender. A replacement old-voter
coordinator with no local fence first fetches and validates a surviving peer's
complete immutable snapshot, adopts its exact report locally, then relays its own
durable report. An already fenced coordinator can relay directly. The receiver
persists before replying; exact retries append nothing. A coordinator cannot start
a fresh proposal when no report survives. Conflicts remain closed instead of
silently changing the target or old authority.

New targeted evidence covers a single surviving minority vote becoming an old
quorum after primary-store deletion, sender/learner rejection, ordinary election
and conflicting-proposal fences, a real process killed after continuation sync
before acknowledgement, both initially fenced and unfenced coordinators over TLS,
peer restart, idempotent retries and rejection when no durable report survives.
The expanded full gate now requires all eleven named approval/continuation
regressions. All 19 domains and 1,159 workspace tests pass on unchanged
source, with zero failures. Targeted coverage passes all 98 VSR, 35 RPC and
57 protocol tests; strict lint checks pass. See
[continuation verification](membership-continuation-verification-2026-10-21.json).

This closes exact-proposal old-quorum completion after coordinator loss. It does
not implement abort, new-quorum activation, learner promotion, distributed control
metadata or atomic client admission. The full membership blocker remains open.

## New-quorum installation decision still required

The proposal identity binds authority and voter sets, not one selected application
history. Different valid old-quorum report subsets can expose different lengths
of an uncommitted accepted prefix and different learned commit/receipt deltas.
Consequently, the next phase must distinguish the proposal ID from the exact
installation plan ID. A digest of one locally selected plan is not itself a
new-quorum decision, even when its old-quorum input is valid.

New voters need durable transition-only promises and accepted-plan state before
acknowledging installation input. This state must not authorize application votes,
ordinary client probes or historical certificates. Recovery must gather a new
quorum's transition reports, preserve any previously chosen plan, and finish
upload/publication after the original uploader disappears. A permanently fixed
first-plan promise can strand split minority uploads; clearing that promise on
restart can instead permit conflicting decisions. The protocol must resolve both
cases with persisted ordered recovery rounds and quorum-intersection rules.

The selected plan must retain its complete old-authority proof, committed and
accepted operations, original certificates and client mappings through the same
validator used on reopen. New-quorum acknowledgement must bind that exact plan
and verified committed-application materialization. Only a replay-validated
completion proof may advance voting roles, and client readiness must additionally
require the matching durable control-metadata admission. Old-quorum approval and continuation do not implement those activation states.
The transition-only decision store below now implements the plan-selection part;
application publication/materialization and admission remain unfinished.

## Implemented transition-only installation decision store

`MembershipDecisionStore` has its own exclusive owner lock, full-sync authority
journal and bounded batch input stores. Opening a new acceptor requires verified
old-quorum approval and a proposed voter identity. Reopen independently validates
the retained original authority, bootstrap, monotonic round/accepted/chosen state,
and complete accepted input histories. It does not contact the original primary.
Missing authority or accepted input fails closed; staging cannot silently recreate
a missing referenced input. The active application WAL and voter set are untouched.

`VsrMembershipInput` canonically orders the old-quorum reports, binding every full
history by its existing count/digest and canonical report bytes. Different report
subsets may have different input IDs for the same membership proposal. Complete
input replay uses the original membership approval validator. Bounded synced WAL
batches retain logical ordinals, accept exact/overlapping retries, reject changed
prefixes/gaps and recover only the preceding complete batch after a torn write.

A `VsrMembershipRound` orders a counter and proposed-voter coordinator ID. Promises
persist before reply and retain the highest accepted input across restart. A
`VsrMembershipDecision` requires a distinct new voting majority at one round and
must retain the highest accepted input from those authenticated reports. Conflicting
reports from any accepted round reject. The coordinator first persists its own
nomination; reusing cached promises after restart cannot nominate a different input
at that same round. Other acceptors bind the received nomination to that coordinator,
proposal, round and exact input. Complete authority replay precedes acceptance.

`VsrMembershipChosen` requires a distinct new majority accepting one exact input
at one round. Retaining this proof pins that input across subsequent recovery
rounds. A minority nomination alone is insufficient. Report identities remain
part of the authenticated crash-fault model; caller-supplied node IDs are not
independently verifiable signatures. Peer wiring must authenticate each promise,
nomination and acceptance before using these library constructors/methods.

Targeted tests pass all 106 VSR, 35 RPC and 57 protocol checks. Strict Clippy passes
for VSR/RPC/protocol/daemon all-targets/no-deps with warnings denied. New tests cover
canonical bounded codecs and truncation, old-quorum input ordering/identity,
minority nomination recovery after deleting old stores, durable same-round binding,
all four-voter majority intersections and report rotations, retained chosen input,
restartable overlapping batches, torn input recovery, bad roles/authority/state,
and process death after synced nomination before acknowledgement.

The production gate adds a dedicated decision domain and requires all seven new
meaningful regressions in actual workspace output. All 20 domains and 1,167 workspace
tests pass with zero failures on unchanged source. All seven new regressions
appear in both the dedicated recorded host run and the full workspace trace.
See [plan-decision verification](membership-decision-verification-2026-10-21.json). This is library plan consensus only. It does not acknowledge application
materialization, publish a new VSR cell, promote a learner or admit clients.
Generation publication, activation, removed-voter handling,
abort/conflict resolution and atomic distributed control admission remain open.

## Peer integration admission boundaries

Decision-store initialization must use an explicitly configured placement-group
path and local node identity. After a bounded authority upload, bind the validated
old-quorum proof to the existing cell or published learner's original bootstrap,
historical configuration chain and committed prefix before acknowledging a round
promise. A descriptor or endpoint list alone cannot create authority for a new
group. A configured learner may participate in transition-only consensus after
this check; it still cannot acknowledge application prepares, commits or probes.

Recovery RPCs must bind reports to authenticated node/cluster identities and
preserve the highest accepted input across coordinator changes. They must support
resending complete retained proof/history after both endpoints restart. Selection
of input is separate from application materialization, active generation publication,
new-quorum installation acknowledgement and matching durable control admission.
Failures or crashes between those boundaries must leave client readiness closed.

## Implemented authenticated peer plan orchestration

`with_membership_paths` configures owned paths only for groups already hosted as
voters or learners. `MembershipAuthorityInbox` receives canonical authority input
in full-sync bounded batches and retains its logical prefix across restart. A
complete upload replays old-quorum histories before decision-store initialization.
Hosted validation requires the original durable bootstrap, every known historical
configuration, unchanged locally committed operations and no conflicting client
request mappings. Unpublished learners and foreign/regressing authority reject
before a usable round promise exists. Endpoint mappings must include every old
and proposed voter identity. Group IDs in incoming messages cannot choose paths.

Peer kinds 22–31 cover authority BEGIN/BATCH/FINISH, round PROMISE, plan-input
BEGIN/BATCH, ACCEPT, CHOSEN and descriptor/history reads. Round messages bind the
proposal digest and authenticated coordinator; accepting peers also check the
durable nomination. Recovery uses bounded ordinal batches and complete original
input validation. These idempotent exchanges may resolve one lost reply. Normal
application voting retains its existing deadline/retry contract.

`choose_membership_plan` provisions anchored authority, gathers a new majority's
promises, obtains its highest accepted input locally or from an authenticated
survivor, stages it, persists the coordinator nomination and gathers acceptance.
A valid new majority yields a chosen-input proof. Chosen-proof dissemination is
resumable: losing those replies cannot erase the already durable accepted quorum.
An unpromoted learner may coordinate transition-only recovery after anchoring and
provisioning; application prepare/commit/tail/probe paths remain closed to it.

The crash-fault model trusts a configured authenticated coordinator to relay the
complete package of reports it obtained from authenticated nodes. Forwarded report
IDs are not independent signatures or Byzantine fault evidence. The normal frame
still binds the cluster, TLS node identity, destination and message kind.

New real TLS tests cover a learner recovering a different, remotely accepted input
after deleting the old primary and original coordinator stores; preserved complete
payloads and original client request mappings; learner restart without application
promotion; unpublished learner/wrong-bootstrap rejection; and exactly one retry
after an already durable acceptance reply is dropped, without another WAL vote.
The expanded release gate requires these regressions plus inbox restart coverage.

Targeted qualification passes all 202 affected tests on unchanged source, including
all four required inbox/TLS regressions. Five-run doctor, strict deterministic
orchestration, recorded host execution, strict trace verification, replay and CI
pass; strict lint is green. The full workspace run passes 1,171 tests with zero
failures and one opt-in stress test ignored. All 74 required regressions are
present, and the workspace doctor/strict/host/trace/replay/CI checks pass on
unchanged source. Fresh release stress qualification remains pending. See
[peer orchestration verification](membership-peer-verification-2026-10-21.json).
The full release gate now has 21 domains;
a complete run on this source remains pending. Active history merge/publication,
application materialization and installation acknowledgements, activation/promotion,
removed-voter handling, abort/conflict resolution, repeated membership generations
and atomic distributed control admission remain unfinished. Frame/batch size is
bounded; cold proof replay, repeated retained-prefix access and growing-history
capacity still require optimization and workload qualification.

## Implemented sealed staged prefixes and materialization receipts

`VsrMembershipStagedPlan` shares the existing view-installation commit/receipt
union. It selects the longest accepted old prefix with a canonical reporter tie
break, learns original historical commit certificates from the chosen old quorum,
and merges its nonconflicting client mappings. Accepted-but-uncommitted operations
are retained and are not passed to the application callback. The canonical plan
ID binds the exact input and merged history, independently of which valid new
quorum subset encoded the same choice.

`MembershipStagingStore` retains complete old-quorum input and the selected prefix
in separate full-sync bounded WAL batches. Checked VSRMST1 metadata seals identity,
input, choice, counts and content. Receiver-local request mappings may be retained
only when backed by the selected committed prefix; the physical stage digest
includes them while the canonical plan ID stays unchanged. Corrupt, missing or
torn sealed history fails reopen and exact factory retry without replacing the
retained metadata. Wrapped staged records cannot be opened as an active VSR cell.

Trusted, idempotent application callbacks run before a VSRMSR1 staged receipt is
synced. Callback failure leaves report admission closed. Reopen revalidates all
proof/history and requires materialization again, even with an existing receipt.
VSRMSQ1 requires distinct proposed voters forming a new majority for the same
input, canonical plan and committed/accepted boundaries. Different receiver-local
receipt suffixes may have different physical digests. These are staged receipts;
they do not change voting roles, publish active generations or admit clients.

Peer STAGE (32) uses the configured authority paths and locally validated anchors.
It explicitly requires `serve_connection_with_apply`; the default peer method
rejects rather than acknowledging through a no-op materializer. A learner can
stage and materialize a chosen prefix while remaining non-voting. Quorum collection
checks each response's authenticated reporter, input, plan and prefix boundaries.
A verified chosen input may also be retained on a voter that missed its upload.

The first 128 KiB TLS workload exposed a deadline failure after the staged prefix
and receipt were already synced. Repeating proof replay/materialization on the
lost-reply retry repeated the slow work. A same-boot receipt now reuses exact
voter-handle/revision or published-learner snapshot stamps. Reopen, anchor change
or unsuccessful materialization prevents that reuse. The payload and 800 ms
socket deadlines were not changed. Tests also observe that warm retry does not
repeat application work, and that restart rebuilds deleted derived files.
`recovery_state_token` is read-only cache identity and does not grant probes,
writes or authority on a fenced cell.

New directory creation syncs both newly allocated directories and their parent
links before using them as durable store roots. This includes VSR, learner,
transition and stage roots, plus initial segment-store root publication. Existing
roots avoid that creation work on the normal append path. Materializer fixtures
sync their file and parent directory entries before reporting success. This is
code/host-test evidence, not independent machine power-loss qualification.

Targeted verification passes 59 storage, 114 VSR, 39 RPC and 57 protocol tests.
Strict Clippy passes for storage/VSR/RPC/protocol/daemon all-targets/no-deps with
warnings denied. Coverage includes split commit/receipt evidence, alternate
choice-proof subsets, local receipts, uncommitted tails, corrupt/torn/missing
stages, a real process killed after synced receipt before reply, derived-file
rebuild after loss, explicit-materializer rejection, 128 KiB binary TLS delivery,
peer restart and a staged quorum after the originally selected primary is lost.

The earlier staging-phase immutable-source verification passes 21 of 22 domains and 1,181 workspace
tests with zero failures, including all nine required staging/directory/retention
regressions. Each passing domain includes five-run doctor, strict deterministic
orchestration, recorded host execution, strict trace verification, replay and CI.
The previously failing TLS staging test passes in the full workspace. Current
evidence is in `membership-staging-verification-2026-10-21.json` and
`artifacts/verification.sDhlKQ`.

That staging-phase release gate failed: release automatic-client takeover rejects a
post-loss SQL BEGIN when durable commit evidence reaches only one replica before
the peer deadline. Debug and a subsequent focused release rerun pass; the
intermittent failure remained unresolved at that source and its complete gate remains failed. Both native-KV
stress profiles pass separately. Earlier ENOSPC and workspace-staging failures
remain in `membership-staging-incomplete-verification-2026-10-21.json`.

The workspace staging deadline failure led to private replay-validated states
retained inside immutable approvals. Exact chosen-input retention and staged
source-copy checks compare complete bytes with validated proof without redundant
replay or directory rewrites. Reopen still fully validates proof from disk;
missing chosen input rejects without recreation. Payload, deadline and retry count
remain unchanged.

Native application installation/publication, voting-role custody, removed-voter
role integration, abort/conflict handling, repeated network membership lifecycles
and atomic distributed control admission remain unfinished. Retained
proof memory/access and independent-host/region capacity/fault qualification
remain required before production readiness.


## Active generation storage kernel

`DurableVsrCell::install_membership` accepts an exact staged/materialized chosen
prefix and matching new voting-majority staged proof. Its existing root must hold
the same durable old-quorum proposal fence. A compact `VSRMAI1` activation record
binds the old-quorum input descriptor, chosen decision, staged quorum, local
materialization receipt and canonical prefix boundary. Replay validates source
history identity, historical commit certificates, receipt mappings and exact
canonical/physical stage identities before installing the new authority.

The immutable generation is sealed and synced, its active pointer is renamed
and parent synced, and a checked `VSR_PUBLISHED` fence is synced before the writer
exposes the new state. A missing active pointer after publication fails recovery
instead of restoring older root authority. Existing valid generations acquire the
fence before a recovered writer is returned. Publication errors after rename
fence the live handle until reopen. Copied logical quorum histories retain the
same protection against unilateral local configuration shortcuts.

The kernel preserves original certificates, receiver-local receipts and accepted
uncommitted data, clears stale uncommitted acknowledgements, and preserves later
commits on an exact warm retry. Tests grow and shrink the voter set across two
activations, retain removed voters' old fence and then perform an ordinary view
change using the new voter set. Real child processes are killed at the sealed,
renamed and published boundaries and recover without an early completion reply.
These are local host crash tests; independent machine power loss is unqualified.

Report identities retain the authenticated crash-fault trust model. They are not
independent signatures or Byzantine quorum proofs. Activation records retain the
existing history-record size limit. New membership initiation now preflights the
complete combined proof budget before old-quorum fencing. The native peer
publisher, role custody and replicated atomic client admission are unfinished. This kernel API does not enable an operator membership
command or mark a cluster-wide transition complete. Scoped evidence is recorded
in `membership-activation-verification-2026-10-21.json`.


The earlier activation-phase immutable-source full gate passes 22/23 domains and all nine new
activation regressions in the workspace. The complete daemon domain and isolated
SQL streaming debug/release runs pass, but workspace SQL setup exhausts its three
same-frame transport attempts at INSERT `stream-91`: 1,192 tests pass, one fails,
one is ignored. The failed trace and checksum-verified fixture are retained.
That run remains a failed qualification result; neither isolated passes nor trace
integrity clear its failure. See `membership-activation-full-verification-2026-10-21.json`.

A separate working copy recovers `stream-91` at version/log index 100, rejects
changed SQL under that identity and preserves its duplicate result across all
three daemon restarts. The first recovered response is `replayed=false`; this
establishes recoverable retry behavior, not prior durable completion before the
original timeout. The original and preserved fixture checksums remain unchanged.


`LearnerHistoryStore::promote_membership` now publishes an already anchored new
voter into a separately owned target. It requires this configured learner's
boot-materialized stage and a matching new-quorum staging proof. A checked
`VSRLP1` identity binds the placement group, local node, original canonical plan
and bootstrap before generation creation. The `VSA2` pointer binds that identity
to the active generation; shared authoritative replay also validates historical
plan provenance. Older unpromoted `VSA1` stores retain their format. Older binaries
cannot open `VSA2` promoted stores.

The factory preserves one exclusive writer lock through physical publication and
the normal recovered-cell constructor. Unpublished claims stay closed to both
existing-history and bootstrap opens. Targets cannot overlap the learner or sit
inside another VSR/learner store's reclaimable directory tree. Missing/corrupt
owner or pointer metadata rejects without stale republication. Warm retries keep
later commits, receipts and ordinary views; a later removal cannot be reversed by
retrying the original promotion. `promoted_replica()` exposes provenance for
configured-node role validation, not client readiness.

Final-source verification passes 381 affected-package tests, including seven
promotion tests with four real parent-issued SIGKILL boundaries and cold recovery
after deleting all old voter and learner directories. Strict recorded trace
verification, replay, CI and Clippy pass. The preceding capacity source passes all
23 domains and 1,203 workspace tests. A fresh full gate is required for this
promotion source. See `learner-promotion-verification-2026-11-02.json` and
`membership-capacity-full-verification-2026-11-02.json`.

## Authenticated peer publication and historical recovery

Peer PUBLISH (33) accepts the chosen staged quorum and PUBLICATION (34) queries
an installed canonical plan. Both require an authenticated proposed voter and a
trusted application materializer. First publication retains the staged quorum
before changing authority. A promoted learner transfers its exclusively owned
cell into the peer's voter map synchronously, then releases its learner handle.
Configured promotion targets are local paths; requests cannot select them.

An acknowledgement proves a historical installation and successful application
of the current committed prefix. It does not grant current leadership or client
admission. Retries consult replay-validated active ancestry, preserve the first
durable publication proof, and leave later views, promises and commits intact.
They neither reinstall an old generation nor refresh primary-contact clocks.
Application errors keep materialization pending and prevent acknowledgement.

Cold transition recovery validates the same decision journal against its retained
chosen input, sealed local stage and active ancestry. The acceptor's initial
authority may differ from its eventual chosen input. This recovery does not
create a new acceptor or reset its promises. Historical stage retries apply the
current committed prefix; stale same-boot tokens cannot hide later application
work or reuse success after a materialization failure.

The TLS regressions delete the original source and nominated candidate, publish
through the surviving voter and learner, recover the promoted role, elect a new
candidate and commit more data. They retain an exact 128 KiB operation, original
request receipt and accepted tail. Additional cases cover failed application,
missing materializers, wrong authority, corrupted owner metadata, one lost
publication reply and an authenticated response naming the wrong reporter.
The affected-package and full-gate reports state the verified source and scope.

## Native recovery of an existing chosen transition

A native peer cell can opt into `"membership_recovery":{"interval_ms":20}` to
recover already provisioned custody at `state/membership-<pg>`. This does not
initiate a proposal or create replacement journals. Strict inbox, decision and
role reopening rejects missing identities, empty/torn-to-zero custody, foreign
owners and noncanonical paths before any listener starts. Complete owned
promotion can recover without the old learner directory.

The scheduler derives work from retained chosen input, stages and active
publication ancestry. It materializes locally, gathers staged receipts, publishes
and queries historical publication through the existing peer APIs. A single
bounded transport worker keeps incoming peer traffic available; only the owning
server thread changes journals, cell custody or application state. Responses are
checked again against the scheduled proposal/input/plan before advancing work.
Groups and peers rotate with bounded backoff. Full local replay/materialization
still processes retained history and is not a constant-time operation.

A receiver can import a surviving chosen certificate using complete input it
previously accepted, without the original uploader. Completed inbox input remains
readable by identity after another upload becomes selected; AUTH_FINISH still
binds the current selection. Missing/corrupt acknowledged input never falls back
to an inbox copy. One completed provision attempt does not turn repeated staging
rejections into successful repair or an endless full-upload loop.
Input creation, reads and append retries reject pre-existing symbolic links in
descriptor and record-storage routes before opening recovery writers. An ordinary
interrupted descriptor temporary can resume through exclusive file creation;
foreign temporary targets stay untouched. BEGIN, APPEND and chosen-input import
also reject structurally valid shortened input that was already retained as
authority, accepted or chosen; such data loss cannot become an upload cursor.
An unacknowledged staged prefix remains resumable after restart. Repeated chosen
imports preserve the existing exact-byte fast path against replayed approval.
These checks rely on exclusive store
ownership and do not claim protection against a hostile process concurrently
replacing arbitrary parent directories.

The existing peer identity gains a monotonic V2 managed-group set. Config removal,
missing transition roots and interrupted enrollment cannot silently enable client
service. V1 remains supported for unmanaged peers; older binaries reject V2.
Managed peers reject new proposals and keep client service closed pending
replicated control admission. Native startup and every client request also check
healthy replay-derived membership ancestry, including ancestry acquired by an
ordinary peer installation without a managed flag or V2 identity. Equal voter
IDs do not admit a later A-to-B-to-A history. Rejection happens before lending
cells, preserving voter probe acknowledgements; ordinary-view-only history
continues through normal client routing. Certified promoted custody can participate in
ordinary elections after restart under its stale learner declaration, once the
managed journal and ancestry validate. Unmanaged manual promotion still requires
explicit voter configuration for elections. Actual recovered membership and the
largest mapped managed voter set must fit the configured election deadlines.
The V2 policy protects custody loss while its identity survives. It cannot detect
replacement of the whole state root and identity by an older or fresh bootstrap;
that requires a separately trusted control binding.

This scheduler currently needs a surviving chosen certificate. If every chosen
certificate is lost, accepted-only higher-round recovery remains to be integrated.
Valid partial decision-input continuation, repeated transition lifecycles,
removed/re-added roles and conflict/abort handling also remain required. A
collected publication quorum is historical evidence, not a durable cluster-wide
admission record. Replicated control/catalog admission, bounded history and
retention, independent-group routing, independent-host/region capacity and machine
power-loss qualification remain open. No operator membership command is enabled.
