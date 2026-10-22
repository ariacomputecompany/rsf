# Certified membership installation

Status: durable old-quorum approval, report recovery and exact-proposal
continuation from a surviving minority report are implemented.
Transition-only new-quorum plan consensus is implemented as a library store.
Authenticated peer plan orchestration is implemented over configured transition paths.
Sealed chosen-prefix staging and trusted materialization receipts are implemented.
Active configuration publication, learner promotion and control-metadata publication remain
unimplemented. Current application operation still uses a fixed admitted voter
set with quorum-selected view changes. Learner catch-up does not grant a vote.
Local configuration methods and reachability probes do not install network
membership. Installed histories reject local authority shortcuts both live and
on replay; [historical authority](vsr-configuration-history.md) documents that fence.

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

Targeted verification passes 59 storage, 113 VSR, 39 RPC and 57 protocol tests.
Strict Clippy passes for storage/VSR/RPC/protocol/daemon all-targets/no-deps with
warnings denied. Coverage includes split commit/receipt evidence, alternate
choice-proof subsets, local receipts, uncommitted tails, corrupt/torn/missing
stages, a real process killed after synced receipt before reply, derived-file
rebuild after loss, explicit-materializer rejection, 128 KiB binary TLS delivery,
peer restart and a staged quorum after the originally selected primary is lost.

The first expanded 22-domain full release gate stopped with host ENOSPC and exited
1 without a summary; partial results do not qualify the source. The failed attempt
is preserved in `membership-staging-incomplete-verification-2026-10-21.json`.
A full rerun after build-cache recovery must pass all eight new directory/staging
regressions. Active
generation publication, native application installation/publication integration,
activation/learner promotion, removed-voter roles, abort/conflict handling, repeated
generations and atomic distributed control admission remain unfinished. Growing
history proof access/memory and independent-host/region capacity/fault qualification
remain required before production readiness.

The subsequent disk-recovery gate passed 21 domains but failed final workspace
cold staging under its unchanged 800 ms deadline. In-process approvals now retain
private replay-validated states; exact chosen-input retries compare retained bytes
without rewriting inputs, and stage source-copy verification compares complete
bytes against the validated proof. Reopen still validates all proof from disk.
A missing chosen input fails without recreation. All 269 affected tests and
strict affected-target Clippy pass; full new-source qualification is pending.
