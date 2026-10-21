# Certified membership installation

Status: durable old-quorum approval, report recovery and exact-proposal
continuation from a surviving minority report are implemented.
New-quorum activation, learner promotion and control-metadata publication remain
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
must remain non-voting until verified installation, regardless of their log height.

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
regressions; qualification of this addition is pending.

This closes exact-proposal old-quorum completion after coordinator loss. It does
not implement abort, new-quorum activation, learner promotion, distributed control
metadata or atomic client admission. The full membership blocker remains open.
