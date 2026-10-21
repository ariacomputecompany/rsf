# Certified membership installation

Status: not implemented. Current network operation uses a fixed admitted voter
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
