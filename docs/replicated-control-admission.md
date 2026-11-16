# Replicated control state and admission

Status: the `rsf-control-state` leaf and exact VSR membership-lineage accessors
are implemented. Managed initiation, authenticated confirmation collection,
receiver gates and application/client admission are still required. The
[scoped verification](control-state-scoped-verification-2026-11-02.json) qualifies
the leaf; it does not establish these missing integrations or production readiness.

## Implemented boundary

`ControlState::recover` projects an actual healthy committed control cell against
an independently provisioned `ControlBootstrap`. Accepted-only commands cannot
update the catalog. Canonical bounded Genesis, Begin and Finalize commands retain
request outcomes, pending intent and original publication evidence. There is no
public recovery constructor from supplied entry lists or admission DTOs.

VSR lineage identifies the original bootstrap and latest certified membership
plan. Ordinary elections preserve lineage; a certified A→B→A transition does not
alias genesis. Uncertified legacy membership changes have unsupported lineage.
Re-preparing the same control operation in a later ordinary view preserves its
semantic outcome; concrete proposal freshness belongs at the actual fence.

The opaque `CommittedAdmission` checks a current live control stamp, complete
data inventory, exact membership lineages/voters, local role/certificate and
routing/policy digests. It currently requires a common data primary; the control
primary may differ. It grants no lease, fresh quorum, application readiness or
permission to recreate a missing data root. Callers must derive routing digests
from the installed router, not copy the expected values from the projection.

The initial regime keeps a fixed PG inventory/routing policy and one voter set
across control and data PGs. Explicit bounds are 64 node records, 64 PGs, 1 MiB
per command, 16,384 control log indices, 64 MiB replay payload, 4,096 request
receipts and 256 completed transitions. Combined plans must fit their encoded
bound; 64×64 is not guaranteed. There is no implicit eviction or checkpoint.
Capacity preflight reserves finalization space before a transition can fence.

## Required first-fence contract

A successful VSR commit can contain a rejected control command. Initiation must
recover `IntentAccepted` for the exact pending Begin, then durably confirm delivery
of that exact committed operation to an old-control majority before **any** PG is
first fenced. A prepare certificate, tail, matching voters or local-only commit
cannot replace this barrier. The raw-kernel diagnostic in the leaf tests shows
why: without intersecting commit delivery, an accepted-only Begin can be carried
into new membership and re-prepared under different membership authority.

The next implementation must retain the first accepted Begin index and original
historical control lineage in replay. Semantic retries cannot move that origin.
An opaque live intent binds the provisioned anchor, exact canonical command and
operation, original index, pending receipt and current control custody. Its
operation digest excludes replaceable ordinary prepare-view/primary fields.

Only authenticated transport observations of the exact locally committed intent
may enter the confirmation collector. Reporter identity must match the old
catalog's certificate/incarnation. Confirmation custody is bounded, immutable,
canonically encoded and atomically published. Raw report DTOs or peer-submitted
report lists must not construct an authoritative capability. Authenticated
transfer from validated retained custody is separate from arbitrary submission;
these are crash-fault attestations, not independently signed Byzantine proofs.

Before first fencing, retain confirmation on a proposed-new majority so loss of
the collector cannot strand recovery after the old quorum disappears. Each
actual receiver also needs its own committed exact Begin and retained valid
confirmation before effects. Missing or damaged acknowledged custody fails
closed; a chosen/staged input cannot manufacture the missing confirmation.

Under one serialized server borrow, each receiver must recheck control custody,
pending intent, target lineage, voters, concrete old view/primary and proposal
immediately before calling the kernel. When target equals control PG, use the
same handle once. Network work and callbacks must precede this final check.
BEGIN, RESUME and AUTH_BEGIN must share this policy; determine first fence versus
continuation from durable state, not the request verb. A different control
primary cannot impersonate a data group's old primary.

## Required integration

| Concern | Remaining implementation |
| --- | --- |
| Writer | Reconcile accepted operations at their original indices; validate bounded accepted tails before new PREPARE; never replace a stale pending Begin with a fresh proposal. |
| Control replication | Route configured control COMMIT/TRANSFER and staged control materialization through verified control replay, outside Fabric command/version processing. |
| Learners | Provide a narrow read-only, custody-checked control replay source; require locally committed Begin before managed upload/promotion. |
| Confirmation | Private authenticated collector, retained proof custody, bounded transfer/fan-out and restart recovery. |
| Receivers | Exact shared gate across first fences, continuation and authority upload; selected control history must contain Begin as committed. |
| Provisioning | Monotonic trusted control binding and complete managed inventory before create-capable openers/listeners; no configuration-removal fallback. |
| Admission | Finalize through authenticated publication collection; enforce the same authority/materialization boundary in native, standalone, shared-router and admin paths. |

A stale concrete proposal remains pending after an ordinary election. Safe
cancellation/supersession requires proof that competing effects cannot occur;
local absence of a fence is insufficient. Repeated transition storage, retired
roles, independent PG/global application ordering, history bounds/checkpoints and
deployment workload qualification remain open.

Bootstrap lineage identifies a configuration, not a unique storage incarnation.
A surviving control root cannot authorize an empty replacement data bootstrap.
Existing admitted data requires strict existing-history custody; complete root
loss or rollback requires independently durable provisioning/recovery authority.

Required fault qualification includes local-only versus majority Begin delivery,
ordinary reprepare and retries, an election between planning and receiver effect,
control-PG aliasing, collector loss at every custody/fan-out/fence boundary,
uncommitted learner denial, forged/stale/corrupt confirmation rejection, and
configuration/root loss across every managed entry point. Existing native client
denial remains in place until these paths are implemented and verified.
