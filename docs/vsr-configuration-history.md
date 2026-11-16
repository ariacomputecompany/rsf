# Historical VSR certificate validation

A committed entry is authorized by the configuration installed for its view.
The current voter set is not authority for older certificates: removing a voter
must not invalidate its earlier votes, and adding a voter must not give it the
ability to authorize earlier history.

`DurableVsrCell` retains a configuration map keyed by installed view. Each entry
contains the primary, voter set and committed tail at installation. The map is
rebuilt from the persisted bootstrap plus ordered membership/view-change WAL
records; runtime changes update it only after their durable append succeeds.
It adds no parallel configuration file or independent checkpoint.

Both WAL commit recovery and committed-prefix catch-up validate against this
history. Validation requires:

- an explicitly installed configuration for the prepare's view;
- its original primary, voter membership and majority quorum;
- a matching prepare/commit/certificate identity and operation;
- an index after the configuration's preceding committed tail and no later than
  the boundary recorded when the next view was installed.

Installing a later view seals the earlier view at the committed tail. Even a
well-formed old-view certificate cannot extend that sealed prefix. Skipped views
are not inferred from neighboring configurations. Duplicate configuration epochs
in recovered WAL are rejected, matching the runtime requirement for strictly
increasing views. Existing contiguous-prefix and conflicting-prepare checks remain
in force.

Historical duplicates remain valid after membership shrink, growth, primary
change and restart. Their certificates are not rewritten using current voters.
An unknown history fails closed before catch-up appends a prepare or commit.

## Network boundary

The authenticated transfer RPC still requires the sender to be the current
remote primary. After that check, historical certificate validation uses the
entry's own installed configuration. This permits common-tip validation and
prefix repair after both stores have installed a membership change.

This is not a network membership protocol. A peer bootstrapped only with a new
configuration cannot invent the older configurations needed to validate an old
prefix. Authenticated history transfer and quorum-selected view installation are
implemented, including opt-in automatic peer/client takeover. Quorum-certified
voter changes and learner promotion remain release work; see
[primary failover](primary-failover.md).
Legacy logs without persisted bootstrap membership still require an explicit
migration before topology changes; use the
[bootstrap migration command](vsr-bootstrap-migration.md). View changes, timeout elections and membership
changes reject those stores before appending anything; their existing data remains
readable under the original configuration. Caller-supplied current voters are not
a substitute for missing historical authority.

## Verification

VSR tests exercise removed historical signers, later voters attempting to sign
old entries, different quorum sizes across views, primary changes, unknown views,
sealed-prefix violations, unchanged state after rejection, restart recovery and
corrupt WAL configuration/commit records.

The peer RPC process test commits through independent peers, stops them, installs
the same reduced membership in the surviving stores, restarts them, validates the
old tip and commits new data. Its local configuration installation deliberately
does not claim quorum-certified network reconfiguration. The VSR and RPC Fozzy
domains run these tests through real host processes with trace verification,
replay and CI checks.

Historical verification covered all 15 gate domains and 90 passing results. Evidence
is indexed in `artifacts/membership-history-verification-summary.json`. The base
run is `artifacts/membership-history-production-final.log`; daemon and workspace
were rerun in `artifacts/membership-history-regression.log` after a 500 ms fixture
deadline expired during catch-up under host load. Those durability fixtures now
use 2 seconds; dedicated RPC deadline tests remain unchanged. These are correctness
and recovery results, not latency benchmark claims.

## Authority changes after quorum installation

A published view installation is a durable authority boundary. Its cell rejects
local `view_change`, `timeout_election`, and `reconfigure` shortcuts; all of these
leave the WAL and installed state unchanged. The fence survives reopen because
it derives from the installed generation. A later view still proceeds through
durable promises, authenticated quorum history selection, and installation.

The shared WAL replayer rejects legacy local view/membership records following
an installation marker, including checksummed records written by older software.
Such histories cannot acquire authority merely by reopening or being selected
for recovery. There is no automatic rewrite that treats them as certified state.
Pre-installation legacy history remains readable under its existing migration
contract; rejecting these later shortcuts does not invent quorum evidence for
older local configuration changes.

A network-backed `VsrBackedFabricRouter` also rejects unilateral authority
changes before the first installation, with or without reachable quorum. The
`VsrTransport` default is fail-closed. Only `LocalVsrTransport`, the explicit
in-process simulation, opts into local model changes; installed cells retain
their own fence even when a caller supplies that transport. A successful probe
or an intersecting voter-set calculation is not a configuration proposal and
cannot authorize changing the set.

Regression tests check unchanged WAL/state across local rejection and restart,
rejection of appended unchecked authority records, and a subsequent genuine
quorum installation that preserves receipts and commits. A real TLS process test
rejects network-router shortcuts with peers running and stopped, then restores a
peer and commits at the original configuration. The current complete verification
report is [verification-2026-10-21.json](verification-2026-10-21.json); historical
evidence above does not replace a fresh run for current sources.

The remaining installation contract is tracked in
[certified membership](certified-membership.md). No current CLI or local model
operation establishes that network transition.
