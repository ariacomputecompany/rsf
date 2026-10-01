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
prefix. Authenticated configuration-history transfer, quorum-certified network
installation, learner admission and automatic failover remain release work.
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

Final verification covers all 15 gate domains and 90 passing results. Evidence
is indexed in `artifacts/membership-history-verification-summary.json`. The base
run is `artifacts/membership-history-production-final.log`; daemon and workspace
were rerun in `artifacts/membership-history-regression.log` after a 500 ms fixture
deadline expired during catch-up under host load. Those durability fixtures now
use 2 seconds; dedicated RPC deadline tests remain unchanged. These are correctness
and recovery results, not latency benchmark claims.
