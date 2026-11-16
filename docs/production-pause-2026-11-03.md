# Production work pause — November 3, 2026

Work stopped at the user's request after saving and pushing the integrated
development checkpoint. **This is not a production-qualified release.** The
preceding committed checkpoint is `46a58b57e18bf7ecaf7f73baebb4a61568c18432`.

## Integrated source

- Autonomous pre-choice scheduling, authenticated active-copy progress and
  adversarial TLS/native-process coverage.
- Ancestor-directory publication barriers for creation/retry, native roots and
  segment files; ownership-aware placement of barriers to avoid repeated syncs
  during read-only recovery.
- Physical owned-storage custody stamps, VSR failure latching and published
  learner tokens; the real managed polling regression now passes after first
  reproducing the missing-manifest failure.
- Five native pre-choice scenarios, corrected routing/client-denial fixture
  setup, and an expanded 25-domain qualification script.
- The control-state leaf remains separate from the pending managed writer and
  receiver/admission integration described in `replicated-control-admission.md`.

## Verification at the pause

Formatting, shell syntax, scenario JSON and whitespace checks pass. Strict
Clippy passed across the six affected packages with `--no-deps --all-targets`.

The latest affected-package run used unchanged source fingerprint
`27b827c3b8707a64cc8fee51a63d7ba2af157fbbdd3ca60447d39638f07fa9f1`.
Control-state passed 10 tests, protocol passed 57, and RPC passed 75 with **one
failure**. Cargo stopped before storage, VSR and rsfd tests. The failing test is
`replication::tests::authenticated_learner_transfer_resumes_and_never_acknowledges_voting_requests`:
the resumed `sync_learner` call at `crates/rsf-rpc/src/replication/tests.rs:1047`
returned `Resource temporarily unavailable (os error 35)`. The exact slow stage
and cause remain undiagnosed. Do not silently increase timeouts, reduce
concurrency or hide the failure with retries.

Fozzy five-run strict doctor and scripted strict scenario checks passed before
the real host run. That host run failed; its recorded trace passes strict
integrity verification. This does not make the failing run successful or prove
deterministic native scheduling. See `prechoice-checkpoint-2026-11-03.json` for
local evidence hashes. Raw traces/logs remain in ignored `artifacts/` locally.

Earlier native coverage passed two of five cases, with three fixture failures.
The routing and denial fixture fixes are integrated; the corrected two-PG case
then passed independently. The complete five-case suite has not passed together
at this final source. Earlier storage/caller test passes predate the final
performance and custody changes. The historical 24-domain full pass applies to
its recorded earlier fingerprint, not this checkpoint.

## Resume order and known gaps

1. Diagnose the authenticated learner-transfer failure with stage timing and
   server-side errors. The test learner server currently discards handler
   errors; preserve evidence without weakening protocol/test requirements.
2. Add custody validation before cached published learner BEGIN/FINISH success.
   Static review found that deleting the already-published inner MANIFEST or
   active segment can still produce a retry ACK from cached ID/tail. The new
   scheduler polling guard does not cover these public history handlers.
3. Guard managed inbound membership/pre-choice mutation and staged/publication
   retry paths. Authenticated PROMISE currently reaches the decision journal
   without revalidating hosted physical custody. Preserve legitimate published
   ancestry and ordinary view changes; do not blanket-validate stale authority.
4. Run final storage/VSR/caller tests, all five native scenarios in both profiles,
   then the frozen 25-domain full gate and actual stress tests. Reconcile gate
   regression names with any new guards. Save failures as well as successes.
5. Continue managed control initiation, exact old-majority Begin commit-delivery
   confirmation, retained proof transfer, receiver gates and application/client
   admission. Repeated lifecycle, unfenced old-voter enrollment, independent PG
   routing, history bounds/retention and deployment/workload qualification remain.

Additional static review boundaries: the new stamp checks metadata/identity and
expected canonical manifests, not an adversarial content scrub; Unix file
identity is currently required. Existing VSR_ACTIVE/VSR_SEAL reads still allocate
before fixed-length validation. Responsive corrupt accepted-minority liveness
requires the pending five-driver diagnostic; no finite schedule proves an
unconditional liveness guarantee. Trusted provisioning is still required after
complete data-root loss.

Unintegrated agent drafts and design notes are retained locally under
`artifacts/native-prechoice-development/` and
`artifacts/control-admission-development/`; they are not qualified source.
No RSF Cargo/Fozzy/native test process remained active at the pause check.
