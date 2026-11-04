# Development checkpoint — 2026-11-03

Stopped at the user's request. This is a preserved development checkpoint, **not a production release**. Linux is the only production target. Delivery uses direct pushes to `main`; no CI/CD, workflow, hosted runner or deployment is included.

## Current source

Source fingerprint: `e1abbc091d01d6130ce1553877b74454f6b3fdec73e8e5566018cbca74156a6d`.

The checked-in work adds bounded storage/VSR replay and original-custody append paths, bounded voter recovery, control-state replay and membership intents, managed RPC routing and control bootstrap/Genesis handling. The stale custody-test assertion now checks the actual public refusal and unchanged private state. Temporary STAGE diagnostics were removed. The pending storage-open optimization described below is **not applied**.

Closing checks passed: **484 tests** (57 protocol, 138 RPC, 289 VSR), zero failures or ignored tests, strict trace verification/replay/local parity, workspace all-target Clippy with warnings denied, and formatting. Exact source, commands, outcomes and hashes are recorded in `closing-checks.json`. They must be interpreted at their stated scope. The current source has not passed a fresh complete 25-domain baseline. The previous full attempt passed 20 domains and failed five; its source-bound record remains in `../../bounded-control-full-v1-attempt-2026-11-03.json`. A corrected assertion or a later scoped pass does not erase the earlier STAGE deadline failures. The older fully passing custody-group report belongs to an earlier source checkpoint.

## Preserved pending work

`pending-work.tar.gz` contains the complete authored draft trees, exact patches, dependency manifests and static reviews for nine pending work packages, plus the STAGE analysis and publication-ordering notes. `pending-work-manifest.json` records every archived file's size and SHA-256 and the archive hash. The archive was read back and checked against that inventory. Source files remain under their original `artifacts/...` paths inside the archive. Extract into an empty directory for inspection; do not apply all patches indiscriminately.

These drafts are not compiled, runtime-qualified or Linux-qualified features:

| Draft | Selected patch identity | Required next work |
| --- | --- | --- |
| Storage-open publication | Combined `ad60f7a9…6143917` | Strengthen the pre-open lock-inode baseline in the test oracle, run tests-only RED and production GREEN, and qualify at original concurrency/deadlines. It preserves the extra directory publication needed for final-file aliases. |
| Provisional disposal | `15cf31ad…150ac883` | Compose and run actual ownership/cleanup contracts, including unprivileged Linux failures. |
| Ordinary bounded disposal | Corrected `0fab7258…7fe8b2f5` | Integrate and qualify bounded A/B/C disposal and strict cold recovery. Compose the initial owner's private context before combining stacks. |
| Disposal caller companion | Corrected `62adcfd8…4d9727` | Run all three physical caller schedules. The correction requires the specific failure-latch error; a generic error was insufficient. |
| Initial learner receiver | `78f27f30…af816c`, companion `4f738696…6ce4d0` | Integrate, compose with ordinary disposal, and qualify the receiver. This does not implement a production sender or admission. |
| Native control identity | Original + R1 + R2 `796d2b20…f80a49b` | Execute the physical readback-defect tests against old/new code and qualify native integration. The original `63723005…` draft has a known pending-file content-verification defect; its preserved R2 correction must be included. |

The archive's original drafts and superseded revisions are preserved for audit; their presence is not approval to ship them. Reviews explicitly distinguish static reconstruction from actual execution. Local full trace files remain in the workspace's ignored `artifacts` directory; diagnostic report hashes and analyses are preserved in the archive.

## Resume boundary

1. Review the current source and this checkpoint; preserve the recorded failures.
2. Qualify the small storage-open correction and actual STAGE timing without changing deadlines, retries, workload size or concurrency. A successful instrumented run still contained first-attempt timeouts recovered by the existing retry.
3. Run a fresh full local baseline on the resulting source, then push a verified milestone directly to `main`.
4. Integrate the pending receiver/disposal/native stacks deliberately, including the recorded dependency rebase and oracle corrections.

Production still requires sender/fresh-voter work, authenticated delivery confirmation, new-voter retention, first-fence and client/operator admission, repeated membership lifecycles, bounded growing-history/receipt retention and migration, independent routing, Linux VM/kernel and power-loss checks, and independent-host fault/capacity qualification. Scoped Darwin tests do not supply those guarantees.
