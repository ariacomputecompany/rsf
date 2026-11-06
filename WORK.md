# RSF Production Work Ledger

## Purpose

This file is the sober implementation ledger for turning RSF from the POC/scaffold into the real custom protocol and full database implementation.

The standard is strict:

- no false production paths
- no simulated behavior in the real source path
- no placeholder consensus
- no fake storage adapters
- no pretend recovery
- no demo digest, fake crypto, or toy erasure in production crates
- no developer-facing feature without failure behavior, recovery behavior, observability, and deterministic tests

## Ledger Rules

- `WORK.md` is a shrinking work ledger, not a changelog.
- When a chunk is fully implemented and verified, remove that chunk from this file with no preamble.
- Do not add "done", "completed", or historical notes for removed work.
- If a chunk changes shape before completion, rewrite the remaining work so only unfinished work stays.
- Commit and push verified milestones regularly to `ariacomputecompany/rsf` remote `main`, as requested. Keep unfinished release blockers explicit; a successful milestone is not full production qualification.

## Current State Audit

Current release status: **not production qualified**. Linux is the only production
target. Verification is local and releases use ordinary pushes to remote `main`;
no CI/CD is authorized.

A clean full baseline remains missing. The latest complete baseline passed 22
of 25 domains and failed membership-peer, peer-rpc and workspace; terminal
evidence is preserved in `docs/decision-owned-baseline-terminal-2026-11-06.json`.
The decision-input writer fix passes standalone full VSR (323 tests) and RPC
(146 tests), recorded in
`docs/decision-input-owned-recovery-qualification-2026-11-06.json`, but STAGE
failures remain in the broader baseline. Workspace stopped at RPC, so its later
binaries still need requalification.
Diagnose and fix failures under the original deadlines, workloads, retries and
concurrency, then requalify the final source through all 25 domains. Preserve
earlier failed traces; scoped recovery fixes do not establish a clean baseline.

The integrated initial-learner path still needs complete RPC qualification: its
historical-Genesis relay Start request timed out in the full RPC run. Full VSR
(372 tests), storage (180) and control-state (24) passed; RPC passed 156 and failed
3 under unchanged controls. Evidence is preserved in
`docs/initial-learner-progress-qualification-2026-11-06.json`. Diagnose the initial
transfer deadline, membership lost-reply deadline and partial-stage deadline
without changing the tests. Native-identity persistence still needs integration,
its retained-file readback correction and actual red/green execution. The prepared
initial sender remains an unqualified draft.

Managed control initiation, authenticated commit-delivery confirmation, new-voter
retention and receiver/client admission remain unfinished. Local policy or passing
scoped tests cannot authorize those operations. Growing-history reopening/replay,
bounded retention, legacy migration, independent-group routing and independent-host/
region fault and numerical capacity qualification remain required. Local Darwin
checks do not establish Linux kernel behavior, machine power-loss durability or
production capacity. Actual Linux VM qualification and a fresh full baseline remain
open.

### Next implementation: durable creation, autonomous choice and control admission

- Resolve the remaining STAGE deadline failures, then obtain a fresh full
  baseline certificate. Preserve original deadlines,
  retry counts, concurrency and every failed attempt.
- Finish initial-learner RPC deadline qualification and integrate native-identity
  persistence, provisional voter creation and the sender. Preserve the integrated
  original owner context and bounded namespace disposal; finite WAL reads alone
  do not bound the complete transfer lifecycle.
- Investigate responsive corrupt accepted minorities with the five-driver
  diagnostic; qualify unfenced old-voter enrollment without treating missing
  acknowledged input as repairable.
- Persist publication completion through replicated control/catalog authority.
  Qualify repeated membership lifecycles, removed/re-added roles and partial
  proposal conflict/abort; the current fixed transition paths are one-shot.
- Replace conservative client denial only when replayed control/catalog authority
  and application materialization agree on exact current membership lineage.
  Enforce the same boundary across native, standalone, cached router and admin
  paths; local policy and equal voter IDs cannot establish admission. Do not
  enable an operator membership command ahead of that integration and its gates.
- Independent-group routing, bounded retention/migration and independent-host/
  region numerical capacity/fault qualification remain required.

### Production-leaning but incomplete

- `crates/rsf-core`
  - Has authoritative base IDs and domain modules, typed cluster/node-incarnation/region/failure-domain/policy/layout/surface/trace/operation identities with parsing/display validation, typed byte/key/value/range wrappers, durable timestamp and commit-clock contracts, explicit commit-version successor/genesis contracts, BLAKE3-backed content IDs for blob/object/chunk payloads, algorithm-tagged digest contracts across audited checksum/content-addressed production crates, operator-facing error categories, cluster node descriptors that use typed region/failure-domain IDs, typed superblock cluster identity, typed RPC trace identity, shared core operation IDs through protocol certificates and VSR protocol operations, typed region/failure-domain boundaries across admission, deployment, provider selection, and upgrade planning, and typed auth surface resources across API, gateway, SQL/session, and POC flows.
- `crates/rsf-storage`
  - Has a real append segment file, versioned segment record envelopes, explicit checksum policy metadata, BLAKE3-backed record checksums and WAL digests, recovery scanning, WAL record encoding, placement-group tail recovery, hash-chain validation, committed-prefix evidence, retention/compaction maintenance records that preserve committed-prefix evidence, crash-released operating-system ownership locks with same-process recovery reentrancy, sync policy coverage, IO error classification, append/replay benchmark coverage, production segment directory layout, manifest lifecycle, multi-segment rotation and recovery, physical compaction rewrite with manifest-carried compacted prefixes, restart-safe retention reclaim, corrupt-segment quarantine that fails closed, multi-segment scan/truncate APIs that retain committed and membership metadata prefixes, larger append/replay recovery benchmarks, a durable published-manifest catalog over segment WAL records with a first-class manifest WAL kind, idempotent duplicate publish for identical manifest content, rejected publish paths that do not persist, fail-closed replay for malformed manifest WAL payloads and duplicate recovered manifest ids, and fault-injection tests for torn writes, bad versions, unsupported checksum policies, lock contention, malformed manifests, cross-segment hash-chain corruption, and corruption.
- `crates/rsf-manifest`
  - Has validated physical extent manifests for SQL, KV, object, blob, snapshot, and backup surfaces; provider replica metadata validation; checksum proof validation; repair planning for missing, corrupt, and under-replicated extents; typed surface manifests; and host-backed deterministic Fozzy trace coverage through the storage-owned durable published-manifest catalog.
- `crates/rsf-page`, `crates/rsf-wal`, `crates/rsf-buffer`
  - Have a real relational page/redo/buffer path with deterministic page-image and redo-record codecs, slot/tombstone capacity validation, idempotent redo replay and checkpoint summaries, WAL-before-page-flush gating, dirty frame flush planning, clean LRU eviction under frame/byte budgets, and a durable relational WAL gate over `rsf-storage` segment records that recovers synced flush eligibility, recovers unsynced redo as not flushable, rejects malformed relational WAL payloads fail-closed, and has host-backed deterministic Fozzy trace coverage for page/WAL/buffer/storage/core recovery behavior.
- `crates/rsf-vsr`
  - Has a prepare/ack/commit/view-change state machine, deterministic timeout election, exact signer-set commit certificates that bind quorum acknowledgements to the prepared operation, durable prepare/commit/view-change/client-table/membership WAL records through multi-segment `rsf-storage` stores, restart recovery for committed prepares, learner catch-up from committed prefixes, view-change committed-prefix proof, quorum-overlap voter-change validation, stale-primary fencing, duplicate client requests, explicit replica transport abstraction, deterministic partition/drop tests at the protocol-certificate boundary, multi-segment restart coverage for prepare/commit/view-change/membership/client-table records, VSR-level committed-prefix compaction rejection until snapshots exist, full binary command persistence on independent follower stores, bounded prepare/commit codecs, contiguous commit and catch-up prefixes, stale-vote-free local reproposal, durability-error fencing, forced follower-process termination recovery, and host-backed Fozzy trace coverage.
  - Has durable election promises in a mandatory WAL kind, fail-closed replay and truncation protection, old-view mutation/probe fences that survive restart, and authenticated per-voter frozen-history collection over TLS. Reports preserve accepted-but-uncommitted operations and client metadata; collecting a quorum does not activate a candidate. Exhaustive three-/five-voter majority tests and real process kill/restart tests cover fencing. See `docs/primary-failover.md`; opt-in peer elections and client takeover are implemented; certified membership changes and deployment qualification remain unfinished.
  - Has shared authoritative replay for cold recovery and election validation; common-installed-view quorum selection with accepted-prefix validation, deterministic source selection, merged commit evidence and client mappings; immutable generation publication with synced checksummed pointer/seal, mandatory installation WAL records, idempotent retry preserving later commits, receiver-local receipt preservation, I/O-failure fencing and inactive-generation cleanup. Tests cover successive views, abrupt process exit at publication boundaries, corruption, learner replay, and actual TLS replication after local installation on both survivors. Mixed-view reconciliation and resumable remote view installation are implemented; opt-in peer/client takeover is implemented; independent-group routing and deployment qualification remain unfinished.
  - Reconciles mixed installed views using last-normal-view priority, accepted operation prefixes (including differing reprepare headers), quorum commit/receipt evidence, and exact-operation authorization for certificates learned after an older view was sealed. Uses versioned compact marker metadata with legacy replay and semantic retry compatibility. Tests reject committed forks and recover a TLS candidate that restarted after activating alone; opt-in peer/client takeover is implemented; broader topology qualification remains unfinished.
- `crates/rsf-protocol`
  - Has complete protocol-only command replication and ordered recovery alongside surface commands, reconstruction of protocol receipts from installed peer cells, exclusive cell ownership transfer, receipt-failure fencing, and rejection of conflicting receipts and request identities. Recovery APIs now support the opt-in peer client listener through scoped live-cell custody and shared application writers.
  - Has the custom protocol profile contract, operation identities, commit certificates, strict/AP mode gates, a durable protocol WAL boundary over `SegmentStore`, restart recovery for commit certificates, corrupt-payload failure tests, multi-segment protocol WAL recovery, durable router recovery, durable client request idempotency through the VSR client table, recovered commit-version/index generation, durable placement-map routing, durable policy catalog gates, multi-segment placement-map/policy recovery, protocol truncation safety for certificates and metadata, VSR view-synchronized certificate epochs, router-integrated VSR membership changes, timeout-driven primary recovery with local-leadership guards, split protocol-WAL/VSR-store layout coverage, and a transport-driven VSR-backed router that only emits protocol certificates after durable quorum VSR commits.
- `crates/rsf-admin`
  - Has durable invite-token verifier admission, node identity validation and duplicate rejection, explicit learner sync before promotion, membership-backed learner promotion with quorum-overlap evidence once voters exist, quorum-preserving voter removal and synced-learner replacement over the VSR membership path, durable append-only admission/learner/promotion/reconfiguration/audit recovery over `rsf-storage`, side-effect-free placement preview, failure-domain-aware replicated placement planning, placement policy preflight explanations, durable protocol placement-map apply integration through `rsf-protocol`, safe protocol policy apply preflight, and accepted/rejected admission/placement/reconfiguration/policy audit events.
- `crates/rsf-policy`
  - Has a durable desired-state policy catalog over `rsf-storage`, versioned full-state policy records, safe replica/placement-group policy validation, primary/read-replica region policy, strict global-write consistency/latency explanations, restart recovery, and rejection tests for unsafe policy changes.
- `crates/rsf-control`
  - Has a policy reconciliation engine that computes desired placement/protocol-policy steps from durable policy/admin/router state, applies expand and shrink changes in safe order, emits typed reconciliation progress events and observability audit records, and keeps `rsfd policy apply` idempotent.
- `crates/rsf-consistency`
  - Has request-level consistency validation across strict quorum, availability-first, bounded-stale, and immutable-verified modes; rejects mutable-truth availability-first downgrades; requires verified immutable content identity before AP blob-payload writes; requires explicit version-lag budgets for bounded-stale reads; rejects stale reads that exceed recovered/required version bounds; requires digest or manifest proof for immutable-verified payload reads; keeps admin and writes on strict paths; and has host-backed deterministic Fozzy trace coverage for consistency/gateway/policy/wire/core enforcement.
- `crates/rsf-health`
  - Has pure node health scoring plus a durable health monitor over `rsf-storage` segment WAL records, first-class health WAL kind, recovered policy/observation replay, recovered promotion/demotion/quarantine action derivation after restart, fail-closed replay for invalid recovered policies, observation validation before persistence, explicit corruption rejection for malformed health WAL payloads, and host-backed deterministic Fozzy trace coverage for health/core/storage recovery.
- `crates/rsf-auth`
  - Has a pure principal/grant/token authorization engine plus a durable auth catalog over `rsf-storage` segment WAL records, first-class auth WAL kind, recovered principal/grant replay, recovered authorization after restart, fail-closed replay when grants reference unknown recovered principals, namespace/resource validation before persistence, explicit corruption rejection for malformed auth WAL payloads, and host-backed deterministic Fozzy trace coverage for auth/API/core/storage recovery.
- `crates/rsf-tenancy`
  - Has a pure tenant/database/namespace control-plane catalog plus a durable tenancy catalog over `rsf-storage` segment WAL records, first-class tenancy WAL kind, recovered tenant/database/namespace replay, recovered surface-gate validation after restart, fail-closed replay when databases reference unknown recovered tenants, namespace rebinding rejection, explicit corruption rejection for malformed tenancy WAL payloads, and host-backed deterministic Fozzy trace coverage for tenancy/core/storage recovery.
- `crates/rsf-catalog`
  - Accepts the protocol router port and has VSR-backed durable router coverage for namespace/table/bucket/binding commit-version and certificate recovery.
- `crates/rsf-blob`
  - Accepts the protocol router port, verifies BLAKE3 content identity on reads, dedupes duplicate payloads without new protocol commits, has durable chunk-backed blob payload persistence with verified read-after-reopen, and has VSR-backed durable router coverage for blob pin commit-version and certificate recovery.
- `crates/rsf-idempotency`
  - Has a pure idempotency/retry domain engine plus a durable idempotency ledger over `rsf-storage` segment WAL records, first-class idempotency WAL kind, recovered claim/finish replay, recovered finished-response replay after restart, recovered unfinished-claim in-flight detection after restart, conflicting-fingerprint rejection, malformed response rejection before persistence, explicit corruption rejection for malformed idempotency WAL payloads, and host-backed deterministic Fozzy trace coverage for durable idempotency/core/storage recovery.
- `crates/rsf-provider`
  - Has a pure provider-selection directory plus a durable provider directory over `rsf-storage` segment WAL records, first-class provider WAL kind, recovered advertisement and health-state replay, explicit corruption rejection for malformed provider WAL payloads, and host-backed deterministic Fozzy trace coverage for provider/object selection recovery.
- `crates/rsf-quota`
  - Has a pure quota policy/usage engine plus a durable quota engine over `rsf-storage` segment WAL records, first-class quota WAL kind, recovered policy and admitted-reservation replay, rejection paths that do not charge usage, explicit corruption rejection for malformed quota WAL payloads, and host-backed deterministic Fozzy trace coverage for quota/POC usage recovery.
- `crates/rsf-cache`
  - Has a pure read-cache admission/strict-read/read-ahead engine plus a durable read-cache wrapper over `rsf-storage` segment WAL records, first-class cache WAL kind, recovered accepted cache entries after restart, replayed access touches that preserve eviction order, recovered stale and explicit invalidations, no WAL persistence for misses or rejected admissions, fail-closed replay for impossible touch/invalidation history, explicit corruption rejection for malformed cache WAL payloads, and host-backed deterministic Fozzy trace coverage for cache/core/storage recovery.
- `crates/rsf-checkpoint`
  - Has checkpoint root and segment compaction planning plus deterministic checkpoint certificates that bind checkpoint/high-watermark/counter fields to root, segment, and decision digests; rejects missing required roots, invalid root checksums, impossible segment live-byte counts, retained-root/compaction/high-watermark counter mismatches, and tampered plan decisions before checkpoint evidence can feed superblock or recovery; and has host-backed deterministic Fozzy trace coverage for checkpoint/superblock/recovery/snapshot/backup/core restart evidence.
- `crates/rsf-snapshot`
  - Has snapshot root/barrier validation for catalog, transaction-log, KV, SQL, object, blob, and chunk-index roots plus a durable snapshot catalog over `rsf-storage` segment WAL records using the first-class snapshot WAL kind, recovered snapshot inventory after restart, idempotent duplicate create for identical snapshot content without extra WAL records, conflicting duplicate ID rejection before persistence, fail-closed replay for malformed snapshot WAL payloads, and host-backed deterministic Fozzy trace coverage for snapshot/core/storage recovery.
- `crates/rsf-superblock`
  - Has required boot-root validation for catalog, transaction-log, KV, SQL, object, blob, and chunk-index roots plus a durable superblock store over `rsf-storage` segment WAL records, first-class superblock WAL kind, recovered latest accepted generation/checkpoint after restart, cluster-stable monotonic generation/layout/checkpoint advancement, rejected advances that do not persist, fail-closed replay for malformed superblock WAL payloads and regressing recovered histories, and host-backed deterministic Fozzy trace coverage for superblock/core/storage recovery.
- `crates/rsf-lease`
  - Has pure primary lease/fence-token validation plus a durable lease table over `rsf-storage` segment WAL records, first-class lease WAL kind, recovered acquire/renew replay, recovered stale-epoch and stale-fence rejection after restart, monotonic fence-sequence continuation after recovery, explicit corruption rejection for malformed lease WAL payloads, and host-backed deterministic Fozzy trace coverage for lease/POC recovery.
- `crates/rsf-jobs`
  - Has a pure background job/dependency/lease/attempt engine plus a durable job queue over `rsf-storage` segment WAL records, first-class job WAL kind, recovered enqueue/lease/complete/fail replay, retry attempt preservation after restart, active lease recovery without duplicate attempt charging, terminal failure recovery, explicit corruption rejection for malformed job WAL payloads, and host-backed deterministic Fozzy trace coverage for jobs/POC recovery.
- `crates/rsf-migration`
  - Has a pure schema migration lifecycle plus a durable migration lifecycle over `rsf-storage` segment WAL records, first-class migration WAL kind, recovered begin/advance/rollback replay, encoded migration plan/table/step compatibility metadata, durable partial-progress resume after restart, completed migration terminality after recovery, rolled-back migration terminality after recovery, explicit corruption rejection for malformed migration WAL payloads, and host-backed deterministic Fozzy trace coverage for migration/POC recovery.
- `crates/rsf-upgrade`
  - Has a pure rolling-upgrade planner plus a durable upgrade rollout tracker over `rsf-storage` segment WAL records, first-class upgrade WAL kind, recovered rollout start/step-completion/failure replay, active step cursor recovery after restart, failed-rollout terminality after recovery, wrong-step rejection, explicit corruption rejection for malformed upgrade WAL payloads, focused rollout/codec module ownership, and host-backed deterministic Fozzy trace coverage for upgrade/POC recovery.
- `crates/rsf-deployment`
  - Has pure package/profile/deployment planning plus a durable deployment rollout ledger over `rsf-storage` segment WAL records, first-class deployment WAL kind, recovered planned/started/applied/failed rollout lifecycle replay, restart-visible pending deployment recovery, manifest/profile validation before persistence and during replay, fail-closed replay for invalid lifecycle transitions, explicit corruption rejection for malformed deployment WAL payloads, focused ledger module ownership outside the planner, and host-backed deterministic Fozzy trace coverage for deployment/core/storage recovery.
- `crates/rsf-objectmeta`
  - Accepts the protocol router port and has VSR-backed durable router coverage for bucket registration, multipart start, part acceptance, and head commit certificate recovery.
- `crates/rsf-object`
  - Accepts the protocol router port through the object facade, has VSR-backed durable router coverage for bucket, blob pin, and object manifest certificate recovery, and has a durable object store that persists bucket/head indexes, composes object payloads through verified durable blobs, records replacement head history, and recovers verified object reads after reopen.
- `crates/rsf-erasure`
  - Has systematic Reed-Solomon-style erasure coding over GF(256) instead of XOR-only parity, deterministic shard matrix generation, BLAKE3-verified shard manifests, recovery from any missing shard set within the configured parity budget, fail-closed rejection for corrupt present shards, duplicate/misreported shard ordinals, impossible policies, and unrecoverable degradation, object cold-layout integration for multi-data-shard loss, and host-backed deterministic Fozzy trace coverage for erasure/object recovery.
- `crates/rsf-stream`
  - Has byte-backed streaming write/read planning with BLAKE3 chunk hashes, read-lane layout, flow-control window enforcement, content verification that detects corrupt chunk bytes, durable stream transfer ledger records over `rsf-storage` segment WAL records, first-class stream WAL kind, recovered pending and completed transfers after restart, duplicate/corrupt chunk rejection before persistence, fail-closed replay for malformed stream WAL payloads or impossible completion history, and host-backed deterministic Fozzy trace coverage for stream/core/storage recovery.
- `crates/rsf-retention`
  - Has pure object/blob retention and legal-hold evaluation plus a durable retention policy catalog over `rsf-storage` segment WAL records, first-class retention WAL kind, recovered namespace policy replay, recovered legal-hold/delete-block behavior after restart, fail-closed replay for invalid recovered policies, explicit policy validation before persistence and during replay, explicit corruption rejection for malformed retention WAL payloads, and host-backed deterministic Fozzy trace coverage for retention/core/storage recovery.
- `crates/rsf-repair`
  - Has pure replica scrub/repair planning plus a durable repair operation ledger over `rsf-storage` segment WAL records, first-class repair WAL kind, recovered planned/started/completed/failed operation lifecycle replay, restart-visible pending repair recovery, action/completion matching before persistence and during replay, fail-closed replay for invalid lifecycle transitions, explicit corruption rejection for malformed repair WAL payloads, focused ledger module ownership outside the planner, and host-backed deterministic Fozzy trace coverage for repair/core/storage recovery.
- `crates/rsf-rebalance`
  - Has pure topology/load rebalance planning plus a durable rebalance operation ledger over `rsf-storage` segment WAL records, first-class rebalance WAL kind, recovered planned/started/completed/failed move lifecycle replay, persisted placement-safety proof replay, restart-visible pending move recovery, action/proof/completion matching before persistence and during replay, fail-closed replay for invalid lifecycle transitions, explicit corruption rejection for malformed rebalance WAL payloads, and host-backed deterministic Fozzy trace coverage for rebalance/core/storage recovery.
- `crates/rsf-backup`
  - Has pure snapshot-manifest and restore planning plus a durable restore operation ledger over `rsf-storage` segment WAL records, first-class backup WAL kind, recovered planned/started/completed/failed restore lifecycle replay, restart-visible pending restore recovery, restore-plan/execution matching before persistence and during replay, fail-closed replay for invalid lifecycle transitions, explicit corruption rejection for malformed backup WAL payloads, and host-backed deterministic Fozzy trace coverage for backup/core/storage recovery.
- `crates/rsf-recovery`
  - Has snapshot/backup/root-aligned database restart planning plus a durable recovery operation ledger over `rsf-storage` segment WAL records, first-class recovery WAL kind, recovered planned/certified/failed restart lifecycle replay, restart-visible pending recovery operations, restart-certificate/plan matching before persistence and during replay, fail-closed replay for orphan certification and invalid lifecycle histories, explicit corruption rejection for malformed recovery WAL payloads, and host-backed deterministic Fozzy trace coverage for recovery/snapshot/backup/core/storage restart evidence.
- `crates/rsf-compaction`
  - Has pure SSTable compaction execution plus a durable compaction operation ledger over `rsf-storage` segment WAL records, first-class compaction WAL kind, recovered planned/started/completed/failed compaction lifecycle replay, restart-visible pending compaction recovery, output/manifest validation against the planned range/input/output-level/byte contract before persistence and during replay, fail-closed replay for invalid lifecycle transitions, explicit corruption rejection for malformed compaction WAL payloads, focused ledger module ownership outside the executor, and host-backed deterministic Fozzy trace coverage for compaction/range/core/storage recovery.
- `crates/rsf-rpc`
  - Has pure RPC routing/retry/deadline/body policy classification plus a durable RPC call ledger over `rsf-storage` segment WAL records, first-class RPC WAL kind, recovered accepted/retryable/rejected call decisions after restart, restart-visible in-flight retryable call recovery, original-envelope audit preservation for rejected calls, completed/failed terminal transition persistence, fail-closed replay for invalid lifecycle transitions, explicit corruption rejection for malformed RPC WAL payloads, authenticated TLS 1.3 peer RPCs with node/cluster binding and bounded absolute-deadline framing, independently durable follower process storage, quorum commit dissemination, committed-prefix repair, lost-response retry recovery, and host-backed deterministic Fozzy trace coverage for RPC/core/storage recovery.
- `crates/rsf-observability`
  - Has bounded metric naming/rendering, service health aggregation, validated audit-event records, durable audit recovery over `rsf-storage` segment WAL records, first-class observability WAL kind, ordered audit sequence recovery after restart, fail-closed replay for malformed observability records, invalid audit rejection before persistence, and host-backed deterministic Fozzy trace coverage for observability/core/storage recovery.
- `crates/rsf-wire`
  - Has canonical versioned binary request-frame encoding/decoding with magic/version checks, bounded principal/namespace/operation/idempotency fields, payload-bearing frame bodies with exact byte-count validation, truncated/trailing/malformed-frame corruption rejection, normalized request envelopes with read/write/admin operation classification, mutating-protocol idempotency enforcement without requiring keys for read-only object/blob/KV operations, body-size gates, aggregate ingress counters over accepted decisions only, and host-backed deterministic Fozzy trace coverage for wire/API/auth/gateway/consistency ingress.
- `crates/rsf-sql`, `crates/rsf-sqlpipeline`, `crates/rsf-sqldml`, `crates/rsf-sqltxn`, `crates/rsf-prepared`, `crates/rsf-portal`
  - Accept protocol router ports through the core SQL, statement pipeline, DML, and transaction-script surfaces; have VSR-backed durable router coverage for create table, create index, statement-level insert, insert, update, delete, and scripted transaction certificate recovery; route statement-level DML through real write planning, constraint validation, transaction coordination, isolation, KV application, and protocol certificate recovery instead of a SELECT-only pipeline; distinguish transaction-staged DML visibility from committed protocol evidence without synthetic commit certificates; support transactional SELECT reads over staged overlay rows and snapshot rows by non-primary-key equality predicates; bind prepared/portal SQL with parser-aware placeholder handling that preserves literals, identifiers, and comments instead of broad text substitution; and have a durable SQL autocommit engine that persists table descriptors plus committed relational KV versions and recovers rows, indexes, constraints, tuple history, and live page slots after reopen; and journals request intents plus atomic receipts to reconcile interrupted protocol commits and replay original SQL results without duplicate mutations.
  - Supports multiple physical pages per table, a live row-to-page index, allocation beyond the buffer capacity, capacity-checked inserts, atomic encoded row-relocation redo for growing updates, recovery with either relocated page already flushed, duplicate/conflicting redo checks, and fail-closed duplicate live-row snapshot recovery. Replicated SQL tests reopen followers after every command and verify large values, deletes, and original receipts after restart. Page admission accounts for serialized header/slot/tombstone bytes before accepting writes.
- `crates/rsf-stats`
  - Has pure optimizer statistics publishing/freshness classification plus a durable statistics catalog over `rsf-storage` segment WAL records, first-class stats WAL kind, recovered table-estimate/histogram replay, recovered optimizer freshness classification after restart, fail-closed replay for invalid recovered estimates, explicit estimate-shape validation before persistence and during replay, explicit corruption rejection for malformed stats WAL payloads, and host-backed deterministic Fozzy trace coverage for stats/core/storage recovery.
- `crates/rsf-kv`, `crates/rsf-txn`, `crates/rsf-isolation`
  - Have durable MVCC KV and transaction coordinator paths over `rsf-storage`, VSR-backed protocol router coverage for commit-version/certificate recovery, strict isolation conflict checks, recovered in-flight prepare/commit/abort handling, full KV write intents and atomic value/request receipts with restart reconciliation, exclusive KV writer ownership and persistence-error fencing, idempotent committed-batch materialization, compaction/snapshot preservation, concurrent non-conflicting execution tests, and route-guarded stale-owner rejection.
- `crates/rsf-range`
  - Has ordered range descriptors, split/non-overlap validation, ownership-transfer fencing, SSTable manifest fitting, compaction planning, and a durable range layout ledger over `rsf-storage` segment WAL records with a first-class range WAL kind, recovered create/split/ownership-transfer/table registration after restart, rejected mutations that do not persist, fail-closed replay for malformed range WAL payloads and orphan split histories, and host-backed deterministic Fozzy trace coverage for range/core/storage recovery.
- `crates/rsf-watch`
  - Has prefix watch registration/delivery/resume over durable KV changes plus a durable watch registry over `rsf-storage` segment WAL records, first-class watch WAL kind, recovered cursor replay after restart, no persistence for no-op acknowledgements, fail-closed replay for malformed watch WAL payloads, duplicate recovered subscriptions, unknown subscriptions, and cursor rewinds, and host-backed deterministic Fozzy trace coverage for watch/core/storage recovery.

- `crates/rsfd`
  - Has production-only command slots, durable one-node `init`, token-verified `join`, state-backed `status`, side-effect-free `policy plan`, idempotent durable `policy apply` with live stdout progress/audit output, durable `policy status`, `policy set`, and `policy add-region` commands over the policy catalog, and `serve`/`status` surfaces that report live durable protocol runtime state, including placement groups, latest version, certificate count, and max log index through stdout, a real TCP health/metrics listener, a canonical `rsf-wire` data-plane listener for multi-process ingress admission, durable protocol-backed KV request execution through `rsf-kv`, payload-backed KV writes, durable SQL autocommit create/insert/select execution, durable chunk-backed blob PUT/GET execution, and durable object bucket/PUT/GET execution over the daemon data-plane listener, operator status, operator profiles that apply distinct standalone, regional, and multi-region readiness gates, and bounded long-running runtime supervision for consecutive transient errors.

Latest complete release-gate verification (2026-10-21): `artifacts/verification.pInTn9/summary.json`
passes all 20 domains against unchanged executable sources (SHA-256
`02f757b43890d1522763a3c024c1d753cafa8d3e29c8188937e8ab659c76d131`).
The workspace host run passes 1,167 tests; its one opt-in stress test is exercised
separately by both required stress domains. Named regression audit confirms the
new SQL journal crash boundaries, torn installation batches, completed-installation
retries, real TLS acknowledgement loss, learner restart and cumulative socket
I/O deadline checks were executed in the full workspace run. All seven membership-decision regressions, eleven membership-approval/continuation regressions, six indexed-candidate regressions, ten SQL streaming regressions, both required KV atomicity regressions and all 24 required SQL/key regressions also ran, including real process-kill COMMIT recovery, state/capacity validation, V5 compatibility, serializable write skew/phantom rejection, legacy isolation recovery, durable read dependencies, savepoint read retention, disjoint commits and process-kill read recovery. Each domain passes
five-run doctor, strict deterministic orchestration, recorded host execution,
strict trace verification, replay and CI checks. The recorded host processes
supply the Rust test evidence; scripted expectations alone do not establish it.

Includes 106 VSR tests, 57 protocol tests, 35 RPC tests, 98 SQL tests, 68 daemon
unit tests and all regular peer-process integration tests. Strict Clippy passes
for core, index, write, relational commit, SQL, SQL DML/transactions, protocol and daemon targets with `--all-targets --no-deps -D warnings`.
Workspace-wide lint cleanliness remains separate work. The complete summary is
saved in `docs/verification-2026-10-21.json`; scoped recovery evidence is in
`docs/recovery-rpc-verification-2026-10-21.json`; SQL session evidence and the corrected earlier failed attempt are in `docs/sql-session-verification-2026-10-21.json`; earlier serializable isolation evidence is in `docs/sql-serializable-verification-2026-10-21.json`; read-dependency/concurrency evidence is in `docs/sql-read-dependencies-verification-2026-10-21.json`; key-identity and index-rebuild evidence is in `docs/sql-key-identity-verification-2026-10-21.json`; shared KV atomicity evidence is in `docs/kv-atomic-batch-verification-2026-10-21.json`; SQL streaming evidence is in `docs/sql-stream-verification-2026-10-21.json`; indexed-candidate evidence is in `docs/sql-index-verification-2026-10-21.json`; membership-approval evidence is in `docs/membership-approval-verification-2026-10-21.json`; continuation evidence is in `docs/membership-continuation-verification-2026-10-21.json`; current plan-decision evidence is in `docs/membership-decision-verification-2026-10-21.json`. Production qualification remains
incomplete because the release blockers below remain open.

Opt-in peer client coordination now retains derived inventory with its cells
returned to the peer between requests. Exact-handle mutation tokens and validated
protocol storage revisions invalidate stale entries. All groups share one physical
protocol writer and cached logical receipt tails. Ordered pending-command and
committed-application indexes avoid scanning completed history during healthy
requests; payload cursors apply newly certified commands before advancing.
External writes detected during coordination, callback failures, and unwinding
discard derived state while preserving cell custody. Reopened application writers
replay their namespace/surface before execution, including after derived-file loss.
Installed prefix receipt recovery confirms quorum before and after reconstruction;
accepted-only commands still require fresh replication.

Both required automatic-client profiles grow 128 acknowledged writes, then exercise
four clients and 32 concurrent identities across primary death and deletion of its
data. Each profile reports three full recoveries and 150 retained uses before the
crash, preserves four acknowledgements, resolves 28 requests without acknowledgements,
and verifies all 162 logical KV writes on both survivors without duplicates.
Coverage also includes an active SQL session and acknowledged predicate read across primary loss, original read replay and phantom rejection, disjoint SQL commits on both survivors, savepoint rollback, atomic schema/row commit, original session results after restart, SQL/object/blob recovery, original certificates, replacement
restart, redirects, certificate revocation, quorum loss/restoration, and isolation
of object-internal pins from user blobs. Separate debug/release stress workloads
verify 198/774 acknowledged writes, 18 process shutdowns, and three uncertain-write
resolutions per profile. The actual workload output is required; zero-test filters
cannot pass. All new named regressions are present in the recorded host decisions,
including the cold materializer and election fixtures in the full workspace run.

Source scope remains single-host loopback TLS with fsync. Retained coordination
is not a durable checkpoint or authority lease. Cold startup/elections, per-namespace
reopen, retained payload memory, segment metadata checks, and SQL physical planning still
need checkpoint/retention work and capacity qualification. Installed/network
states reject unilateral authority shortcuts; certified voter changes, learner
promotion, independent-group routing, and distributed control metadata remain open.
See `docs/primary-failover.md`, `docs/peer-replication.md`, and
`docs/certified-membership.md`.

## Remaining Release Blockers

- Replace remaining local transport paths in admin/policy orchestration. Add cross-group application scheduling and durable checkpoint/recovery cursors. Retained client replay cursors are implemented; cold recovery and elections still load complete histories. Complete quorum-certified configuration changes and learner promotion, independent-group client routing, distributed control metadata, and production qualification of automatic takeover. Installed-cell coordinator recovery and four-surface recovery/write integration are implemented; the daemon now optionally schedules peer elections, repairs stale voters, and serves client listeners through scoped installed-cell recovery with shared application writers and primary redirects. Independent-group routing and distributed control metadata remain unfinished. Mixed-view selection, atomic generation installation, and authenticated resumable remote view installation are implemented. Authenticated non-voting history catch-up does not install membership. Prove end-to-end quorum durability under machine failure.
- Implement explicit migration of legacy unscoped daemon data and nonempty metadata-only KV/SQL/object/blob stores into namespace-bound replicated stores while preserving values and request receipts. Establish the purpose and payload history of old unmarked immutable pins before adopting them as dependencies. The secured daemon currently rejects shared top-level surface directories.
- Qualify growing-history SQL read/write throughput, cold/session-snapshot memory and incremental write planning. Bounded indexed SELECT candidates and maintained planning counts are locally qualified. Native autocommit result streaming, bounded legacy responses and incremental session result admission are implemented; expanded qualification passes all 19 domains locally; workload capacity remains open. Current native sessions retain bounded durable row/predicate dependencies, including reads and implicit mutation preconditions across savepoint rewinds and failover. Disjoint point transactions can commit; matching predicates, phantoms, tombstones and dependent-key changes reject COMMIT. Validation remains conservative across full predicates and catalog changes, and scans retained history. Legacy active sessions retain namespace-wide snapshot validation because earlier reads were not tracked. Durable principal-owned sessions replicate BEGIN, staged mutations/schema, savepoints, COMMIT and ROLLBACK through complete commands. Legacy snapshot-only command receipts remain readable; all voters and uncertain pre-upgrade writes must be drained/upgraded before qualifying serializable operation. Namespace-bound SQL appends complete commands and synced acknowledged-history heads, then periodically checkpoints and reclaims covered journal records; unbound legacy persistence still rewrites full state. Replicated planning still rebuilds canonical page placement per mutation. Checkpoint MVCC/receipts and maintenance metadata remain unbounded; growing-table capacity and throughput still need qualification.
- Define bounded durable request-receipt retention with an explicit retry horizon before reclaiming KV/SQL/object/blob intent/receipt history and unreachable pinned blobs.
- Reproduce and eliminate the earlier unexplained listener address collision, which did not recur in the latest full gate. Exercise the complete production fault matrix on independent daemon processes and publish latency, throughput, recovery, and rebalance measurements. Structural requirement coverage and scripted process scenarios cannot establish production readiness.
- Qualify primary request latency and growing-history behavior against the deployment workload. Bounded primary runtime batches and resident peer materializers reduce repeated replay, and the opt-in peer client now retains coordination across scoped requests. Legacy listeners still reconstruct history between batches, and cold recovery/elections still load full histories. Both `tests/stress-host.pass.fozzy.json` and the larger optimized workload in `tests/stress-release-host.pass.fozzy.json` are required release checks. Loopback results alone cannot establish independent-host capacity or machine-failure readiness.

## Custom Protocol Target

The production protocol is the custom RSF protocol:

```text
Universal VSR-RADOS spine
+ FoundationDB transaction/KV discipline
+ Aurora/Socrates page/WAL path
+ Dual-mode CP/AP policy gates
+ Geo-local WPaxos locality/ownership policy
```

The source path must encode this as one system:

- strict replicated metadata cells for mutable truth
- placement groups for physical distribution and repair
- durable segment/WAL storage
- ordered MVCC KV
- strict serializable transaction coordinator
- typed manifests for every physical layout
- content-addressed chunks for immutable payloads
- page/WAL path for native relational workloads
- object/blob first-class surfaces
- policy-driven topology and DX
- deterministic failure verification

## Completion Definition

The goal is complete only when:

- `WORK.md` items are implemented or explicitly superseded by stronger production implementation.
- the custom RSF protocol has durable replicated state, real recovery, policy control, and end-to-end database surfaces.
- every production surface has deterministic tests, recovery tests, and observability.

## Qualification Scope

Linux is the only production operating system. Implement and validate as much as
possible on the development host, using targeted mocks and fault injection for
platform-specific seams where useful. Development-host and simulated results do
not qualify Linux kernel, filesystem, locking, crash recovery or performance
behavior. Finish real Linux validation and any platform-specific fixes on the
user-provided VM. No CI/CD is requested: run verification manually and publish
verified milestones through ordinary pushes to remote `main`.

The requested release target includes standalone, independent-host regional,
and multi-region deployment profiles, with SQL, KV, object, and blob workloads.
No numeric data-size, throughput, or p99 SLO was supplied; publish measured limits
for each profile/surface and establish acceptance targets before claiming capacity
qualification. Existing stress evidence measures native KV on single-host loopback
TLS with fsync; it does not qualify all surfaces or deployment profiles.

## Relational key identity milestone (qualified locally)

Confirmed and fixed two physical isolation failures: delimited row keys could
alias across tables, and an index rebuild could erase another table's same-named
index entries. New stores now use framed row and constraint identities, explicit
V7/V8 checkpoint formats, KEY2 requests and RSFSC3 commands. Legacy stores preserve
original encodings and certificates, reject ambiguous table prefixes, and permit
blank derived peers to inherit the format only from their first certified command.
Mixed, unowned or downgraded row histories fail recovery without rewriting data.
Index rebuilds validate all mutations before replacing only their table/index scope.

Targeted regressions pass, including 90 SQL tests, shared core/write/transaction
consumers, and a real TLS recovery test that deletes a legacy follower's derived
SQL directory before catch-up. Strict Clippy passes for the nine changed production
packages and daemon targets. Full immutable-source Fozzy qualification passes all 18 domains and 1,129
workspace tests with zero failures, including all 24 required SQL/key regressions
and the prior recovery audit. Both automatic takeover profiles prove table-key
isolation after primary data deletion and replacement restart. Debug/release stress
verify 198/774 acknowledgements and 18 abrupt process shutdowns per profile.
Implementation ce39bc8 is pushed to remote main. Complete evidence is saved in
`docs/sql-key-identity-verification-2026-10-21.json` and the current full report.
This remains single-host loopback evidence; the remaining release blockers above
are open, including explicit legacy key migration.

## Shared KV atomicity milestone (qualified locally)

A baseline regression proved that a late rejection in the public committed-batch
API left an earlier mutation in MVCC state. The API now validates all mutations
before publishing values, tombstones or change events, with idempotent exact
replay and rejection of conflicting duplicate keys. No full-store clone is used.
The durable journal already validates its complete batch before persistence; its
encoding and receipt contract remain unchanged. Two regressions cover late
version conflicts, duplicate conflicts, every rejection position, existing
tombstones, binary keys, empty batches and exact replay.

Targeted qualification passes 28 KV, 13 session-transaction and 90 SQL tests.
Strict Clippy passes for those three packages with all targets and no dependencies;
four existing KV style findings were corrected without changing their behavior.
The full immutable-source Fozzy gate passes all 18 domains and 1,131 workspace
tests with zero failures. Both atomicity regressions, all 24 SQL/key regressions
and the prior recovery audit are present in actual host output. Both automatic
takeover profiles recover the four data surfaces after primary data deletion.
Debug/release stress passes 198/774 acknowledgements, 18 abrupt shutdowns and
three uncertain-write resolutions per profile. Implementation e1f53ed is pushed
to remote main. Complete evidence is in
`docs/kv-atomic-batch-verification-2026-10-21.json`; broad production qualification
remains incomplete for the release blockers above.

## SQL result streaming milestone (qualified locally)

Replaced eager candidate/result scans with pull-based typed execution, borrowed
KV prefix iteration and ordered staged-row merging. LIMIT and sink errors stop
the source. Fallback query statistics no longer decode/collect a whole table, and
only the latest query report is retained alongside its cumulative count. Removed
the obsolete eager transaction query-overlay helper.

Standalone and elected-peer native listeners accept RSF_SQL_STREAM_V1 SELECTs,
sending JSON rows in bounded 4 KiB hex chunks with explicit row/completion counts.
The public wire decoder rejects incomplete, oversized or inconsistent streams.
Native legacy reads retain at most 64 KiB of JSON and explicitly direct larger
queries to streaming. Session result admission checks its existing 24 KiB bound
as rows are produced. The cumulative socket I/O budget includes stream writes
while excluding application CPU; blocked clients cannot renew the deadline.

Targeted qualification passes 29 KV, 10 query, 93 SQL, 13 wire and 68 daemon unit
tests. Strict Clippy passes for KV/query/SQL/wire/daemon all-targets, no-deps with
warnings denied. The real TLS workload streams all 128 rows and 264,960 JSON
bytes, checks projection/empty results/buffered overflow and rejects quorum loss.
The full Fozzy gate is expanded to 19 domains, debug/release streaming workloads,
replacement-primary streaming and ten required named workspace regressions. It
passes all 19 domains and 1,141 workspace tests with zero failures on unchanged
source. Both streaming profiles deliver all 128 rows / 264,960 JSON bytes with
4 KiB chunks. Both takeover profiles read a stream from the replacement primary.
Debug/release stress retains 198/774 acknowledged writes, 18 abrupt shutdowns and
three uncertain-write resolutions per profile. Scoped evidence is in
`docs/sql-stream-verification-2026-10-21.json`.
See `docs/sql-result-streaming.md`; retained histories, cold/session snapshot
reconstruction, incremental planning and independent-host capacity remain open.

Streaming qualification review caught and reproduced an indexed nullable-row
compatibility regression after the initial implementation push. Stopped attempt
`artifacts/verification.m0Qt5Z` is incomplete and cannot qualify the source.
The lazy iterator now applies the secondary predicate before typed projection,
including staged rows, preserving the prior behavior without collecting candidates.
The new named regression is required by the full gate; the failing baseline is
`artifacts/sql-stream-index-filter-baseline.log`. The final expanded gate was
restarted on corrected, frozen source and passed; its trace directory is
`artifacts/verification.oq1pB1`. Corrected implementation 95647f6 is pushed to
remote main.

## Indexed SQL candidate milestone (qualified locally)

Reproduced two failures: an indexed SELECT decoded an unrelated malformed row,
and live typed secondary lookups missed noncanonical int/bool spellings that
recovery normalized. Added borrowed ordered index lookup, direct matching-key
resolution and canonical typed index publication/deletion/lookup. Transaction
overlays merge inserts, predicate moves and deletes in key order, including
savepoint rewinds. Visited missing/mismatched index references reject corruption.

Derived committed row/encoded-byte counts now update with row effects and rebuild
on recovery. SELECT fallback planning and row_count no longer scan the table.
Counts are not new durable authority; explicit ANALYZE precedence is unchanged.
The targeted suite passes 98 SQL and nine index tests. Strict Clippy passes for
index/SQL/daemon all-targets/no-deps with warnings denied. Full unchanged-source
qualification passes all 19 domains and 1,147 workspace tests with zero failures.
All six new named regressions and prior required regressions appear in actual
host output. Both native streaming profiles verify indexed queries around row
movements/deletion, and both automatic takeover profiles perform an indexed read
from the replacement after old-primary data loss. Debug/release stress retains
198/774 acknowledgements, 18 abrupt shutdowns and three uncertain-write
resolutions per profile. Implementation 783d13f is pushed to remote main.
See `docs/sql-index-candidates.md` and
`docs/sql-index-verification-2026-10-21.json`.

Full-state write planning, cold/session snapshot reconstruction, history/receipt
retention and independent-host workload qualification remain open.

## Durable membership approval milestone (qualified locally)

Added exact old-configuration membership proposals, synced old-voter fences and
replay-checked majority approval over full accepted/committed histories and client
mappings. The new fence survives restart and cannot be discarded by ordinary
elections, local authority shortcuts or ordinary view installation. New voters
remain non-voting; approval never grants client readiness. WAL tag 41 and history
tag 6 preserve this distinct control state.

New authenticated peer RPCs initiate proposals only from the old primary and let
old voters recover existing frozen reports/history after initiator loss. Exact
recovery retries are idempotent; configured learners are rejected. Targeted tests
pass 20 core, 57 storage, 95 VSR, 33 RPC and 57 protocol checks. Strict Clippy passes
for core/storage/VSR/RPC/protocol/daemon all-targets/no-deps with warnings denied.
Evidence includes a process killed after fence sync before acknowledgement and
real TLS peer restart/recovery after deleting the initiating primary's store.
The unchanged-source full gate passes all 19 domains and 1,154 workspace
tests with zero failures. All six meaningful approval regressions and all prior
required regressions appear in recorded host output. Both automatic takeover
profiles preserve 162 logical KV writes on both survivors after old-primary data
loss; native SQL streaming delivers all 128 rows / 264,960 JSON bytes in bounded
chunks. Debug/release stress preserves 198/774 acknowledgements, 18 abrupt process
shutdowns and three uncertain-write resolutions per profile. Implementation
379e049 is pushed to remote main. See
`docs/membership-approval-verification-2026-10-21.json`.

This is the old-quorum approval phase, not completed membership. New-quorum
activation, partial-proposal abort/conflict resolution, new-quorum coordinator
recovery, learner promotion, control metadata and client admission remain unfinished. No
operator membership-change command is enabled. See `docs/certified-membership.md`.

## Membership minority continuation (qualified locally)

Added authenticated continuation of an existing exact proposal from a surviving
old voter. A previously unfenced replacement coordinator fetches/replays a peer's
complete snapshot before adopting and relaying its own durable report. RESUME
(21) binds the report to the TLS sender. No surviving report means no new fence;
ordinary election promises and conflicting proposals cannot be replaced.

New regressions cover minority-to-majority completion after initiating-primary
store deletion, initially fenced/unfenced coordinators, restart, exact retry,
spoofed sender/configured learner rejection and process death after synced
continuation before acknowledgement. The production gate now requires eleven
approval/continuation regressions. Full local qualification passes all 19 domains
and 1,159 workspace tests with zero failures against unchanged sources. Both
debug/release automatic takeover and SQL streaming workloads pass; crash stress
preserves 198/774 acknowledgements with 18 abrupt shutdowns and three uncertain
write resolutions per profile. All 98 VSR, 35 RPC and 57 protocol targeted tests
and strict Clippy pass. Implementation 307cbcd is pushed to remote main; scoped
evidence is in `docs/membership-continuation-verification-2026-10-21.json`. This completes
only the exact-proposal old-quorum continuation path: abort/conflict lifecycle,
new-quorum installation/activation, learner promotion and control/admission remain.

## Transition-only membership plan consensus (qualified locally)

Implemented a separately owned, full-sync decision store backed by canonical
old-quorum input descriptors and bounded history batches. New-quorum round
promises preserve accepted plans across coordinator loss. The coordinator binds
its nomination durably before relay; higher rounds must retain the highest
accepted plan reported by their new quorum. A distinct new majority forms a
chosen-input proof, which remains pinned on restart. Authority, accepted inputs,
bootstrap and monotonic journal state are replay-validated before reuse.

All 106 VSR, 35 RPC and 57 protocol targeted tests pass, with strict Clippy for
VSR/RPC/protocol/daemon. Evidence includes all four-voter quorum intersections,
exact/overlapping retries, torn batches, old-store deletion and a real process
killed after nomination sync before reply. The full Fozzy gate is expanded to
20 domains and seven required decision regressions. All 20 domains and 1,167 workspace
tests pass on unchanged executable sources with zero failures. All seven decision
regressions appear in both the dedicated host trace and full workspace output.
Debug/release takeover and SQL streaming pass; crash stress preserves 198/774
acknowledgements with 18 abrupt shutdowns and three uncertain-write resolutions
per profile. Implementation c0d357d is pushed to remote main. Scoped evidence is
in `docs/membership-decision-verification-2026-10-21.json`.

This is transition-only library consensus, not active configuration installation.
Peer orchestration, application materialization/acknowledgement, active generation
publication, activation, learner promotion, retired roles, abort/conflict handling
and atomic distributed control admission remain required release work.

## Authenticated membership peer orchestration (targeted and workspace qualified)

Added configured transition paths, a full-sync restartable authority inbox and
bootstrap/history/committed-prefix checks for existing voters and published
learners. Bounded TLS RPCs provision authority, promise rounds, stage input,
accept an exact coordinator nomination, retain a chosen proof and fetch accepted
input from survivors. The high-level coordinator preserves the highest accepted
input even when its initial candidate uses another valid old-quorum subset.

Real TLS cases now cover learner-led recovery after primary/coordinator data
deletion, complete payload and original request mapping preservation, unpublished
learner/foreign-bootstrap rejection, and one lost acceptance reply resolving
without a second durable vote. The complete gate is expanded to 21 domains;
targeted host qualification passes all 202 affected tests, strict five-run doctor,
strict deterministic orchestration, recorded host execution, strict trace verification,
replay and CI. Strict Clippy passes for VSR/RPC/protocol/daemon. The full workspace
recorded host run passes 1,171 tests with zero failures and one opt-in stress test
ignored; fresh release stress qualification on this source remains pending. All
74 required regressions appear in recorded workspace output. Workspace doctor,
strict test, strict trace verification, replay and CI also pass on unchanged
source. Implementation 1685f4c is pushed to remote main. Evidence is in
`docs/membership-peer-verification-2026-10-21.json` and
`artifacts/membership-peer.lusqwzzd`. The prior
complete 20-domain release report above remains scoped to its recorded source.

Peer choice is followed by sealed staged-prefix materialization below. Active
generation publication and native installation integration, activation/learner promotion,
removed roles, abort/conflict lifecycle, repeated generations and atomic control
admission remain unfinished. Retained-prefix access and growing-history proof
transfer capacity also remain to be qualified and optimized.

## Sealed membership staging and peer materialization

Chosen-prefix merge uses the shared view-installation union, original historical
certificates, accepted uncommitted tails and receiver-local request mappings.
Separate full-sync stage stores replay complete proof on open, seal exact
input/count/content identities and cannot become writable VSR cells. Receipts
require trusted materialization on this boot and a synced receipt. New-majority
proofs bind chosen input and canonical prefix; learners remain non-voting.

Same-boot receipt reuse binds exact voter recovery stamps or published learner
snapshot IDs. Immutable approvals retain private replay-validated states; exact
chosen-input and staged source-copy checks compare complete bytes with validated
proof without redundant replay or directory rewrites. Reopen fully validates all
proof, and missing chosen input rejects without recreation. Original 128 KiB TLS
payload, 800 ms deadlines and one retry remain unchanged. Warm retries avoid
repeated application work; restart rebuilds deleted derived files. New directory
creation syncs new directories and parent links before durable publication.

All 269 affected tests and strict affected-target Clippy pass. The frozen-source
full workspace passes 1,181 tests with zero failures, including all nine required
staging/directory/retention cases and the previously failing TLS staging case.
The full gate passes 21/22 domains, including both native-KV stress profiles,
streaming, object/blob, recovery and end-to-end checks. Release automatic-client
takeover fails on a post-loss SQL BEGIN with insufficient durable peer commit
evidence before the deadline; its debug profile passes. This remains an unresolved
release failure; an isolated rerun passes, but does not clear the failed full
gate. Current evidence is in
`docs/membership-staging-verification-2026-10-21.json`; earlier disk exhaustion and
workspace-staging failures remain preserved separately.

Resolve the release automatic-client failure and rerun the complete gate. Active
generation publication, native application publication, activation/learner
promotion, retired roles, conflict/abort/repeated-generation lifecycle, atomic
control admission and independent-host workload/fault qualification remain open.

## Uncertain SQL BEGIN client contract and diagnostics

A copy of the actual failed fixture recovers the original `parallel-left-begin`
result, exact session token and replay identity across both survivor restarts,
with changed-payload rejection. The preserved original intent hash is unchanged.
A repeatable regression withholds both caller commit observations only after real
TLS peers durably commit/materialize BEGIN. Exact retry retains its certified
version/token across two peer restarts and adds no commit entries.

The qualification client performs at most three identical attempts only for
missing durable commit confirmation; semantic errors and persistent quorum
failure still fail. Normal debug/release takeover and the forced regression pass
in recorded host runs with strict trace verification, replay and CI. Normal runs
required zero commit-confirmation retries. Strict daemon all-target Clippy passes.
Peer RPC logs now identify total service time, materialization time/count and last
group/index. These changes do not eliminate fsync/scheduling latency or qualify
a capacity/p99 target. Run the full expanded gate on this final source; it now
requires the new uncertainty regression in workspace and both build profiles.

The attempted full gate on `0f1eb7a` (`verification.oLAzTk`) terminated with host
ENOSPC and no summary; it does not qualify a release. Subsequent process inspection
found no surviving RSF gate children. At failure the host had about 53 MiB free,
while RSF's generated target directory was 1.4 GiB. A stable-headroom full rerun
remains required. Evidence: `docs/uncertain-sql-begin-full-attempt-2026-10-21.json`.
