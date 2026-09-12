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
- After the first production pass is ready, create `ariacomputecompany/rsf` with `gh`, push to remote `master`, and then push after each verified pass that removes work from this ledger.

## Current State Audit

### Production-leaning but incomplete

- `crates/rsf-core`
  - Has authoritative base IDs and domain modules, typed cluster/node-incarnation/region/failure-domain/policy/layout/surface/trace/operation identities with parsing/display validation, typed byte/key/value/range wrappers, durable timestamp and commit-clock contracts, explicit commit-version successor/genesis contracts, BLAKE3-backed content IDs for blob/object/chunk payloads, algorithm-tagged digest contracts across audited checksum/content-addressed production crates, operator-facing error categories, cluster node descriptors that use typed region/failure-domain IDs, typed superblock cluster identity, typed RPC trace identity, shared core operation IDs through protocol certificates and VSR protocol operations, typed region/failure-domain boundaries across admission, deployment, provider selection, and upgrade planning, and typed auth surface resources across API, gateway, SQL/session, and POC flows.
- `crates/rsf-storage`
  - Has a real append segment file, versioned segment record envelopes, explicit checksum policy metadata, BLAKE3-backed record checksums and WAL digests, recovery scanning, WAL record encoding, placement-group tail recovery, hash-chain validation, committed-prefix evidence, retention/compaction maintenance records that preserve committed-prefix evidence, reentrant lock-file single-writer guards, sync policy coverage, IO error classification, append/replay benchmark coverage, production segment directory layout, manifest lifecycle, multi-segment rotation and recovery, physical compaction rewrite with manifest-carried compacted prefixes, restart-safe retention reclaim, corrupt-segment quarantine that fails closed, multi-segment scan/truncate APIs that retain committed and membership metadata prefixes, larger append/replay recovery benchmarks, a durable published-manifest catalog over segment WAL records with a first-class manifest WAL kind, idempotent duplicate publish for identical manifest content, rejected publish paths that do not persist, fail-closed replay for malformed manifest WAL payloads and duplicate recovered manifest ids, and fault-injection tests for torn writes, bad versions, unsupported checksum policies, lock contention, malformed manifests, cross-segment hash-chain corruption, and corruption.
- `crates/rsf-manifest`
  - Has validated physical extent manifests for SQL, KV, object, blob, snapshot, and backup surfaces; provider replica metadata validation; checksum proof validation; repair planning for missing, corrupt, and under-replicated extents; typed surface manifests; and host-backed deterministic Fozzy trace coverage through the storage-owned durable published-manifest catalog.
- `crates/rsf-page`, `crates/rsf-wal`, `crates/rsf-buffer`
  - Have a real relational page/redo/buffer path with deterministic page-image and redo-record codecs, slot/tombstone capacity validation, idempotent redo replay and checkpoint summaries, WAL-before-page-flush gating, dirty frame flush planning, clean LRU eviction under frame/byte budgets, and a durable relational WAL gate over `rsf-storage` segment records that recovers synced flush eligibility, recovers unsynced redo as not flushable, rejects malformed relational WAL payloads fail-closed, and has host-backed deterministic Fozzy trace coverage for page/WAL/buffer/storage/core recovery behavior.
- `crates/rsf-vsr`
  - Has a prepare/ack/commit/view-change state machine, deterministic timeout election, exact signer-set commit certificates that bind quorum acknowledgements to the prepared operation, durable prepare/commit/view-change/client-table/membership WAL records through multi-segment `rsf-storage` stores, restart recovery for committed prepares, learner catch-up from committed prefixes, view-change committed-prefix proof, quorum-overlap voter-change validation, stale-primary fencing, duplicate client requests, explicit replica transport abstraction, deterministic partition/drop tests at the protocol-certificate boundary, multi-segment restart coverage for prepare/commit/view-change/membership/client-table records, VSR-level committed-prefix compaction rejection until snapshots exist, and host-backed Fozzy trace coverage.
- `crates/rsf-protocol`
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
  - Accepts the protocol router port, verifies BLAKE3 content identity on reads, dedupes duplicate payloads without new protocol commits, and has VSR-backed durable router coverage for blob pin commit-version and certificate recovery.
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
  - Accepts the protocol router port through the object facade and has VSR-backed durable router coverage for bucket, blob pin, and object manifest certificate recovery.
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
  - Has pure RPC routing/retry/deadline/body policy classification plus a durable RPC call ledger over `rsf-storage` segment WAL records, first-class RPC WAL kind, recovered accepted/retryable/rejected call decisions after restart, restart-visible in-flight retryable call recovery, original-envelope audit preservation for rejected calls, completed/failed terminal transition persistence, fail-closed replay for invalid lifecycle transitions, explicit corruption rejection for malformed RPC WAL payloads, and host-backed deterministic Fozzy trace coverage for RPC/core/storage recovery.
- `crates/rsf-observability`
  - Has bounded metric naming/rendering, service health aggregation, validated audit-event records, durable audit recovery over `rsf-storage` segment WAL records, first-class observability WAL kind, ordered audit sequence recovery after restart, fail-closed replay for malformed observability records, invalid audit rejection before persistence, and host-backed deterministic Fozzy trace coverage for observability/core/storage recovery.
- `crates/rsf-wire`
  - Has canonical versioned binary request-frame encoding/decoding with magic/version checks, bounded principal/namespace/operation/idempotency fields, truncated/trailing/malformed-frame corruption rejection, normalized request envelopes with read/write/admin operation classification, mutating-protocol idempotency enforcement without requiring keys for read-only object/blob/KV operations, body-size gates, aggregate ingress counters over accepted decisions only, and host-backed deterministic Fozzy trace coverage for wire/API/auth/gateway/consistency ingress.
- `crates/rsf-sql`, `crates/rsf-sqlpipeline`, `crates/rsf-sqldml`, `crates/rsf-sqltxn`, `crates/rsf-prepared`, `crates/rsf-portal`
  - Accept protocol router ports through the core SQL, statement pipeline, DML, and transaction-script surfaces; have VSR-backed durable router coverage for create table, create index, statement-level insert, insert, update, delete, and scripted transaction certificate recovery; route statement-level DML through real write planning, constraint validation, transaction coordination, isolation, KV application, and protocol certificate recovery instead of a SELECT-only pipeline; distinguish transaction-staged DML visibility from committed protocol evidence without synthetic commit certificates; support transactional SELECT reads over staged overlay rows and snapshot rows by non-primary-key equality predicates; and bind prepared/portal SQL with parser-aware placeholder handling that preserves literals, identifiers, and comments instead of broad text substitution.
- `crates/rsf-stats`
  - Has pure optimizer statistics publishing/freshness classification plus a durable statistics catalog over `rsf-storage` segment WAL records, first-class stats WAL kind, recovered table-estimate/histogram replay, recovered optimizer freshness classification after restart, fail-closed replay for invalid recovered estimates, explicit estimate-shape validation before persistence and during replay, explicit corruption rejection for malformed stats WAL payloads, and host-backed deterministic Fozzy trace coverage for stats/core/storage recovery.
- `crates/rsf-kv`, `crates/rsf-txn`, `crates/rsf-isolation`
  - Have durable MVCC KV and transaction coordinator paths over `rsf-storage`, VSR-backed protocol router coverage for commit-version/certificate recovery, strict isolation conflict checks, recovered in-flight prepare/commit/abort handling, compaction/snapshot preservation, concurrent non-conflicting execution tests, and route-guarded stale-owner rejection.
- `crates/rsf-range`
  - Has ordered range descriptors, split/non-overlap validation, ownership-transfer fencing, SSTable manifest fitting, compaction planning, and a durable range layout ledger over `rsf-storage` segment WAL records with a first-class range WAL kind, recovered create/split/ownership-transfer/table registration after restart, rejected mutations that do not persist, fail-closed replay for malformed range WAL payloads and orphan split histories, and host-backed deterministic Fozzy trace coverage for range/core/storage recovery.
- `crates/rsf-watch`
  - Has prefix watch registration/delivery/resume over durable KV changes plus a durable watch registry over `rsf-storage` segment WAL records, first-class watch WAL kind, recovered cursor replay after restart, no persistence for no-op acknowledgements, fail-closed replay for malformed watch WAL payloads, duplicate recovered subscriptions, unknown subscriptions, and cursor rewinds, and host-backed deterministic Fozzy trace coverage for watch/core/storage recovery.

- `crates/rsfd`
  - Has production-only command slots, durable one-node `init`, token-verified `join`, state-backed `status`, side-effect-free `policy plan`, idempotent durable `policy apply` with live stdout progress/audit output, durable `policy status`, `policy set`, and `policy add-region` commands over the policy catalog, and `serve`/`status` surfaces that report live durable protocol runtime state, including placement groups, latest version, certificate count, and max log index through stdout, a real TCP health/metrics listener, a canonical `rsf-wire` data-plane listener for multi-process ingress admission, durable protocol-backed KV request execution through `rsf-kv`, operator status, operator profiles that apply distinct standalone, regional, and multi-region readiness gates, and bounded long-running runtime supervision for consecutive transient errors.

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
