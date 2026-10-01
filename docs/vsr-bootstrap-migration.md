# Migrate legacy VSR bootstrap authority

Older VSR stores may contain committed entries without a persisted bootstrap
configuration. Those stores can be read using their original configuration, but
view changes, elections and membership changes are blocked until it is persisted.

Stop the process owning the cell and run:

```sh
rsfd vsr migrate-bootstrap \
  --store /path/to/cell \
  --pg 1 \
  --original-view 1 \
  --original-primary 0 \
  --original-voters 0,1,2
```

`--store` is the individual VSR cell directory. For a native daemon it is normally
`<state>/protocol.vsr/pg-<id>`; for a peer service it is
`<peer-state>/pg-<id>`. Migrate each legacy cell independently.

The original view, primary and voters must come from the cluster's original
bootstrap records. Do not substitute current membership or infer the voter set
from certificate signers: a certificate can contain only a subset of the original
voters. The migration validates every existing commit and configuration transition
against the supplied original configuration, but it cannot prove external
bootstrap facts that were never recorded in the store.

The command acquires exclusive writer ownership and fully recovers the existing
log before appending. Unknown or duplicate flags, malformed voter lists, missing
stores, missing placement groups and conflicting persisted bootstrap declarations
are rejected. The library entry point is
`DurableVsrCell::migrate_legacy_bootstrap` with the same explicit inputs.

## Durability and recovery

One synced `Membership` WAL record with the `VSRG1` payload persists the original
bootstrap. It binds the canonical `VSRB1` configuration to the immediately preceding
physical WAL index and hash. Existing prepares, certificates, request receipts,
configuration records and their hashes are preserved; no log rewrite or independent
sidecar file is involved.

Recovery discovers and validates the migration record before replaying the legacy
prefix. It then uses that persisted original configuration, regardless of newer
bootstrap arguments supplied when reopening. It rejects multiple migration records,
a migration mixed with an existing native bootstrap record, incorrect placement,
wrong prefix binding, malformed lengths and a migration epoch inconsistent with
the replayed state.

An identical retry returns `changed: false` without another WAL append. A first
successful migration reports `changed: true` in a `vsr.bootstrap-migration` JSON
event. A conflicting retry fails. The same command is an idempotent no-op on a
native bootstrapped store only when the declared original configuration matches.

If interrupted during append, storage recovery discards an incomplete record and
the same command can be retried. If the complete synced record exists but its
response was lost, retry discovers it and returns the existing result. Later view
and membership changes retain the historical authority needed by old certificates.

Use binaries that understand `VSRG1` for subsequent recovery. Older readers do not
provide the migrated-bootstrap contract; this is not a downgrade format.

## Verification and remaining scope

Tests verify exact preservation of the original WAL prefix and client receipts,
restart with changed bootstrap arguments, later membership changes, idempotent and
conflicting retries, wrong original voters, missing stores, malformed records and
a deliberately torn migration tail. CLI tests invoke the real daemon binary and
reopen the resulting cell.

The full gate passed 90 results across 15 domains. Local evidence is in
`artifacts/bootstrap-migration-production.log`,
`artifacts/bootstrap-migration-verification-summary.json` and
`artifacts/verification.5ZdO97/`.

This migrates local bootstrap authority. Authenticated configuration-history
transfer, quorum-certified membership installation and automatic failover remain
separate work. It does not migrate legacy KV, SQL, object or blob application data.
