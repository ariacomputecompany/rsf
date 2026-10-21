# Bounded indexed SQL candidates

SELECT physical plans that choose a secondary equality index now traverse that
index's matching primary-key set directly. `IndexStore::lookup_iter` borrows keys
in byte order; SQL does not collect a matching-key vector or scan unrelated rows.
Each key resolves its committed KV value only when the executor pulls it. LIMIT
and sink failure stop the source. Primary lookups remain direct KV lookups, and
unindexed predicates retain the borrowed ordered range scan.

The executor merges committed candidates with staged transaction mutations in
primary-key order. Staged inserts and updates moving into a predicate enter the
result; deletes and updates moving out suppress matching committed entries.
Savepoint rewinds restore the staged view. Nullable nonmatching candidates are
filtered before typed projection. The full typed predicate is still applied after
candidate selection, including conjunctions and projection/LIMIT semantics.

A visited index entry pointing to a missing committed row or a row with a different
indexed value is a corruption error. The executor does not fall back to a table
scan. Native streaming then lacks a terminal completion and clients must reject
the incomplete result. Recovery continues to rebuild derived indexes from validated
committed rows. A query need not decode unrelated rows or candidates beyond LIMIT;
cold recovery validates the complete persisted state separately.

## Canonical values and compatibility

SQL index publication and deletion use the same canonical typed values as the
physical row codec. Int64 `001` and boolean `TRUE` therefore index as `1` and `true`.
The public equality lookup canonicalizes its input, and staged indexed candidates
normalize their typed values before filtering. Text remains unchanged. This fixes
typed catalog-backed SQL library lookups that previously differed before and after
recovery. The native CREATE TABLE grammar continues to create text columns; this
change does not add a typed DDL grammar.

Index key framing, complete SQL commands, checkpoints and receipt encodings are
unchanged. These are derived index/publication changes, with no new durable
configuration or implicit legacy-data migration. Existing table/index scoping and
versioned relational row identities remain enforced.

## Planning sizes

Each table retains derived counts for committed visible rows and encoded row bytes.
Insert, update, delete and transaction commit maintain those counts with the engine
state; recovery rebuilds them from validated live KV rows. Tombstones contribute
neither rows nor bytes. Empty tables retain zero counts. A count underflow or
overflow rejects as inconsistent metadata.

Fallback SELECT planning uses these counts in constant time instead of scanning
all table keys for an estimate. Explicit ANALYZE estimates retain their existing
precedence and may be stale. Staged rows are not added to committed counts until
commit. `row_count()` also reads the derived counts. No size counters are persisted
as a new source of authority.

This bounds hot-path candidate and planning work for selective index reads. It
does not remove full-state mutation planning, cold recovery, session snapshot
reconstruction, or retained-history/receipt costs. Those and production workload
capacity remain release work.

## Qualification

The failing baseline demonstrated both unrelated physical-row decoding during an
indexed SELECT and a typed canonical-key lookup miss. Regressions require borrowed
index storage/order/scope, selective traversal and LIMIT, typed live/recovered keys,
transaction entry/movement/deletion/savepoint behavior, corrupt-reference rejection,
and committed planning counts through recovery. Native TLS streaming checks real
indexed results before and after row movements/deletion. Automatic takeover also
requires an indexed streamed read from the replacement after primary data loss.

The current full report remains historical until unchanged-source qualification
finishes. All six named regressions and debug/release indexed native/takeover
metrics are required by the expanded gate. These are local correctness and
execution-path checks; independent-host capacity remains unqualified.
