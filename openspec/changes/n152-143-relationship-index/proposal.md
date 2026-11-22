# N152-143: connect ordinary relationship indexes to the equality owner

## Approved outcome

An ordinary relationship RANGE index supplies the selective equality access path
for the recovered single-hop/single-hit query. Reuse the existing Badger derived
relationship index for population, committed mutation maintenance, lookup and
candidate verification. The parent approved this bounded slice on2026-11-21.

The existing complete scan remains authoritative for multiple selected edges,
unsupported values/views/backends, pinned old snapshots and other traversal
shapes. No multi-hit acceleration, general WHERE/MERGE optimization or historical
large-fixture qualification is claimed. TEXT/POINT declarations are out of scope.

## Canonical correction

1. Add a declaration-shaped preparation entry point under the existing equality
   index owner. Constraint preparation delegates to it after its existing gate.
2. Ordinary CREATE INDEX prepares the existing owner before publishing catalog
   metadata. Build failure leaves the catalog unchanged; no rollback can delete
   a concurrently admitted declaration. IF NOT EXISTS keeps its existing no-op
   behavior and never calls preparation. The index-set mutex joins population
   with committed-write membership independently of schema publication. A later
   catalog persistence failure can retain only an undiscoverable bounded cache.
3. Move covering-key discovery to one SchemaManager owner over RANGE relationship
   catalog rows. Prefer constraint-owned keys, then shortest complete key with
   lexical property-order ties. Return detached property names after releasing
   the schema lock.
4. Existing pinned0/1-edge traversal consumes the selected index; multiple hits
   keep its complete scan. No second posting store or traversal evaluator.

## Lifecycle and state

The ordinary declaration now has actual build/maintenance/read owners. ONLINE
retains its logical-index meaning; existing view-dependent access fallback is
not a new population state. DROP removes the catalog gate; bounded derived
entries need no new reference counter and cannot be selected after the last
matching declaration disappears. Surviving declarations can still use that
owner, and restart/eviction rebuild through the existing lazy owner. Existing
persisted ordinary definitions participate in generic discovery without a
population-specific migration or deletion.

## Evidence and limitations

DeepSeek investigation773fa695baa84e658ba63b9cf89712c2 failed before recording
completion; its owner map/fixtures remain useful read-only evidence, not an
accepted implementation. Radeon mechanical-copy jobef9efa1dfc5941e3acc2da9b5a17b60c
rewrote fixtures and was cancelled; rejected output is preserved outside this
checkout. Parent restored exact originals and verified both SHA256 hashes.

The original fixture includes a desired fail-first lookup test and a separate
observation of the old no-op behavior. Preserve and run both before fixing;
replace only that bug-observation assertion afterwards with intended2/2reads,
retaining the undeclared11/515 sensitivity and exact rows/catalog assertions.
