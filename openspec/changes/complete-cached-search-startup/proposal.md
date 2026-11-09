# Complete cached relationship-vector startup

## Problem

Restoring saved node/BM25/HNSW artifacts can mark search ready while its
relationship vectors contain only a pre-build refresh. Public relationship
vector queries then omit stored relationships without reporting an error.
This remains after #1753 on `release/v1.5.0`.

Release verification also reports endpoint-inventory records that no longer
match the transaction and lease-renewal paths integrated by #1763.

## Required outcome

- Complete the existing relationship population stream before cached startup
  publishes readiness, preserving reuse of saved node artifacts.
- Preserve build error, cancellation, snapshot and queued-write ownership.
- Verify queryRelationships and both scalar MATCH forms against storage.
- Reconcile endpoint records against their current canonical owners without
  changing scanner rules or introducing exceptions.
- Track the correction and release CI result through NOR-386, NOR-10 and #1772.

## Scope

No new storage format, public Go API, readiness fallback, duplicate-checker
change or dogfood database operation. Full release qualification remains a
separate exact-head result on #1772.
