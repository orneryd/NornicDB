# Ordinary relationship index ownership (N152-143 / NOR-42)

## Required outcome (from the ticket, unchanged)

The ticket's open acceptance criterion is:

> Ordinary relationship indexes have an actual build/maintenance/read owner or an
> explicit supported-operation failure instead of false ONLINE success.

The already-satisfied criteria stay satisfied and are not re-opened: the
constraint-owned equality slice (2 reads at 8 and 512 unrelated relationships,
both parsers, auto-commit and explicit transaction), the snapshot/pending/
durability/fallback/surrogate controls, and the explicit deferral of broader
`WHERE`/`MERGE`/optimizer scope and of cold/retained-heap product qualification.

Established defect, from the preserved evidence: `CREATE INDEX pd_relation_id_ordinary
FOR ()-[r:PD_RELATION]-() ON (r.id)` is accepted, `SHOW INDEXES` reports it
`ONLINE`, `entityType RELATIONSHIP`, no `owningConstraint` — and the reported
`MATCH (a)-[r:PD_RELATION {id:$id}]->(b)` reads 11 entities at 8 unrelated
relationships and 515 at 512, exactly the undeclared complete scan. Rows are
correct; the declaration is a no-op.

## Scope of this change record

Investigation only. This change adds no production or test code, runs no build or
test, and makes no performance claim. It preserves the ticket and its fixtures
verbatim and records the owner map, the gaps and the two admissible fix shapes so
the parent can decide.

Deliverables:

- `evidence/README.md`, `evidence/ticket/`, `evidence/fixtures/` — exact ticket
  copy, exact extracted fixtures, hashes and provenance, extraction script.
- `evidence/owner-map.md` — build/mutation/read/discovery/drop/persistence owners
  for node vs relationship equality indexes, with source locations and twelve
  named gaps.
- `evidence/duplicate-review.md` — candidate comparison and the unresolved
  decision, with the graph-retrieval failure recorded as missing, not empty.
- `tasks.md` — what was completed here and what remains for the parent.

## Findings that constrain any fix

1. One canonical relationship equality owner already exists and already has
   build, maintenance, lookup, candidate read-back, pending-write merge and
   namespace/tier resolution: `relationshipUniqueIndexes` in
   `pkg/storage/badger_relationship_unique_index.go` plus
   `pkg/storage/relationship_property_equality_lookup.go`. It is reached only from
   `UNIQUE`/`RELATIONSHIP KEY` declarations
   (`pkg/cypher/schema.go:1212` → `relationship_property_equality_lookup.go:216`).
2. The declaration lands in a different, definition-only structure:
   `sm.rangeIndexes` via `AddRangeIndexForEntity`
   (`pkg/cypher/index_creation.go:105` → `pkg/storage/schema.go:1343,1347`). The
   legacy `RangeIndex` has no production insert/delete/query caller at all, so it
   is not the owner a fix should populate.
3. The false `ONLINE` is not relationship-specific: `SHOW INDEXES` and
   `CALL db.indexes()` hardcode `ONLINE`/100% for every non-vector row
   (`pkg/cypher/executor_show.go:198`, `pkg/cypher/call.go:1459`). Truthful state
   needs a real population/ownership signal, not an entity-type special case.
4. Nothing evicts a retained derived index when the declaration that justified it
   is dropped (`pkg/storage/schema.go:1725`, `pkg/storage/schema_drop_constraint.go:122`),
   and one derived entry can be claimed by two declarations (constraint plus
   equivalent ordinary index) because the key is `(namespace, type,
   property-set)`, not the declaration name.

## Decision requested from the parent

Choose one, with the accompanying consequence:

- **A — reuse the canonical derived owner.** Build it at declaration, extend the
  qualification in `relationshipPropertySeedEdge` to accept a covering declared
  ordinary index, add one removal/reference-count owner, and derive the reported
  state from ownership. Accepts the existing owner's limits (no write-behind,
  decay, tiered or temporal-viewport acceleration; no temporal/composite value
  keys; 16 retained entries).
- **B — explicit supported-operation failure (or truthful non-ONLINE state).**
  Refuse the relationship `CREATE INDEX` form with a stable semantic error, or
  keep accepting it while reporting a state that reflects "no owner". Requires an
  answer for declarations already persisted by earlier versions, because import
  recreates them and removing them is an operator decision (standing principle 19).

Not proposed here: broader optimizer/`WHERE`/`MERGE` access paths, a large
historical fixture, timing thresholds, or any product performance claim.
