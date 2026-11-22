# Owner map: ordinary relationship `CREATE INDEX` (N152-143)

Read-only investigation at `f77ec7706de099588ac232b32b6908860511d3da`.
All locations are in this checkout. The ticket's preserved runs are in
`evidence/fixtures/`; see `evidence/README.md` for provenance.

**Finding in one sentence.** `CREATE INDEX ... FOR ()-[r:T]-() ON (r.p)` is
accepted into the schema catalog as a `RangeIndex` *definition*, and the catalog
is the only thing that ever learns about it: there is no population, no write
maintenance, no query read, no discovery and no eviction owner for that
definition. The one working relationship equality owner
(`relationshipUniqueIndexes` in `pkg/storage/badger_relationship_unique_index.go`)
is reached only from `UNIQUE` / `RELATIONSHIP KEY` constraint declaration, and
both `SHOW INDEXES` and `CALL db.indexes()` hardcode `ONLINE` for every non-vector
catalog row, so the operator sees a healthy index that no code implements.

## 1. What the statement does today, in owner order

| Step | Canonical owner | Location | Behaviour for a relationship target |
| --- | --- | --- | --- |
| Parse / route DDL | `executeSchemaCommand` → `executeCreateIndex` | `pkg/cypher/schema.go:86,125`; `pkg/cypher/index_creation.go:16` | `CREATE INDEX`/`CREATE RANGE INDEX` on `()-[r:T]-()` is parsed by the same `parseCreateIndexDDL` owner as nodes; `entityType = RELATIONSHIP`, `label = relationshipType`. |
| Admission (duplicate/conflict rules) | `admitSchemaIndexCreation` | `pkg/cypher/schema_index_admission.go:15-90` | Reads the catalog, rejects an equivalent/duplicate rule, and reports `ConstraintAlreadyExists` for a catalog row owned by a constraint. Entity-type aware. |
| Declaration | `AddRangeIndexForEntity` → `addIndexForEntity` | `pkg/storage/schema.go:1343,1347` | Creates a `RangeIndex{Kind:RANGE, EntityType:RELATIONSHIP, Label:T, Properties:[p]}` with empty `entries`/`nodeValue`; idempotent by name; persists the definition. **No build call, no engine notification, no backfill.** |
| Constraint-owned counterpart | `applyConstraintLocked` | `pkg/storage/schema.go:330,347` | A `UNIQUE`/`RELATIONSHIP KEY` constraint also registers a `RangeIndex` named after `c.OwnedIndex` with `OwningConstraint` set. It is likewise never populated; its only purpose is catalog/SHOW INDEXES representation. |
| Population (build) | `BuildRelationshipPropertyEqualityIndex` ← `PrepareRelationshipPropertyEqualityIndexForEngine` | `pkg/storage/relationship_property_equality_lookup.go:182,216`; caller `pkg/cypher/schema.go:1212` | Exists, but is gated to `ConstraintEntityRelationship` + (`ConstraintUnique` or `ConstraintRelationshipKey`) (`lookup.go:225-231`). Never called for a standalone index. |
| Mutation maintenance | `publishIndexMembership` → `publishRelationshipIndexMembership` | `pkg/storage/badger_property_index_maintenance.go:42,65`; `pkg/storage/badger_relationship_unique_index.go:226` | Maintains only entries already present in `b.relationshipUniqueIndexes`; it iterates that set, never the schema catalog. A declared ordinary index therefore has no maintenance owner. |
| Query read | `relationshipPropertySeedEdge` | `pkg/cypher/traversal_relationship_property_seed.go:27`; call site `pkg/cypher/traversal.go:412` | Resolves inline one-hop equality (`MATCH (a)-[r:T {p:$v}]->(b)`) from the equality index **only after requiring a covering declared constraint** (`:52-60`, `relationshipConstraintIndexProperties` `:90`). An ordinary index is invisible here, so the complete type scan runs. |
| Storage read | `LookupEdgesByPropertyEqualityInNamespace` | `pkg/storage/relationship_property_equality_lookup.go:111` | Postings from `CompositeIndex.LookupFull`, each candidate read back and re-checked (`:95`), namespace-prefix filtered; builds the index from stored edges if absent (`badger_relationship_unique_index.go:131`). Declines write-behind, decay views and blank namespace (`lookup.go:117`). |
| Transaction read | `BadgerTransaction.LookupEdgesByPropertyEquality` → wrapper | `pkg/storage/badger_transaction_relationship_property_lookup.go:11`; `pkg/cypher/transaction_storage_wrapper.go:735`; `pkg/storage/namespaced.go:437` | Merges committed postings with the transaction's pending edges and deletions; declines when the namespace snapshot is not current. |
| State reporting | `executeShowIndexes` / `callDbIndexes` | `pkg/cypher/executor_show.go:198`; `pkg/cypher/call.go:1459` | `state, populationPercent = "ONLINE", 100.0` for every non-`VECTOR` row, including a `RANGE RELATIONSHIP` row that nothing populates. Only `VECTOR` consults a population record (`vectorIndexShowState`). |
| Catalog / stats | `GetIndexes`, `GetIndexStats` | `pkg/storage/schema.go:1874` (range loop `:1926`), `:2373` (range loop `:2428`) | Lists the declaration (name, kind, label/type, entityType, owningConstraint, properties). Stats report `TotalEntries = len(idx.entries)` (always 0) and `Selectivity = 1.0`. |
| Drop index | `DropIndex` | `pkg/storage/schema.go:1725`; range branch `:1750-1756` | Removes the definition; refuses a constraint-owned row. Nothing else holds state, so nothing else is cleaned up. |
| Drop constraint | `DropConstraint` → `dropConstraintLocked` | `pkg/storage/schema_drop_constraint.go:22,79,122-135` | Removes the constraint and its owned `RangeIndex` definition. The derived equality index entries in `b.relationshipUniqueIndexes` are **not** evicted. |
| Persistence / restart | `exportDefinitionLocked` / import | `pkg/storage/schema_persistence.go:222-238` (export), `:469-482` (import) | Range indexes persist as definitions only (`Kind, Label, Property, Properties, EntityType, OwningConstraint`); `entries`/`nodeValue` are recreated empty. No index postings are persisted for any range index; the derived equality index is process-memory only. |
| Derived index lifecycle | `resetRelationshipUniqueIndexes`, `relationshipUniqueIndexLimit` | `pkg/storage/badger_relationship_unique_index.go:16,168-176,245`; callers `pkg/storage/badger_schema.go:20,282` | Reset on schema load and on namespace retire/replace; otherwise bounded to 16 retained entries with arbitrary eviction on publication, then rebuilt lazily by the next lookup. |
| Composite/tiered store | `relationshipPropertyEqualityLookupUnder` | `pkg/storage/relationship_property_equality_lookup.go:40-53` | Stops at `*CompositeEngine` (and `*NamespacedEngine`) and reports "not handled", so a tiered store keeps the complete scan; `pkg/storage/composite_engine.go:1288-1300` only re-creates RANGE definitions in the merged catalog for SHOW/DDL. |
| Admin surfaces | `DB.GetIndexes`; schema export | `pkg/nornicdb/db_admin.go:1199-1221`; `pkg/adminimport/neo4j_csv.go:831,928-960` | Both read the catalog, so both report/export the ordinary relationship index as an existing index (the exporter renders the `RELATIONSHIP` `FOR` pattern and skips constraint-owned rows). |

## 2. Which behaviours have an owner for which declaration

| Behaviour | Constraint-owned relationship index | Ordinary relationship index |
| --- | --- | --- |
| Catalog row | yes (`schema.go:347`) | yes (`schema.go:1347`) |
| Build from stored edges | yes (`lookup.go:216` at declaration; lazy `badger_relationship_unique_index.go:131`) | **none** |
| Write maintenance | yes (`badger_relationship_unique_index.go:226`) | **none** |
| Query read (inline one-hop equality) | yes, when the constraint covers the bound properties (`traversal_relationship_property_seed.go:52-67`) | **none** — the constraint gate rejects it |
| Discovery for planners/EXPLAIN | partial: EXPLAIN diagnostics only match node-shaped catalog rows (`pkg/cypher/explain.go:515-547`); `ValidateIndexHints` would accept it (`pkg/cypher/index_hints.go:398-433`) but has no production caller (`openspec/changes/converge-cypher-execution/tasks.md:319`) | same |
| Truthful state | `ONLINE` happens to be defensible (an owner exists) | **false** — hardcoded, no owner |
| Drop | definition removed; derived entries survive until reset/eviction | definition removed; nothing else existed |
| Persisted population | no (rebuilt) | no |

## 3. Gaps, with the owner that is missing

- **G1 — no build owner for a standalone declaration.** `pkg/cypher/index_creation.go:104-107`
  calls `AddRangeIndexForEntity` and returns. Compare the node branch
  (`pkg/cypher/schema.go:1238-1280`): `addPropertyIndex` → `AddCompositeIndex` +
  `backfillCompositeIndex`, and `addSchemaConstraint` (`:1180-1215`) backfills a
  constraint-owned node index. Neither call has a relationship analogue for a
  standalone index.
- **G2 — no mutation-maintenance owner.** `publishIndexMembership`
  (`badger_property_index_maintenance.go:42`) forwards relationship frames to
  `publishRelationshipIndexMembership`, which updates only
  `b.relationshipUniqueIndexes`. Nothing maps a schema declaration to a
  maintained set, so a counter added at declaration time would not survive the
  first write.
- **G3 — no read/discovery owner.** `relationshipPropertySeedEdge`
  (`traversal_relationship_property_seed.go:52-60`) asks the *constraint* catalog.
  The schema has no relationship-shaped equality discovery function at all: the
  node owners are `MaintainsPropertyIndex`/`HasPropertyIndex`/`GetPropertyIndex`
  (`pkg/storage/schema_index_discovery.go:99,128,139`) over
  `sm.compositeIndexes`, which is node-only by construction
  (`constraintPropertyIndexKey`, `pkg/storage/schema.go:383`).
- **G4 — false `ONLINE`/100% state.** `pkg/cypher/executor_show.go:198` and
  `pkg/cypher/call.go:1459` default every non-vector row to ONLINE. There is no
  population authority for `RANGE`, `TEXT`, `POINT`, `FULLTEXT` or `LOOKUP` rows;
  only `VECTOR` has one (`vectorIndexShowState`).
- **G5 — the legacy `RangeIndex` has no production reader at all.**
  `RangeIndexInsert` / `RangeIndexDelete` / `RangeQuery`
  (`pkg/storage/schema.go:1417,1458,1484`) have no non-test callers anywhere in
  the tree (verified by repository-wide search), and `GetRangeIndex` (`:1978`) has
  no non-test caller either. Node equality reads run on `CompositeIndex` /
  `PropertyIndex` snapshots (`pkg/storage/schema_property_index_snapshot.go`,
  `schema_composite_index_lookup.go`). So "build the declared `RangeIndex`" is not
  a reusable owner — it is dead machinery whose only live consumer is the catalog.
- **G6 — no eviction owner on drop.** `DropIndex` (range branch
  `pkg/storage/schema.go:1750-1756`) and `DropConstraint`
  (`pkg/storage/schema_drop_constraint.go:122-135`) remove definitions; nothing
  removes the corresponding retained derived index from
  `b.relationshipUniqueIndexes`. Today that set is only cleared wholesale on
  schema load / namespace retire (`pkg/storage/badger_schema.go:20,282`) and
  evicted arbitrarily at 16 entries
  (`pkg/storage/badger_relationship_unique_index.go:16,168-176`).
- **G7 — one derived index, possibly two declaration owners.** The derived index
  is keyed by `(namespace, relationshipType, property-set)`
  (`badger_relationship_unique_index.go:17,131`), not by declaration name. An
  ordinary index and a constraint on the same type and property set would share
  one entry; dropping either must not evict an index the other still needs.
  Admission is asymmetric today: an ordinary declaration equivalent to a
  constraint-owned row is rejected
  (`pkg/cypher/schema_index_admission.go:70-90`), but a constraint added after an
  equivalent standalone relationship index is not checked, because
  `constraintPropertyIndexKey`/`constraintCompositeKeyIndexWanted`
  (`pkg/storage/schema.go:383,398`) are node-only.
- **G8 — value coverage is narrower than a Neo4j property index.** The derived
  index files keys via `relationshipConstraintValueKeys`
  (`badger_relationship_unique_index.go:35-62`), which accepts
  `float64/int64/uint64/string/bool` and declines composite keys, and — because
  the switch has no case for `temporalIndexKey` — declines temporal values that
  the node index keys (`pkg/storage/schema_value_keys.go:36-62,261`). Declines are
  reported as "not handled", so the scan keeps correctness.
- **G9 — deployment gates.** The derived owner declines write-behind and
  decay-filtered views (`relationship_property_equality_lookup.go:117`), a
  temporal viewport (`traversal_relationship_property_seed.go:39`), and any
  `*CompositeEngine` (`relationship_property_equality_lookup.go:40-53`). A world
  where the declaration is supported only sometimes needs the reported state to
  say which one applies.
- **G10 — acceptance consumers of the catalog.** `DB.GetIndexes`
  (`pkg/nornicdb/db_admin.go:1199`) and `exportSchemaCypher`/`renderIndexStatement`
  (`pkg/adminimport/neo4j_csv.go:831,928`) treat any catalog row as a real index;
  a dump/export therefore round-trips the no-op declaration. `EXPLAIN`
  diagnostics do not consult relationship rows at all
  (`pkg/cypher/explain.go:515`), so plans stay silent rather than wrong.
- **G11 — statistics.** `GetIndexStats` reports `TotalEntries 0`,
  `Selectivity 1.0` for every range row (`pkg/storage/schema.go:2428-2452`), which
  is indistinguishable from a correctly maintained empty index.
- **G12 — adjacent, same class, out of scope.** `CREATE TEXT INDEX` /
  `CREATE POINT INDEX` go through `executeCreateTypedPropertyIndex`
  (`pkg/cypher/schema_typed_property_index.go:12-50`) →
  `AddTypedIndexForEntity` (`pkg/storage/schema_typed_indexes.go:31`), i.e. the
  same definition-only `RangeIndex` with `Kind` TEXT/POINT, for nodes and
  relationships alike, with the same hardcoded ONLINE reporting and no reader.
  This is recorded as context only; it is not part of N152-143's agreed scope.
  Likewise, `isPropertyInStructuralIndex`
  (`pkg/storage/schema_knowledgepolicy.go:493-498`) looks range indexes up by
  `"Label:property"` while `sm.rangeIndexes` is keyed by index *name*, so that
  check can never match — noted, not investigated further.

## 4. Option A — make the canonical derived index the ordinary index's owner

Reuse target: `relationshipUniqueIndexes` and its existing build / maintain /
lookup / read-back / transaction-merge owners. No second posting store is needed;
the key shape already matches a standalone declaration.

| Required change | Existing owner reused | New/absent owner |
| --- | --- | --- |
| Build at declaration | `BuildRelationshipPropertyEqualityIndex` (`relationship_property_equality_lookup.go:182`), `relationshipPropertyEqualityIndexOwnerFor` (`:67`) | a declaration-shaped entry point: `PrepareRelationshipPropertyEqualityIndexForEngine` (`:216`) is the only caller and is constraint-gated (`:225-231`) |
| Maintenance | `publishRelationshipIndexMembership` (`badger_relationship_unique_index.go:226`) — maintains every retained entry regardless of why it exists | nothing, provided the build registers the entry (it sets `set.active`) |
| Read | `LookupEdgesByPropertyEqualityInNamespace` (`:111`) + transaction merge (`badger_transaction_relationship_property_lookup.go:11`) | extend the qualification in `relationshipPropertySeedEdge` (`traversal_relationship_property_seed.go:52-60`) from "a covering constraint exists" to "a covering constraint **or declared ordinary index** exists", using one discovery owner (G3) |
| Discovery | nothing reusable | one schema owner that answers "is `(type, properties)` covered by a declared relationship equality index", next to the node owners in `schema_index_discovery.go` |
| Drop | `DropIndex` / `DropConstraint` call sites | one engine-level removal/reference-count owner (G6, G7) |
| Persistence | definition persistence unchanged (`schema_persistence.go:222,469`) | none; the derived index stays in-memory and rebuilds lazily, exactly as the constraint path does today |
| Truthful state | `SHOW INDEXES`/`db.indexes` state derivation | a population/ownership signal for RANGE rows (G4) |

Consequences to accept explicitly before this is chosen: the declared index
inherits every limitation of the derived owner (G8 value coverage, G9
write-behind/decay/tiered/viewport decline, 16-entry retention), and it shares
one entry with a constraint on the same key (G7). Correctness is preserved in all
of those cases because the seed path falls back to the complete scan and every
posting is read back and re-checked.

## 5. Option B — explicit supported-operation failure / truthful rejection

| Shape | Touch point | Consequence |
| --- | --- | --- |
| Reject the declaration | `pkg/cypher/index_creation.go:104-107` (the only creator of a relationship RANGE declaration; the typed TEXT/POINT path is the same class but out of scope, G12) | `CREATE INDEX ... FOR ()-[r:T]-() ON (r.p)` fails with a stable semantic error instead of succeeding; no catalog row, so no false ONLINE. Constraint-owned relationship rows must keep working (`applyConstraintLocked`, `schema.go:347`). |
| Accept but never claim ONLINE | state owner in `executor_show.go:198` / `call.go:1459`; would need a real population/ownership signal from storage | keeps DDL compatibility while telling the truth; needs the same ownership signal Option A needs, without the acceleration. |

Two consequences need an operator decision, not an agent inference:

1. **Already-persisted declarations.** A database that ran an earlier version can
   hold such a definition, and import recreates it
   (`schema_persistence.go:469-482`). Failure-at-DDL does not retroactively remove
   it; it would load, list and (under Option B) either be reported with a real
   state or need a documented drop path. Silently deleting persisted operator
   declarations, or adding a permanent fix-up for that known population, is
   excluded by the standing principles (19).
2. **Meaning of the report for undeclarable configurations.** If the operator's
   accepted result is "an index declaration is either honoured or refused", then
   write-behind/decay/tiered/temporal configurations (G9) need the same treatment;
   if it is "honoured or refused **where the engine can own it**", the scope stays
   narrow but SHOW INDEXES must say which case applies.

## 6. What this map does not establish

- No code was executed: no test, build, profile, benchmark or container run. The
  RED/GREEN numbers are the ticket's preserved logs, not a fresh run.
- No fix sizing beyond the owner list; no performance claim, no millisecond
  target, no estimate of an index's benefit.
- The `CompositeEngine` path was read, not exercised; its DDL/SHOW behaviour is
  inferred from `composite_engine.go:1180-1305` and
  `relationship_property_equality_lookup.go:40-53`.
- Docs were not consulted through the documentation graph (server unavailable);
  repository docs were read directly (`docs/neo4j-migration/feature-parity.md`,
  `docs/neo4j-migration/cypher-compatibility.md`, `docs/development/index-publication.md`).
  Nothing in them claims ordinary relationship index acceleration; they claim
  constraint-owned backing indexes only.
