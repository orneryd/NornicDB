# Duplicate review: N152-143 relationship index owner

Applied per the `nornicdb-duplicate-review` skill. The question is not "is there a
second implementation to delete", but "does an existing owner already implement
the behaviour an ordinary relationship `CREATE INDEX` implies, so that a fix
reuses it instead of adding a parallel index?"

## Retrieval status (missing, not empty)

- `nornicdb` code graph and `nornicdb_docs` documentation MCP servers were
  unreachable for this investigation: tool discovery returned zero tools and the
  catalog error
  `http/request failed: error sending request` for both
  `https://radeon7.tail2f19fb.ts.net/mcp/nornicdbcode` and `…/mcp/nornicdocs`.
  The advisory Graph RAG result in the task context likewise records
  `"status": "incomplete"` with no code or docs hits.
- Therefore no graph-based duplicate analysis is claimed. Candidate discovery was
  done with repository-wide source search plus direct reading at
  `f77ec7706de099588ac232b32b6908860511d3da`; each candidate and its callers were
  inspected, and the ticket's source revision
  `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5` was diffed against HEAD for the
  affected files (`git diff --stat`: only `executor_show.go`,
  `transaction_storage_wrapper.go`, `traversal.go`, `composite_engine.go` differ,
  none in the ownership code paths).

## Candidates examined

| Candidate | Location | Overlap with the required behaviour | Verdict |
| --- | --- | --- | --- |
| Constraint-derived relationship equality index (`relationshipUniqueIndexes`, `CompositeIndex` postings, read-back, transaction merge) | `pkg/storage/badger_relationship_unique_index.go:16-256`; `pkg/storage/relationship_property_equality_lookup.go:23-234`; `pkg/storage/badger_transaction_relationship_property_lookup.go:11`; `pkg/storage/namespaced.go:437` | Key shape `(namespace, relationship type, ordered property set)` is exactly what a standalone index declares; build, maintenance, lookup, candidate verification and pending-write merge already exist and are exercised by the ticket's PASS controls (2 reads at 8 and 512, both parsers, auto and explicit transaction) | **Reusable owner.** Only the declaration-to-owner link (build at declaration + qualification for reads) is missing; see `owner-map.md` §4. |
| Node equality index chain (`compositeIndexes` / `PropertyIndex` snapshot reads, `ReplaceNode` maintenance, `HasPropertyIndex` discovery, backfill) | `pkg/storage/schema.go:966,1347`; `pkg/storage/badger_property_index_maintenance.go:59`; `pkg/storage/schema_property_index_snapshot.go`; `pkg/storage/schema_index_discovery.go:99-150`; `pkg/cypher/schema.go:1238-1280` | Same four responsibilities the relationship fix needs, but the postings are keyed by label+property and hold node IDs; reusing it for relationship types would require a second entity dimension in the oldest and hottest index structure | **Not directly reusable.** Read as the structural template for what a complete owner set looks like. |
| Legacy `RangeIndex` (`entries`/`nodeValue`, `RangeIndexInsert/Delete/Query`) | `pkg/storage/schema.go:1417,1458,1484` | Name matches "range index"; entity-type parameter exists | **Dead owner.** No non-test caller of insert/delete/query/get; node reads never use it; entries are not persisted (`schema_persistence.go:469-482`). Building it would create a *parallel* implementation beside `CompositeIndex`, i.e. exactly the duplication the skill forbids. |
| `TEXT`/`POINT` typed index path | `pkg/cypher/schema_typed_property_index.go`; `pkg/storage/schema_typed_indexes.go:31` | Same definition-only `RangeIndex` storage and same hardcoded ONLINE reporting | Same class of gap, different declaration kind. Out of N152-143 scope; recorded so the parent can scope it deliberately rather than discover it later. |
| Adjacency / between-endpoint / type indexes | `pkg/storage/badger_queries.go:1445-2190`; `edge_type_stream.go` | Relationship access paths, but keyed by endpoint or type only — no property postings | Not a duplicate of property equality; no sharing opportunity. |
| `edges_between`/`badger_index_catalog.go` byte-key indexes | `pkg/storage/badger_index_catalog.go:108` | Physical edge index keys derived from endpoints/type | Unrelated storage-format owner. |

## Behavioural comparison of the overlapping pair

The only genuine overlap is Option A's reuse pair (declared ordinary index vs
constraint-derived index). Requirements checked in source:

- **Population.** Both need every stored relationship of the type, including
  those a visibility layer hides: `relationshipUniqueIndexLocked` scans
  `GetStoredEdgesByType` (`badger_relationship_unique_index.go:131-160`,
  `badger_queries.go:1042`). A standalone index needs identical population.
- **Maintenance.** `publishRelationshipIndexMembership` folds the same accepted
  commit frames for every retained entry, independent of declaration origin
  (`badger_relationship_unique_index.go:226-243`).
- **Read semantics.** Posting is a candidate only; each candidate is read back and
  re-checked for type and all indexed values
  (`relationship_property_equality_lookup.go:95,160-168`), so an ordinary index
  reusing this owner cannot return a row a scan would not.
- **Differences that would remain.** None of the differences are about *why* the
  index exists: they are deployment gates (write-behind/decay/tiered/temporal),
  value-key coverage (no temporal or composite keys) and the 16-entry retention
  bound. They are enumerated as G8/G9/G10 in `owner-map.md` and apply equally to
  the constraint path today.

## Parent-approved correction

The parent approved reuse of the canonical derived owner for ordinary RANGE
indexes. Preparation precedes catalog publication; committed writes maintain
retained entries independently of declaration origin. One SchemaManager selector
now discovers both ordinary and constraint-owned relationship RANGE rows, keeping
constraint-owned priority. The old Cypher-only constraint selector is removed.
No new posting, transaction merge, maintenance or drop owner was introduced.

The benchmark fixture and timed body are shared with existing constraint tests;
ordinary cases add only a declaration option and matched 8/512 populations.
Multi-hit and unsupported-view complete scans remain explicit limits. Original
RED and unchanged controls were run before production edits. Focused normal/race checks, package vet, scoped localization verification and
matched ordinary/constraint benchmarks completed; see `verification.md` and
`benchmark.md`. The staged checker was invoked and failed analysis/ingestion
readiness. Graph analysis remains incomplete; this is not a clean checker claim
or whole-ticket qualification.
