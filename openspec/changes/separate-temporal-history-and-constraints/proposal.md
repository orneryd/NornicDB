# Separate historical entity reads from generic temporal constraints for v1.5.0

Approved plan tracked in NOR-394; implementation in progress.
Source inspected: 365b908820b552aafc6e969a63310400521cac5e.

## Problem

The existing db.temporal.asOf accepts label, key property/value, two interval-property names, an interval lookup time, and optional MVCC time/sequence. It combines a configurable interval lookup with historical storage selection. The related assertNoOverlap procedure orders its arguments differently. Users cannot read these calls without knowing the positional signatures.

The intended historical API returns complete historical states of eligible entities that still exist, within retained history. It must not resurrect deleted entities or cross an ID-recreation boundary. It does not create, restore or materialize a whole-graph snapshot. Generic temporal constraints independently enforce configured interval rules over arbitrary user properties; no domain intent or reserved property names are implied.

Observed source costs and semantic gaps:

- pkg/cypher/call_temporal.go: callDbTemporalAsOf uses the temporal interval index only without the optional MVCC selector. With systemTime it calls temporalNodesByLabel and then filters key and interval in Go.
- pkg/storage/badger_mvcc_indexed_reads.go: getNodesByLabelVisibleAtWithView calls iterateNodesVisibleAtInScopeInTxn and applies the label filter after each node body is resolved. It accumulates matching nodes into a slice before the procedure's remaining filters.
- pkg/storage/badger_mvcc.go: iterateNodesVisibleAtInScopeInTxn walks the scoped MVCC node-head keyspace and resolves current bodies or retained historical records. This is not restoration from backup snapshots. Its default path can hydrate embeddings while resolving bodies.
- pkg/storage/badger_mvcc_indexed_reads.go: getNodeAtInTxn rejects a tombstone when the requested version is at/after deletion, but can consult prior records when the requested version precedes deletion. That generic reader alone does not establish the newly approved no-resurrection contract.
- Scoped enumeration currently skips heads below their retention floor. That must not silently turn an incomplete historical query into an apparently complete empty/partial result under the new contract.

These are source observations, not fresh performance measurements. Graph retrieval was unavailable; the actual source was inspected directly.

## User journey

1. An operator retains versions of currently existing Account nodes.
2. A user calls asOf with a timestamp and a parameterized read-only MATCH query.
3. The query selects and reconstructs complete node states at that time, within current entity lifetimes and retained history.
4. A node deleted before read admission is excluded, even for an earlier timestamp. Recreating its ID does not expose the former incarnation.
5. A user separately declares a TEMPORAL constraint naming arbitrary interval bounds and grouping fields. Writes enforce NO OVERLAP atomically.
6. **Desired end state:** ordinary Cypher selects entities; asOf supplies the historical cutoff for those eligible results; temporal constraints enforce independently declared interval rules. None implicitly activates another.

**Out of scope:** whole-graph physical snapshot creation/restoration; undelete or time travel across deletion; retaining deleted histories for this API; new backup/WAL formats; storage migration; automatic application-domain versioning; UI work; a separate selector DSL; unapproved new temporal rules.

## Acceptance criteria

- [ ] Introduce the historical signature below, returning one row per matched result, with retained labels, properties and metadata when nodes are returned; historical embedding vectors are unavailable.
- [ ] Historical reads require neither interval properties nor temporal constraints.
- [ ] Ordinary MATCH supports labels, element IDs, ID lists, indexed properties, relationship types/direction/endpoints and bounded traversals through one canonical historical execution owner.
- [ ] Historical labels and properties determine matching, limited to eligible currently existing entity incarnations. Current index membership must not incorrectly exclude historical matches.
- [ ] Nodes created after the cutoff do not appear. Nodes deleted before admission do not appear even if older records remain internally.
- [ ] Reusing an ID never crosses its deletion/recreation boundary.
- [ ] Relationship reads respect equivalent lifecycle boundaries and require eligible endpoints; no historical traversal resurrects deleted topology.
- [ ] Existing entities with unavailable/pruned requested history produce an explicit unavailable-history error, not current/earliest substitution or silent incomplete success. A genuinely absent matching population is an empty result.
- [ ] Resolve one committed time/sequence cutoff per call; preserve it across nested execution and result consumption without materializing the graph.
- [ ] Reject mutating statements, nested mutating procedures and other write routes using the canonical effect/admission machinery.
- [ ] Preserve database authorization, suppression policy, cancellation and resource ownership. Historical time is not a permission bypass.
- [ ] Stream results through existing execution/result owners; avoid precollecting a graph-wide node list. Explicit collect() remains caller-directed materialization.
- [ ] Support CREATE CONSTRAINT with temporal.epoch(...) and explicit GROUP BY for nodes and relationships.
- [ ] Support arbitrary configured property names and composite grouping keys without assigning domain meaning.
- [ ] Enforce constraints atomically during writes, including concurrent writes; preflight assertions are not enforcement.
- [ ] Deprecate the old asOf signature and tuple DDL in v1.5.0, documenting compatibility behavior and a separately approved removal schedule.
- [ ] Both DDL spellings resolve to one canonical persisted definition/enforcement owner. Existing databases require no migration or storage-version bump.
- [ ] SHOW/export exposes the new spelling; existing stored constraints round-trip without losing their configuration.
- [ ] Add public fail-first regressions for demonstrated defects and qualify affected packages, transports, cancellation and allocations. Leave full release verification to the established CI workflow.

## Alternatives considered

| Option | Why not (or why later) |
| --- | --- |
| Chosen: read-only Cypher executed through a historical entity view | Reuses MATCH, planning, authorization and result contracts; avoids a second selector language. |
| Match today's entities/properties, then fetch old bodies | Misses still-existing entities whose identifying properties or labels changed; historical predicates must use historical values after current-lifetime eligibility is established. |
| Restore a backup/whole graph snapshot for each request | Not needed; retained per-entity versions already supply historical bodies. Adds unrelated I/O and materialization. |
| Generic selector map covering every MATCH feature | Duplicates predicate, traversal, ordering and pagination semantics already expressed by Cypher. |
| Treat interval properties as database history | Conflates user-configured data rules with commit history and forces fields that historical reads do not require. |
| New public overlap-preflight procedure as a write guarantee | Races concurrent writers. Schema constraints own enforcement. A conflict-inspection API can be considered separately when a concrete user journey requires it. |

## Test matrix

| # | Scenario | Setup | Steps | Expected |
| --- | --- | --- | --- | --- |
| 1 | Historical complete node | Live node, two retained property versions | Query cutoff between commits | Complete earlier node state |
| 2 | No temporal configuration | Ordinary nodes; no interval fields/constraint | Run asOf MATCH | Historical read succeeds |
| 3 | Historical property matching | Still-live node changes identifying property | Match old/new value at earlier cutoff | Old value matches, new value does not |
| 4 | Historical labels | Still-live node relabeled | Query earlier label at earlier cutoff | Correct historical membership |
| 5 | Element IDs and ID lists | Multiple live nodes with history | Query one ID and an ID list | Correct subset, no duplicate artifacts |
| 6 | Indexed equality/range | Changed values; index present/absent | Compare indexed route with historical scan oracle | Identical rows and ordering where requested |
| 7 | Deleted node | Delete after cutoff but before read admission | Query older time by ID/property/label | Deleted incarnation excluded |
| 8 | Recreated ID | Delete then recreate same logical ID | Query before/after new creation | No prior-incarnation state returned |
| 9 | Created after cutoff | Current node has no earlier existence | Query preceding cutoff | No row |
| 10 | Retention floor | Live node with expired requested version | Read below/at/above floor | Explicit unavailable error below; correct boundary behavior |
| 11 | Mixed available/unavailable histories | Broad query encounters expired history | Consume results | No silent complete-success claim; streamed errors follow existing protocol contract |
| 12 | Relationships | Live typed edges with retained changes | Query type, direction, endpoints and properties | Same historical cutoff throughout |
| 13 | Deleted/recreated topology | Delete edge or endpoint, recreate ID | Traverse at old cutoff | No resurrection or cross-incarnation traversal |
| 14 | Current eligibility under concurrency | Read races ordinary delete/update | Exercise canonical visibility boundary | Documented linearization; no new deleted-history retention promise |
| 15 | Reads versus writes | Direct/nested mutation variants | Invoke historical API | Write rejected before effects |
| 16 | Namespace/authorization/suppression | Two databases and restricted callers | Run equivalent historical queries | Existing access boundaries preserved |
| 17 | Cancellation/result lifetime | Large generated match | Cancel before execution/during iteration; exhaust results | Work stops and reader resources retire |
| 18 | Complete payload ownership | Nodes include named/managed vectors and nested values | Return whole node, mutate embedded result | Required complete image; no retained-state aliasing |
| 19 | Single cutoff and timestamp ties | Multiple commits share timestamp | Query time; exercise internal sequence resolution | One documented cutoff consistently used |
| 20 | Interval boundaries | Configurable start/end fields | Test adjacent, overlapping, null, malformed, reversed and zero-length bounds | Approved existing semantics preserved and explicitly documented |
| 21 | Composite grouping | Same intervals across different group tuples | Create/update nodes | Reject overlap only within the configured group |
| 22 | Relationship constraints | Arbitrarily named bounds on typed edges | Create/update overlaps | Atomic NO OVERLAP enforcement |
| 23 | Concurrent constraint writes | Two overlapping writes for same group | Race commits and cancellation | Canonical atomic enforcement; no preflight race |
| 24 | DDL equivalence/migration | Existing old-syntax constraint | Open database; SHOW/export; use new declaration | Same definition/enforcement; no migration |
| 25 | Deprecated procedure | Existing six-to-eight-argument calls | Execute during deprecation period | Existing semantics retained and clearly deprecated |
| 26 | Parser parity | Both parsers | Repeat new DDL and historical calls | Equivalent results, errors and side effects |
| 27 | Population scaling | Fixed matched set, growing unrelated population | Profile ID, property, label and traversal workloads | Document examined entities, latency, allocated/retained bytes; no whole-graph result materialization |
| 28 | History depth scaling | Fixed entity count, increasing versions | Profile cold/warm historical point reads | Measured version-resolution cost; no unqualified constant-time claim |

**Automated coverage:** extend temporal procedure/viewport tests and native MVCC visibility/lifecycle suites; public Execute plus Bolt/HTTP result/error tests; constraint DDL/registration/export tests; focused race/cancellation controls; CPU-profiled repeated generated-workload benchmarks. Record immutable baseline/head, configuration, examined-row counters, ns/op, B/op, allocs/op and retained memory. Do not call static inspection, skipped CI or old-head evidence a fresh gate pass.

## Proposed implementation areas

### NornicDB: historical read API

Proposed public signature:

```text
db.temporal.asOf(at :: DATETIME, query :: STRING, parameters = {} :: MAP)
YIELD row :: MAP
```

```cypher
CALL db.temporal.asOf(
  datetime('2026-03-15T12:30:00Z'),
  'MATCH (a:Account)-[r:OWNS]->(asset)
   WHERE a.accountId = $id
   RETURN a, r, asset',
  {id: 'acct-42'}
) YIELD row
RETURN row.a, row.r, row.asset
```

- pkg/cypher/call_temporal.go and procedure_registry_builtin.go: distinguish new two/three-argument historical query from deprecated six/eight-argument interval lookup. Registration/effect analysis must recognize the inner executable text and preserve read-only enforcement, parameter isolation and native result ownership.
- Reuse existing dynamic-query admission/execution, parameter binding, planners and transport streaming. Do not add another textual parser, result cursor protocol or forwarding-wrapper stack.
- Introduce one explicit historical read scope carrying committed cutoff, database, current-lifetime eligibility and reader lifetime. Carry through normal node/relationship/predicate/traversal kernels. Do not reuse TemporalViewport blindly: its current filtering is tied to temporal-current interval policy, not the new independent historical contract.
- pkg/storage/types.go MVCCVisibilityEngine/MVCCIndexedVisibilityEngine and native read owners: use existing version resolution while enforcing the new live-incarnation boundary. Inventory point/label/property/type/adjacency readers and wrappers before changing interfaces. Go API changes must be documented together with caller migration.
- Optimize candidate acquisition before hydration. Property-index historical completeness must be proven; current indexes alone are insufficient when values changed. Existing explicit-transaction physical-snapshot label optimization is not automatically valid for arbitrary requested historical timestamps.
- Retention checks must distinguish a never-existing/nonmatching node from unavailable history. The current enumeration's floor skip is not adequate proof of completeness. Document behavior when a scan cannot establish completeness without versions already pruned; fail explicitly instead of guessing.
- Define read/delete linearization using existing concurrency ownership. No promise to preserve deleted-node history for asOf, and no changes to unrelated transaction/recovery retention semantics merely to implement this API.
- Whole node means an owned complete entity image, not a graph-wide materialized collection. Track unavoidable payload bytes separately from avoidable intermediate work.

### NornicDB: interval values and constraint DDL

```cypher
CREATE CONSTRAINT account_intervals
FOR (n:Account)
REQUIRE temporal.epoch(n.starts_at, n.ends_at)
GROUP BY n.accountId
```

```cypher
CREATE CONSTRAINT ownership_intervals
FOR ()-[r:OWNS]-()
REQUIRE temporal.epoch(r.starts_at, r.ends_at)
GROUP BY r.tenantId, r.accountId, r.assetId
```

- temporal.epoch(start,end) declares an exclusive half-open span using two user-configured bounds; GROUP BY supplies grouping keys. No reserved bound names or application intent.
- Represent interval values independently of storage MVCC. Preserve the established [start,end) overlap convention, subject to exact source/test verification of malformed/null/reversed/zero-length cases; do not silently change those semantics while renaming syntax.
- Keep supported DDL bound/group expressions scoped to existing property-based constraint capabilities. General computed-expression constraints are not implied by the expression spelling.
- Parse both spellings to the existing canonical constraint definition and index/enforcement owners; do not change Badger keys, existing serialized definitions, WAL or storage version.
- Review both parser front ends, retained syntax registration, query analysis, SHOW/export/import and documentation as one change. Preserve IF NOT EXISTS, naming and ordinary DROP behavior.
- Deprecate assertNoOverlap's use as an enforcement mechanism. If compatibility remains during deprecation, describe it honestly as a read-only preflight, never a concurrent-write guarantee. Do not introduce a new inspection procedure without an approved use case.

### Documentation and rollout

- Update docs/user-guides/historical-reads-mvcc-retention.md, cypher-queries.md, managing-constraints.md, canonical-graph-ledger.md and compatibility/API metadata examples together.
- Clearly distinguish logical historical version reads from physical backup/compaction snapshots. State deletion, recreation and retention boundaries prominently.
- Include before/after calls, node and relationship DDL, composite groups, result rows, errors and performance access-path caveats.
- Introduce deprecation in v1.5.0. Exact removal release and warning-delivery mechanism require a maintainer decision; do not invent a deadline or alter existing result schemas to carry warnings.
- The public query API returns a sequence of rows, not a bespoke paginated list. Reuse established transport/result consumption; explicit collect/order/limit stays in Cypher. Verify resource ownership for those consumers rather than promising unsupported incremental execution routes.
- This draft is planning only. No runtime or test execution was performed to qualify these proposed changes.

## Approved historical payload clarification

Historical reads preserve the existing labels/properties/metadata payload. Embedding vectors are unavailable in historical versions; do not substitute current vectors or add vector history storage. No storage migration is authorized or required.

## Final constraint spelling

The canonical declaration is `CREATE CONSTRAINT name FOR (n:Label) REQUIRE temporal.epoch(n.start, n.end) GROUP BY n.key`. There is no `IS NO OVERLAP` suffix. This supersedes the unshipped `CREATE TEMPORAL CONSTRAINT` proposal; only the previously shipped tuple syntax remains as deprecated compatibility. No general `AS OF` query/DDL clause is in scope.

## N-arity qualification and cost documentation

- Apply NO OVERLAP to nodes and relationships with grouping widths 1, 2, 4, 8 and 16, in forward and reversed property order. A change to any grouping component creates an independent comparison group; identical tuples reject overlap.
- Verify creation and updates, movement between groups, adjacent intervals, wrong/missing references, legacy-definition equivalence and SHOW/export/recreation under both parsers.
- Document the actual cost of initial constraint validation, ongoing indexed versus complete-scan validation, scalar/composite/list-valued keys, and historical query acquisition. Ordinary property indexes and the constraint-owned temporal index are different capabilities.
- Profile the canonical overlap owner with identical generated populations and inputs in indexed and complete label-scan modes; report ns/op, B/op, allocations and limits. Do not describe a kernel benchmark as end-to-end query or commit performance.


Approved refinement: the unshipped dedicated NO OVERLAP declaration head is removed. The preferred CREATE CONSTRAINT / temporal.epoch form and deprecated ≤1.4.x tuple syntax share the same stored definition and enforcement.
