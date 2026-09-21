# NornicDB — what still needs converging or deleting, and is NOT easy

Basis: upstream main at 994b3a68 (the four easy PRs #483–#486 are already merged and excluded here). Evidence comes from the per-component graphify graphs, the deterministic analysis (`graphify/analyze.py` → `report/data.json`, details and full tables in `DIVERGENCE_REPORT.md`) and Go's `deadcode` tool (`graphify/deadcode/`). No LLM was used; the "why it is hard" and "approach" parts are my assessment, not tool output. Ordered by how much defect risk each item carries, highest first.

Legend — **Risk**: how likely the divergence is to produce (or already produced) user-visible bugs. **Effort**: S = days, M = 1–2 weeks, L = multi-week.

---

## 1. Two Cypher routers (auto-commit vs explicit transaction) — Risk: very high · Effort: L

**What:** `executeWithoutTransaction` (executor_query_routing.go) dispatches to 17 handlers, `executeQueryAgainstStorage` (transaction.go) to 10; only 8 are shared. Reached only outside a transaction: `executeReturn`, `executeTopLevelUnwind`, `tryFastPathCompoundQuery`, `DetectQueryPattern`, `isCallSubquery`/`hasSubqueryPattern`, `collectTopLevelMergeClauseBoundaries`. Reached only inside one: the `reveal` handling.

**Bugs already traced to it:** #397→#410, #399→#459, #457, #475 (different wrong answers per mode), #461 (relationship invisible to transactional reads).

**Why hard:** the two routers encode different assumptions about storage (live engine vs snapshot view), and each fast path was written against one of them. Unifying them changes which handler serves a query in one of the modes, i.e. behaviour changes, and the existing tests mostly call `exec.Execute` directly and cover only one side.

**Approach:** one router taking a storage view as a parameter; auto-commit = implicit transaction around one statement. Needs a test harness that runs every Cypher test in both modes over Bolt first (see #482, test plan 1.1/1.4), otherwise regressions will be invisible.

## 2. Five entry points into execution — Risk: high · Effort: L

**What:** `StorageExecutor.Execute`, `ExecuteOptimized`, `executeInTransaction`, `executeInternal`, `executePipeline` (644 lines together), plus the fast-path family (`tryFastPath…`, at least 5 functions with 0.5–0.67 callee overlap between the vector-cosine variants and their `…Projection` twins).

**Why hard:** each entry point applies a different subset of caching, validation, routing and fast paths; this is the text-driven executor itself. Deleting one means proving the others cover its query shapes.

**Approach:** part of the #482 overhaul (single front end → single pipeline); fast paths become optimisation rules with the pipeline as reference result. Not worth attempting piecemeal.

## 3. Expression evaluation in several flavours — Risk: high · Effort: M–L

**What:** `evaluateExpression` / `…WithContext` / `…WithContextFull` / `…WithPathContext`, and the value-assignment twins `applySetToNode` (set_helpers.go) vs `applySetToNodeWithContext` (merge.go) with 11 shared callees, Jaccard 0.73, plus `executeCreate` vs `executeCreateWithRefs` (0.64).

**Bugs already traced to it:** #462 (`SET rel.prop = expr` stores `'<nil>1'`), #474 (`SET n += {k: r.x}` stores text), #455, #460, #463, #465–#468, #475.

**Why hard:** the variants differ in which variables are in scope (node only / nodes+rels / paths), and the fallthrough "return the expression text" hides every gap. Merging them changes results for queries that currently "work" by accident.

**Approach:** first make evaluation strict (`(value, error)`, no text fallthrough — #482 §1.3), which surfaces the gaps as errors; then collapse to one evaluator with one scope type.

## 4. Storage read variants multiplied across four wrappers — Risk: high · Effort: M

**What:** one logical operation exists as up to 6 hand-written variants, each repeated on every wrapper:
- `getNode`: `GetNode`, `GetNodeProjected`, `GetNodeVisibleAt`, `GetNodeWithoutEmbeddings`, `getNodeVisibleAtInTxn`, `getNodeVisibleAtWithView` (BadgerEngine, 245 lines) and 3–4 of them again on Namespaced/Async/WAL.
- `streamNodes`: 5 variants × 4 engines (`…ByPrefix`, `…Projected`, `…WithoutEmbeddings`, combinations) = 20 functions, 620 lines.
- `getEdge` ×4, `getNodesByLabel` ×3, `getOutgoingEdges`/`getIncomingEdges` ×3 each, `decodeNode` ×3, `copyNode` ×4.

**And forwarding is uneven:** 22 BadgerEngine methods are forwarded by some wrappers of the production chain and missing on others (e.g. `GetNodeProjected` missing on WALEngine and AsyncEngine; the `On*Created/Deleted/Updated` hooks missing on WAL and Namespaced); the multidb wrappers (`sizeTrackingEngine`, `CompositeEngine`) have 42 such gaps. A caller that type-asserts for a capability silently gets a slow or different path depending on the wrapper it holds.

**Bugs already traced to it:** #420 (filter did full reads in the server), #424 (batching not effective), #473 (backup falls to the in-memory exporter), #436/#448 (snapshot vs live read differences).

**Why hard:** 162 exported methods on the inner engine; the wrappers add real behaviour (namespacing, async overlay, WAL) to some methods and must merely forward others; Go has no automatic delegation, so every new capability needs 3–4 manual forwards. Tests use the inner engine.

**Approach:** (a) replace the variant explosion with one read call taking options (`ReadOpts{AsOf, Projection, WithEmbeddings}`); (b) define the capability interfaces explicitly and add a compile-time assertion per wrapper (`var _ ProjectedReader = (*AsyncEngine)(nil)`) so a missing forward fails the build; (c) a contract test suite that runs the same storage tests against every wrapper stack, not only BadgerEngine.

## 5. Node/edge twin functions — Risk: medium · Effort: M

**What:** 32 near-identical node-vs-edge pairs, ~1,040 duplicated lines, almost all in storage: `StreamNodes`/`StreamEdges` (69 lines, 0.95 similar), `rebuildNodeMVCCHeadsFromVersions`/`…Edge…`, `resolveOrAllocateNodeNumIDInTxn`/`…Edge…`, `withViewNodeMVCCVersionsFromKey`/`…Edge…`, `loadNodeMVCCRecord…`/`…Edge…` (two pairs), `iterateNodesVisibleAtInTxn`/`iterateEdges…` (86 lines), `NodeCountByPrefix`/`EdgeCountByPrefix`, `mergePendingNodesLocked`/`…Edges…`, `namespaceForNodeIDs`/`…EdgeIDs`, decay filters, `coercePathNodes`/`coercePathRels` (bolt).

**Why hard:** not deletable, because both are needed; converging means a generic kernel (`[T Node|Edge]` with key-prefix and decode function as parameters). The MVCC ones are correctness-critical and performance-sensitive, so each needs benchmarks before and after.

**Approach:** generics, one pair per PR, starting with the pure-read ones (`Stream*`, `CountByPrefix`, `namespaceFor*IDs`); leave MVCC head rebuild/load for last.

## 6. DDL parsers written once per statement kind — Risk: medium · Effort: M

**What:** six `parseCreateConstraint…DDL` functions in cypher/schema.go with pairwise callee overlap 0.64–0.78 and two clone groups of three (`…ForRequireDDL`, `…TypeDDL`, `…SimplePropertyDDL`; `…Cardinality`, `…Policy`, `…Relationship…`), ~100 lines each; same pattern in knowledgepolicy_ddl.go (`parseAlterDecayProfile` / `parseAlterPromotionProfile` / `parseAlterPromotionPolicy`, 0.75) and storage `AlterDecayProfile`/`AlterPromotionProfile`, `CreateDecayProfileBundle`/`CreatePromotionProfile` (0.94–0.96).

**Why hard:** each parser accepts slightly different optional clauses; a shared parser needs a small grammar table per statement. Low bug history so far, but every new option has to be added N times.

**Approach:** table-driven DDL parser (or take DDL from the ANTLR grammar once #482 stage 1 exists).

## 7. HNSW search-layer and search-entry variants — Risk: medium · Effort: S–M

**What:** `HNSWIndex.searchLayer` has 7 variants (`searchLayer`, `…Heap`, `…HeapPooled`*, `…HeapPooledFromEntriesWithContext`, `…Single`, `…SingleWithContext`; two were removed in #483), `searchWithEf` 3, `HNSWCandidateGen.searchCandidates` 3, `VectorSearchPipeline.search` 3, `Service.vectorQueryNodes` 3. `HNSWIndex.Add` and `addWithLevel0Candidates` are 128-line near-copies (0.75).

**Bug history:** #425/#429/#433/#446: each recall fix had to be threaded through several of these.

**Why hard:** hot path; the variants exist for allocation/latency reasons, so merging needs benchmarks. `Add` vs `addWithLevel0Candidates` share the neighbour-selection and back-link code that #433 fixed, and a future fix can land in one only.

**Approach:** one layer search with options (entry points, context, pooled buffers) and keep thin wrappers; extract the shared insertion core of `Add`/`addWithLevel0Candidates`.

## 8. Three GPU build accelerators — Risk: low–medium · Effort: M

**What:** `CudaHNSWBuildAccelerator`, `MetalHNSWBuildAccelerator`, `VulkanHNSWBuildAccelerator`: `candidateSearchGraph…` and `CandidateSearchGraph…` are three copies at 0.92–0.96 similarity (100 + 63 lines each).

**Why hard:** behind build tags (cuda / metal / vulkan) that cannot all be compiled or tested on one machine; CI builds them in separate Docker images.

**Approach:** one generic driver over a small `gpuDistanceBackend` interface (upload, batched distance, download); the three types keep only the device calls. Needs the maintainer's hardware matrix to verify.

## 9. Code reachable only from tests — Risk: low (maintenance cost) · Effort: M, needs owner decisions

**What:** 1,143 functions in `pkg/` are unreachable from every binary and kept alive only by tests: cypher 114 (parser.go 14, index_hints.go 12, cache.go 11, parallel.go 10, typed_results.go 8, operators.go 7), storage 113, cypher/antlr 94, config 77, heimdall 76, temporal 70, localization 57, gpu 55, filter 53, search 52, mcp 39, encryption 38, nornicdb 34, inference 30, fabric 23, multidb 22.

**Why hard:** deleting them means deleting their tests, and many are exported, so they may be intended public API for embedding NornicDB as a Go library — only the maintainer can say which. Some are the beginnings of the very things #482 proposes (the ANTLR analyzer, the AST builder, `parallel.go`), which should be wired in rather than deleted.

**Approach:** maintainer triage per package into "public API" (keep, document), "planned" (keep, link to an issue) and "abandoned" (delete with tests). `pkg/temporal`, `pkg/filter`, `pkg/inference`, `pkg/fabric` are the first candidates to ask about because most of their surface is test-only.

## 10. Smaller items that still need a decision — Effort: S each

- `splitPropertyPairs` (52 lines) is the third member of the SET-splitter clone group merged in #484; it splits `{k: v, …}` maps with the same quote/bracket scanner. Converging it changes map parsing, which is where #474 lives — do it together with the #474 fix, not before.
- `buildNodeFulltextDoc` (call_fulltext.go) vs `buildEdgeFulltextDoc` (call_compat.go): 0.86 similar, in different files.
- `registerNornicDBRoutes` vs `registerAdminRoutes` (server_router.go), `handleRetentionPolicies` vs `handleRetentionHolds`: 0.88–0.91 similar handler boilerplate.
- `evaluateRelationshipConstraintContractExpressionEngine` vs `…Locked`, `countMatchingPatternEdgesEngine` vs `…Locked` (storage/constraint_contracts.go): the same logic once against the engine and once against a transaction — a small instance of item 4.
- `bfsSpanningTree` vs `dfsSpanningTree` (call_apoc_path.go, 101/104 lines, 0.75): differ in queue vs stack.
- The top-level `apoc/` tree (23,500 lines) is reported dead by `deadcode` but is built as a Go plugin; **do not delete**. Its own internal duplication was not analysed.

---

## Suggested order

1. Strict evaluation + both-modes test harness (#482 §1.3, §1.1) — prerequisite that makes items 1–3 safe to touch.
2. Item 4(b): compile-time capability assertions on the wrappers — small, and stops the recurring "fixed on the inner type only" class immediately.
3. Item 5, read-only twins first (mechanical, benchmarkable).
4. Items 1–3 as the staged overhaul.
5. Items 6–8 opportunistically; item 9 when the maintainer has decided what is public API.
