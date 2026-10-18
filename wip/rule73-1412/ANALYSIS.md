
# Rule 73 family analysis (gate, 18 Oct ~12:30Z; head 7f42f0ffa; no code changed since the gate; WIP parked in `git stash` "rule73-cc2-holder-wip")

**Family contract:** a statement's charge equals the memory it holds live, for every structure that keeps nodes, relationships or their IDs read from storage. Each is charged once, for as long as the structure keeps them, and released when it stops. Over the limit the statement fails with Neo.TransientError.General.TransactionMemoryLimit; pools return to zero; nothing costs anything when no limit is set.

## 1. Family inventory (mechanical)
Full inventory, tab-separated, 15,132 lines: `sig/handoff/fix145-1412-inventory.tsv` (records S/G/R/V/P/L/C/E). Reachability, read-to-holder feeds and duplicates: `scratch1343/graphify-reach.tsv`.

**Commands**
1. Typed extractor: go/types over pkg/cypher with the source importer; probe `scratch1343/rule73-probes/zz_rule73_inventory_test.go`, run on TrueNAS:
   `bash sig/tnrun.sh go wt-fmem1412-test --allow-dirty --shards 1 -- test -count=1 -v -run '^TestZZRule73Inventory$' ./pkg/cypher`
   Jobs i20261018111055-53348-21ec (first pass) and i20261018120144-79724-6d4a (with typed use edges). A type "holds entities" if it is or contains (slice, array, map, chan or struct field, at any depth) storage.Node, Edge, NodeID or EdgeID.
2. graphify (local shared container, own root). Repo copy `C:/Users/mm0ns/Desktop/codex/builds/repos/nornic-wt-fmem1412` (worktree at 7f42f0ffa):
   `docker exec graphify graphify extract /repos/nornic-wt-fmem1412/pkg/cypher --out /graphs/nornic-wt-fmem1412/cypher --code-only --no-cluster --max-workers 2` (19,226 nodes, 74,830 edges), and the same for pkg/storage (7,708 nodes, 35,657 edges);
   `docker exec -i graphify python - /graphs/nornic-wt-fmem1412/cypher/graphify-out/graph.json /repos/nornic-wt-fmem1412/zz-inventory.tsv < scratch1343/rule73-probes/graphify_reach.py`.
   graphify's call graph alone is incomplete: 2,579 of the 13,764 typed call/use edges are missing from it. Examples: `pipeline_executor.go:1939` calling `collectNodesWithStreamingProjection`; `executeInternal` reaches only parsing helpers. It also resolves interface calls by name onto test engines. Reachability therefore runs over the **union** of graphify's 12,138 call pairs and the typed edges (176 graphify-only pairs kept). Entries: `StorageExecutor.Execute`, `executeInTransaction`, `executeInternal`, and init/registration functions.

**Counts (head 7f42f0ffa)**
| Record | What | Count |
|---|---|---|
| S | struct fields whose type holds entities | 96 (about 40 are statement, transaction or process state, not entity data, such as `*StorageExecutor` back-pointers) |
| G | package vars | 0 |
| C | context values holding entities | 11; 2 hold entity data: the batch prefetch maps (unwind_multi_match_create.go:176, unwind_relationship_merge_batch.go:87) |
| R | storage reads returning entities in bulk | 213 call sites: 67 wrapped by held*, in 42 functions. The rest are 58 GetSchema-type metadata reads, 36 reviewed one-node reads, transactionStorageWrapper delegation, and functions that run outside statements |
| V | storage visitors | 17 |
| P | functions returning entity containers | 241 (141 read storage transitively) |
| L | locals holding entities in functions that read storage | 790: 140 assigned from a storage read, 300 from a call, **272 built** (sets, maps, filtered lists, working state derived from reads), 78 declared |
| E | typed use edges (for reachability) | 13,764 |
| REACH | inventory functions reachable from a statement entry | 368 reachable, 94 not |

**Structures that keep entities beyond one read (S and C), with lifetime and review status**
| Member (file:line) | Lifetime | Charged at head? | Reviewed before? |
|---|---|---|---|
| MATCH candidate caches (`pipelineNodeMatchSourceWithHint` caches, pipeline_row_stream.go; `pipelineApplyInitialNodeMatch` candidateCache, pipeline_executor.go) | statement | yes (owner, PR commit 1) | F-MEM final review |
| `relationshipMergeIdentityCache.entries/byKey` (relationship_merge_identity_cache.go:33,39) | one MERGE clause | yes since f5f13bad8 | r1 finding 1 |
| batch prefetch maps in ctx (unwind_multi_match_create.go:176, unwind_relationship_merge_batch.go:87) | one batch | yes since f5f13bad8 | r1 finding 4 |
| `indexedOrderHeap.nodes` (match_index_order_groups.go:41) | one seek | the window yes; the heap no (bounded by LIMIT) | listed as a known limit, never reviewed |
| `pathSearch.edges` adjacency cache, `products`, `index`, `distance`, `ends`, `nogoods`, `dominators` (path_selector_search_run.go:107-125); `pathSearchWalks.*` (path_selector_search_pair.go:54-57); `pathSearchWalkSide.reached/more/frontier/next` (path_selector_search_walk.go:37-40) | one path search | **its own accountant** (`pathSearch.retain` into `s.memory.reserveBytes`: 8 B per relationship, `searchEntryBytes` = 48) | **no F-MEM round looked at it** |
| `TraversalContext.usedEdges/paths/nodeCache` (traversal.go:48-50) | one traversal | paths yes (`reservePath`); nodeCache and usedEdges **no** | not looked at |
| `quantifiedPathState.nodes/relationships/used` (quantified_path_pattern.go:278-280) | one QPP expansion | its own retention (pre-existing) | not looked at |
| `apocExpansion.starts/allowNodes/denyNodes/terminatorNodes/endNodes` (call_apoc_path.go:83-92) | one procedure call | **no**; the starts come from arguments, the allow/deny/terminator sets are built from arguments | not looked at (listed only as "APOC path inputs") |
| `gdsNeighborhood.nodes` (gds_functions.go:162), `linkPredictionConfig.TargetNodes` (linkprediction.go:490) | one function call | **no** | not looked at |
| `deletedEntities.nodes/edges`, `deletedEntityViews.*` (deleted_entity_projection.go:17-18, 96-98): ID sets of everything a statement deleted | statement | **no** | not looked at |
| `statementPropertyViews.nodes/edges/nodeBodies/edgeBodies` (statement_property_views.go:15-19): a detached snapshot of every entity a read statement projects, when property decay is enabled | statement | **no** | not looked at |
| `transactionStorageWrapper.txNodeLookupCache` (transaction_storage_wrapper.go:115) | explicit transaction, across statements | **no**; the per-transaction limit resets at each statement (`end()`), so no statement can own it | not looked at (the ADMIT put "tx state" out of route scope) |
| `StorageExecutor.nodeLookupCache` (executor.go:368; capped at 10,000 at cache.go:1181) | process | n/a (bounded process cache) | not looked at |
| `createOutcome/createPlan` (create.go:20-33) | one CREATE | bounded by the pattern | n/a |
| `PathResult`, `PathContext`, `traversalOptRow` (traversal.go:32-33, 2251-2256; optional_match_traversal.go:31-32) | rows and paths | row accounting / `reservePath` | F-MEM r1 |
| `vectorNodeScoreHeap.scores` (executor_match_vector_cosine_fastpath.go:766) | one search | top-k, bounded | n/a |
| `heldNodeList.nodes` (transaction_memory_retention.go:427) | the accountant's own | yes | n/a |

**Derived structures built from reads (L "built": 272 in 54 functions).** At head the read is charged; what is built from it never is:
- fast relationship aggregations: `tryFastChainedRelationshipAggregation` 21, `tryFastSingleHopAgg` 10, `tryFastRevenueByProduct` 5, the optimized count executors 3 each;
- graph algorithms: `computeBetweenness` 7, `computePageRank`, `computeCloseness`, `computeLouvain`, `computeWCC`, `dijkstra`, `astar`, `findAllSimplePaths`, `findPaths`, `getNeighborsByhop`;
- `linkPredictionGraph` 6, `runApocExpansion` 4, `runSearchRequest` 3;
- the index seeks' ID sets: `lookupPatternCandidatesUsingPropertyIndex`, `collectNodesFromIndexedProperty`, `tryCollectNodesFromPropertyIndex*` (2 each);
- `pipelineApplyMerge` 4, `pathSearch.relationships/bindWalks/uniformPair`, `applyDeleteReturnProjection` 3, `projectVisibleQueryValue` 3.

**Not reachable from any statement entry: dead code at head.** Checked on the typed + graphify union and confirmed by grepping for non-test callers:
- `tryExecuteDeleteWithWithLimitHotPath` and `collectDeleteWithLimitCandidates`;
- `tryExecuteBoundRelationshipDelete(WithLimit)`, `collectBoundRelationshipDeleteSources`, `boundRelationshipDeleteCandidateEdges`;
- `ExecuteOptimized` and its four `execute*Optimized` executors;
- `evaluateRelationshipPatternInWhere` and `traverseChain`;
- `lookupRelationshipBatchExistingEdge`, `ApplyIndexHint`, `seedNodesFromOuterMatch`, `evaluateMatchClauseNodes`, `tryFastRevenueByProduct`.

Earlier rounds converted several of these (the DELETE "known limit" and the subquery seeds) although no statement runs them.

**Duplicate implementations (same data, separate routes; DUP lines).** AllNodes is read by 15 functions, AllEdges by 16, GetNodesByLabel by 14, GetOutgoingEdges by 16, GetIncomingEdges by 13, GetEdgesByType by 3, BatchGetNodes by 2, MatchEdgesBetween by 2 (`findRelationshipsForMerge` and `relationshipMergeIdentityCache.relationships`), StreamNodesByLabelProjected by 4.

**The same sizing decision is made in several places:**
- `entityReferenceBytes` = 64 (transaction_memory_estimate.go:23), used by held*, reserveCandidates and the row estimator;
- `pathSearch.retain`: 8 B per relationship plus `searchEntryBytes` = 48 (path_selector_search_run.go:240, :846);
- ad hoc `reserveSlots(n, entityReferenceBytes)` at relationship_merge_identity_cache.go:84, subquery_value_expression.go:739, unwind_relationship_merge_batch.go:430 and :507.

Engine-shaped duplicates: `systemGraphView` (system_procedure_scope.go:51-93) and `transactionStorageWrapper` (transaction_storage_wrapper.go:359-957) both implement the read API. The wrapper's reads, which merge pending writes, are exempt from the guard wholesale.

## 3. Cause: why each earlier fix left a new hole

Every round attached the charge to **a place in the code**: the operator reviewers named, then the storage read call. It was never attached to **the structure that keeps the entities**, and nothing ever enumerated the structures. Per attempt:

| Attempt | What it charged | What it missed | Mechanism that let it drift |
|---|---|---|---|
| #1368 / #1381 (operator reservations) | rows and operator state at the operators reviewers listed | each review found the next operator (r1: 8, rebase: semi-joins, final: comprehension payloads and MATCH caches) | per-operator decisions; no enumeration; growRows walked rows rather than following owners |
| #1412 commit 1 (builders and candidate collectors) | comprehension, UNWIND, reduce and collect payloads; MATCH candidates via an owner parameter | six "known limits" | the same per-site approach, applied to more sites |
| #1412 course change 1 (held* at the storage read, name-derived guard) | every bulk *read call* | (a) structures that keep entities **without being a read's direct result**: caches (MERGE identity cache), prefetch maps in ctx, the 272 derived structures (ID sets, maps, algorithm and aggregation state); (b) lifetime: the owner was an additive bag released all at once, so a loop's lists accumulated (type seeds) and a reader's owner died before the map it filled (prefetch); (c) two reads of what ends up as one structure were charged twice (committed IDs plus the merged list) | the guard matched **method names** of storage reads, so it could only see read sites; the exemption lists (adjacency, visitor, kept-across-iterations) were written by hand, and the MERGE cache got in through one of them |
| r1 point fixes f5f13bad8 (superseded) | the four named sites | everything in the unreviewed rows of the table above | per-finding patches: Rule 73 trigger |

The split owner, named:
1. **No owner for the structure.** Five mechanisms charge entities with separate sizing and lifetimes:
   - `memoryRetention` reservations: an additive owner, released all at once;
   - `held*` read wrappers (64 B per entity);
   - `pathSearch.retain` (its own retention, 8 B per relationship);
   - the row estimator (entities in rows, 64 B);
   - `expressionBudget`.

   A member drifts depending on which mechanism its author happened to use: path search under-charges 8 times, the derived structures use none.
2. **The inventory was always hand-made.** The reviewers' findings and the "known limits" lists drove the work, never the code. Unreachable functions got fixed while live members (path search state, traversal nodeCache, deleted-entity sets, APOC expansion sets, property views, transaction caches) were never looked at.
3. **Exemption by judgment.** "One node's relationships are a cursor" was a reviewed list. The degree count shows one node's 30,000 relationships materialized and uncharged (probe below), and the MERGE cache was hidden behind the same label.

## 4. Plan: one owner per structure, from the structure's type
- **One owner: `entityHolder`, bound to the structure.**
  - Every value whose type holds entities in bulk (the extractor's type predicate) and that lives past the expression that made it has exactly one holder, declared with it: a field beside a container field in a struct (caches, prefetch, path search and traversal state, deleted-entity sets, property views); a local beside a built set or map; the caller's holder for a returned list.
  - The holder's charge is *what the structure holds now*: `set` on read or replace, `grow` on insert, `drop` once when the structure is discarded. It is never additive across replaced lists, never released before its structure, and never charged twice for one structure.
- **One sizing function** (`entityBytes(n)`, entity reference = 64 B, as the row estimator) replaces `reserveCandidates`, the entity uses of `reserveSlots(n, entityReferenceBytes)`, `heldMergedIDs`, `holdAppended`, and `pathSearch.retain`'s per-relationship 8 B for entity containers. Path search's non-entity bookkeeping (search entries) stays in its retention.
- **What it replaces:** the held* methods on `memoryRetention`; `heldNodeList`'s owner; `memoryRetention.reserveCandidates`, `holdAppended`, `heldMergedIDs`; the hand lists `nodeAdjacencySites`, `entityVisitorSites` (a visitor that keeps what it sees holds it in a holder; one that keeps nothing needs no list) and `entityReadsKeptAcrossIterations` (set semantics make them unnecessary).
- **One-node reads:** no more exemption list. A one-node relationship list is a structure like any other and is held while it lives (a holder `set` per read, so a per-node loop costs one list, not the sum). That closes the degree count and the existence checks.
- **Transaction-lifetime state** (`txNodeLookupCache`) and the process cache (`nodeLookupCache`, capped): the per-transaction limit resets every statement, so no statement can own them. Proposed: leave both as they are, documented. The process cache is bounded at 10,000; the transaction cache is bounded by the transaction's own writes. **Scope question for the orchestrator**: charging transaction state would change what the per-transaction limit means (it is per statement today, as for Neo4j query memory).
- **Dead code** (the list above): leave it unconverted and delete nothing in a bugfix PR; the guard skips unreachable functions using the same reachability. Or ask the maintainer to remove it separately.
- **Enforcement by construction, from types, not names:** the extractor's go/types predicate becomes the guard test (`TestEntityHoldersOwnTheirStructures`):
  - (i) every struct type with a field that holds entities in bulk has an `entityHolder` field, or is in a reviewed list of row/path types charged by the row estimator;
  - (ii) every local of such a type assigned from a storage read, a producer call or a built literal, in a function reachable from a statement entry, has a holder bound to it in the same function, or is returned into a caller's holder;
  - (iii) every context value of such a type is held by the function that stores it;
  - (iv) each holder binds one structure (one variable or field);
  - (v) a checked-in inventory (`testdata/entity_holders.tsv`, the extractor's S/C/L output for reachable functions) must equal the extraction, so a new structure shows up as a diff.
- **Verification across the whole inventory through public paths:**
  - the self-attack matrix below becomes a regression test, extended to every S/C member and to each function group with derived structures (fast aggregations, algorithms, link prediction, APOC expansion, path search, QPP, deleted-entity sets, property views with decay enabled);
  - each member runs over-limit (fails with the limit's error), near-limit (about 1.6 times its live entities: succeeds with the no-limit rows), and checks that the pools return to zero;
  - a holder observer records which reachable inventory functions charged during the matrix; every reachable function with an inventory structure must appear, or the test fails naming it;
  - both parsers with -race; then the full ./pkg/cypher on both parsers and the paired A/B, with any remaining cost stated plainly in the PR (no 5% gate).
