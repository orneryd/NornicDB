# NornicDB — divergent code paths that are candidates for convergence

Source: upstream main at 994b3a68, non-test Go files of 8 components. Built with graphify (code-only AST graphs, one per component, no LLM) plus a deterministic analysis of the graphs and sources (`graphify/analyze.py`). Nothing here was judged by a model; every item is a structural signal and needs a human look before acting. Not exhaustive.

## 0. Graphs

| component | graph nodes | call edges | graph |
| --- | --- | --- | --- |
| pkg/cypher | 2993 | 3970 | `graphify/out/cypher/graphify-out/graph.json` |
| pkg/storage | 2266 | 2562 | `graphify/out/storage/graphify-out/graph.json` |
| pkg/search | 1092 | 986 | `graphify/out/search/graphify-out/graph.json` |
| pkg/nornicdb | 454 | 357 | `graphify/out/nornicdb/graphify-out/graph.json` |
| pkg/server | 473 | 323 | `graphify/out/server/graphify-out/graph.json` |
| pkg/bolt | 386 | 278 | `graphify/out/bolt/graphify-out/graph.json` |
| pkg/multidb | 206 | 103 | `graphify/out/multidb/graphify-out/graph.json` |
| pkg/embed | 166 | 91 | `graphify/out/embed/graphify-out/graph.json` |

## 1. The two Cypher routers (auto-commit vs explicit transaction)

`executeWithoutTransaction` (executor_query_routing.go) dispatches to 17 functions, `executeQueryAgainstStorage` (transaction.go) to 10; 8 are shared.

- reached **only from the auto-commit router** (9): `.executeReturn()`, `.executeTopLevelUnwind()`, `.tryFastPathCompoundQuery()`, `DetectQueryPattern()`, `collectTopLevelMergeClauseBoundaries()`, `countKeywordOccurrences()`, `findKeywordIndexInContext()`, `hasSubqueryPattern()`, `isCallSubquery()`
- reached **only from the transaction router** (2): `hasRevealCall()`, `setRevealOnEngine()`

Every handler in the first list is a query shape that behaves differently inside `BEGIN … COMMIT` (the pattern behind #397→#410, #399→#459, #457). Convergence target: one router, with the transaction supplying only the storage view.

## 2. Capabilities that are forwarded by some wrappers and not by others

Method present on the inner type and on at least one wrapper of the production chain, but missing on another wrapper of the same chain. A caller that type-asserts for the capability gets it or silently falls back depending on which wrapper it happens to hold (the pattern behind #420, #424, #473).

### storage engine stack: inner `BadgerEngine` (162 exported methods), wrappers `WALEngine`, `AsyncEngine`, `NamespacedEngine`, `MemoryEngine`

| method | defined at | forwarded by | **missing in** |
| --- | --- | --- | --- |
| `ConsumeCleanShutdownMarker` | badger_maintenance_state.go:15 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `EdgeCountByPrefix` | badger_stats.go:138 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `GetNodeProjected` | badger_nodes.go:194 | NamespacedEngine | **WALEngine, AsyncEngine, MemoryEngine** |
| `GetSchemaForNamespace` | badger_schema.go:265 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `IterateNodes` | badger_stats.go:537 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `ListNamespaces` | badger_namespaces.go:9 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `MarkCleanShutdown` | badger_maintenance_state.go:54 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `NodeCountByLabelInNamespace` | badger_label_count.go:262 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `NodeCountByPrefix` | badger_stats.go:95 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `OnEdgeCreated` | badger.go:372 | AsyncEngine | **WALEngine, NamespacedEngine, MemoryEngine** |
| `OnEdgeDeleted` | badger.go:388 | AsyncEngine | **WALEngine, NamespacedEngine, MemoryEngine** |
| `OnEdgeUpdated` | badger.go:380 | AsyncEngine | **WALEngine, NamespacedEngine, MemoryEngine** |
| `OnNodeCreated` | badger.go:348 | AsyncEngine | **WALEngine, NamespacedEngine, MemoryEngine** |
| `OnNodeDeleted` | badger.go:364 | AsyncEngine | **WALEngine, NamespacedEngine, MemoryEngine** |
| `OnNodeUpdated` | badger.go:356 | AsyncEngine | **WALEngine, NamespacedEngine, MemoryEngine** |
| `PendingEmbeddingsCount` | badger_stats.go:365 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `PruneMVCCVersions` | badger_mvcc.go:2042 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `PruneTemporalHistory` | badger_temporal_index.go:653 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `RebuildMVCCHeads` | badger_mvcc.go:1516 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `RebuildTemporalIndexes` | badger_temporal_index.go:622 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |
| `StreamNodesByLabelProjected` | badger_queries.go:223 | NamespacedEngine | **WALEngine, AsyncEngine, MemoryEngine** |
| `UpdateNodeEmbedding` | badger_nodes.go:546 | WALEngine, AsyncEngine | **NamespacedEngine, MemoryEngine** |

Forwarded by no wrapper at all (12; only reachable by type-asserting down to `BadgerEngine`), data-access ones only: `Backup`, `BatchGetNodesLatestVisible`, `BeginTransaction`, `GetAccessMeta`, `GetDeindexWorkItem`, `GetEntityMeta`, `GetIndexEntryCatalog`, `GetTemporalNodeAsOfInNamespace`, `IterateLatestVisibleEdges`, `IterateLatestVisibleNodes`, `IterateMVCCHeads`, `IterateMVCCVersions`

### storage transaction: inner `BadgerEngine` (162 exported methods), wrappers `BadgerTransaction`

No partially forwarded methods.

### multidb engine wrappers: inner `NamespacedEngine` (75 exported methods), wrappers `sizeTrackingEngine`, `CompositeEngine`

| method | defined at | forwarded by | **missing in** |
| --- | --- | --- | --- |
| `AllEdges` | namespaced.go:715 | CompositeEngine | **sizeTrackingEngine** |
| `AllNodes` | namespaced.go:700 | CompositeEngine | **sizeTrackingEngine** |
| `BatchGetNodes` | namespaced.go:977 | CompositeEngine | **sizeTrackingEngine** |
| `Close` | namespaced.go:1003 | CompositeEngine | **sizeTrackingEngine** |
| `DeleteByPrefix` | namespaced.go:1281 | CompositeEngine | **sizeTrackingEngine** |
| `EdgeCount` | namespaced.go:1083 | CompositeEngine | **sizeTrackingEngine** |
| `ForEachNodeIDByLabel` | namespaced.go:453 | sizeTrackingEngine | **CompositeEngine** |
| `GetAllNodes` | namespaced.go:730 | CompositeEngine | **sizeTrackingEngine** |
| `GetEdge` | namespaced.go:375 | CompositeEngine | **sizeTrackingEngine** |
| `GetEdgeBetween` | namespaced.go:576 | CompositeEngine | **sizeTrackingEngine** |
| `GetEdgeCurrentHead` | namespaced.go:850 | sizeTrackingEngine | **CompositeEngine** |
| `GetEdgeLatestVisible` | namespaced.go:815 | sizeTrackingEngine | **CompositeEngine** |
| `GetEdgeVisibleAt` | namespaced.go:828 | sizeTrackingEngine | **CompositeEngine** |
| `GetEdgesBetween` | namespaced.go:557 | CompositeEngine | **sizeTrackingEngine** |
| `GetEdgesBetweenVisibleAt` | namespaced.go:682 | sizeTrackingEngine | **CompositeEngine** |
| `GetEdgesByType` | namespaced.go:590 | CompositeEngine | **sizeTrackingEngine** |
| `GetEdgesByTypeVisibleAt` | namespaced.go:663 | sizeTrackingEngine | **CompositeEngine** |
| `GetFirstNodeByLabel` | namespaced.go:432 | CompositeEngine | **sizeTrackingEngine** |
| `GetInDegree` | namespaced.go:745 | CompositeEngine | **sizeTrackingEngine** |
| `GetIncomingEdges` | namespaced.go:501 | CompositeEngine | **sizeTrackingEngine** |
| `GetIncomingEdgesVisibleAt` | namespaced.go:644 | sizeTrackingEngine | **CompositeEngine** |
| `GetInnerEngine` | namespaced.go:116 | sizeTrackingEngine | **CompositeEngine** |
| `GetNode` | namespaced.go:235 | CompositeEngine | **sizeTrackingEngine** |
| `GetNodeCurrentHead` | namespaced.go:841 | sizeTrackingEngine | **CompositeEngine** |
| `GetNodeLatestVisible` | namespaced.go:789 | sizeTrackingEngine | **CompositeEngine** |
| `GetNodeVisibleAt` | namespaced.go:802 | sizeTrackingEngine | **CompositeEngine** |
| `GetNodesByLabel` | namespaced.go:413 | CompositeEngine | **sizeTrackingEngine** |
| `GetNodesByLabelVisibleAt` | namespaced.go:606 | sizeTrackingEngine | **CompositeEngine** |
| `GetOutDegree` | namespaced.go:749 | CompositeEngine | **sizeTrackingEngine** |
| `GetOutgoingEdges` | namespaced.go:483 | CompositeEngine | **sizeTrackingEngine** |
| `GetOutgoingEdgesVisibleAt` | namespaced.go:625 | sizeTrackingEngine | **CompositeEngine** |
| `GetSchema` | namespaced.go:757 | CompositeEngine | **sizeTrackingEngine** |
| `GraphMutationVersion` | graph_mutation_version.go:85 | sizeTrackingEngine | **CompositeEngine** |
| `LifecycleStatus` | namespaced.go:872 | sizeTrackingEngine | **CompositeEngine** |
| `NodeCount` | namespaced.go:1013 | CompositeEngine | **sizeTrackingEngine** |
| `PauseLifecycle` | namespaced.go:892 | sizeTrackingEngine | **CompositeEngine** |
| `RegisterSnapshotReader` | namespaced.go:859 | sizeTrackingEngine | **CompositeEngine** |
| `ResumeLifecycle` | namespaced.go:900 | sizeTrackingEngine | **CompositeEngine** |
| `SetLifecycleSchedule` | namespaced.go:908 | sizeTrackingEngine | **CompositeEngine** |
| `StreamNodesByPrefix` | namespaced.go:1163 | sizeTrackingEngine | **CompositeEngine** |
| `TopLifecycleDebtKeys` | namespaced.go:917 | sizeTrackingEngine | **CompositeEngine** |
| `TriggerPruneNow` | namespaced.go:883 | sizeTrackingEngine | **CompositeEngine** |

Forwarded by no wrapper at all (11; only reachable by type-asserting down to `NamespacedEngine`), data-access ones only: `BatchGetNodesWithoutEmbeddings`, `BatchGetNodesWithoutEmbeddingsSupported`, `FindNodeNeedingEmbedding`, `GetAdjacentEdges`, `GetNodeProjected`, `GetNodeWithoutEmbeddings`, `GetTemporalNodeAsOf`, `StreamNodesByLabelProjected`, `StreamNodesByPrefixProjected`, `StreamNodesByPrefixWithoutEmbeddings`, `StreamNodesWithoutEmbeddings`

### embedder decorators: inner `VoyageEmbedder` (14 exported methods), wrappers `CachedEmbedder`, `TracedEmbedder`, `OpenAIEmbedder`, `OllamaEmbedder`, `LocalGGUFEmbedder`

| method | defined at | forwarded by | **missing in** |
| --- | --- | --- | --- |
| `EmbedBatchWithInputType` | voyage.go:171 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |
| `EmbedDocumentBatchChunks` | voyage.go:316 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |
| `EmbedDocumentChunks` | voyage.go:280 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |
| `EmbedDocumentPropertyBatchChunks` | voyage.go:235 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |
| `EmbedDocumentPropertyChunks` | voyage.go:222 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |
| `EmbedWithInputType` | voyage.go:163 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |
| `EmbeddingSpace` | voyage.go:515 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |
| `UsesDocumentProperties` | voyage.go:216 | CachedEmbedder, TracedEmbedder | **OpenAIEmbedder, OllamaEmbedder, LocalGGUFEmbedder** |

## 3. Variant families: one operation, several hand-written variants

Functions on the same receiver whose names differ only by a variant suffix (`WithContext`, `Locked`, `VisibleAt`, `InTxn`, `WithoutEmbeddings`, `Fast`, `Full`, `Batch`, …). Each variant is a place where a fix can land in one copy and not the others.

| component | receiver | operation | variants | total lines | members |
| --- | --- | --- | --- | --- | --- |
| search | `HNSWIndex` | `searchLayer` | 7 | 264 | `searchLayer` (hnsw_index.go:1229, 6L), `searchLayerHeap` (hnsw_index.go:1236, 90L), `searchLayerHeapPooled` (hnsw_index.go:1333, 4L), `searchLayerHeapPooledFromEntriesWithContext` (hnsw_index.go:1342, 114L), `searchLayerHeapPooledWithContext` (hnsw_index.go:1338, 3L), `searchLayerSingle` (hnsw_index.go:1180, 4L), `searchLayerSingleWithContext` (hnsw_index.go:1185, 43L) |
| storage | `BadgerEngine` | `getNode` | 6 | 245 | `GetNode` (badger_nodes.go:135, 53L), `GetNodeProjected` (badger_nodes.go:194, 44L), `GetNodeVisibleAt` (badger_mvcc_indexed_reads.go:9, 3L), `GetNodeWithoutEmbeddings` (badger_nodes.go:241, 53L), `getNodeVisibleAtInTxn` (badger_mvcc_indexed_reads.go:31, 78L), `getNodeVisibleAtWithView` (badger_mvcc_indexed_reads.go:13, 14L) |
| cypher | `StorageExecutor` | `execute` | 5 | 644 | `Execute` (executor.go:1247, 356L), `ExecuteOptimized` (optimized_executors.go:25, 35L), `executeInTransaction` (transaction.go:286, 86L), `executeInternal` (executor_internal.go:21, 62L), `executePipeline` (pipeline_executor.go:307, 105L) |
| storage | `AsyncEngine` | `streamNodes` | 5 | 293 | `StreamNodes` (async_engine.go:2927, 69L), `StreamNodesByPrefix` (async_engine.go:3049, 81L), `StreamNodesByPrefixProjected` (async_engine.go:3133, 48L), `StreamNodesByPrefixWithoutEmbeddings` (async_engine.go:3184, 48L), `StreamNodesWithoutEmbeddings` (async_engine.go:2999, 47L) |
| storage | `BadgerEngine` | `streamNodes` | 5 | 177 | `StreamNodes` (badger_stats.go:575, 43L), `StreamNodesByPrefix` (badger_stats.go:663, 42L), `StreamNodesByPrefixProjected` (badger_stats.go:710, 49L), `StreamNodesByPrefixWithoutEmbeddings` (badger_stats.go:762, 5L), `StreamNodesWithoutEmbeddings` (badger_stats.go:621, 38L) |
| storage | `NamespacedEngine` | `streamNodes` | 5 | 92 | `StreamNodes` (namespaced.go:1117, 27L), `StreamNodesByPrefix` (namespaced.go:1163, 15L), `StreamNodesByPrefixProjected` (namespaced.go:1182, 23L), `StreamNodesByPrefixWithoutEmbeddings` (namespaced.go:1208, 14L), `StreamNodesWithoutEmbeddings` (namespaced.go:1147, 13L) |
| storage | `WALEngine` | `streamNodes` | 5 | 59 | `StreamNodes` (wal_engine.go:1119, 19L), `StreamNodesByPrefix` (wal_engine.go:1153, 16L), `StreamNodesByPrefixProjected` (wal_engine.go:1172, 8L), `StreamNodesByPrefixWithoutEmbeddings` (wal_engine.go:1183, 8L), `StreamNodesWithoutEmbeddings` (wal_engine.go:1141, 8L) |
| storage | `BadgerEngine` | `getEdge` | 4 | 115 | `GetEdge` (badger_edges.go:150, 35L), `GetEdgeVisibleAt` (badger_mvcc.go:1067, 3L), `getEdgeVisibleAtInTxn` (badger_mvcc.go:321, 63L), `getEdgeVisibleAtWithView` (badger_mvcc.go:1071, 14L) |
| storage | `(func)` | `copyNode` | 4 | 72 | `CopyNode` (transaction.go:210, 3L), `copyNode` (transaction.go:108, 25L), `copyNodeProjectedWithoutEmbeddings` (transaction.go:165, 14L), `copyNodeWithoutEmbeddings` (transaction.go:134, 30L) |
| storage | `NamespacedEngine` | `getNode` | 4 | 51 | `GetNode` (namespaced.go:235, 13L), `GetNodeProjected` (namespaced.go:249, 16L), `GetNodeVisibleAt` (namespaced.go:802, 11L), `GetNodeWithoutEmbeddings` (namespaced.go:266, 11L) |
| cypher | `StorageExecutor` | `evaluateExpression` | 4 | 43 | `evaluateExpression` (functions.go:25, 3L), `evaluateExpressionWithContext` (functions.go:34, 3L), `evaluateExpressionWithContextFull` (functions.go:38, 34L), `evaluateExpressionWithPathContext` (functions.go:30, 3L) |
| storage | `BadgerTransaction` | `validateConstraintContracts` | 3 | 128 | `validateConstraintContracts` (constraint_contracts.go:1129, 72L), `validateConstraintContractsForEdgeLocked` (constraint_contracts.go:1278, 25L), `validateConstraintContractsForNodeLocked` (constraint_contracts.go:1246, 31L) |
| storage | `BadgerEngine` | `getNodesByLabel` | 3 | 114 | `GetNodesByLabel` (badger_queries.go:143, 77L), `GetNodesByLabelVisibleAt` (badger_mvcc_indexed_reads.go:110, 3L), `getNodesByLabelVisibleAtWithView` (badger_mvcc_indexed_reads.go:114, 34L) |
| search | `Service` | `vectorQueryNodes` | 3 | 104 | `VectorQueryNodes` (vector_query_spec.go:86, 39L), `vectorQueryNodesIndexed` (vector_query_spec.go:211, 6L), `vectorQueryNodesIndexedWithOptions` (vector_query_spec.go:218, 59L) |
| storage | `BadgerEngine` | `getOutgoingEdges` | 3 | 63 | `GetOutgoingEdges` (badger_queries.go:706, 26L), `GetOutgoingEdgesVisibleAt` (badger_mvcc.go:1086, 3L), `getOutgoingEdgesVisibleAtWithView` (badger_mvcc.go:1090, 34L) |
| storage | `BadgerEngine` | `getIncomingEdges` | 3 | 63 | `GetIncomingEdges` (badger_queries.go:896, 26L), `GetIncomingEdgesVisibleAt` (badger_mvcc.go:1125, 3L), `getIncomingEdgesVisibleAtWithView` (badger_mvcc.go:1129, 34L) |
| search | `VectorSearchPipeline` | `search` | 3 | 55 | `Search` (vector_pipeline.go:469, 4L), `searchWithExhaustion` (vector_pipeline.go:474, 3L), `searchWithExhaustionFromEntries` (vector_pipeline.go:478, 48L) |
| storage | `AsyncEngine` | `getNode` | 3 | 41 | `GetNode` (async_engine.go:1246, 17L), `GetNodeVisibleAt` (async_engine_events.go:83, 6L), `GetNodeWithoutEmbeddings` (async_engine_node_reads.go:6, 18L) |
| storage | `BadgerEngine` | `decodeNode` | 3 | 41 | `decodeNode` (badger_helpers.go:1003, 3L), `decodeNodeProjected` (badger_helpers.go:1007, 30L), `decodeNodeWithEmbeddings` (badger_helpers.go:1040, 8L) |
| search | `HNSWCandidateGen` | `searchCandidates` | 3 | 40 | `SearchCandidates` (vector_pipeline.go:226, 4L), `searchCandidatesWithExhaustion` (vector_pipeline.go:231, 3L), `searchCandidatesWithLexicalEntries` (vector_pipeline.go:235, 33L) |
| storage | `SchemaManager` | `addPropertyTypeConstraint` | 3 | 35 | `AddPropertyTypeConstraint` (schema.go:575, 7L), `AddPropertyTypeConstraintWithOptions` (schema.go:584, 3L), `addPropertyTypeConstraint` (schema.go:588, 25L) |
| search | `HNSWIndex` | `searchWithEf` | 3 | 17 | `SearchWithEf` (hnsw_index.go:608, 6L), `SearchWithEfFromEntries` (hnsw_index.go:620, 7L), `searchWithEf` (hnsw_index.go:628, 4L) |
| storage | `WALEngine` | `getNode` | 3 | 15 | `GetNode` (wal_engine.go:796, 3L), `GetNodeVisibleAt` (wal_engine.go:157, 6L), `GetNodeWithoutEmbeddings` (wal_engine.go:803, 6L) |

## 4. Parallel implementations: function pairs that call largely the same helpers

From the graphify call graph: pairs with ≥6 callees each and callee-set Jaccard ≥0.45. High overlap means two functions orchestrate the same steps; the columns show what each one does that the other does not.

| component | A | B | shared callees | Jaccard | only in A | only in B |
| --- | --- | --- | --- | --- | --- | --- |
| cypher | `.applySetToNode() (set_helpers.go:L73)` | `.applySetToNodeWithContext() (merge.go:L2322)` | 11 | 0.73 | .applySetMapMergeToNode(), .evaluateSetExpression(), .splitSetAssignments() | .evaluateSetExpressionWithContext() |
| cypher | `.tryFastPathMatchVectorCosine() (executor_match_vector_cosine_fastpath.go:L35)` | `.tryFastPathMatchWithVectorCosineProjection() (executor_match_vector_cosine_fastpath.go:L209)` | 11 | 0.61 |  | .fetchCosineNodeScoresNoIndexExactFiltered(), .parseScoreComparisonPredicate(), appendProjectedProperty(), compareScore(), scorePredicateRejectsLeadingResults(), trimOptionalDistinctPrefix() |
| cypher | `.tryFastPathMatchRelationshipVectorCosine() (executor_match_vector_cosine_fastpath.go:L496)` | `.tryFastPathMatchWithRelationshipVectorCosineProjection() (executor_match_vector_cosine_fastpath.go:L624)` | 10 | 0.67 |  | .parseScoreComparisonPredicate(), compareScore(), scorePredicateRejectsLeadingResults(), trimOptionalDistinctPrefix(), withProjectionContainsVariable() |
| cypher | `.executeCreate() (create.go:L18)` | `.executeCreateWithRefs() (create.go:L396)` | 9 | 0.64 | .validateCreatePatternPropertyMap(), containsReservedKeyword(), isValidIdentifier(), parseCreatePathAssignment() |  |
| cypher | `.tryFastPathMatchWithRelationshipVectorCosineProjection() (executor_match_vector_cosine_fastpath.go:L624)` | `.tryFastPathMatchWithVectorCosineProjection() (executor_match_vector_cosine_fastpath.go:L209)` | 11 | 0.5 | .buildRelationshipContextInto(), .fetchCosineRelationshipScores(), .parseSimpleMatchRelationshipPattern(), evaluateExpressionBoolWithContext() | .fetchCosineNodeScores(), .fetchCosineNodeScoresNoIndexExact(), .fetchCosineNodeScoresNoIndexExactFiltered(), appendProjectedProperty(), nodeProjectionPropertiesForVectorFastPath(), parseFastPathPropertyNotNullPredicate() |
| cypher | `.parseCreateConstraintForRequireDDL() (schema.go:L1503)` | `.parseCreateConstraintNodeKeyDDL() (schema.go:L1378)` | 7 | 0.78 |  | .parseNodeKeyPropertyList() |
| cypher | `.parseCreateConstraintForRequireDDL() (schema.go:L1503)` | `.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() (schema.go:L1894)` | 7 | 0.78 |  | .parseRelationshipKeyOrCompositeUniquePredicate() |
| cypher | `.parseCreateConstraintForRequireDDL() (schema.go:L1503)` | `.parseCreateConstraintSimplePropertyDDL() (schema.go:L2052)` | 7 | 0.78 |  | parseConstraintPredicate() |
| cypher | `.callDbTemporalAsOf() (call_temporal.go:L99)` | `.callDbTemporalAssertNoOverlap() (call_temporal.go:L26)` | 7 | 0.78 |  | intervalsOverlap() |
| cypher | `.parseCreateConstraintForRequireDDL() (schema.go:L1503)` | `.parseCreateConstraintTypeDDL() (schema.go:L1947)` | 7 | 0.7 |  | parseConstraintTypePredicate(), parsePropertyType() |
| cypher | `.parseCreateConstraintNodeKeyDDL() (schema.go:L1378)` | `.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() (schema.go:L1894)` | 7 | 0.7 | .parseNodeKeyPropertyList() | .parseRelationshipKeyOrCompositeUniquePredicate() |
| cypher | `.parseCreateConstraintNodeKeyDDL() (schema.go:L1378)` | `.parseCreateConstraintSimplePropertyDDL() (schema.go:L2052)` | 7 | 0.7 | .parseNodeKeyPropertyList() | parseConstraintPredicate() |
| cypher | `.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() (schema.go:L1894)` | `.parseCreateConstraintSimplePropertyDDL() (schema.go:L2052)` | 7 | 0.7 | .parseRelationshipKeyOrCompositeUniquePredicate() | parseConstraintPredicate() |
| cypher | `parseAlterDecayProfile() (knowledgepolicy_ddl.go:L767)` | `parseAlterPromotionProfile() (knowledgepolicy_ddl.go:L894)` | 6 | 0.75 | parseDecayProfileBinding() |  |
| cypher | `parseAlterPromotionPolicy() (knowledgepolicy_ddl.go:L1205)` | `parseAlterPromotionProfile() (knowledgepolicy_ddl.go:L894)` | 6 | 0.75 | parsePromotionPolicyDefinition() |  |
| cypher | `.parseCreateConstraintNodeKeyDDL() (schema.go:L1378)` | `.parseCreateConstraintTypeDDL() (schema.go:L1947)` | 7 | 0.64 | .parseNodeKeyPropertyList() | parseConstraintTypePredicate(), parsePropertyType() |
| cypher | `.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() (schema.go:L1894)` | `.parseCreateConstraintTypeDDL() (schema.go:L1947)` | 7 | 0.64 | .parseRelationshipKeyOrCompositeUniquePredicate() | parseConstraintTypePredicate(), parsePropertyType() |
| cypher | `.parseCreateConstraintSimplePropertyDDL() (schema.go:L2052)` | `.parseCreateConstraintTypeDDL() (schema.go:L1947)` | 7 | 0.64 | parseConstraintPredicate() | parseConstraintTypePredicate(), parsePropertyType() |
| cypher | `.executeMatchRelationshipsWithClause() (match_with_rel.go:L91)` | `.executeMatchWithClause() (match_with.go:L14)` | 9 | 0.47 | .evaluateExpressionFromValues(), .evaluateWhereOnComputedRow(), containsAggregateFunc(), toFloat64() | .executeMatchWithOptionalMatch(), compareForSort(), isCaseExpression(), isWhitespace(), valueToCypherLiteral() |
| cypher | `.applySetToNodeWithContext() (merge.go:L2322)` | `.applySetToRelationshipWithContext() (merge.go:L2218)` | 7 | 0.58 | containsReservedKeyword(), containsString(), isValidIdentifier(), setNodeProperty(), toStringSlice() |  |
| cypher | `parseAlterDecayProfile() (knowledgepolicy_ddl.go:L767)` | `parseAlterPromotionPolicy() (knowledgepolicy_ddl.go:L1205)` | 6 | 0.67 | parseDecayProfileBinding() | parsePromotionPolicyDefinition() |
| cypher | `.parseCreateConstraintCardinalityDDL() (schema.go:L1687)` | `.parseCreateConstraintForRequireDDL() (schema.go:L1503)` | 6 | 0.6 | parseCardinalityRequireExpr(), parseRelationshipForDirection() | parseCreateIndexForPattern() |
| cypher | `.parseCreateConstraintForRequireDDL() (schema.go:L1503)` | `.parseCreateConstraintPolicyDDL() (schema.go:L1844)` | 6 | 0.6 | parseCreateIndexForPattern() | parsePolicyForClause(), parsePolicyModeRequireExpr() |
| cypher | `.parseCreateConstraintForRequireDDL() (schema.go:L1503)` | `.parseCreateIndexDDL() (schema.go:L821)` | 6 | 0.6 | splitDDLOptionsTail() | .parseQualifiedIndexProperties(), extractIndexPropertiesSegment() |
| cypher | `parseAlterPromotionProfile() (knowledgepolicy_ddl.go:L894)` | `parseCreatePromotionProfile() (knowledgepolicy_ddl.go:L825)` | 5 | 0.71 | parseRawValue() |  |
| cypher | `.parseCreateConstraintCardinalityDDL() (schema.go:L1687)` | `.parseCreateConstraintNodeKeyDDL() (schema.go:L1378)` | 6 | 0.55 | parseCardinalityRequireExpr(), parseRelationshipForDirection() | .parseNodeKeyPropertyList(), parseCreateIndexForPattern() |
| cypher | `.parseCreateConstraintCardinalityDDL() (schema.go:L1687)` | `.parseCreateConstraintPolicyDDL() (schema.go:L1844)` | 6 | 0.55 | parseCardinalityRequireExpr(), parseRelationshipForDirection() | parsePolicyForClause(), parsePolicyModeRequireExpr() |
| cypher | `.parseCreateConstraintCardinalityDDL() (schema.go:L1687)` | `.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() (schema.go:L1894)` | 6 | 0.55 | parseCardinalityRequireExpr(), parseRelationshipForDirection() | .parseRelationshipKeyOrCompositeUniquePredicate(), parseCreateIndexForPattern() |
| cypher | `.parseCreateConstraintCardinalityDDL() (schema.go:L1687)` | `.parseCreateConstraintSimplePropertyDDL() (schema.go:L2052)` | 6 | 0.55 | parseCardinalityRequireExpr(), parseRelationshipForDirection() | parseConstraintPredicate(), parseCreateIndexForPattern() |
| cypher | `.parseCreateConstraintNodeKeyDDL() (schema.go:L1378)` | `.parseCreateConstraintPolicyDDL() (schema.go:L1844)` | 6 | 0.55 | .parseNodeKeyPropertyList(), parseCreateIndexForPattern() | parsePolicyForClause(), parsePolicyModeRequireExpr() |
| cypher | `.parseCreateConstraintNodeKeyDDL() (schema.go:L1378)` | `.parseCreateIndexDDL() (schema.go:L821)` | 6 | 0.55 | .parseNodeKeyPropertyList(), splitDDLOptionsTail() | .parseQualifiedIndexProperties(), extractIndexPropertiesSegment() |
| cypher | `.parseCreateConstraintPolicyDDL() (schema.go:L1844)` | `.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() (schema.go:L1894)` | 6 | 0.55 | parsePolicyForClause(), parsePolicyModeRequireExpr() | .parseRelationshipKeyOrCompositeUniquePredicate(), parseCreateIndexForPattern() |
| cypher | `.parseCreateConstraintPolicyDDL() (schema.go:L1844)` | `.parseCreateConstraintSimplePropertyDDL() (schema.go:L2052)` | 6 | 0.55 | parsePolicyForClause(), parsePolicyModeRequireExpr() | parseConstraintPredicate(), parseCreateIndexForPattern() |
| cypher | `.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL() (schema.go:L1894)` | `.parseCreateIndexDDL() (schema.go:L821)` | 6 | 0.55 | .parseRelationshipKeyOrCompositeUniquePredicate(), splitDDLOptionsTail() | .parseQualifiedIndexProperties(), extractIndexPropertiesSegment() |
| cypher | `.parseCreateConstraintSimplePropertyDDL() (schema.go:L2052)` | `.parseCreateIndexDDL() (schema.go:L821)` | 6 | 0.55 | parseConstraintPredicate(), splitDDLOptionsTail() | .parseQualifiedIndexProperties(), extractIndexPropertiesSegment() |
| cypher | `.parseCreateFulltextIndexDDL() (schema.go:L1122)` | `.parseCreateIndexDDL() (schema.go:L821)` | 6 | 0.55 | extractFulltextPropertiesSegment(), parseCreateFulltextForPattern() | extractIndexPropertiesSegment(), parseCreateIndexForPattern() |
| cypher | `parseAlterDecayProfile() (knowledgepolicy_ddl.go:L767)` | `parseCreatePromotionProfile() (knowledgepolicy_ddl.go:L825)` | 5 | 0.62 | parseDecayProfileBinding(), parseRawValue() |  |
| cypher | `parseAlterPromotionPolicy() (knowledgepolicy_ddl.go:L1205)` | `parseCreatePromotionProfile() (knowledgepolicy_ddl.go:L825)` | 5 | 0.62 | parsePromotionPolicyDefinition(), parseRawValue() |  |
| cypher | `.parseCreateConstraintCardinalityDDL() (schema.go:L1687)` | `.parseCreateConstraintTypeDDL() (schema.go:L1947)` | 6 | 0.5 | parseCardinalityRequireExpr(), parseRelationshipForDirection() | parseConstraintTypePredicate(), parseCreateIndexForPattern(), parsePropertyType() |
| cypher | `.parseCreateConstraintPolicyDDL() (schema.go:L1844)` | `.parseCreateConstraintTypeDDL() (schema.go:L1947)` | 6 | 0.5 | parsePolicyForClause(), parsePolicyModeRequireExpr() | parseConstraintTypePredicate(), parseCreateIndexForPattern(), parsePropertyType() |

## 5. Near-duplicate function bodies

Token-shingle comparison with identifiers and literals normalised (functions ≥90 tokens, similarity ≥0.70 or containment ≥0.85): 113 pairs, about 3934 duplicated lines, 4 pairs across components.

### Groups of 3 or more copies

1. `cypher/schema.go:1503 (StorageExecutor).parseCreateConstraintForRequireDDL [51 lines]` · `cypher/schema.go:1947 (StorageExecutor).parseCreateConstraintTypeDDL [104 lines]` · `cypher/schema.go:2052 (StorageExecutor).parseCreateConstraintSimplePropertyDDL [97 lines]`

2. `search/hnsw_build_cuda.go:184 (CudaHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` · `search/hnsw_build_gpu.go:288 (MetalHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` · `search/hnsw_build_vulkan.go:229 (VulkanHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]`

3. `cypher/pattern_parser.go:281 (StorageExecutor).splitPropertyPairs [52 lines]` · `cypher/set_helpers.go:286 (StorageExecutor).splitSetAssignments [43 lines]` · `cypher/set_helpers.go:345 (StorageExecutor).splitSetAssignmentsRespectingBrackets [43 lines]`

4. `cypher/match_index_seek.go:333 (StorageExecutor).tryCollectNodesFromPropertyIndex [46 lines]` · `cypher/match_index_seek.go:386 (StorageExecutor).tryCollectNodesFromPropertyIndexIn [49 lines]` · `cypher/match_index_seek.go:440 (StorageExecutor).tryCollectNodesFromPropertyIndexInLiteral [49 lines]`

5. `cypher/schema.go:1687 (StorageExecutor).parseCreateConstraintCardinalityDDL [70 lines]` · `cypher/schema.go:1844 (StorageExecutor).parseCreateConstraintPolicyDDL [49 lines]` · `cypher/schema.go:1894 (StorageExecutor).parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL [52 lines]`

6. `search/hnsw_build_cuda.go:153 (CudaHNSWBuildAccelerator).CandidateSearchGraph [30 lines]` · `search/hnsw_build_gpu.go:257 (MetalHNSWBuildAccelerator).CandidateSearchGraph [30 lines]` · `search/hnsw_build_vulkan.go:198 (VulkanHNSWBuildAccelerator).CandidateSearchGraph [30 lines]`

7. `storage/badger_mvcc.go:743 (BadgerEngine).loadNodeMVCCHeadInTxn [23 lines]` · `storage/badger_mvcc.go:804 (BadgerEngine).loadNodeMVCCRecordExactInTxn [23 lines]` · `storage/badger_mvcc.go:828 (BadgerEngine).loadEdgeMVCCRecordExactInTxn [23 lines]`

8. `storage/composite_engine.go:692 (CompositeEngine).GetOutgoingEdges [27 lines]` · `storage/composite_engine.go:722 (CompositeEngine).GetIncomingEdges [27 lines]` · `storage/composite_engine.go:801 (CompositeEngine).GetEdgesByType [27 lines]`

9. `cypher/knowledgepolicy_ddl.go:1252 parseDropPromotionPolicy [19 lines]` · `cypher/knowledgepolicy_ddl.go:805 parseDropDecayProfile [19 lines]` · `cypher/knowledgepolicy_ddl.go:924 parseDropPromotionProfile [19 lines]`

10. `server/server_retention.go:15 (Server).registerRetentionRoutes [11 lines]` · `server/server_router.go:156 (Server).registerNornicDBRoutes [25 lines]` · `server/server_router.go:182 (Server).registerAdminRoutes [18 lines]`

11. `storage/edge_meta.go:218 (EdgeMetaStore).GetBySignalType [14 lines]` · `storage/edge_meta.go:267 (EdgeMetaStore).GetByOrigin [14 lines]` · `storage/edge_meta.go:283 (EdgeMetaStore).GetBySession [14 lines]`

### Largest pairs

| A | B | similarity | containment | lines |
| --- | --- | --- | --- | --- |
| `search/hnsw_build_cuda.go:52 (CudaHNSWBuildAccelerator).candidateSearch [100 lines]` | `search/hnsw_build_gpu.go:156 (MetalHNSWBuildAccelerator).candidateSearch [100 lines]` | 0.96 | 0.96 | 100 |
| `search/hnsw_index.go:270 (HNSWIndex).Add [128 lines]` | `search/hnsw_index.go:399 (HNSWIndex).addWithLevel0Candidates [128 lines]` | 0.75 | 0.87 | 128 |
| `cypher/schema.go:1947 (StorageExecutor).parseCreateConstraintTypeDDL [104 lines]` | `cypher/schema.go:2052 (StorageExecutor).parseCreateConstraintSimplePropertyDDL [97 lines]` | 0.79 | 0.88 | 97 |
| `cypher/call_apoc_path.go:767 (StorageExecutor).bfsSpanningTree [101 lines]` | `cypher/call_apoc_path.go:870 (StorageExecutor).dfsSpanningTree [104 lines]` | 0.75 | 0.86 | 101 |
| `cypher/match_index_seek.go:20 (StorageExecutor).tryCollectNodesFromIDEquality [94 lines]` | `cypher/match_index_seek.go:124 (StorageExecutor).tryCollectNodesFromIDEqualityParam [109 lines]` | 0.77 | 0.89 | 94 |
| `storage/badger_mvcc.go:1175 (BadgerEngine).iterateNodesVisibleAtInTxn [86 lines]` | `storage/badger_mvcc.go:1262 (BadgerEngine).iterateEdgesVisibleAtInTxn [86 lines]` | 0.79 | 0.85 | 86 |
| `storage/async_engine.go:2927 (AsyncEngine).StreamNodes [69 lines]` | `storage/async_engine.go:3234 (AsyncEngine).StreamEdges [68 lines]` | 0.95 | 0.95 | 68 |
| `search/hnsw_build_cuda.go:184 (CudaHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` | `search/hnsw_build_gpu.go:288 (MetalHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` | 0.92 | 0.92 | 63 |
| `search/hnsw_build_cuda.go:184 (CudaHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` | `search/hnsw_build_vulkan.go:229 (VulkanHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` | 0.92 | 0.92 | 63 |
| `search/hnsw_build_gpu.go:288 (MetalHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` | `search/hnsw_build_vulkan.go:229 (VulkanHNSWBuildAccelerator).candidateSearchGraphGroup [63 lines]` | 0.92 | 0.92 | 63 |
| `storage/badger_mvcc.go:1539 (BadgerEngine).rebuildNodeMVCCHeadsFromVersions [59 lines]` | `storage/badger_mvcc.go:1599 (BadgerEngine).rebuildEdgeMVCCHeadsFromVersions [59 lines]` | 0.89 | 0.89 | 59 |
| `storage/async_engine.go:2087 (AsyncEngine).NodeCountByPrefix [66 lines]` | `storage/async_engine.go:2177 (AsyncEngine).EdgeCountByPrefix [58 lines]` | 0.83 | 0.9 | 58 |
| `cypher/optimized_executors.go:138 (StorageExecutor).executeIncomingCountOptimized [61 lines]` | `cypher/optimized_executors.go:206 (StorageExecutor).executeOutgoingCountOptimized [61 lines]` | 0.78 | 0.78 | 61 |
| `storage/badger_mvcc.go:1681 (BadgerEngine).withViewNodeMVCCVersionsFromKey [51 lines]` | `storage/badger_mvcc.go:1733 (BadgerEngine).withViewEdgeMVCCVersionsFromKey [51 lines]` | 0.91 | 0.91 | 51 |
| `cypher/call_compat.go:949 (StorageExecutor).callDbCreateSetNodeVectorProperty [65 lines]` | `cypher/call_compat.go:1017 (StorageExecutor).callDbCreateSetRelationshipVectorProperty [65 lines]` | 0.71 | 0.76 | 65 |
| `cypher/schema.go:821 (StorageExecutor).parseCreateIndexDDL [65 lines]` | `cypher/schema.go:1122 (StorageExecutor).parseCreateFulltextIndexDDL [67 lines]` | 0.71 | 0.82 | 65 |
| `cypher/set_helpers.go:286 (StorageExecutor).splitSetAssignments [43 lines]` | `cypher/set_helpers.go:345 (StorageExecutor).splitSetAssignmentsRespectingBrackets [43 lines]` | 0.99 | 0.99 | 43 |
| `storage/id_dictionary.go:508 (idDictionary).resolveOrAllocateNodeNumIDInTxn [57 lines]` | `storage/id_dictionary.go:577 (idDictionary).resolveOrAllocateEdgeNumIDInTxn [45 lines]` | 0.91 | 0.91 | 45 |
| `cypher/match_index_seek.go:386 (StorageExecutor).tryCollectNodesFromPropertyIndexIn [49 lines]` | `cypher/match_index_seek.go:440 (StorageExecutor).tryCollectNodesFromPropertyIndexInLiteral [49 lines]` | 0.8 | 0.84 | 49 |
| `storage/async_engine.go:1680 (AsyncEngine).GetOutgoingEdges [54 lines]` | `storage/async_engine.go:1735 (AsyncEngine).GetIncomingEdges [46 lines]` | 0.85 | 0.85 | 46 |
| `cypher/schema.go:1844 (StorageExecutor).parseCreateConstraintPolicyDDL [49 lines]` | `cypher/schema.go:1894 (StorageExecutor).parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL [52 lines]` | 0.75 | 0.83 | 49 |
| `storage/badger_decay_filter.go:56 (BadgerEngine).filterNodeByDecay [49 lines]` | `storage/badger_decay_filter.go:107 (BadgerEngine).filterEdgeByDecay [49 lines]` | 0.7 | 0.81 | 49 |
| `cypher/call_apoc_helpers.go:15 (StorageExecutor).findMatchingParen [35 lines]` | `cypher/call_apoc_helpers.go:175 (StorageExecutor).findMatchingBrace [35 lines]` | 0.97 | 0.97 | 35 |
| `cypher/match_index_seek.go:333 (StorageExecutor).tryCollectNodesFromPropertyIndex [46 lines]` | `cypher/match_index_seek.go:440 (StorageExecutor).tryCollectNodesFromPropertyIndexInLiteral [49 lines]` | 0.72 | 0.79 | 46 |
| `cypher/call_shared_utils.go:12 splitTopLevelComma [44 lines]` | `cypher/set_merge_strict.go:54 splitTopLevelCommaKeepEmpty [42 lines]` | 0.77 | 0.92 | 42 |
| `storage/badger_transaction.go:1581 (BadgerTransaction).mergePendingNodesLocked [35 lines]` | `storage/badger_transaction.go:1617 (BadgerTransaction).mergePendingEdgesLocked [35 lines]` | 0.89 | 0.89 | 35 |
| `storage/constraint_contracts.go:1095 countMatchingPatternEdgesEngine [33 lines]` | `storage/constraint_contracts.go:1367 (BadgerTransaction).countMatchingPatternEdgesLocked [33 lines]` | 0.94 | 0.94 | 33 |
| `storage/loader.go:367 loadNodesFromReader [37 lines]` | `storage/loader.go:420 loadRelationshipsFromReader [36 lines]` | 0.83 | 0.83 | 36 |
| `storage/badger_mvcc.go:1811 (BadgerEngine).collectNodeBootstrapBatch [40 lines]` | `storage/badger_mvcc.go:1896 (BadgerEngine).collectEdgeBootstrapBatch [44 lines]` | 0.73 | 0.83 | 40 |
| `storage/badger_edge_between_index.go:85 (BadgerEngine).startEdgeBetweenIndexBackfill [40 lines]` | `storage/badger_label_index_backfill.go:120 (BadgerEngine).startLabelIndexBackfill [38 lines]` | 0.76 | 0.85 | 38 |
| `storage/badger_mvcc.go:852 (BadgerEngine).loadNodeMVCCRecordAtOrBeforeInTxn [31 lines]` | `storage/badger_mvcc.go:884 (BadgerEngine).loadEdgeMVCCRecordAtOrBeforeInTxn [31 lines]` | 0.93 | 0.93 | 31 |
| `search/hnsw_build_cuda.go:153 (CudaHNSWBuildAccelerator).CandidateSearchGraph [30 lines]` | `search/hnsw_build_gpu.go:257 (MetalHNSWBuildAccelerator).CandidateSearchGraph [30 lines]` | 0.96 | 0.96 | 30 |
| `storage/badger_cache.go:163 estimateCacheValueBytes [30 lines]` | `search/search.go:687 estimateSearchCacheValueBytes [30 lines]` | 0.96 | 0.96 | 30 |
| `storage/badger_mvcc.go:1966 (BadgerEngine).bootstrapNodeMVCCFromCurrentStateInTxn [37 lines]` | `storage/badger_mvcc.go:2004 (BadgerEngine).bootstrapEdgeMVCCFromCurrentStateInTxn [37 lines]` | 0.77 | 0.81 | 37 |
| `cypher/schema.go:1687 (StorageExecutor).parseCreateConstraintCardinalityDDL [70 lines]` | `cypher/schema.go:1844 (StorageExecutor).parseCreateConstraintPolicyDDL [49 lines]` | 0.58 | 0.85 | 49 |
| `cypher/schema.go:1503 (StorageExecutor).parseCreateConstraintForRequireDDL [51 lines]` | `cypher/schema.go:2052 (StorageExecutor).parseCreateConstraintSimplePropertyDDL [97 lines]` | 0.55 | 0.93 | 51 |
| `cypher/pattern_parser.go:281 (StorageExecutor).splitPropertyPairs [52 lines]` | `cypher/set_helpers.go:286 (StorageExecutor).splitSetAssignments [43 lines]` | 0.65 | 0.86 | 43 |
| `cypher/pattern_parser.go:281 (StorageExecutor).splitPropertyPairs [52 lines]` | `cypher/set_helpers.go:345 (StorageExecutor).splitSetAssignmentsRespectingBrackets [43 lines]` | 0.65 | 0.86 | 43 |
| `storage/constraint_contracts.go:402 evaluateRelationshipConstraintContractExpressionEngine [34 lines]` | `storage/constraint_contracts.go:1336 (BadgerTransaction).evaluateRelationshipConstraintContractExpressionLocked [30 lines]` | 0.93 | 0.93 | 30 |
| `storage/wal_engine.go:952 (WALEngine).NodeCountByPrefix [29 lines]` | `storage/wal_engine.go:1016 (WALEngine).EdgeCountByPrefix [29 lines]` | 0.95 | 0.95 | 29 |
| `cypher/call_compat.go:733 (StorageExecutor).callDbIndexFulltextCreateNodeIndex [39 lines]` | `cypher/call_compat.go:775 (StorageExecutor).callDbIndexFulltextCreateRelationshipIndex [39 lines]` | 0.7 | 0.73 | 39 |
| `cypher/schema.go:1503 (StorageExecutor).parseCreateConstraintForRequireDDL [51 lines]` | `cypher/schema.go:1947 (StorageExecutor).parseCreateConstraintTypeDDL [104 lines]` | 0.53 | 0.91 | 51 |
| `cypher/pattern_parser.go:510 (StorageExecutor).evaluateScalarPropertyExpressionFast [41 lines]` | `cypher/unwind_multi_match_create.go:451 (StorageExecutor).evaluateBatchArithmeticLookupExpr [38 lines]` | 0.7 | 0.86 | 38 |
| `cypher/typed_results.go:148 decodeMap [44 lines]` | `nornicdb/db_admin.go:363 decodeMapToStruct [37 lines]` | 0.71 | 0.83 | 37 |
| `storage/badger_mvcc.go:998 (BadgerEngine).GetNodeLatestVisible [35 lines]` | `storage/badger_mvcc.go:1034 (BadgerEngine).GetEdgeLatestVisible [32 lines]` | 0.81 | 0.81 | 32 |
