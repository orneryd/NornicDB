# PR #1525 duplicate-check follow-up

This inventory records all findings from the enabled checker on staged tree
`270b2341200f51809ffc962f24cca61b028c12e1`, with PR head
`31bfb2bddd405f6b3d50d24d415f729363a2d396` and the release graph ingested at
`a9265090a5d7bd8e34932cf8f504765c01aa147e`. The checker analyzed 1,234
functions without candidate-cap skips and reported 83 reuse candidates and
2 refactor targets. Its graph-baseline warning noted the graph was ingested at
the release commit rather than the PR head used as its comparison base.

At the maintainer's direction these findings are recorded for follow-up after
a passing `make test` checkpoint is pushed. They remain open until corrected
and verified against the then-current staged tree. Later source changes are
not covered by this run.

| # | Kind | Candidate | Matched owner(s) |
| --- | --- | --- | --- |
| 1 | refactor | `pkg/cypher/executor_match_vector_cosine_fastpath_test.go` — `TestMatchVectorCosineFastPath_DoesNotReplaceMismatchedSearchService()` | `pkg/cypher/executor_match_vector_cosine_fastpath_test.go` — `TestMatchVectorCosineFastPath_AscendingOrderWithParamVector()`<br>`pkg/cypher/executor_match_vector_cosine_fastpath_test.go` — `TestMatchVectorCosineFastPath_DoesNotCreateSearchServiceWhenUnwired()` |
| 2 | refactor | `pkg/search/build_settings_test.go` — `TestComposeRoutingBuildSettings_DoesNotEncodeSeedMode()` | `pkg/search/build_settings_test.go` — `TestComposeStrategyBuildSettings_CompressedIncludesFingerprint()`<br>`pkg/search/build_settings_test.go` — `TestComposeRoutingBuildSettings_ClampsAndDefaults()` |
| 3 | reuse | `pkg/search/fulltext_index_v2_persist.go` — `.Save()` | `pkg/search/fulltext_index_v2_persist.go` — `.Save()` |
| 4 | reuse | `pkg/search/fulltext_index_v2_persist.go` — `.SaveNoCopy()` | `pkg/search/fulltext_index_v2_persist.go` — `.SaveNoCopy()` |
| 5 | reuse | `pkg/search/fulltext_index_v2_persist.go` — `.applyV2Snapshot()` | `pkg/search/fulltext_index_v2_persist.go` — `.applyV2Snapshot()` |
| 6 | reuse | `pkg/search/search_test.go` — `TestSearchServiceSearchDirect()` | `pkg/search/search_test.go` — `TestRRFHybridSearchDirect()` |
| 7 | reuse | `pkg/search/search_test.go` — `TestRRFHybridSearchDirect()` | `pkg/search/search_test.go` — `TestSearchServiceSearchDirect()` |
| 8 | reuse | `pkg/cypher/retrieve_vector_filter_test.go` — `TestDbRetrieveFiltersReachPastNearerRejectedVectors()` | `pkg/cypher/retrieve_where_test.go` — `TestDbRetrieveWhereReachesPastNearerRejectedVectors()` |
| 9 | reuse | `pkg/cypher/retrieve_where_test.go` — `TestDbRetrieveWhereReachesPastNearerRejectedVectors()` | `pkg/cypher/retrieve_vector_filter_test.go` — `TestDbRetrieveFiltersReachPastNearerRejectedVectors()` |
| 10 | reuse | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationRankedThenIDShortPrefixCanStillDeepen()` | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationNonGrowingExpansionGetsSecondChance()` |
| 11 | reuse | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationNonGrowingExpansionGetsSecondChance()` | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationRankedThenIDShortPrefixCanStillDeepen()` |
| 12 | reuse | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationIDModeInvalidatesAfterMutation()` | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationIDModeInvalidatesAfterPolicyChange()` |
| 13 | reuse | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationIDModeInvalidatesAfterPolicyChange()` | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationIDModeInvalidatesAfterMutation()` |
| 14 | reuse | `pkg/search/vector_pipeline_test.go` — `TestVectorSearchCandidates_AutoStrategyDefaultsToHNSW()` | `pkg/search/vector_pipeline_test.go` — `TestVectorSearchCandidates_CPUBruteForceThresholdOptIn()` |
| 15 | reuse | `pkg/search/vector_pipeline_test.go` — `TestVectorSearchCandidates_CPUBruteForceThresholdOptIn()` | `pkg/search/vector_pipeline_test.go` — `TestVectorSearchCandidates_AutoStrategyDefaultsToHNSW()` |
| 16 | reuse | `pkg/search/filter_regression_test.go` — `TestFilterByTypeAndProperties_PropertyFilter_ScalarMatch()` | `pkg/search/filter_regression_test.go` — `TestFilterByTypeAndProperties_CombinedTypeAndProperty()` |
| 17 | reuse | `pkg/search/filter_regression_test.go` — `TestFilterByTypeAndProperties_CombinedTypeAndProperty()` | `pkg/search/filter_regression_test.go` — `TestFilterByTypeAndProperties_PropertyFilter_ScalarMatch()` |
| 18 | reuse | `pkg/search/search_metrics_test.go` — `TestSearch_AttachMetricsIdempotent()` | `pkg/search/search_metrics_test.go` — `TestSearch_AttachMetricsNilClearsBindings()` |
| 19 | reuse | `pkg/search/build_triage_regression_test.go` — `newTriageService()` | `pkg/search/failed_population_persistence_test.go` — `populationPersistService()` |
| 20 | reuse | `pkg/search/failed_population_persistence_test.go` — `populationPersistService()` | `pkg/search/build_triage_regression_test.go` — `newTriageService()` |
| 21 | reuse | `pkg/search/search_metrics_test.go` — `TestSearch_AttachMetricsNilClearsBindings()` | `pkg/search/search_metrics_test.go` — `TestSearch_AttachMetricsIdempotent()` |
| 22 | reuse | `pkg/search/build_settings_test.go` — `TestComposeStrategyBuildSettings_CompressedIncludesFingerprint()` | `pkg/search/build_settings_test.go` — `TestComposeRoutingBuildSettings_DoesNotEncodeSeedMode()` |
| 23 | reuse | `pkg/search/search_adaptive_overfetch_test.go` — `TestAdaptiveVectorSearchUsesIVFPQRerankDepthIndependentOfResultLimit()` | `pkg/search/search_adaptive_overfetch_test.go` — `TestCompressedVectorSearchRescoresFloorBeforeTrimmingResults()` |
| 24 | reuse | `pkg/search/search_adaptive_overfetch_test.go` — `TestCompressedVectorSearchRescoresFloorBeforeTrimmingResults()` | `pkg/search/search_adaptive_overfetch_test.go` — `TestAdaptiveVectorSearchUsesIVFPQRerankDepthIndependentOfResultLimit()` |
| 25 | reuse | `pkg/search/fulltext_index.go` — `.GetDocument()` | `pkg/storage/schema.go` — `.GetFulltextIndex()` |
| 26 | reuse | `pkg/nornicdb/embed_queue_chunk_test.go` — `TestChunkEmbeddingSearch()` | `pkg/nornicdb/embed_queue_chunk_test.go` — `TestChunkEmbeddingRemoval()` |
| 27 | reuse | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_GetStats()` | `pkg/inference/kalman_adapter_test.go` — `TestKalmanAdapter_GetStats()` |
| 28 | reuse | `pkg/search/vector_id_collapse_test.go` — `TestVectorSearchCandidates_CollapsesNamedIDsToNodeID()` | `pkg/search/vector_id_collapse_test.go` — `TestVectorSearchCandidates_CollapsesChunkIDsToNodeID_MaxScore()` |
| 29 | reuse | `pkg/search/clustered_search_test.go` — `TestRRFHybridSearch_UsesClusteringWhenEnabled()` | `pkg/search/clustered_search_test.go` — `TestRRFHybridSearch_FallsBackToVectorOnClusterError()` |
| 30 | reuse | `pkg/search/search_vector_dims_more_test.go` — `TestService_VectorQueryNodes_FileStorePropertyVectorsSupplementsStaleHNSW()` | `pkg/search/search_vector_dims_more_test.go` — `TestService_VectorQueryNodes_FileStorePropertyVectors()` |
| 31 | reuse | `pkg/cypher/vector_procedures_test.go` — `TestVectorIndexQueryNodes_WithUnifiedSearchService_PreservesCypherSemantics()` | `pkg/cypher/vector_procedures_test.go` — `TestVectorIndexQueryNodes_PrefersNamedEmbeddingsOverPropertyAndFallsBack()` |
| 32 | reuse | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_GetPredictedLatency()` | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_GetLatencyTrend()` |
| 33 | reuse | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_GetLatencyTrend()` | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_GetPredictedLatency()` |
| 34 | reuse | `pkg/nornicgrpc/lexical_completion_cancellation_test.go` — `TestSearchTextCancellationDuringLexicalSearch()` | `pkg/search/lexical_completion_cancellation_test.go` — `TestSearchTextChunksCancellationDuringLexicalSearch()` |
| 35 | reuse | `pkg/search/lexical_completion_cancellation_test.go` — `TestSearchTextChunksCancellationDuringLexicalSearch()` | `pkg/nornicgrpc/lexical_completion_cancellation_test.go` — `TestSearchTextCancellationDuringLexicalSearch()` |
| 36 | reuse | `pkg/cypher/vector_cosine_query_shapes_e2e_test.go` — `TestE2E_VectorCosine_TinyChunkPropertyPatternPreservesExactResults()` | `pkg/cypher/vector_cosine_query_shapes_e2e_test.go` — `BenchmarkE2E_VectorCosine_TinyChunkPropertyPattern()` |
| 37 | reuse | `pkg/cypher/vector_cosine_query_shapes_e2e_test.go` — `BenchmarkE2E_VectorCosine_TinyChunkPropertyPattern()` | `pkg/cypher/vector_cosine_query_shapes_e2e_test.go` — `TestE2E_VectorCosine_TinyChunkPropertyPatternPreservesExactResults()` |
| 38 | reuse | `pkg/search/index_flags_test.go` — `TestBuildIndexes_VectorDisabled()` | `pkg/search/index_flags_test.go` — `TestBuildIndexes_BothDisabled()` |
| 39 | reuse | `pkg/search/search_test.go` — `TestFuseRRF_MinimumScoreControlsSingleBranchRank50()` | `pkg/search/rrf_min_score_report_test.go` — `TestFuseRRFCountingReportsMinimumScoreDrops()` |
| 40 | reuse | `pkg/search/hybrid_cluster_routing_test.go` — `TestApplyBM25SeedHints_AppliesPreferredSeedsWhenAvailable()` | `pkg/search/hybrid_cluster_routing_test.go` — `TestApplyBM25SeedHints_NoSeedsIsNoOp()` |
| 41 | reuse | `pkg/search/hybrid_cluster_routing_test.go` — `TestApplyBM25SeedHints_NoSeedsIsNoOp()` | `pkg/search/hybrid_cluster_routing_test.go` — `TestApplyBM25SeedHints_AppliesPreferredSeedsWhenAvailable()` |
| 42 | reuse | `pkg/search/fulltext_index_v2_persist.go` — `.Load()` | `pkg/search/fulltext_index_v2_persist.go` — `.Load()` |
| 43 | reuse | `pkg/search/search_test.go` — `TestVectorQueryNodes_MissingIndexFallsThroughToManagedNamedEmbeddings()` | `pkg/cypher/vector_procedures_test.go` — `TestCallDbIndexVectorQueryNodes_MissingIndexFallsThroughToManagedEmbeddings()` |
| 44 | reuse | `pkg/search/rerank_mmr_extra_test.go` — `TestSearchRerankExtraApplyMMRErrorBranches()` | `pkg/search/rerank_mmr_extra_test.go` — `TestSearchRerankExtraApplyMMRBranches()` |
| 45 | reuse | `pkg/search/search_adaptive_overfetch_test.go` — `TestAdaptiveBM25SearchStopsWhenSourceIsExhausted()` | `pkg/search/search_adaptive_overfetch_test.go` — `TestAdaptiveBM25SearchWidensAfterFiltering()` |
| 46 | reuse | `pkg/search/search_test.go` — `TestEnableClustering_ReentrantAndEnvOverrides()` | `pkg/search/hybrid_cluster_routing_test.go` — `TestEnableClustering_DefaultMaxIterationsIsFive()` |
| 47 | reuse | `pkg/search/rerank_mmr_extra_test.go` — `TestSearchRerankExtraApplyMMRBranches()` | `pkg/search/rerank_mmr_extra_test.go` — `TestSearchRerankExtraApplyMMRErrorBranches()` |
| 48 | reuse | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_GetService()` | `pkg/search/kalman_adapter_test.go` — `TestNewKalmanSearchAdapter()` |
| 49 | reuse | `pkg/search/vector_query_unreachable_test.go` — `TestAdaptiveVectorSearchStopsWhenContextEnds()` | `pkg/search/search_adaptive_overfetch_test.go` — `TestAdaptiveVectorSearchStopsAtConfiguredCap()` |
| 50 | reuse | `pkg/search/hybrid_cluster_routing_test.go` — `TestEnableClustering_DefaultMaxIterationsIsFive()` | `pkg/search/search_test.go` — `TestEnableClustering_ReentrantAndEnvOverrides()` |
| 51 | reuse | `pkg/cypher/call_rag_test.go` — `TestRerankTruthfulnessFencedStructuredPublicPath()` | `pkg/cypher/call_rag_test.go` — `TestRerankTruthfulnessRejectsAmbiguousStructuredPublicPath()` |
| 52 | reuse | `pkg/search/search_vector_strategy_test.go` — `TestResolveCompressedVectorStrategy_FallbackOnLoadError()` | `pkg/search/search_vector_strategy_test.go` — `TestResolveCompressedVectorStrategy_UsesIVFPQWhenReady()` |
| 53 | reuse | `pkg/search/search_strategy_transition_test.go` — `TestRuntimeStrategyTransition_ReplayAndClearDeltaLog()` | `pkg/search/search_strategy_transition_test.go` — `TestRuntimeStrategyTransition_ReplayHelpers_BruteGPUAndClear()` |
| 54 | reuse | `pkg/eval/harness_test.go` — `TestHarnessBasic()` | `pkg/eval/harness_test.go` — `TestHarnessMultipleTests()` |
| 55 | reuse | `pkg/search/search_test.go` — `TestSearchService_BuildIndexes()` | `pkg/search/search_test.go` — `TestSearchService_BuildIndexesFromStorage()` |
| 56 | reuse | `pkg/search/search_parallel_test.go` — `TestRRFHybridSearch_ParallelRetrievalOverlaps()` | `pkg/search/search_parallel_test.go` — `TestRRFHybridSearch_ParallelRetrievalJoinsBM25AfterCancellation()` |
| 57 | reuse | `pkg/search/search_test.go` — `TestSearchHelpers_MaybeRebuildHNSW_RebuildAndCancelBranches()` | `pkg/search/search_test.go` — `TestSearchHelpers_BuildHNSWForTransition_Branches()` |
| 58 | reuse | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationIDModeRejectsMutationDuringBuild()` | `pkg/search/text_continuation_test.go` — `TestSearchTextContinuationIDModeInvalidatesAfterMutation()` |
| 59 | reuse | `pkg/search/search_test.go` — `TestSearchService_RemoveNode_OnlyRemovesTargetNode()` | `pkg/search/search_test.go` — `TestSearchService_RemoveNode()` |
| 60 | reuse | `pkg/bolt/search_stream_lifecycle_test.go` — `TestSearchStreamLifecycleBolt()` | `pkg/bolt/search_stream_lifecycle_test.go` — `TestSearchStreamLifecycleBoltDisconnectBeforeDelivery()` |
| 61 | reuse | `pkg/bolt/search_stream_lifecycle_test.go` — `TestSearchStreamLifecycleBoltDisconnectBeforeDelivery()` | `pkg/bolt/search_stream_lifecycle_test.go` — `TestSearchStreamLifecycleBolt()` |
| 62 | reuse | `pkg/search/ivfpq_persist_test.go` — `TestIVFPQPersist_ServiceBranches()` | `pkg/search/ivfpq_persist_test.go` — `TestIVFPQPersist_ServiceSuccessAndCacheBranches()` |
| 63 | reuse | `pkg/cypher/query_embedding_result_cache_test.go` — `TestQueryEmbeddingFabricCacheProviderReplacement()` | `pkg/cypher/query_embedding_result_cache_test.go` — `TestQueryEmbeddingFabricCachedStartupFallback()` |
| 64 | reuse | `pkg/cypher/query_chunk_limit_test.go` — `TestRetrieveRowsReportQueryChunks()` | `pkg/cypher/query_chunk_limit_test.go` — `TestRetrieveContinuationReportsQueryChunksWithoutHits()` |
| 65 | reuse | `pkg/search/rerank_provider_presence_test.go` — `TestRerankProviderRepresentationShapeMatrix()` | `pkg/search/rerank_provider_presence_test.go` — `TestRerankCandidatesRejectsAmbiguousProviderOutput()` |
| 66 | reuse | `pkg/search/search_vector_strategy_test.go` — `TestResolveCompressedVectorStrategy_UsesIVFPQWhenReady()` | `pkg/search/search_vector_strategy_test.go` — `TestResolveCompressedVectorStrategy_FallsBackWhenNotReady()` |
| 67 | reuse | `pkg/search/managed_embedding_property_test.go` — `TestManagedEmbeddingContentIsExcludedFromDefaultSearchText()` | `pkg/search/managed_embedding_property_test.go` — `TestManagedEmbeddingContentIsExcludedFromSearchResultProperties()` |
| 68 | reuse | `pkg/search/managed_embedding_property_test.go` — `TestManagedEmbeddingContentIsExcludedFromSearchResultProperties()` | `pkg/search/managed_embedding_property_test.go` — `TestManagedEmbeddingContentIsExcludedFromDefaultSearchText()` |
| 69 | reuse | `pkg/search/search_test.go` — `TestSearchWithMMROption()` | `pkg/nornicdb/embed_queue_chunk_test.go` — `TestChunkEmbeddingSearch()` |
| 70 | reuse | `pkg/search/refresh_reads_test.go` — `newCountingService()` | `pkg/search/unchanged_fulltext_reindex_test.go` — `newCountingFulltextService()` |
| 71 | reuse | `pkg/cypher/vector_procedures_test.go` — `TestCallDbIndexVectorQueryNodes_LazyWiredServiceWarmsBeforeQuery()` | `pkg/cypher/vector_procedures_test.go` — `TestCallDbIndexVectorQueryNodes_DoesNotCreateSearchServiceWhenUnwired()` |
| 72 | reuse | `pkg/search/node_mutations_test.go` — `TestNodeMutationReservationPreservesNewerLiveMutation()` | `pkg/search/node_mutations_test.go` — `TestNodeMutationConcurrentReservationsAndDirectWrites()` |
| 73 | reuse | `pkg/search/unchanged_fulltext_reindex_test.go` — `newCountingFulltextService()` | `pkg/search/refresh_reads_test.go` — `newCountingService()` |
| 74 | reuse | `pkg/search/service_options_test.go` — `TestServiceOptionsSearchResultCachePolicy()` | `pkg/search/service_options_test.go` — `TestSearchResultCachePolicyAppliesToLiveService()` |
| 75 | reuse | `pkg/search/build_write_queue_order_test.go` — `TestBuildStoredVectorChoosesTheDimensionBeforeQueuedVectors()` | `pkg/search/build_write_queue_test.go` — `TestBuildFirstVectorChoosesTheDimensionInOrder()` |
| 76 | reuse | `pkg/search/index_flags_test.go` — `TestEnsureWarm_TriggersOnceAndWaiters()` | `pkg/search/index_flags_test.go` — `TestEnsureWarm_NoOp_WhenNotLazy()` |
| 77 | reuse | `pkg/search/search_test.go` — `TestSearchEnrichResultsBranches()` | `pkg/search/search_test.go` — `TestSearchService_EnrichResults()` |
| 78 | reuse | `pkg/search/vector_exact_stage_test.go` — `exactStageService()` | `pkg/search/ivf_hnsw_candidate_gen_test.go` — `TestIVFHNSW_UsedAfterClustering_CPUOnly()` |
| 79 | reuse | `pkg/search/search_strategy_transition_test.go` — `TestRuntimeStrategy_DefaultsToHNSW()` | `pkg/search/search_strategy_transition_test.go` — `TestRuntimeStrategy_HNSWDefaultIndexesLiveWrites()` |
| 80 | reuse | `pkg/search/fulltext_index_v2_correctness_test.go` — `TestFulltextIndexV2AdaptiveScratchPolicy()` | `pkg/search/fulltext_index_v2_correctness_test.go` — `TestFulltextIndexV2RareQueryUsesSparseScratch()` |
| 81 | reuse | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_ApplyRankingStability()` | `pkg/search/kalman_adapter_test.go` — `TestKalmanSearchAdapter_RecordRanking()` |
| 82 | reuse | `pkg/search/service_options_test.go` — `TestSearchServiceDoesNotIndexManagedVectorsFromAnotherModelSpace()` | `pkg/search/search_test.go` — `TestSearchService_VectorSearchOnly()` |
| 83 | reuse | `pkg/search/chunked_search_rank_test.go` — `TestSearchTextChunksKeepsRankProvenance()` | `pkg/search/chunked_search_rank_test.go` — `TestSearchTextChunksKeepsVectorSimilarity()` |
| 84 | reuse | `pkg/search/unchanged_fulltext_reindex_test.go` — `TestExtractSearchableTextOrdersPropertiesByKey()` | `pkg/indexing/config_test.go` — `TestExtractSearchableText()` |
| 85 | reuse | `pkg/search/continuation_exhaustion_test.go` — `testContinuationDefaultHNSWPopulation()` | `pkg/search/continuation_exhaustion_test.go` — `TestContinuationDefaultHNSWFinishesFinitePopulation()` |

## Work after this inventory

Candidates 23 and 24 were consolidated into one table-driven compressed-vector
rescore test, preserving both fixtures and their assertions; its focused race
check passed. The full PR-delta checker still needs to reanalyze the integrated
current tree before either finding can be marked resolved. Later source changes
are not covered by this historical inventory.
