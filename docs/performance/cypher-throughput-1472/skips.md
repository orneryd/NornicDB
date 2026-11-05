# Full-package skipped tests

These are skips, not passes. Opt-in diagnostics, external fixtures and report workloads were not enabled.

```text
## nornic

    bolt_concurrent_retract_diagnostic_test.go:44: set NORNICDB_TEST_BOLT_DIAGNOSTIC=1 for disposable backend
--- SKIP: TestBoltConcurrentRetractDiagnostic (0.00s)

    bolt_shared_edge_contention_diagnostic_test.go:25: set NORNICDB_TEST_BOLT_DIAGNOSTIC=1 for disposable backend
--- SKIP: TestBoltSharedEdgeContentionDiagnostic (0.00s)

    demo_shortest_path_bench_test.go:231: set -demobench to run the demo shortestPath harness
--- SKIP: TestDemoShortestPath_E2E (0.00s)

    demo_shortest_path_bench_test.go:317: set -demobench to run the demo latency distribution test
--- SKIP: TestDemoShortestPath_LatencyDistribution (0.00s)

    demo_shortest_path_bench_test.go:388: set -demobench to run the demo latency-over-time test
--- SKIP: TestDemoShortestPath_LatencyOverTime (0.00s)

    demo_shortest_path_largescale_test.go:400: set -largescale to run; this seeds 500K nodes (~30-90s).
--- SKIP: TestLargeScaleShortestPath_HopBuckets (0.00s)

    fastpath_test.go:16: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestFastPath_MatchCreateDeleteRel (0.00s)

    fastpath_test.go:67: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestFastPath_LDBCPattern (0.00s)

    function_catalog_test.go:155: set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare the shared function inventory
--- SKIP: TestSharedFunctionInventoryMatchesPinnedNeo4j (0.00s)

    graphify_push_profile_bench_test.go:149: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestGraphifyPushProfile_WithIndexes (0.00s)

    graphify_push_profile_bench_test.go:159: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestGraphifyPushProfile_WithoutIndexes (0.00s)

    graphify_push_profile_bench_test.go:190: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestGraphifyPushProfile_EdgeShapeHypothesis (0.00s)

    parser_benchmark_report_test.go:91: benchmark-style parser report; set NORNIC_RUN_PARSER_REPORTS=1 to run
--- SKIP: TestParserValidationBenchmarkReport (0.00s)

    parser_benchmark_report_test.go:140: benchmark-style parser report; set NORNIC_RUN_PARSER_REPORTS=1 to run
--- SKIP: TestParserParseBenchmarkReport (0.00s)

    parser_scaling_report_test.go:170: asymptotic parser study; set NORNIC_RUN_PARSER_SCALING=1 to run
--- SKIP: TestParserScaling (0.00s)

    parser_scaling_report_test.go:258: asymptotic parser study; set NORNIC_RUN_PARSER_SCALING=1 to run
--- SKIP: TestParserTailLatency (0.00s)

    procedure_differential_neo4j_test.go:19: NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD not configured
--- SKIP: TestProcedureDifferentialNeo4j (0.00s)

    procedure_metadata_localization_test.go:52: set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare procedure metadata
--- SKIP: TestSharedProcedureMetadataMatchesPinnedNeo4j (0.00s)

    procedure_metadata_localization_test.go:56: set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare procedure metadata
--- SKIP: TestSharedTokenProcedureMetadataMatchesPinnedNeo4j (0.00s)

    profile_match_create_test.go:29: set NORNICDB_PROFILE_MATCH_CREATE=1 to run the profile harness
--- SKIP: TestProfileMatchCreateDelete (0.00s)

    range_semantics_test.go:289: large issue #772 workload
--- SKIP: TestUnwindRangeReportedWorkload (0.00s)

    range_semantics_test.go:309: large issue #772 aggregate correctness workload
--- SKIP: TestUnwindRangeReportedAggregateWorkload (0.00s)

## antlr

    bolt_concurrent_retract_diagnostic_test.go:44: set NORNICDB_TEST_BOLT_DIAGNOSTIC=1 for disposable backend
--- SKIP: TestBoltConcurrentRetractDiagnostic (0.00s)

    bolt_shared_edge_contention_diagnostic_test.go:25: set NORNICDB_TEST_BOLT_DIAGNOSTIC=1 for disposable backend
--- SKIP: TestBoltSharedEdgeContentionDiagnostic (0.00s)

    demo_shortest_path_bench_test.go:231: set -demobench to run the demo shortestPath harness
--- SKIP: TestDemoShortestPath_E2E (0.00s)

    demo_shortest_path_bench_test.go:317: set -demobench to run the demo latency distribution test
--- SKIP: TestDemoShortestPath_LatencyDistribution (0.00s)

    demo_shortest_path_bench_test.go:388: set -demobench to run the demo latency-over-time test
--- SKIP: TestDemoShortestPath_LatencyOverTime (0.00s)

    demo_shortest_path_largescale_test.go:400: set -largescale to run; this seeds 500K nodes (~30-90s).
--- SKIP: TestLargeScaleShortestPath_HopBuckets (0.00s)

    executor_test.go:1825: Skipping: ANTLR parser doesn't support multiple MATCH...CREATE blocks without semicolons (NornicDB extension)
--- SKIP: TestExecuteMultipleMatchCreateBlocks (0.00s)

    fastpath_test.go:16: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestFastPath_MatchCreateDeleteRel (0.00s)

    fastpath_test.go:67: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestFastPath_LDBCPattern (0.00s)

    function_catalog_test.go:155: set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare the shared function inventory
--- SKIP: TestSharedFunctionInventoryMatchesPinnedNeo4j (0.00s)

    graphify_push_profile_bench_test.go:149: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestGraphifyPushProfile_WithIndexes (0.00s)

    graphify_push_profile_bench_test.go:159: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestGraphifyPushProfile_WithoutIndexes (0.00s)

    graphify_push_profile_bench_test.go:190: profiling and timing tests require NORNICDB_RUN_PERFORMANCE_TESTS=1, without -race or -short
--- SKIP: TestGraphifyPushProfile_EdgeShapeHypothesis (0.00s)

    parser_benchmark_report_test.go:91: benchmark-style parser report; set NORNIC_RUN_PARSER_REPORTS=1 to run
--- SKIP: TestParserValidationBenchmarkReport (0.00s)

    parser_benchmark_report_test.go:140: benchmark-style parser report; set NORNIC_RUN_PARSER_REPORTS=1 to run
--- SKIP: TestParserParseBenchmarkReport (0.00s)

    parser_scaling_report_test.go:170: asymptotic parser study; set NORNIC_RUN_PARSER_SCALING=1 to run
--- SKIP: TestParserScaling (0.00s)

    parser_scaling_report_test.go:258: asymptotic parser study; set NORNIC_RUN_PARSER_SCALING=1 to run
--- SKIP: TestParserTailLatency (0.00s)

    procedure_differential_neo4j_test.go:19: NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD not configured
--- SKIP: TestProcedureDifferentialNeo4j (0.00s)

    procedure_metadata_localization_test.go:52: set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare procedure metadata
--- SKIP: TestSharedProcedureMetadataMatchesPinnedNeo4j (0.00s)

    procedure_metadata_localization_test.go:56: set NORNICDB_NEO4J_REFERENCE_HTTP_URI to compare procedure metadata
--- SKIP: TestSharedTokenProcedureMetadataMatchesPinnedNeo4j (0.00s)

    profile_match_create_test.go:29: set NORNICDB_PROFILE_MATCH_CREATE=1 to run the profile harness
--- SKIP: TestProfileMatchCreateDelete (0.00s)

    range_semantics_test.go:289: large issue #772 workload
--- SKIP: TestUnwindRangeReportedWorkload (0.00s)

    range_semantics_test.go:309: large issue #772 aggregate correctness workload
--- SKIP: TestUnwindRangeReportedAggregateWorkload (0.00s)

```
