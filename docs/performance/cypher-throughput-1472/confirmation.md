# Confirmation results

Five alternating rounds, 1s per case, both parsers. No tests, builds or verifier run concurrently. An earlier overlapping confirmation was canceled and excluded. Same binaries and harnesses as the initial matrix. Medians below; small differences do not establish gains.

| Parser | Workload | Control µs | Current µs | Latency change | B/op control → current | Allocs/op control → current |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| nornic | PipelineLimitedCartesianCreateDelete | 28.807 | 28.722 | -0.3% | 22357 → 22021 | 287 → 285 |
| nornic | PipelineWithScope/projection | 275.152 | 155.482 | -43.5% | 14399 → 14025 | 874 → 872 |
| nornic | PipelineWithScope/predicate | 343.185 | 342.474 | -0.2% | 14950 → 14951 | 894 → 894 |
| nornic | SemanticValidationDistinctText | 16.533 | 16.777 | +1.5% | 3558 → 3556 | 43 → 43 |
| antlr | PipelineLimitedCartesianCreateDelete | 29.059 | 28.936 | -0.4% | 22486 → 22150 | 288 → 286 |
| antlr | PipelineWithScope/projection | 276.430 | 154.741 | -44.0% | 14502 → 14125 | 875 → 873 |
| antlr | PipelineWithScope/predicate | 337.824 | 335.257 | -0.8% | 15055 → 15053 | 895 → 895 |
| antlr | SemanticValidationDistinctText | 16.698 | 16.740 | +0.3% | 3558 → 3557 | 43 → 43 |
