# Traversal seed cost qualification (NOR-414)

## Workload and method

Baseline: `896fa3a1f0980ba61b55bc7b6aa87f643bd73e66` (the merged #1771 on main). Candidate: the production changes accompanying this report. Both binaries use identical test/benchmark and observer source; only the candidate changes production traversal planning.

Apple M2 Max, darwin/arm64, Go 1.27.3, default GOMAXPROCS 12. Tests use the production Badger-backed in-memory engine, one namespace, real unique indexes, public `Execute`, and disabled query-result caching. Data is generated and released with the fixture; no persistent/dogfood database is changed.

The membership and inline-key populations contain one collection, 351 documents and 800 unrelated outgoing relationships per document: 281,151 relationships. The selective-end control contains 200 collections, two documents, six memberships and two unrelated relationships. Each probe asserts its exact result multiset; membership returns 351 rows, inline keys one row, and the control six rows.

Two samples per revision, five timed executions per sample, run in baseline/candidate/candidate/baseline order without simultaneous tests or profiling. The table gives sample means, not statistical significance. Fixture setup, BEGIN/ROLLBACK and a warming exact-result probe are outside the timer. Both parser implementations have public regression coverage; these timing samples use the default parser.

## Results

| Workload | Mode | Baseline µs/op | Candidate µs/op | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single_collection_indexed_end | autocommit | 126,021.1 | 1,906.4 | 117,200,814 | 1,174,176 | 910,968.0 | 8,075.0 |
| single_collection_indexed_end | transaction | 803,074.2 | 4,091.3 | 433,689,410 | 2,367,540 | 6,584,926.5 | 30,632.0 |
| inline_keys_both_ends | autocommit | 317.3 | 218.4 | 309,680 | 158,652 | 2,634.0 | 1,686.5 |
| inline_keys_both_ends | transaction | 1,892.0 | 965.4 | 991,251 | 460,514 | 18,529.5 | 8,594.5 |
| many_collections_tiny_end | autocommit | 99.3 | 111.7 | 33,065 | 36,305 | 374.0 | 448.0 |
| many_collections_tiny_end | transaction | 104.2 | 114.1 | 41,657 | 44,897 | 488.0 | 562.0 |

Membership adjacency falls from 351 listings / 281,151 entries to one listing / 351 entries (autocommit observer). The transaction serves adjacency inside its own wrapper; its allocation measurements establish the reduction there. Both inline keys reuse the bound-end contract, rejecting unrelated final-hop endpoints before hydration. The selective-end control still reads two adjacency lists / eight entries.

**Measured cost:** the small selective-end control adds 3,240 B and 74 allocations per statement for count-store planning reads. Mean latency increases about 12.5 µs (autocommit) and 9.8 µs (transaction). The lower-bound comparison already avoids a redundant start-adjacency count. These are real measured costs, not a claim of an across-the-board speedup; retaining count-based orientation prevents the demonstrated large expansion regression. No persistent statistics, cache or storage format is introduced.

## Profiling and limits

Separate ten-execution CPU/allocation profiles were captured for the explicit-transaction membership case. The baseline query stacks spend about 0.74 sampled CPU seconds in `traversalEdges` / `transactionStorageWrapper.OutgoingEdgeHeaders`; the candidate has only about 0.03 sampled CPU seconds across Execute stacks. Whole-process profiles include fixture creation and GC and must not be interpreted as query-only profiles; the candidate query sample is short. Deterministic adjacency counters and B/op/allocs/op support the causal conclusion more strongly than the CPU ratio.

The generated population reproduces the reported shape, not the unavailable private application dataset or its exact hardware timings. First-hop average degree is an estimate, not an optimality guarantee under arbitrary skew. Raw counters only choose orientation: zero or unavailable statistics never decide an empty result. Decay/temporal views decline these statistics, and views without usable metadata retain indexed/cardinality planning. A real empty index answer remains authoritative. Counter errors propagate.

## Reproduction

Use this PR's `traversal_seed_cost_test.go` and the observer updates in `relationship_id_pinned_edge_test.go` in both the baseline and candidate checkouts; do not copy production traversal changes to the baseline. The observer must expose both `ReadCountStore` and `Namespace`, or explicit transactions decline its counter capability and the comparison is not equivalent.

```sh
go test -tags=noui,nolocalllm ./pkg/cypher -run '^TestTraversalSeedCost' -count=1 -v
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' -bench '^BenchmarkTraversalSeedCost/' -benchtime=5x -count=1 -benchmem
go test -tags=noui,nolocalllm ./pkg/cypher -run '^$' -bench '^BenchmarkTraversalSeedCost/single_collection_indexed_end/transaction$' -benchtime=10x -cpuprofile=traversal.cpu -memprofile=traversal.mem
go tool pprof -top -cum -focus="StorageExecutor.*Execute" traversal.cpu
```

The build tags omit UI/native local-LLM integration; native full-suite CI remains separate qualification. Run benchmarks independently of test runs. This report does not claim full release verification or a completed remote duplicate check.
