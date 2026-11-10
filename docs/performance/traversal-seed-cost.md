# Traversal seed cost qualification (NOR-414)

## Workload and method

Baseline: `896fa3a1f0980ba61b55bc7b6aa87f643bd73e66` (the merged #1771 on main). Candidate: the production changes accompanying this report. Both binaries use identical benchmark and observer source; only the candidate changes production traversal planning.

Apple M2 Max, darwin/arm64, Go 1.27.3, default GOMAXPROCS 12. Tests use the production Badger-backed in-memory engine, one namespace, real unique indexes, public `Execute`, and disabled query-result caching. Data is generated and released with the fixture; no persistent/dogfood database is changed.

The membership and inline-key populations contain one collection, 351 documents and 800 unrelated outgoing relationships per document: 281,151 relationships. The selective-end control contains 200 collections, two documents, six memberships and two unrelated relationships. The two selective-document controls contain one collection with 1,000 memberships and select only one document, through WHERE and through two inline keys. Each must keep a single-entry reverse traversal. Each probe asserts its exact result multiset; membership returns 351 rows, inline keys one row, and the control six rows.

Two samples per revision, five timed executions per sample, run in baseline/candidate/candidate/baseline order without simultaneous tests or profiling. The table gives sample means, not statistical significance. Fixture setup, BEGIN/ROLLBACK and a warming exact-result probe are outside the timer. Both parser implementations have public regression coverage; these timing samples use the default parser.

## Results

| Workload | Mode | Baseline µs/op | Candidate µs/op | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| single_collection_indexed_end | autocommit | 126,723.3 | 2,425.9 | 117,168,428 | 1,544,674 | 910,905.0 | 11,417.5 |
| single_collection_indexed_end | transaction | 783,424.1 | 5,741.4 | 436,365,600 | 3,408,002 | 6,578,248.5 | 45,843.5 |
| inline_keys_both_ends | autocommit | 320.8 | 238.8 | 309,680 | 166,886 | 2,634.0 | 1,836.5 |
| inline_keys_both_ends | transaction | 1,921.8 | 973.2 | 992,250 | 470,328 | 18,577.5 | 8,767.5 |
| many_collections_tiny_end | autocommit | 109.0 | 110.9 | 33,065 | 36,305 | 374.0 | 448.0 |
| many_collections_tiny_end | transaction | 113.7 | 123.5 | 41,657 | 44,905 | 488.0 | 562.5 |
| single_collection_selective_end | autocommit | 99.3 | 99.5 | 12,824 | 16,625 | 182.0 | 270.0 |
| single_collection_selective_end | transaction | 90.8 | 97.7 | 16,932 | 20,745 | 204.0 | 292.0 |
| inline_keys_selective_end | autocommit | 86.8 | 90.4 | 14,569 | 19,174 | 174.0 | 270.0 |
| inline_keys_selective_end | transaction | 87.1 | 91.9 | 18,684 | 20,292 | 196.0 | 285.0 |

Membership adjacency falls from 351 listings / 281,151 entries to one listing / 351 entries (autocommit observer). The transaction serves adjacency inside its own wrapper; its allocation measurements establish the reduction there. Both inline keys reuse the bound-end contract, rejecting unrelated final-hop endpoints before hydration. The selective-end control still reads two adjacency lists / eight entries.

**Measured planning cost:** the small selective-end control adds 3,240 B and 74 allocations per statement for count-store planning reads. The new single-document controls also add planning bytes/allocations relative to the original main plan; their exact totals are in the table. These are measured costs, not an across-the-board speedup. The lower-bound comparison avoids a redundant start-adjacency count, and count-based orientation prevents the demonstrated wide expansion. No persistent statistics, cache or storage format is introduced.

## Review corrections

Public regressions additionally prove that anonymous inline starts honor both present and absent keys, and that planning does not hydrate all 1,000 `Doc {status: 'active'}` candidates before choosing one indexed Entity. No-WHERE inline start preparation uses the existing pattern-seek posting-count estimator; only a proven zero/single posting is eagerly read. Larger/unknown inline populations stay on the normal traversal access path.

A one-node label and two one-node index answers no longer decide direction by themselves. Directional adjacency costs decide first, then an unused single end candidate may bind a forward traversal's endpoint. The selective-document controls read one adjacency entry, rather than all 1,000 collection memberships. Complete start-pattern admission has one slice owner before sequential/parallel dispatch, avoiding the former repeated check on ordinary sequential traversals.

## Profiling and limits

Separate ten-execution CPU profiles were refreshed for the explicit-transaction membership case. Query stacks in the baseline pass through reverse traversal and document adjacency reads; the candidate uses the collection-side traversal. Whole-process profiles include fixture creation and GC, and the short candidate query sample is not a statistically reliable CPU ratio. Deterministic adjacency counters and measured B/op/allocs/op support the causal conclusion.

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
