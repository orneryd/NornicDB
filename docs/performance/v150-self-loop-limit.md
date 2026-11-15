# Self-loop LIMIT qualification

RC report 38.9 uses 1,000 `P` nodes, each with one `SELF` relationship to itself:

```cypher
MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 1
```

The old traversal materialized all paths before rejecting unequal endpoints. It
therefore disabled its early limit for repeated variable names. The correction
supplies each walk's required end identity before accepting paths: non-loops do
not consume the limit, and the first qualifying loop can finish the request.
Sequential, parallel and bound-start walks use this same endpoint decision.

## Measured workload

Apple M2 Max, macOS arm64, Go benchmark process with 12 logical workers;
1,000 generated nodes/relationships in a namespaced memory engine backed by
in-memory Badger. Query-result caching is disabled. The same test-only benchmark
fixture was copied to release `a492f2acd`; production baseline source was unchanged.
The candidate was measured with the correction in this PR.

| Measure | Release baseline | Candidate |
| --- | ---: | ---: |
| Adjacency requests for LIMIT 1 | 1,000 | 1 |
| Median ns/op, three profiled runs | 3,295,874 | 52,501 |
| Median B/op | 4,133,915 | 12,819 |
| Median allocations/op | 45,262 | 172 |

Each run used 300 measured reads; fixture creation is outside the benchmark timer.
These short shared-host timings are diagnostic, not an isolated throughput or
percentile guarantee. The public result/read-count assertions independently prove
the removed work. The allocation profile attributes about 64% of baseline sampled
allocation space cumulatively to parallel traversal, including path construction
and detached node copies. Full-process profiles also include fixture construction;
the candidate profile is predominantly setup and is not a per-read attribution.

Repeat in each checkout with the identical benchmark fixture:

```sh
go test -tags nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkRepeatedEndpointLimitedRead$' -benchmem \
  -benchtime=300x -count=3 -cpuprofile cpu.out -memprofile mem.out
```

## Correctness controls

`TestRepeatedEndpointLimitStopsAtTheSelfLoop` fails on the baseline with 1,000
requests instead of one. Candidate controls assert that returned rows belong to
the nodes actually walked, LIMIT 2 returns two real loops, SKIP preserves its
window, non-loop populations return no rows, and repeated nodes in chained paths
retain their identity rule. Both parsers and focused race checks pass. This is
not qualification of unrelated traversal shapes or full release verification.
