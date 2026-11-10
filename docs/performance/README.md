# Performance

Benchmark reports describe their recorded workloads, hardware, and software builds. Query latency and end-to-end throughput measure different parts of a run; compare matching conditions.

## Benchmark reports

| Report | Scope |
| --- | --- |
| [Database comparison](benchmarks/database-comparison/sweep.md) | Latest recorded Northwind sweep across NornicDB, Neo4j, FalkorDB, Memgraph, and LadybugDB. |
| [Shortest-path traversal](benchmarks/shortest-path.md) | 500,000 nodes and 3,969,418 edges; seed cost and latency across hop depths. |
| [Parser scaling](benchmarks/parser-scaling/report.md) | Input-size growth, allocations, bytes, latency, and GC for the native and ANTLR parsers. |
| [Northwind parser modes](benchmarks/database-comparison/parser-modes.md) | Native and ANTLR query latency over the same seeded graph. |
| [Neo4j comparison](benchmarks/database-comparison/comparison.md) | Query-level results, ingestion, storage, power, and measurement definitions. |
| [Retrieval quality](retrieval-recall-benchmark.md) | BEIR SciFact recall, ranking scores, and reproduction protocol. |
| [Hybrid queries](hybrid-query-benchmarks.md) | Combined retrieval and graph-query workloads. |

Report directories use stable topic names. Build identifiers, versions, and dates remain inside the reports and raw artifacts so the measurements can be reproduced.

## Profiling and tuning

- [Search methodology](searching.md)
- [Hot-path query cookbook](hot-path-query-cookbook.md)
- [HTTP optimization](http-optimization-options.md)
- [Profiling with pprof](pprof-quick-guide.md)
- [Single-request measurements](single-request-benchmark.md)
