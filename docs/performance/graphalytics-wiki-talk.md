# LDBC Graphalytics on wiki-Talk

NornicDB matched the official reference outputs on all five workloads. Neo4j Community 2026.10.2 with Graph Data Science 2026.10.2 matched two using the tested configurations. Both ran natively on the same Windows x64 machine with an Intel Core i9-9900KF and 32 GiB of RAM. The directed graph has 2,394,385 vertices and 5,021,410 edges. [Official Graphalytics dataset](https://ldbcouncil.org/benchmarks/graphalytics/datasets/)

| Workload | NornicDB | Neo4j GDS |
| --- | ---: | ---: |
| Breadth-first search distances | 0.120 s | 67.045 s |
| Weakly connected components | 52.352 s | 61.768 s |
| Community detection via label propagation | 2.400 s | Reference mismatch |
| Local clustering coefficient | 39.942 s | Reference mismatch |
| PageRank | 119.659 s | Reference mismatch |

The timings exclude graph import, projection, and reference validation. NornicDB formats results in process. Neo4j streams results over local Bolt and writes them to a scratch file. Its breadth-first distances come from unweighted Delta-Stepping. The table measures these execution paths on this graph. A separate Neo4j WCC stats run on the projected graph reported 0.331 s of engine compute time.

## Connected-components follow-up

NornicDB originally took 183.999 s. The patch streams relationships once for a complete stored graph. The three follow-up runs took 52.535 s, 51.824 s, and 52.352 s. Every run matched the official partition for all 2,394,385 vertices. [WCC patch and verification](https://github.com/orneryd/NornicDB/pull/1969)

## Import and projection

NornicDB used transactional batch import and took 426.09 s in the baseline run, followed by 7.92 s to build its benchmark projection. Neo4j used its offline bulk loader and took 18.63 s, plus 5.52 s for CSV preparation and 2.57 s for the directed GDS projection. The undirected GDS projection took a further 0.77 s.

The 426.09 s result measures the Graphalytics adapter's transactional import. NornicDB's `nornicdb-admin database import full` also accepts CSV offline and writes through the same transactional storage APIs. A diagnostic with 168,285 vertices and 240,000 relationships from `wiki-Talk`, with search-index construction disabled, spent most of the importer CPU time in storage transactions. Increasing the chunk size from 1,000 to 8,192 rows made the sample slower, from 16.35 s to 20.91 s. A separate comparison found no improvement from the existing high-performance storage profile or write-behind. Batching the endpoint-label reads yielded 17.03–17.45 s against a 17.17–18.31 s baseline in the same experiment. The prototype was not adopted. These sample times do not replace the full-graph result above.

## Reference validation

Both runs used the same validator and a byte-identical copy of the official archive. The validator checks exact traversal distances and labels, partition equivalence for connected components, and a relative tolerance of 0.0001 for numeric scores.

- Neo4j label propagation used the original vertex IDs as seeds and returned different labels.
- Neo4j's clustering procedure requires an undirected projection. Its values differed from the reference for the directed graph. [GDS clustering documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/local-clustering-coefficient/)
- Neo4j PageRank used the archive's damping and iteration settings with L1 normalization. Its scores missed the reference tolerance.

The baseline used NornicDB v1.5.2 with a local validation adapter and a PageRank patch. The WCC follow-up used the hotfix commit in PR #1969. The NornicDB adapter used native APOC procedures for WCC and PageRank and an adjacency projection from storage for the other listed workloads.
