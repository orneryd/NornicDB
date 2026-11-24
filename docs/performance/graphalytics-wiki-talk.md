# LDBC Graphalytics on wiki-Talk

NornicDB and Neo4j Community 2026.10.2 ran in native ARM64 Docker containers on Louise, an Apple M2 Max host. Each database ran separately with six CPUs, a 6 GiB memory limit, and Docker-managed storage. The [official directed wiki-Talk dataset](https://ldbcouncil.org/benchmarks/graphalytics/datasets/) has 2,394,385 vertices and 5,021,410 relationships.

## Bulk import

| Load path | NornicDB | Neo4j |
| --- | ---: | ---: |
| Offline CLI, including process shutdown | 403.79 s | 10.34 s |
| Offline CLI plus shared CSV preparation | 405.46 s | 12.01 s |
| Online bulk transactions over Bolt | 752.83 s | 170.29 s |

The offline commands used identical CSV inputs. CSV preparation took 1.67 s. NornicDB used `nornicdb-admin database import full` with search-index construction disabled. Neo4j used `neo4j-admin database import full` with six threads. Both loaders reported the full graph counts.

The online runs used the same Python Bolt client, an index on the source vertex ID, and sequential Cypher `UNWIND` batches of 1,000 rows. The timer includes client parsing, index creation and readiness, and completion of all node and relationship transactions. Server startup and the final graph-count checks are outside the timer. Both databases returned the expected stored counts.

NornicDB’s offline load was about 1.9 times as fast as its online load, including CSV preparation. Bulk-import tuning is in progress.

## Weakly connected components over Bolt

| Database | Result |
| --- | --- |
| NornicDB | Client timeout after 600 s |
| Neo4j GDS | 24.99 s with the official partition validated |

Both runs used the same Bolt client and consumed the full output into a scratch file when the query completed. Neo4j’s directed GDS projection took 2.01 s separately. NornicDB’s [earlier embedded WCC optimization](https://github.com/orneryd/NornicDB/pull/1969) measures the in-process path.

NornicDB was built from release hotfix commit `2a98121eba3278395b7747478069311a53c3a756`. The Neo4j container and its Graph Data Science plugin were used for the Bolt run.
