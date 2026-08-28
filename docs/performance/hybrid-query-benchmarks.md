# Hybrid Query Benchmarks

This benchmark focuses on the query shape that matters most for NornicDB's positioning: semantic retrieval followed by graph expansion in the same engine.

The goal is not to claim a universal leaderboard result. The goal is to show what happens when vector search and one-hop graph traversal share one execution path instead of being stitched together across multiple systems.

## Summary

- **Full RRF retrieval** stayed in sub-millisecond to low-millisecond territory locally, depending on transport.
- **Full RRF retrieval + one-hop graph traversal** added a small incremental cost locally.
- **Remote latency tracked client-to-server RTT**, which means end-to-end latency became network-bound rather than database-bound.

## Test Setup

| Item          | Value                                        |
| ------------- | -------------------------------------------- |
| Nodes         | 67,280                                       |
| Edges         | 40,921                                       |
| Embeddings    | 67,298                                       |
| Vector index  | HNSW, CPU-only                               |
| Request count | 800 per query type                           |
| Query types   | Vector top-k; Vector top-k + 1-hop traversal |

Local environment:

- Apple M3 Max
- 64 GB RAM
- Native macOS installer

Remote environment:

- GCP
- 8 vCPU
- 32 GB RAM

## Verified Full-RRF Regression Run

Run on 2026-08-28 at revision `5408494199ef` using the self-contained E2E traversal fixture:

| Item                    |                             Value |
| ----------------------- | --------------------------------: |
| Nodes                   |                             2,715 |
| Edges                   |                             2,706 |
| Indexed root embeddings |                                 9 |
| Vector dimensions       |                                 3 |
| Measured requests       | 800 per query shape and transport |
| Warmup requests         |  10 per query shape and transport |

Every request used a unique query cache key so the measured samples executed vector retrieval, BM25 retrieval, and RRF rather than returning a cached search response. Every response asserted:

- `search_method = rrf_hybrid`
- `vector_rank > 0`
- `bm25_rank > 0`
- `rrf_score > 0`
- `fallback_triggered = false`
- the expected root node, plus the expected neighbor for the one-hop shape

| Workload                   | Transport |  Throughput |    Mean |     P50 |     P95 |     P99 |     Max |
| -------------------------- | --------- | ----------: | ------: | ------: | ------: | ------: | ------: |
| Full RRF retrieval         | HTTP      | 3,201 req/s |  312 us |  275 us |  380 us |  502 us | 4.50 ms |
| Full RRF retrieval         | Bolt      |   947 req/s | 1.06 ms |  983 us | 1.27 ms | 1.91 ms | 5.07 ms |
| Full RRF retrieval + 1 hop | HTTP      | 1,901 req/s |  525 us |  487 us |  650 us |  755 us | 4.87 ms |
| Full RRF retrieval + 1 hop | Bolt      |   610 req/s | 1.64 ms | 1.52 ms | 2.13 ms | 4.75 ms | 6.43 ms |

Reproduce the focused run:

```bash
NORNICDB_TRAVERSAL_RRF_ONLY=1 \
NORNICDB_TRAVERSAL_MATRIX_ITERS=800 \
NORNICDB_TRAVERSAL_MATRIX_MIN_SAMPLES=800 \
NORNICDB_TRAVERSAL_MATRIX_WARMUP=10 \
go test -tags=e2e ./testing/e2e \
  -run '^TestVectorTraversalShapeMatrix_BoltVsHTTP$' -count=1 -v
```

The following local and remote tables are historical measurements from the larger 67K-node direct-vector workload. They are retained for comparison and should not be compared as before/after measurements with the self-contained full-RRF fixture.

## Historical Local Direct-Vector Results

| Workload       | Transport |   Throughput |   Mean |    P50 |     P95 |     P99 |     Max |
| -------------- | --------- | -----------: | -----: | -----: | ------: | ------: | ------: |
| Vector only    | HTTP      | 19,342 req/s | 511 us | 470 us |  750 us |  869 us | 1.02 ms |
| Vector only    | Bolt      | 22,309 req/s | 444 us | 428 us |  629 us |  814 us |  968 us |
| Vector + 1 hop | HTTP      | 11,523 req/s | 859 us | 699 us | 1.54 ms | 3.46 ms | 4.71 ms |
| Vector + 1 hop | Bolt      | 13,291 req/s | 747 us | 637 us | 1.29 ms | 3.24 ms | 4.47 ms |

## Traversal queries

| Depth | Transport |   Throughput |   Mean |    P50 |     P95 |     P99 |     Max |
| ----: | --------- | -----------: | -----: | -----: | ------: | ------: | ------: |
|     1 | HTTP      | 23,492 req/s | 419 us | 365 us |  773 us | 1.00 ms | 1.50 ms |
|     1 | Bolt      | 24,668 req/s | 402 us | 386 us |  575 us |  784 us | 2.59 ms |
|     2 | HTTP      | 19,257 req/s | 514 us | 415 us | 1.00 ms | 2.29 ms | 5.81 ms |
|     2 | Bolt      | 25,188 req/s | 393 us | 390 us |  508 us |  617 us |  747 us |
|     3 | HTTP      | 18,105 req/s | 548 us | 541 us |  816 us | 1.22 ms | 2.47 ms |
|     3 | Bolt      | 22,212 req/s | 446 us | 427 us |  572 us |  754 us | 2.42 ms |
|     4 | HTTP      | 21,793 req/s | 453 us | 368 us |  789 us | 1.35 ms | 4.23 ms |
|     4 | Bolt      | 25,035 req/s | 396 us | 387 us |  517 us |  612 us |  764 us |
|     5 | HTTP      | 21,884 req/s | 450 us | 369 us |  786 us | 1.10 ms | 4.09 ms |
|     5 | Bolt      | 25,230 req/s | 393 us | 389 us |  499 us |  627 us |  985 us |
|     6 | HTTP      | 18,715 req/s | 528 us | 412 us | 1.15 ms | 3.19 ms | 3.53 ms |
|     6 | Bolt      | 24,487 req/s | 403 us | 399 us |  509 us |  607 us |  720 us |

> Bolt is nearly zero allocation. this was under concurrent load with mixed http and bolt queries. The tail latency spikes are from GC calls from hitting the http path at the same time. Bolt is far more efficient than HTTP for tail latency.

## Historical Remote Direct-Vector Results

Client-to-server latency was about **110 ms**.

| Workload       | Environment |      P50 |
| -------------- | ----------- | -------: |
| Vector only    | Remote GCP  | 110.7 ms |
| Vector + 1 hop | Remote GCP  | 112.9 ms |

The practical result is straightforward: once local compute for hybrid retrieval is in low single-digit milliseconds, network RTT dominates the user-visible latency budget.

## Why This Matters

Most systems make this query shape a composition problem:

1. embed the query
2. call a vector store
3. move the results into a graph store or application layer
4. expand neighbors and shape the result there

NornicDB keeps that inside one execution engine. The benchmark does not prove every workload is constant-time, but it does show that shallow hybrid retrieval can stay tight enough locally that deployment topology matters more than extra database-side micro-optimizations.

## Caveats

- These are **single-node** measurements.
- The dataset is **not billion-scale**.
- Remote throughput is **latency-bound**, not compute-bound.
- These numbers are useful for query-shape comparison, not as a blanket claim for every graph or vector workload.

## Full-RRF Verification Queries

Full RRF retrieval:

```bash
curl -s -u "$NORNIC_USERNAME:$NORNIC_PASSWORD" "$ENDPOINT" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{
    "statements":[
      {
        "statement":"CALL db.retrieve($request) YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered RETURN node.originalText AS originalText, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered ORDER BY rrf_score DESC LIMIT 5",
        "parameters":{"request":{"query":"chain baseline","embedding":[0.95,0.05,0.0],"limit":5,"types":["OriginalText"]}},
        "resultDataContents":["row"]
      }
    ]
  }'
```

Full RRF retrieval + one-hop graph traversal:

```bash
curl -s -u "$NORNIC_USERNAME:$NORNIC_PASSWORD" "$ENDPOINT" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{
    "statements":[
      {
        "statement":"CALL db.retrieve($request) YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered MATCH (node)-[:BENCH_HOP]->(neighbor:BenchmarkHop) RETURN node.originalText AS originalText, elementId(neighbor) AS neighborID, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered ORDER BY rrf_score DESC LIMIT 5",
        "parameters":{"request":{"query":"chain baseline","embedding":[0.95,0.05,0.0],"limit":5,"types":["OriginalText"]}},
        "resultDataContents":["row"]
      }
    ]
  }'
```

## Related Reading

- [Benchmarks vs Neo4j](benchmarks-vs-neo4j.md)
- [Retrieval Recall Benchmark](retrieval-recall-benchmark.md)
- [Graph-RAG: Typical Distributed vs NornicDB In-Memory](../architecture/graph-rag-nornicdb-comparison.md)
- [Canonical Graph + Mutation Log Guide](../user-guides/canonical-graph-ledger.md)
