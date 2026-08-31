# Hybrid Query Benchmarks

This benchmark focuses on the query shape that matters most for NornicDB's positioning: semantic retrieval followed by graph expansion in the same engine.

The goal is not to claim a universal leaderboard result. The goal is to show what happens when vector search and one-hop graph traversal share one execution path instead of being stitched together across multiple systems.

## Summary

- **Vector-only** queries stayed in sub-millisecond to low-millisecond territory locally, depending on transport.
- **Vector + one-hop graph traversal** added a small incremental cost locally.
- **Full RRF retrieval** averaged 19.47 ms over Bolt and 20.40 ms over HTTP on the production-scale fixture.
- **Full RRF retrieval + one-hop graph traversal** averaged 20.00 ms over Bolt and 20.28 ms over HTTP.
- **Remote latency tracked client-to-server RTT**, which means end-to-end latency became network-bound rather than database-bound.

## Original 67K Direct-Vector Benchmark

### Test Setup

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

### Local Results

| Workload       | Transport |   Throughput |   Mean |    P50 |     P95 |     P99 |     Max |
| -------------- | --------- | -----------: | -----: | -----: | ------: | ------: | ------: |
| Vector only    | HTTP      | 19,342 req/s | 511 us | 470 us |  750 us |  869 us | 1.02 ms |
| Vector only    | Bolt      | 22,309 req/s | 444 us | 428 us |  629 us |  814 us |  968 us |
| Vector + 1 hop | HTTP      | 11,523 req/s | 859 us | 699 us | 1.54 ms | 3.46 ms | 4.71 ms |
| Vector + 1 hop | Bolt      | 13,291 req/s | 747 us | 637 us | 1.29 ms | 3.24 ms | 4.47 ms |

### Traversal Queries

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

> Bolt is nearly zero allocation. This was measured under concurrent load with mixed HTTP and Bolt queries. The tail-latency spikes came from garbage collection triggered by the HTTP path. Bolt is more efficient than HTTP for tail latency.

### Remote Results

Client-to-server latency was about **110 ms**.

| Workload       | Environment |      P50 |
| -------------- | ----------- | -------: |
| Vector only    | Remote GCP  | 110.7 ms |
| Vector + 1 hop | Remote GCP  | 112.9 ms |

Once local direct-vector compute is in the sub-millisecond range, network RTT dominates the user-visible latency budget.

## Larger 300K-Node Full-RRF Benchmark

Run on 2026-08-28 at revision `c27fff0fbfd2` using the real BGE-M3 fixture in
`data/test-200kembed`. The writable data directory was cloned from that source
before mutation. The benchmark waited for the search readiness endpoint to
report `ready` and verified the node and edge counts before measuring.

### Test Setup

| Item                 | Value                                        |
| -------------------- | -------------------------------------------- |
| Nodes                | 300,825                                      |
| Deterministic edges  | 1,000,000                                    |
| Stored embeddings    | 221,625                                      |
| Embedding dimensions | 1,024                                        |
| Model                | Local BGE-M3 GGUF with Metal GPU offload     |
| Vector index         | HNSW                                         |
| Request count        | 800 per query shape and transport            |
| Query shapes         | Full RRF retrieval; Full RRF retrieval + hop |

Local environment:

- Apple M3 Max
- 64 GB RAM
- Source build with the `localllm` tag

The server used no search-result or embedding cache and had zero background
embedding workers. Every request used a unique query cache key, so every sample
executed vector retrieval, BM25 retrieval, and RRF fusion. Each response
asserted `search_method = rrf_hybrid`, a positive RRF score, at least one source
rank, and `fallback_triggered = false`; the hop shape also required a neighbor.

### Results

| Workload                   | Transport |   Throughput |       Min |      Mean |       P50 |       P95 |       P99 |       Max |
| -------------------------- | --------- | -----------: | --------: | --------: | --------: | --------: | --------: | --------: |
| Full RRF retrieval         | Bolt      | 51.357 ops/s | 18.445 ms | 19.468 ms | 19.272 ms | 21.130 ms | 23.400 ms | 24.363 ms |
| Full RRF retrieval         | HTTP      | 49.011 ops/s | 18.668 ms | 20.401 ms | 19.549 ms | 23.963 ms | 32.526 ms | 39.108 ms |
| Full RRF retrieval + 1 hop | Bolt      | 50.005 ops/s | 18.636 ms | 19.995 ms | 19.604 ms | 22.509 ms | 25.352 ms | 30.784 ms |
| Full RRF retrieval + 1 hop | HTTP      | 49.312 ops/s | 18.794 ms | 20.276 ms | 19.719 ms | 22.851 ms | 31.073 ms | 42.243 ms |

Reproduce the focused run:

```bash
NORNICDB_LARGE_RRF_E2E=1 \
NORNICDB_LARGE_RRF_HTTP_ADDR=127.0.0.1:17474 \
NORNICDB_LARGE_RRF_BOLT_ADDR=127.0.0.1:17687 \
NORNICDB_LARGE_RRF_DATABASE=translations \
NORNICDB_LARGE_RRF_EDGES=1000000 \
NORNICDB_LARGE_RRF_WARMUP=3 \
NORNICDB_LARGE_RRF_ITERS=800 \
go test -tags=e2e ./testing/e2e \
  -run '^TestLargeDatasetRRFRoundTrip_BoltVsHTTP$' -count=1 -v
```

The address variables attach the test to an already configured benchmark server.
Omit them to let the test copy-on-write clone `data/test-200kembed`, build the
local-embedding binary, and manage the server itself.

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

## Verification Queries

Vector-only:

```bash
curl -s -u "$NORNIC_USERNAME:$NORNIC_PASSWORD" "$ENDPOINT" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{
    "statements":[
      {
        "statement":"CALL db.index.vector.queryNodes('\''idx_original_text'\'', $topK, $text) YIELD node, score RETURN node.originalText AS originalText, score ORDER BY score DESC LIMIT $topK",
        "parameters":{"text":"get it delivered","topK":5},
        "resultDataContents":["row"]
      }
    ]
  }'
```

Vector + one-hop graph traversal:

```bash
curl -s -u "$NORNIC_USERNAME:$NORNIC_PASSWORD" "$ENDPOINT" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d '{
    "statements":[
      {
        "statement":"CALL db.index.vector.queryNodes('\''idx_original_text'\'', $topK, $text) YIELD node, score MATCH (node:OriginalText)-[:TRANSLATES_TO]->(t:TranslatedText) WHERE t.language = $targetLang RETURN node.originalText AS originalText, score, t.language AS language, coalesce(t.auditedText, t.translatedText) AS translatedText ORDER BY score DESC, language LIMIT $topK",
        "parameters":{"text":"get it delivered","topK":5,"targetLang":"es"},
        "resultDataContents":["row"]
      }
    ]
  }'
```

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
