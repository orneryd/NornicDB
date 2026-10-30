# Hybrid Search

**Complete guide to combined vector + keyword search in NornicDB**

Last Updated: December 2025

For durable START/PULL/DISCARD pagination, complete `id` and
`ranked_then_id` populations, and grouped child passages, see
[Search Continuation](search-continuation.md).

---

## Overview

NornicDB provides a **hybrid search system** that combines three retrieval strategies for best-of-both-worlds results:

1. **Vector Search** — Cosine similarity on high-dimensional embeddings for semantic understanding
2. **BM25 Full-Text Search** — Keyword search with TF-IDF scoring for exact term matching
3. **RRF (Reciprocal Rank Fusion)** — Industry-standard algorithm to merge rankings from both strategies

By default, **both sides see all node properties**:

- **Embedding text / vector search**: embeddings are built from **all node properties plus node labels** by default, excluding built-in metadata fields like `embedding`, `has_embedding`, timestamps, and similar internal keys. This is configurable through embedding include/exclude settings if you want to restrict which properties contribute to embeddings.
- **BM25 full-text search**: BM25 also indexes **all node properties**. A small set of common text fields such as `content`, `text`, `title`, and `name` are added first for better ranking, but searchability is not limited to those fields.

This is the same approach used by Azure AI Search, Elasticsearch, Weaviate, and Google Cloud Search.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Search Service                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  Vector Index   │  │  Fulltext Index │  │   Storage   │ │
│  │  (Cosine Sim)   │  │    (BM25)       │  │   Engine    │ │
│  └────────┬────────┘  └────────┬────────┘  └──────┬──────┘ │
│           │                    │                   │        │
│           └────────────┬───────┘                   │        │
│                        │                           │        │
│              ┌─────────▼─────────┐                 │        │
│              │   RRF Fusion      │─────────────────┘        │
│              │ score = Σ(w/(k+r))│                          │
│              └─────────┬─────────┘                          │
│                        │                                    │
│              ┌─────────▼─────────┐                          │
│              │   Ranked Results  │                          │
│              └───────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Full-Text Search Properties

BM25 indexes **all node properties**, but these properties are treated as **priority fields** for ranking:

| Property       | Description                |
| -------------- | -------------------------- |
| `content`      | Main content field         |
| `text`         | Text content (file chunks) |
| `title`        | Node titles                |
| `name`         | Node names                 |
| `description`  | Node descriptions          |
| `path`         | File paths                 |
| `workerRole`   | Agent worker roles         |
| `requirements` | Task requirements          |

After those priority fields, **all remaining properties are also indexed**. Property names are included alongside values, so searches can match both general text and field-oriented content.

Example: a search for `docker configuration` can match a node through `content`, `title`, or any other indexed property that contains those terms.

BM25 uses NFKC normalization, Unicode case folding, Unicode token splitting,
and exact tokens by default. NornicDB does not ship any language stemmers. If a
database needs stemming, install and select a trusted local plugin as described
in [BM25 Stemmer Plugins](bm25-stemmers.md).

## Embedding Text Inputs

The vector side of hybrid search depends on whatever text was embedded for the node.

By default, managed embeddings are generated from:

- node labels
- all node properties
- excluding built-in metadata/internal fields

That means hybrid search is not limited to a single `content` field unless you configure it that way.

If you set embedding include/exclude options, the vector side follows those rules, while the BM25 side still indexes all properties.

---

## RRF Algorithm

**Formula**: `RRF_score(doc) = Σ (weight_i / (k + rank_i))`

Where:

- `k` = constant (default: 60)
- `rank_i` = rank of document in result set i (1-indexed)
- `weight_i` = importance weight for result set i

### Adaptive Weights

The system automatically adjusts weights based on query characteristics:

| Query Type | Words | Vector Weight | BM25 Weight | Rationale              |
| ---------- | ----- | ------------- | ----------- | ---------------------- |
| Short      | 1-2   | 0.5           | 1.5         | Exact keyword matching |
| Medium     | 3-5   | 1.0           | 1.0         | Balanced               |
| Long       | 6+    | 1.5           | 0.5         | Semantic understanding |

---

## Usage

### REST API

The primary way to run hybrid searches is the REST search endpoint:

```bash
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "machine learning algorithms",
    "limit": 10,
    "labels": ["Document"]
  }'
```

Results include RRF metadata so you can see how each ranking strategy contributed:

```json
{
  "results": [
    {
      "id": "node-42",
      "score": 0.0312,
      "rrf_score": 0.0312,
      "vector_rank": 2,
      "bm25_rank": 1,
      "labels": ["Document"],
      "properties": { "title": "Introduction to ML Algorithms", "...": "..." },
      "matches": [
        { "kind": "chunk", "chunk": 0, "score": 0.83, "property": "title", "start": 0, "end": 29 },
        { "kind": "term", "term": "algorithms", "property": "title", "start": 19, "end": 29 }
      ]
    }
  ]
}
```

### HTTP Search Request Fields

| Field                | Default | Description                                                        |
| -------------------- | ------- | ------------------------------------------------------------------ |
| `query`              | —       | The search text (required)                                         |
| `limit`              | 10      | Maximum number of results                                          |
| `labels`             | all     | Restrict results to nodes with any of these labels                 |
| `filters`            | none    | Candidate filters as `{"key": ["value1", "value2"]}`              |
| `include_properties` | all     | Return only these node property keys                               |
| `exclude_properties` | none    | Omit these keys; exclusion wins over `include_properties`          |
| `database`           | default | Logical database name to search                                    |
| `relationships`      | false   | Search relationships instead of nodes (see below)                  |

Property projection changes only the returned `properties` map. It does not
change matching or ranking. Use `NORNICDB_SEARCH_BM25_PROPERTIES` to control
which stored properties enter BM25 and rerank text, and
`NORNICDB_SEARCH_BM25_LABELS` to keyword-index only nodes carrying one of the
listed labels.

RRF tuning constants (`k`, vector/BM25 weights) are applied internally by the search service and are not exposed as HTTP request fields. For programmatic control over fusion weights or k, use the embedded Go API or the MCP `discover` tool. Internal RRF defaults are `k = 60` with adaptive vector/BM25 weights driven by query length (see the table above).

### Cypher Procedure

You can also run vector search from Cypher using `db.index.vector.queryNodes`:

```cypher
CALL db.index.vector.queryNodes('embeddings', 10, $queryEmbedding)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

See the [Vector Search Guide](vector-search.md) for full details on Cypher-based search.

---

## Fallback Chain

The search automatically falls back when needed:

1. **RRF Hybrid** (if embedding provided)
2. **Vector Only** (if BM25 returns no results)
3. **Full-Text Only** (if no embedding or vector search fails)

## Caching

Search results are cached so that **repeated identical requests** (same query and options) return immediately from memory. The cache is shared by all entry points (HTTP `/nornicdb/search`, Cypher vector procedures, MCP, etc.).

- **Key:** Query text + options (limit, types/labels, rerank, MMR settings). Same inputs ⇒ cache hit.
- **Size:** Up to 1000 entries (LRU); entries expire after 5 minutes (TTL).
- **Invalidation:** The cache is cleared whenever the index changes (node created, updated, or removed), so results stay correct after mutations.

Use the same query and options for repeated calls to benefit from the cache (e.g. same search box query and limit).

---

## Performance (Apple M3 Max)

| Operation     | Scale          | Time   |
| ------------- | -------------- | ------ |
| Vector Search | 10K vectors    | ~8.5ms |
| BM25 Search   | 10K documents  | ~255µs |
| RRF Fusion    | 100 candidates | ~27µs  |
| Index Build   | 38K nodes      | ~5.4s  |

## Per-database search index control

Each database has two orthogonal master switches and two warming triggers. The default is `enabled=true, warming=startup` for both BM25 and vector — today's behaviour. Per-DB overrides via `PUT /admin/databases/{name}/config` always win over the global default.

| BM25         | Vector       | First search request                                                                |
| ------------ | ------------ | ----------------------------------------------------------------------------------- |
| on / startup | on / startup | Hybrid (this guide's default).                                                      |
| on / startup | on / lazy    | Synchronous wait while vector warms; first response includes vector results.        |
| on / lazy    | on / lazy    | Synchronous wait while both warm; first response is fully ranked.                   |
| on / startup | off / —      | Lexical-only 200, response carries `vector_enabled: false`.                         |
| off / —      | on / startup | Vector-only 200 (HNSW falls back to random insertion order; recall slightly lower). |
| off / —      | off / —      | 503 `search_disabled_for_database`, `retryable: false` — permanent.                 |

Search response shapes:

```jsonc
// 200 — both indexes serving
{
  "results": [...],
  "bm25_enabled": true,
  "vector_enabled": true
}

// 200 — vector disabled
{
  "results": [...],   // BM25-only
  "bm25_enabled": true,
  "vector_enabled": false
}

// 503 — both disabled (permanent)
{
  "error": "search is disabled for this database",
  "request_status": "search_disabled_for_database",
  "retryable": false,
  "bm25_enabled": false,
  "vector_enabled": false
}
```

The `bm25_enabled` and `vector_enabled` fields are present on every 200 response so clients can see which path produced the result set.

When BM25 is disabled but vector is enabled, the HNSW build falls back to **random insertion order** (no BM25 lexical seed). This regresses ANN recall slightly compared to the BM25-seeded build. Operators choosing this combination are doing so deliberately for memory or storage reasons; it's a documented tradeoff.

See **[Per-Database Search Index Flags](../operations/configuration.md#per-database-search-index-control)** for yaml examples and the full configuration matrix.

## Relationship Search

Text stored on a relationship, such as why two documents are linked, can be
searched by keyword and by meaning. Relationship search keeps indexes of its
own, so node search is unchanged, and is off by default:

```bash
NORNICDB_SEARCH_RELATIONSHIPS_ENABLED=true   # or search.relationships_enabled: true
```

- **Keyword text**: a relationship's type and every property, or only the
  listed property values: `NORNICDB_SEARCH_RELATIONSHIP_BM25_PROPERTIES`
  (`search.relationship_bm25_properties`) when set, otherwise
  `NORNICDB_SEARCH_BM25_PROPERTIES`, as for nodes.
- **Embedding text**: its properties as
  `NORNICDB_EMBEDDING_RELATIONSHIP_PROPERTIES_INCLUDE` / `_EXCLUDE`
  (`embedding_worker.relationship_properties_include` / `_exclude`) select
  them, each defaulting to `NORNICDB_EMBEDDING_PROPERTIES_INCLUDE` /
  `EXCLUDE`, after `type: <TYPE>` when `NORNICDB_EMBEDDING_INCLUDE_LABELS` is
  on. Relationships are embedded in the background with the database's
  embedder.
- **Only some relationships**: a relationship with none of the listed
  properties has no keyword text and no embedding text, so it isn't found.
  Setting both relationship lists to an opt-in property (for example
  `search_text`) indexes just the relationships that carry it, whatever the
  nodes' lists are.
- **Persistence**: the keyword index is built from storage when the database
  opens. With `NORNICDB_PERSIST_SEARCH_INDEXES`, the vectors are kept beside
  the BM25 index (`bm25.v2.relationships`), so a restart embeds only
  relationships that changed; without it, every relationship is embedded
  again at startup. Existing gob snapshots are converted during the authorized
  v1.4.4 upgrade; see [Recovery format compatibility](../operations/recovery-format-compatibility.md#persisted-relationship-search-vectors).

Search with `relationships: true` over HTTP, or with
`db.retrieveRelationships` in Cypher. Results are ranked by keyword and vector
similarity, fused by reciprocal rank as node results are; `labels` (HTTP) or
`types` (Cypher) filters by relationship type. Each HTTP result has a
`relationship` instead of a `node`:

```bash
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Content-Type: application/json" \
  -d '{"query": "why these papers disagree", "relationships": true, "limit": 5}'
```

```json
[{"relationship": {"id": "r1", "type": "CONTRADICTS", "startNode": "a", "endNode": "b",
  "properties": {"reason": "..."}}, "score": 0.03, "vector_rank": 1, "bm25_rank": 2}]
```

```cypher
CALL db.retrieveRelationships({query: 'why these papers disagree', limit: 5})
YIELD relationship, start, end, score
RETURN type(relationship), start.title, end.title, score
```

A relationship search returns one page: continuation fields (`n`, `qid`,
`mode`, `group_by`, `ranked_limit`) are refused with 400. A relationship hidden
from reads (decay, `db.visibility.suppress`) isn't returned. Without an
embedder, relationship search is keyword-only.

## Related Guides

- [Vector Search](vector-search.md) — dedicated vector search guide with Cypher index examples
- [Data Import/Export](data-import-export.md) — importing data for search indexing
- [Per-Database Search Index Flags](../operations/configuration.md#per-database-search-index-control) — full flag reference
