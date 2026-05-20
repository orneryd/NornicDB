---
name: nornicdb-vector-search
description: Run vector and full-text indexes from Cypher in NornicDB — CREATE/DROP VECTOR INDEX, CREATE/DROP FULLTEXT INDEX, db.index.vector.queryNodes / queryRelationships, db.index.fulltext.queryNodes, similarity functions, dimensions. Use when building semantic search, kNN, or BM25 lookups in NornicDB; the API is Neo4j-compatible Cypher with NornicDB extensions.
---

# Vector & Full-Text Search (Cypher API)

NornicDB exposes Neo4j-compatible vector and full-text indexes plus search procedures. Most queries that work on Neo4j 5 vector indexes work unchanged on NornicDB; the extensions are documented inline.

## Vector indexes

### Create

DDL form (Neo4j-compatible):

```cypher
CREATE VECTOR INDEX docEmbeddings IF NOT EXISTS
FOR (n:Document) ON (n.embedding)
OPTIONS { indexConfig: {
  `vector.dimensions`: 1024,
  `vector.similarity_function`: 'cosine'   -- 'cosine' | 'euclidean' | 'dot'
}}
```

Procedure form (handy from drivers that prefer CALL):

```cypher
CALL db.index.vector.createNodeIndex(
  'docEmbeddings',     -- index name
  'Document',          -- node label
  'embedding',         -- property holding the vector
  1024,                -- dimensions
  'cosine'             -- similarity function
)
```

For relationship vectors:

```cypher
CALL db.index.vector.createRelationshipIndex(
  'edgeEmbeddings', 'CO_ACCESSED', 'embedding', 1024, 'cosine'
)
```

Drop:

```cypher
DROP INDEX docEmbeddings IF EXISTS
-- or
CALL db.index.vector.drop('docEmbeddings')
```

### Similarity functions

| Name | Range | Use when |
|---|---|---|
| `cosine` (default) | [-1, 1] mapped to [0, 1] for ranking | Most embeddings — the model trained with normalized cosine |
| `dot` | unbounded; valid only on normalized vectors | Speed-optimized cosine on guaranteed-unit-norm vectors |
| `euclidean` | distance, not similarity | Use when the model was trained with L2 distance |

### Dimensions

Every node indexed under a vector index must have the same dimension as the index's configured `vector.dimensions`. Mismatched dimensions are silently skipped on write, and produce `400 Bad Request` from the search HTTP endpoint when queried.

| Model | Dim |
|---|---|
| `all-MiniLM-L6-v2` | 384 |
| `bge-m3`, `mxbai-embed-large`, `e5-large` | 1024 |
| `text-embedding-3-small` | 1536 |
| `text-embedding-3-large` | 3072 |

### Query

The flagship procedure accepts three input shapes. Pick whichever fits the call site:

```cypher
-- 1. With an explicit vector (Neo4j-compatible)
CALL db.index.vector.queryNodes('docEmbeddings', 10, [0.1, 0.2, ...])
YIELD node, score
RETURN node.title, score

-- 2. With a parameter
:param queryVector => [0.1, 0.2, ...]
CALL db.index.vector.queryNodes('docEmbeddings', 10, $queryVector)
YIELD node, score
RETURN node, score

-- 3. With a string — NornicDB extension
-- The server embeds the string using the configured embedder.
CALL db.index.vector.queryNodes('docEmbeddings', 10, 'machine learning patterns')
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

For relationships use `db.index.vector.queryRelationships(indexName, k, query)`. Yields `(relationship, score)`.

### Filter results

The procedure already orders by score descending. Wrap with WHERE / SKIP / LIMIT for app-side filtering:

```cypher
CALL db.index.vector.queryNodes('docEmbeddings', 50, $queryVector)
YIELD node, score
WHERE node.tenantId = $tenant AND score >= 0.6
RETURN node.id, score
LIMIT 10
```

### Server-side embedding

`db.index.vector.embed` returns an embedding for arbitrary text using the configured embedder. Useful for chained calls or multi-stage retrieval:

```cypher
CALL db.index.vector.embed('zero-trust authentication')
YIELD embedding
CALL db.index.vector.queryNodes('docEmbeddings', 20, embedding)
YIELD node, score
RETURN node, score
```

Requires `NORNICDB_EMBEDDING_ENABLED=true`. See the `nornicdb-managed-embeddings` skill.

## Full-text (BM25) indexes

### Create

```cypher
CREATE FULLTEXT INDEX docFulltext IF NOT EXISTS
FOR (n:Document) ON EACH [n.title, n.content]

-- With analyzer option
CREATE FULLTEXT INDEX docFulltext IF NOT EXISTS
FOR (n:Document) ON EACH [n.title, n.content]
OPTIONS { analyzer: 'standard' }

-- Procedure form
CALL db.index.fulltext.createNodeIndex(
  'docFulltext',
  ['Document'],
  ['title', 'content']
)

-- Available analyzers
CALL db.index.fulltext.listAvailableAnalyzers()
```

For relationships: `db.index.fulltext.createRelationshipIndex(name, types[], properties[])`.

Drop:

```cypher
DROP INDEX docFulltext IF EXISTS
CALL db.index.fulltext.drop('docFulltext')
```

### Query

```cypher
CALL db.index.fulltext.queryNodes('docFulltext', 'authentication security')
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
LIMIT 10

CALL db.index.fulltext.queryRelationships('edgeFulltext', 'co-occurrence')
YIELD relationship, score
RETURN relationship, score
```

`score` is BM25; higher is better. NornicDB's BM25 indexer also indexes properties beyond those listed in `ON EACH` for substring lookup, but only the listed properties contribute to the priority ranking.

## Discovery

```cypher
SHOW INDEXES                         -- standard Cypher form
CALL db.indexes() YIELD name, type, labelsOrTypes, properties, state
CALL db.index.stats() YIELD name, type, totalEntries, uniqueValues, selectivity
```

## Hybrid + RAG

For combined vector + BM25 + RRF + optional rerank, see the `nornicdb-rag-procedures` skill (`db.retrieve`, `db.rretrieve`, `db.rerank`, `db.infer`).

## Common pitfalls

- **Dimensions must match.** Build embeddings with the model the index expects. Mixing models silently corrupts results.
- **Cosine vs dot vs euclidean.** Pick the function that matches the model's training objective. `cosine` is the right default unless you know otherwise.
- **String-in-vector procedure** is a NornicDB extension. Drivers written against Neo4j may strip/escape the input.
- **`db.index.vector.queryNodes` requires the index to exist.** It does not auto-create indexes.
- **Vector index applies to nodes that have the property set.** Nodes without the embedding property are simply not searchable through that index.
- **Properties of type `LIST<FLOAT>` are eligible.** Mixed-type lists or shorter-than-expected vectors are dropped.
- **Suppression interaction.** If decay is enabled, suppressed entities are filtered out of vector and BM25 results unless the query is wrapped in `reveal()` (see the knowledge-policies skill).

## Worked example

```cypher
-- 1. Ensure both indexes
CREATE VECTOR INDEX docEmbeddings IF NOT EXISTS
FOR (n:Document) ON (n.embedding)
OPTIONS { indexConfig: { `vector.dimensions`: 1024, `vector.similarity_function`: 'cosine' } }

CREATE FULLTEXT INDEX docFulltext IF NOT EXISTS
FOR (n:Document) ON EACH [n.title, n.content]

-- 2. Insert with managed embedding
UNWIND $docs AS d
MERGE (n:Document {id: d.id})
SET   n.title   = d.title,
      n.content = d.content
WITH EMBEDDING
RETURN count(n) AS upserted

-- 3. Query both
CALL db.index.vector.queryNodes('docEmbeddings', 20, 'graph database performance')
YIELD node, score
RETURN node.id, node.title, score
```

## Per-database master switches

Each database has independent BM25 and vector master switches plus warming triggers. Defaults reproduce today's behaviour: both enabled, both built at startup. Any caller (HTTP, Bolt, GraphQL, gRPC, Cypher procedures) sees the same contract because the gating lives on `search.Service`.

| Setting (per DB or global)                  | What changes                                                                                                  |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `NORNICDB_SEARCH_BM25_ENABLED=false`        | No BM25 build, no fulltext docs ingested. `db.index.fulltext.queryNodes` returns empty.                       |
| `NORNICDB_SEARCH_VECTOR_ENABLED=false`      | No ANN substrate built or loaded. `db.index.vector.queryNodes` returns empty + WARN log; query does not error. |
| `NORNICDB_SEARCH_*_WARMING=lazy`            | First inbound search blocks synchronously while the build runs; subsequent calls are warm.                     |
| Both `*_ENABLED=false`                      | `POST /nornicdb/search` returns 503 `search_disabled_for_database`, `retryable: false`.                       |

Health/liveness probes must NOT target `/nornicdb/search` for `warming=lazy` or any `*_enabled=false` database — every probe would either trigger the build or stream `503` responses that look like real failures. Use `/nornicdb/health` (DB-agnostic) or `/admin/databases/{name}/config` (lookup-only) instead.

See [docs/operations/configuration.md#per-database-search-index-control](../operations/configuration.md#per-database-search-index-control) for the full matrix and yaml examples.
