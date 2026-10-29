# Release Notes — Since v1.0.11

> **Scope:** All changes between tag `v1.0.11` and `main` (69 commits, 332 files changed, +34,077 / −13,473 lines).

---

## Table of Contents

1. [Search & Hybrid Retrieval Pipeline](#1-search-hybrid-retrieval-pipeline)
2. [Cypher Query Engine](#2-cypher-query-engine)
3. [Auth & RBAC](#3-auth-rbac)
4. [Heimdall AI Assistant & MCP Integration](#4-heimdall-ai-assistant-mcp-integration)
5. [Storage Engine & WAL](#5-storage-engine-wal)
6. [Server & Per-Database Configuration](#6-server-per-database-configuration)
7. [macOS App & File Indexer](#7-macos-app-file-indexer)
8. [Web UI (Bifrost)](#8-web-ui-bifrost)
9. [Multi-Database, Replication & GraphQL](#9-multi-database-replication-graphql)
10. [Infrastructure & Build](#10-infrastructure-build)
11. [Experimental: Index Persistence & IVF-HNSW at Scale](#11-experimental-index-persistence-ivf-hnsw-at-scale)
12. [Upgrade Notes](#12-upgrade-notes)

---

## 1. Search & Hybrid Retrieval Pipeline

This is the most significant engineering investment in this release. The entire search stack has been re-architected for **large-scale, low-latency retrieval**. Real-world measurements from production logs show end-to-end graph-RAG searches — including live embedding of the incoming query string — completing in **under 10 ms**, even on datasets with millions of nodes and embeddings.

```
2026/02/18 08:01:14 ⏱️ Search timing: method=rrf_hybrid total_ms=0 vector_ms=0 bm25_ms=0
[HTTP] POST /nornicdb/search 200 7.96575ms

2026/02/18 08:01:36 ⏱️ Search timing: method=rrf_hybrid total_ms=0 vector_ms=0 bm25_ms=0
[HTTP] POST /nornicdb/search 200 7.334291ms
```

The full round-trip time includes query string embedding, RRF-fused vector + BM25 retrieval, result hydration, and HTTP serialization. Both queries above hit live data with no warm cache.

---

### 1.1 BM25 v2 — Compact Postings for Million-Scale Datasets

A compact full-text index replaces the previous in-memory token map for large corpora. It is the supported implementation.

**What changed internally:**
- Stores per-document term frequency as compact `uint32` doc-numbers and `uint16` TF values rather than full string IDs per posting — a 4–8× memory reduction per index entry.
- Implements a **top-k pruning / early-exit** loop: candidates whose remaining upper-bound score cannot beat the current competitive minimum are pruned during the scan, not after.
- Query plans (term expansion, IDF weights, suffix upper-bounds) are cached per-query so repeated queries return immediately with zero recomputation.
- Batch index mutations (`IndexBatch`) allow the embedding queue to feed hundreds of nodes in a single lock acquisition, dramatically reducing contention.
- A sorted `lexicon` slice enables O(log n) prefix expansion for partial-word queries.

**Benefit:** A 1M-node dataset that previously required ~2 GB of BM25 index RAM now fits in ~200–400 MB. Indexing throughput also improves 3–5× thanks to batch operations.

---

### 1.2 BM25-Seeded HNSW Construction

HNSW graph quality depends heavily on insertion order. When vectors are inserted randomly, early nodes acquire neighbors by chance and the graph backbone is poorly connected. The fix: use BM25 to identify the most lexically diverse, representative documents and insert those **first**.

**How it works:**

`LexicalSeedDocIDs` scans the BM25 index for the highest-IDF terms — terms that appear in only a small fraction of the corpus and therefore have the most discriminative power. For each of those terms it selects the top documents by term frequency. The default parameters select up to **256 high-IDF terms × 8 top documents per term = up to 2,048 seed nodes** that collectively span the lexical space of the entire corpus.

These seed nodes are inserted into the HNSW graph first, in a dedicated seed-first pass:

```
Pass 1: insert ~2,048 BM25-seeded nodes  → establishes a diverse, well-connected backbone
Pass 2: insert remaining N−2,048 nodes   → each new node finds good neighbors immediately
```

Because the backbone nodes are semantically distributed across the corpus, every subsequent insert can locate a close neighbor in O(log N) steps rather than wandering through the graph looking for a foothold. This dramatically reduces the number of long-range rewiring operations the HNSW algorithm has to perform.

**Measured result:** HNSW construction time for a 1M-embedding corpus dropped from **~27 minutes to ~10 minutes** — a **2.7× speedup** — with no change to recall or graph quality.

The same seeded-ordering is applied for both the in-memory build path and the file-store (`VectorFileStore`) chunked build path. The BM25 index must be built before HNSW construction begins, which is already the case since embeddings are indexed into BM25 as they arrive.

**Why 2.7× is the right number — first-principles verification**

The claim can be checked from the actual HNSW parameters in the codebase (fast preset: M=16, ef\_construction=100).

*Graph layer structure for N = 1,000,000, M = 16:*

| Property | Value |
|---|---|
| Level multiplier `ml` = 1 / ln(16) | 0.3607 |
| P(node at layer ≥ 1) = 1/M | 6.25% |
| Average layers per inserted node | 1.067 |
| Layer-0 max connections `M_max0` = 2×M | 32 |

*Ideal distance computations per insertion (no wasted traversals):*

```
Upper-layer comps:  0.067 avg upper layers × ef_c(100) × M(16)  =   107
Layer-0 comps:      1 layer × ef_c(100) × M_max0(32)            = 3,200
                                                           Total = 3,307
```

93.75% of nodes live only in layer 0 (P(level ≥ 1) = 1/16 = 6.25%), so construction is completely dominated by the layer-0 greedy search.

*Time per distance computation (1024-dim float32, Go runtime, ~4 GB working set):*

| Component | Estimate |
|---|---|
| SIMD compute (partial SSE vectorisation in Go) | ~80–140 ns |
| Cache miss (random access into 4 GB graph, avg L3/DRAM) | ~80 ns |
| **Total** | **~160–220 ns** |

*Ideal base time for 1M insertions at 160 ns/op:*

```
1,000,000 inserts × 3,307 ops × 160 ns = 529 seconds ≈ 8.8 minutes
```

*The β (wasted-work) factor — what accounts for the gap above the ideal:*

With **random insertion order** the construction degrades in three phases:

| Phase | Nodes | β (overhead multiplier) | Cause |
|---|---|---|---|
| A — sparse | 0–10K (1%) | ×1.0 | Few nodes, search terminates fast but selects poor neighbors |
| B — uneven | 10K–200K (19%) | ×2.5–3.5 | Dead-end traversals: greedy search enters dense sub-clusters and cannot escape cheaply |
| C — snowball | 200K–1M (80%) | ×3.0–4.5 | Phase-B's poor neighbors become routing hubs; every future insertion navigates around them; Go GC pressure adds ~15–20% |

Weighted average β\_random across the full build: **≈ 3.0** *(implied by the measured 27 min)*:

```
27 min / 8.8 min (ideal) = β_random = 3.07
```

With **BM25-seeded insertion order**, 2,048 diverse nodes are inserted first and form a well-connected backbone. Any subsequent node can reach a backbone node in ≈ log₁₆(1,000,000 / 2,048) ≈ **2.2 hops** from the entry point:

```
99.8% of insertions find a close neighbor immediately → β_seeded ≈ 1.13
10 min / 8.8 min (ideal) = β_seeded = 1.13  ✓
```

*Speedup ratio from first principles:*

```
β_random / β_seeded  =  3.07 / 1.13  =  2.72×  ≈ claimed 2.7×  ✓
```

Both absolute timings (27 min, 10 min) and their ratio are fully consistent with theory. β\_seeded = 1.13 means the seeded build runs within **13% of the theoretical minimum** for this parameter set — essentially optimal construction with no wasted traversals.

**k-means++ seeding (same mechanism, different beneficiary)**

The same `LexicalSeedDocIDs` output is passed to k-means centroid initialisation via the `bm25+kmeans++` seed mode (the default when k-means is enabled). Standard k-means++ selects initial centroids with D²-weighted probability; seeding with BM25 documents that already span the lexical space means the initial centroids start pre-spread across the semantic space. For k=1,000 clusters on 1M nodes:

```
Cost per k-means iteration: 1,000 × 1,000,000 × 160 ns = ~160 seconds = ~2.7 min
Standard k-means++:         ~15–20 iterations to convergence  →  ~45 min
BM25-seeded k-means++:       ~8–12 iterations to convergence  →  ~27 min
Speedup:                    ~1.7×
```

Both HNSW and k-means benefit from the same 2,048-node BM25 seed set with zero additional cost — the seeds are computed once from the already-built BM25 index.

The HNSW graph consumes this lexical metadata automatically. Its metadata
budget follows the graph's `M` topology parameter, so there is no independent
enable switch or seed-count configuration to drift away from the graph shape.

---

### 1.3 Hybrid Cluster Routing

A new `HybridClusterRouter` combines **semantic** and **lexical** cluster selection to decide which k-means clusters to probe for a given query.

**How it works:**
- After k-means clustering, per-cluster **lexical profiles** are built: the top-N weighted tokens for documents in each cluster.
- At query time, the router probes `defaultN × 2` clusters semantically, then re-scores them by lexical overlap with the query tokens.
- Clusters are ranked by a weighted combination: `w_sem × semantic_score + w_lex × lexical_score`.
- This ensures that a query like `"where to get prescriptions"` routes to the cluster containing pharmacy/medical records even if the embedding is equidistant between two clusters.

```bash
# Tuning (defaults shown)
NORNICDB_VECTOR_ROUTING_MODE=hybrid
NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM=0.7
NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX=0.3
NORNICDB_VECTOR_HYBRID_ROUTING_LEX_TOP_TERMS=64
```

> **Note:** Hybrid cluster routing requires k-means clustering to be enabled. K-means has significant startup cost and is only beneficial at large scale — see [§11](#11-experimental-index-persistence-ivf-hnsw-at-scale) for guidance on when to enable it.

---

### 1.4 Search Result Cache

Search responses are now cached in an LRU cache (default: 1,000 entries, 5-minute TTL) using the same semantics as the Cypher query cache. The cache key includes query string, limit, result types, rerank settings, and MMR flag, so different callers sharing the same parameters share the cached response automatically.

The cache is invalidated when nodes are indexed or removed, ensuring freshness without manual intervention.

---

### 1.5 Stage-2 Reranking

Two new reranking backends are available to improve result precision after the initial RRF retrieval pass:

**Local Cross-Encoder (`LocalReranker`):**
Uses a locally-loaded GGUF cross-encoder model (e.g. `bge-reranker-v2-m3`) to re-score top-k candidates after initial retrieval. This is the most accurate and private option — the reranker reads both the query and the full candidate document and produces a relevance score, unlike the bi-encoder used for initial retrieval.

**LLM Reranker (`LLMReranker`):**
A fail-open reranker that calls Heimdall (or any `LLMFunc`-compatible provider) with a structured prompt. If the LLM errors or returns malformed output, results fall back to the original RRF ranking — it never degrades search quality.

```bash
# Enable reranking
NORNICDB_SEARCH_RERANK_ENABLED=true
NORNICDB_SEARCH_RERANK_PROVIDER=local        # or "ollama", "openai", "http"
NORNICDB_SEARCH_RERANK_MODEL=bge-reranker-v2-m3-Q4_K_M.gguf
```

Or via YAML:
```yaml
search_rerank:
  enabled: true
  provider: local
  model: bge-reranker-v2-m3-Q4_K_M.gguf
```

The reranker loads asynchronously at startup — the server starts immediately and reranking becomes available once the model is ready. If loading fails, the server logs a warning and continues with RRF ordering only.

---

### 1.6 Configurable Embedding Properties

You can now control exactly which node properties are used to build the text that gets embedded. This is useful when nodes have large binary or irrelevant fields you want to exclude.

```bash
# Only embed "title" and "content" properties
NORNICDB_EMBEDDING_PROPERTIES_INCLUDE=title,content

# Embed everything except "raw_html" and "blob"
NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE=raw_html,blob

# Disable label prepending (default: true)
NORNICDB_EMBEDDING_INCLUDE_LABELS=false
```

---

### 1.7 Search Readiness API & UI Progress

A new `IsReady()` method and `GetBuildProgress()` API expose index-build status. The UI now shows an ETA while indexes are being built so users know when full search capability is available, instead of seeing empty results during startup.

```
GET /admin/stats
→ { "search": { "ready": false, "build_phase": "building_hnsw", "progress_pct": 43 } }
```

---

### 1.8 Orphan Detection Improvement

The search service now detects and cleans up **orphaned vector index entries** — vectors whose corresponding nodes have been deleted from storage but whose HNSW/cluster entries were not removed (e.g. due to a crash or race condition). This prevents stale results from appearing in semantic search responses.

---

## 2. Cypher Query Engine

56 files changed. The Cypher engine received correctness fixes for several real-world query patterns, along with improved transactional semantics and better error messages.

### 2.1 Proper Transactional Semantics for Multi-Statement Batches

Cypher execution now correctly wraps multi-statement batches in proper transactions. System commands (`CREATE DATABASE`, `DROP DATABASE`, `SHOW DATABASES`) are detected and routed directly without the async write path, preventing them from interleaving with graph mutations.

A `transactionStorageWrapper` is introduced so that within a `BEGIN`/`COMMIT` block all writes go to the same BadgerDB transaction, giving correct read-your-own-writes semantics:

```cypher
BEGIN
CREATE (a:Person {name: 'Alice'})
MATCH (a:Person {name: 'Alice'}) RETURN a.name
COMMIT
-- a.name = "Alice" is visible within the same transaction
```

### 2.2 SET with Expressions Evaluated at Runtime

Previously, `SET` assignments parsed the right-hand side as a literal in certain code paths. Expressions like `reduce(...)`, list concatenation (`list1 + list2`), and indexed list access (`list[n]`) were stored as raw strings instead of being evaluated.

This is fixed: `SET` now evaluates the full RHS expression with row/alias scope before persisting the value.

```cypher
-- Now works correctly: reduce() evaluated at runtime
MATCH (f:File {path: $path})
SET f.file_tags = reduce(acc = '', t IN tags | acc + ',' + t)

-- List + concatenation also works
SET f.all_tags = f.existing_tags + ['newTag']
```

### 2.3 SET Assignment Splitting Handles `{}` and `[]` in All Positions

The SET clause parser was splitting on commas inside object literals `{}` and array literals `[]`, treating them as separate assignments. This caused queries like the following to fail:

```cypher
SET n += {name: 'Alice', scores: [1, 2, 3], meta: {a: 1}}
```

Bracket-depth tracking now handles `(`, `)`, `[`, `]`, `{`, `}` symmetrically, so nested structures in any position are treated as a single token.

### 2.4 SET += with Parameters

`SET n += $props` (map merge from a Bolt parameter) now works correctly when `$props` is passed as a parameter map. Previously the `+=` path did not resolve `$` parameters — the parameter name was stored literally.

```python
# Python driver — now works
session.run(
    "MATCH (n:Item {id: $id}) SET n += $props RETURN n",
    id="item-1",
    props={"price": 99.99, "stock": 42}
)
```

### 2.5 Map Literal Parsing Fix

Map literals embedded inside Cypher strings (e.g. `{key: value, key2: value2}`) were being parsed incorrectly in some positions, causing property-extraction failures. The pattern parser now correctly handles map literals in all positions including relationship property patterns.

### 2.6 Complex Relationship Patterns with Properties

Queries matching relationship patterns that include inline property filters are now handled correctly:

```cypher
MATCH (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
RETURN a.name, b.name, r.since
```

Previously these queries could silently drop the property filter or return incorrect results.

### 2.7 Relationship Pattern in WHERE Clause

`WHERE` clauses containing relationship pattern sub-expressions are now handled:

```cypher
MATCH (n:Person)
WHERE (n)-[:MANAGES]->(:Team)
RETURN n.name
```

### 2.8 OPTIONAL MATCH Compound Patterns

Compound `OPTIONAL MATCH` queries with multiple patterns chained together and WHERE clauses filtering optional results now return correct NULL/non-NULL combinations as per Neo4j semantics.

### 2.9 Parameters in `toLower()` and `DISTINCT`

```cypher
-- Now works
MATCH (n) WHERE toLower(n.name) = toLower($name) RETURN DISTINCT n.name
```

Parameters passed to built-in string functions like `toLower()` and `toUpper()` were not resolved in some code paths. The fix ensures parameter substitution happens before function evaluation.

### 2.10 Better Error Messages for Invalid SET Syntax

When `SET` is used without `variable.property` syntax and the RHS is not a map, the error message now explicitly tells the user what to do:

```
SET assignment requires: variable.property = value  OR  variable = {property: value}
```

### 2.11 ANTLR Grammar: REDUCE and FOREACH Support

The ANTLR-based parser now supports `REDUCE()` and `FOREACH` expressions, which are used heavily by Swift-generated Cypher queries (e.g. from the macOS File Indexer):

```cypher
-- REDUCE: compute aggregate from a list
RETURN reduce(total = 0, x IN [1,2,3] | total + x) AS sum

-- FOREACH: iterate and mutate
FOREACH (tag IN tags | SET f.tag = tag)
```

---

## 3. Auth & RBAC

10 new files. Full per-database role-based access control is now implemented, aligned with Neo4j's security model.

### 3.1 Per-Database Access Control (Allowlist)

Each role can now be restricted to a specific list of databases. This is stored as graph nodes in the system database (label `_RoleDbAccess`) so it survives restarts and is queryable.

**How it works:**
- Built-in roles (`admin`, `editor`, `viewer`) are seeded with an **empty allowlist**, meaning they have access to all databases by default — including dynamically created ones.
- Admins can restrict a role to specific databases via the API.
- A `DatabaseAccessMode` interface is enforced at every entry point: HTTP, Bolt, GraphQL, and Heimdall.

```http
# Grant "viewer" role access only to the "analytics" database
PUT /auth/access/databases
Content-Type: application/json
{"role": "viewer", "databases": ["analytics"]}

# Remove all restrictions (access all databases) — set databases to []
PUT /auth/access/databases
Content-Type: application/json
{"role": "viewer", "databases": []}
```

### 3.2 Roles, Entitlements, and Privileges

A clean hierarchy of auth primitives is now modelled:

- **`Role`** — named principal group (`admin`, `editor`, `viewer`, or user-defined)
- **`Entitlement`** — what a role is allowed to do within a database (read, write, admin)
- **`Privilege`** — fine-grained permission scoped to a database reference

This mirrors Neo4j's `SecurityContext` model so existing Neo4j tooling and mental models apply directly.

### 3.3 Request RBAC Context

A `RequestRBACContext` is now attached to every HTTP/Bolt request, capturing the authenticated principal's roles and resolved database access mode. Handlers no longer need to re-derive permissions per-request.

### 3.4 New Security UI Page

A new `/security/database-access` page in the web UI allows administrators to visually manage which roles can access which databases, with a live list of available databases pulled from the server.

---

## 4. Heimdall AI Assistant & MCP Integration

13 Heimdall files changed, 7 MCP files changed. Heimdall can now run as a **remote service** backed by OpenAI or Ollama, the agentic loop gained native tool-call support with streaming, and MCP tools are now accessible inside the agentic loop.

### 4.1 Remote Heimdall via OpenAI or Ollama

Previously Heimdall only supported a local GGUF model. It now supports three provider modes:

```yaml
# Use OpenAI (gpt-4o-mini or any compatible model)
heimdall:
  provider: openai
  api_url: https://api.openai.com
  api_key: sk-...
  model: gpt-4o-mini

# Use a local Ollama instance
heimdall:
  provider: ollama
  api_url: http://localhost:11434
  model: qwen3:8b

# Use local GGUF (original mode, unchanged)
heimdall:
  provider: local
  model: Heimdall-3.1-8B.gguf
```

```bash
NORNICDB_HEIMDALL_PROVIDER=ollama
NORNICDB_HEIMDALL_API_URL=http://localhost:11434
NORNICDB_HEIMDALL_MODEL=qwen3:8b
```

### 4.2 Streaming Agentic Loop with Native Tool Calls

The agentic loop now uses native tool-call APIs (`GenerateWithTools`) for OpenAI and Ollama providers, rather than prompt-based multi-round heuristics. This means:

- Tool calls are dispatched as structured JSON — eliminating hallucination of tool parameters.
- Up to `MaxAgenticRounds` tool calls can be chained in a single conversation turn.
- Tool execution notifications are streamed as SSE chunks in real-time so the UI shows progress as actions complete.
- Large tool results are truncated per-message to stay within model context limits (128K tokens for GPT-4o).

### 4.3 MCP Tools Available Inside the Agentic Loop

MCP tools (in-process store/recall/discover memory operations) are now injected into the tool list during the agentic loop. Heimdall can autonomously store memories, retrieve relevant context, and run graph queries — all in one turn.

```
User: "Remember that Alice manages DevOps, then find who else is on that team"
Heimdall: [calls store(content="Alice manages DevOps")] ✓
          [calls cypher_query("MATCH (t:Team {name:'DevOps'})<-[:MEMBER]-(p) RETURN p.name")] ✓
          "I've saved that. Alice's team: Bob, Carol, Dave."
```

### 4.4 Plugin Refactor — Actions Decoupled from Core

Built-in Heimdall actions are now external plugins rather than hard-coded handlers. New actions can be added by implementing the plugin interface without modifying the Heimdall package (`plugins/heimdall/` has the reference implementation).

### 4.5 Improved RAG Pipeline

- Skips embedding the query when not needed (e.g. for questions fully answered by BM25).
- Exponential backoff with capped retry for embedding worker failures on remote providers.
- Never silently falls back to fulltext-only when embeddings are configured — retries instead.
- Improved large-conversation context handling in the agentic window.

---

## 5. Storage Engine & WAL

21 files changed. Correctness improvements for the async engine, embedding metadata cleanup, WAL snapshot retention, and removal of legacy code.

### 5.1 AsyncEngine Lock Order Fix (Eliminates Reader-Writer Deadlocks)

`NodeCount()`, `EdgeCount()`, `NodeCountByPrefix()`, and `EdgeCountByPrefix()` previously held the cache read-lock (`ae.mu.RLock`) while calling the underlying BadgerDB engine — a potentially slow I/O operation. This caused write goroutines and flush goroutines to stall waiting for the lock.

The fix snapshots all in-memory cache counts under the lock, releases the lock, and then calls the engine:

```
Before: mu.RLock → engine.NodeCount() [slow I/O while holding lock] → mu.RUnlock
After:  mu.RLock → snapshot cache counts → mu.RUnlock → engine.NodeCount() [lock free]
```

**Benefit:** Write and query throughput no longer degrades when count queries are issued concurrently with bulk imports.

### 5.2 Embedding Metadata Moved to EmbedMeta Struct Field

Previously, embedding metadata (`_embeddings_stored_separately`, `_embedding_chunk_count`) was stored in the node's `Properties` map — polluting user-visible properties and causing subtle issues when Cypher queries projected `n.*`.

These flags are now stored in a dedicated `EmbedMeta` struct field and a boolean `EmbeddingsStoredSeparately` flag on the `Node` type. User properties are no longer contaminated with internal bookkeeping keys.

**Migration:** Existing nodes are transparently upgraded on first read — no manual action required.

### 5.3 Separate Embedding Storage in Bounded Transactions

Large embeddings (multi-chunk nodes) are now written in separate transactions *after* the main node transaction commits, instead of being bundled into the same transaction. This prevents BadgerDB's transaction size limit from being hit for nodes with many embedding chunks (e.g. a file with 50+ chunks).

### 5.4 Delete Cleans Up Pending Embedding Queue

When a node is deleted, it is now also removed from the pending-embeddings index, preventing the embed worker from trying to embed a node that no longer exists. Previously, deleted nodes could re-appear in the embed queue after an async flush if the delete raced with an in-flight embedding attempt.

### 5.5 WAL Snapshot Retention

WAL snapshots now enforce configurable retention policies so disk usage stays bounded:

```bash
# Keep at most 3 snapshot files (default)
NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_COUNT=3

# Also delete snapshots older than 7 days
NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_AGE=168h
```

Snapshots are written as compact JSON (previously indented), reducing snapshot file size by 2–3×.

### 5.6 WAL Atomic Record v2 with Buffered I/O

The WAL atomic record writer (`writeAtomicRecordV2Bufio`) now uses buffered I/O for improved write throughput, especially on high-ingest workloads.

### 5.7 System Commands Use Synchronous Writes

Operations like `CREATE DATABASE` and `DROP DATABASE` bypass the async engine and write synchronously, ensuring they are durable before the API response is returned. This prevents the race where a database appeared created but the metadata hadn't flushed yet.

### 5.8 Legacy Loader Removed

The legacy data loader compatibility shim (for importing data from an earlier bundled format) has been removed. If you need to migrate data from a previous installation, use `scripts/migrate_neo4j_to_nornic.go` instead.

---

## 6. Server & Per-Database Configuration

18 server files changed, 8 config files changed. Two major additions: per-database configuration overrides and write forwarding to the replication leader.

### 6.1 Per-Database Configuration Overrides

Each database can now have its own embedding and search configuration, overriding the global defaults. This allows running different embedding models or similarity thresholds for different databases on the same server.

**API:**
```http
# Get effective config for a database (overrides merged with global)
GET /admin/databases/nornic/config

# Set overrides for a specific database
PUT /admin/databases/nornic/config
Content-Type: application/json
{
  "NORNICDB_EMBEDDING_MODEL": "bge-m3",
  "NORNICDB_EMBEDDING_DIMENSIONS": "1024",
  "NORNICDB_SEARCH_RERANK_ENABLED": "true"
}

# List all valid override keys with types and categories
GET /admin/databases/config/keys
```

Supported key categories: `Embeddings`, `Search`, `HNSW`, `IVF-HNSW`, `K-means`, `Auto-links`, `Auto-TLP`, `Embed worker`.

Overrides are persisted in the system database and applied when each database's search/embedding services are initialized.

### 6.2 Write Forwarding to Replication Leader

In replication deployments, write requests sent to a follower node are now automatically forwarded to the current leader, rather than returning an error. Reads are still served locally. This means clients can connect to any node in the cluster without needing to track which is the leader.

### 6.3 Independent Stage-2 Reranking at Server Startup

The search reranker (§1.5) is now initialized independently of Heimdall during server startup. The server starts immediately and reranking becomes available once the model loads in the background. If the model fails to load, the server logs a warning and continues with RRF ordering only — it never fails to start.

### 6.4 Multi-DB Aware Heimdall Router

Heimdall now receives a full `DatabaseRouter` at initialization, giving it access to all databases. This means Heimdall's agentic loop can query, store, and search across multiple databases in a single conversation.

---

## 7. macOS App & File Indexer

12 files changed. The macOS menu bar app's File Indexer received significant UX improvements and correctness fixes for file tagging.

### 7.1 Tag-Safe File Indexer Labels

File tags are now stored as **node labels** rather than a property string, making them first-class graph citizens that can be used in `MATCH` patterns:

```cypher
-- Find all TypeScript files tagged "backend"
MATCH (f:File:backend) WHERE f.extension = '.ts' RETURN f.path
```

Tags are sanitized before storage: the first character must be a letter or underscore (invalid characters are stripped), ensuring all tags produce valid Cypher label identifiers.

### 7.2 Folder-Level Tag Inheritance

Tags assigned to a watched folder are automatically inherited by all files indexed from that folder. When a folder's tags change, all files in that folder are updated — labels added for new tags and removed for dropped tags — in a single batch Cypher operation.

### 7.3 File Browser View

A new `FileIndexerFileBrowserView.swift` adds a file browser panel inside the File Indexer window where users can browse indexed folders, see individual files, and add/remove per-file tags from the UI without writing Cypher.

### 7.4 File Indexer Reliability Fixes

- Fixed a connection issue where the File Indexer could fail to connect to the local database in certain network configurations.
- Fixed keychain prompt focus — the window no longer loses focus when a keychain authentication dialog appears.
- Folder deletion now reliably removes the corresponding database from the graph.
- Dark mode: the dimensions control and contrast settings are now visible in dark mode.

### 7.5 Reranking Model in macOS Installer

The macOS installer and preset configuration now includes a reranking model setting (replacing K-means presets which are now under Settings). The installer guides users through selecting and downloading a reranker model during first-run setup.

---

## 8. Web UI (Bifrost)

17 UI files changed.

### 8.1 Database Selector in Browser

The Browser page now has a **database selector** in the toolbar. All queries, searches, and node edits target the selected database. The selection is persisted in the URL as `?database=<name>` so deep-links work correctly.

```
/browser?database=analytics  → all queries run against "analytics"
```

### 8.2 RRF Score and Rank Visibility

Search result nodes in the graph browser now show their retrieval scores in the node details panel:

- **RRF Score** — combined rank-fusion score
- **Vector Rank** — position in the semantic search results
- **BM25 Rank** — position in the fulltext results

This helps diagnose search quality and understand why a particular node was ranked where it was.

### 8.3 Search Panel Shows Active Database

The search panel now shows which database is being searched (`Searching in: analytics`) so users always know the scope of their queries.

### 8.4 Search Readiness ETA

While indexes are building on startup, the UI displays a progress indicator and estimated time until full search capability is available, preventing confusing "no results" responses during the build window. For a 1m embeddings dataset, rebuilding the HNSW can take ~10 minutes on a M3 Max 64GB Macbook Pro,

### 8.5 Database Access Management Page

A new `/security/database-access` page exposes the per-database RBAC allowlist (§3.1) with a visual editor for managing which roles can access which databases.

### 8.6 Admin Users: Dynamic Role List

The Admin Users page now loads available roles from the server (`/auth/roles`) rather than using a hard-coded list, so custom roles defined by admins are immediately available in the create/edit user form.

---

## 9. Multi-Database, Replication & GraphQL

### 9.1 Multi-Database Embedding Stats

Embedding statistics in `/admin/stats` now aggregate across all open databases, not just the default database. This gives a complete picture of total pending embeddings and model usage across the entire server.

### 9.2 GraphQL Namespace-Aware Helpers

GraphQL resolvers now use namespace-aware helper functions, ensuring that queries routed through GraphQL correctly scope node/edge lookups to the target database rather than falling back to the default.

### 9.3 Replication Transport Improvements

- Raft transport stability improvements for multi-region deployments.
- Handler-side improvements for replication of large embedding payloads.

---

## 10. Infrastructure & Build

### 10.1 Script: Convert Search Index to msgpack

A new utility script (`scripts/convert_search_index_to_msgpack/`) converts legacy gob-format search indexes to the new msgpack format. Run this once after upgrading if you have persisted indexes from v1.0.11:

```bash
go run scripts/convert_search_index_to_msgpack/main.go \
  --data-dir /var/nornicdb/data \
  --database nornic
```

### 10.2 Seed/Search Performance Testing Script

A new `scripts/seed_and_search.py` Python script provides an end-to-end benchmark: seed N nodes with text content and measure search latency. Useful for validating performance on your hardware before deploying to production.

```bash
python3 scripts/seed_and_search.py \
  --url http://localhost:7687 \
  --nodes 100000 \
  --queries 1000
```

### 10.3 Dockerfiles Updated for Reranker Model

`Dockerfile.arm64-metal-heimdall` and `Dockerfile.amd64-cuda-heimdall` now include the reranking model (`bge-reranker-v2-m3-Q4_K_M.gguf`) alongside the Heimdall GGUF model in Docker images that ship with AI capabilities.

### 10.4 Windows Build Target Fix

The Windows build target in the Makefile was fixed to correctly set CGO and cross-compilation flags for the `vulkan` native variant.

### 10.5 CONTRIBUTORS.md

A `CONTRIBUTORS.md` file has been added to the repository root acknowledging project contributors.

---

## 11. Experimental: Index Persistence & IVF-HNSW at Scale

> **These features are disabled by default** and gated behind `NORNICDB_PERSIST_SEARCH_INDEXES=true`. They are production-ready for the right use case but carry tradeoffs that make them unsuitable as universal defaults. Read this section before enabling them.

---

### 11.1 File-Backed Vector Store (VectorFileStore)

A new append-only `VectorFileStore` stores normalized float32 vectors on disk with only an `id→file-offset` map kept in RAM.

**How it works:**
- Each vector is written once to a binary `.vec` file (magic header + per-record id-length, id bytes, float32 data). The in-memory map holds only string→int64 offsets — roughly **16 bytes per vector** instead of `dimensions × 4` bytes (e.g. 4,096 bytes for a 1024-dim vector).
- Stale records from updates/deletes are tracked as `obsoleteCount`; a compaction pass reclaims space without interrupting reads.
- A `.meta` msgpack file persists the `id→offset` map and a `build_indexed_count` checkpoint so `BuildIndexes` can **resume from where it left off** across restarts rather than rebuilding from scratch.

**Primary benefit:** `BuildIndexes` can ingest large datasets without holding all vectors in RAM simultaneously. This matters most during the initial import of a very large corpus (tens of millions of nodes).

**Tradeoff:** Requires fast NVMe storage for acceptable build-time I/O. Spinning disk will make initial index construction significantly slower.

---

### 11.2 HNSW & IVF-HNSW Persistence — Skip Rebuild on Restart

Both HNSW and IVF-HNSW indexes are **persisted to disk and restored on startup** when `NORNICDB_PERSIST_SEARCH_INDEXES=true`.

```yaml
# nornicdb.example.yaml
database:
  persist_search_indexes: true
```

**What this means:**
- Without persistence: every restart triggers a full HNSW rebuild. At 1M embeddings this takes ~30 seconds; at 10M it can take 5–15 minutes; at 40M+ it can take hours.
- With persistence: the index is restored in seconds from the saved graph file.
- K-means clustering is skipped after restore when the cluster count hasn't changed, saving additional startup time.
- `build_settings` metadata is saved alongside the index; if the HNSW config (M, ef_construction, dimensions) changes, NornicDB detects the mismatch and rebuilds automatically.

**Tradeoff:** Index files can be large. At 10M × 1024-dim embeddings the HNSW graph file is ~40 GB. Ensure your data directory has sufficient disk space before enabling persistence.

---

### 11.3 Index Persistence Debounce & Non-Blocking Saves

When persistence is enabled, index saves (BM25, vector, HNSW) are debounced and written in a background goroutine so they never block query execution or writes. The persist delay is configurable:

```bash
NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC=30   # default: wait 30s idle before writing
```

BM25 dirty-tracking ensures saves only happen when the index actually changed. The `.meta`/`.vec` files are always written before the BM25 index to ensure atomic resume semantics across an unclean shutdown.

---

### 11.4 IVF-HNSW and K-means Clustering: Scale Guidance

#### When to keep K-means and IVF-HNSW **off** (the default)

For the vast majority of deployments — up to roughly **5–10 million embeddings** — a flat HNSW index delivers the best combination of recall, latency, and operational simplicity. There is no clustering overhead, no cluster routing complexity, and the sub-10 ms latency shown in the production logs above was measured on a dataset of this scale with a **flat index**.

**Recommendation: keep `NORNICDB_KMEANS_CLUSTERING_ENABLED=false` (the default) unless you are at or approaching 40–50M embeddings.**

#### When IVF-HNSW starts to pay off

IVF-HNSW breaks even around **40–50 million embeddings**, depending on hardware and embedding dimensions. Below that threshold:

- A flat HNSW index already has O(log N) traversal that is fast enough for sub-10 ms results.
- K-means clustering adds significant startup cost (the first clustering run on 10M vectors can take 10–30 minutes) with no latency benefit.
- IVF cluster routing introduces additional complexity and tuning surface area (`num_clusters`, `probe_count`, routing weights) with no gain at this scale.

At 40–50M+, the HNSW graph becomes large enough that search traversal starts visiting many nodes before converging. IVF-HNSW improves this by first routing to a small number of semantically relevant clusters, then running HNSW only within those clusters — reducing the effective search space by 10–100×.

```bash
# Only enable these at 40M+ embeddings
NORNICDB_KMEANS_CLUSTERING_ENABLED=true
NORNICDB_KMEANS_NUM_CLUSTERS=2000          # rule of thumb: sqrt(N) ≈ sqrt(40M) ≈ 6300, but 1000-3000 is practical
NORNICDB_VECTOR_IVF_HNSW_ENABLED=true
NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE=10000
```

#### Memory requirements at scale

Understanding what hardware you need is critical before planning a large deployment. The dominant cost is the **HNSW index**, which stores each node's vector inline alongside its graph connections (neighbors).

**Memory formula for a single database (1024-dim float32, HNSW M=16):**

| Component | Formula | Per-node cost |
|-----------|---------|---------------|
| HNSW vectors | `N × dims × 4 bytes` | 4,096 bytes |
| HNSW graph edges (layer 0) | `N × 2M × 4 bytes` | 128 bytes |
| HNSW upper layers | `N × ~0.05 × per-node` | ~25 bytes |
| BM25 v2 index | `~500 bytes × N` (varies by text length) | 500 bytes |
| OS + runtime + cache | fixed ~10–20 GB overhead | — |
| **Total per-node** | | **~4,750 bytes** |

**Scale reference table (1024-dim float32 embeddings):**

| Embeddings | HNSW index | BM25 v2 | OS + misc | **Total RAM** | Recommended server |
|------------|-----------|---------|-----------|---------------|-------------------|
| 100K | 400 MB | 50 MB | 15 GB | **~16 GB** | Any developer machine |
| 1M | 4 GB | 500 MB | 15 GB | **~20 GB** | 32 GB RAM |
| 5M | 20 GB | 2.5 GB | 15 GB | **~40 GB** | 64 GB RAM |
| 10M | 40 GB | 5 GB | 15 GB | **~60 GB** | 96–128 GB RAM |
| 20M | 80 GB | 10 GB | 15 GB | **~105 GB** | 128–192 GB RAM |
| 40M | 160 GB | 20 GB | 15 GB | **~195 GB** | 256 GB RAM |
| 50M | 200 GB | 25 GB | 15 GB | **~240 GB** | 384 GB RAM |

**For 512-dim embeddings** (e.g. `nomic-embed-text-v1`, `bge-small-en`), cut the HNSW and vector figures in half:

| Embeddings | HNSW index | BM25 v2 | OS + misc | **Total RAM** |
|------------|-----------|---------|-----------|---------------|
| 10M | 20 GB | 5 GB | 15 GB | **~40 GB** |
| 40M | 80 GB | 20 GB | 15 GB | **~115 GB** |
| 50M | 100 GB | 25 GB | 15 GB | **~140 GB** |

#### What a 40–50M embedding server looks like

To run 40M × 1024-dim embeddings with flat HNSW (the recommended path), you need approximately **256 GB RAM**. Example configurations:

| Option | Spec | Notes |
|--------|------|-------|
| AWS `r6i.8xlarge` | 256 GB RAM, 32 vCPU | ~$2/hr on-demand; sufficient for 40M |
| AWS `r6i.12xlarge` | 384 GB RAM, 48 vCPU | ~$3/hr; comfortable headroom for 50M |
| Hetzner `AX162-R` | 256 GB RAM, 32-core AMD EPYC | ~€3.5/hr dedicated; bare metal |
| Bare metal (DIY) | 2× Xeon / EPYC + 256–512 GB DDR4/5 ECC | Best $/GB at steady-state scale |
| Apple M-series (dev) | M2 Ultra: 192 GB unified RAM | Covers ~35M 1024-dim embeddings |

> **Practical advice:** If you are approaching 40M embeddings, first try flat HNSW with 256 GB RAM. Enable IVF-HNSW only if you observe search latency rising above your target (e.g. >50 ms) or if HNSW recall is degrading. The flat index is simpler to operate and has no clustering warm-up cost.

---

## 12. Upgrade Notes

- **Per-database config** is opt-in and additive — existing deployments are unaffected. The global config remains authoritative unless a database-level override is set.
- **BM25** uses compact postings to reduce index memory for large text corpora.
- **Legacy loader removed** — if you are importing legacy-format data, switch to `scripts/migrate_neo4j_to_nornic.go`.
- **Index persistence** is off by default. Enable with `NORNICDB_PERSIST_SEARCH_INDEXES=true` to avoid rebuild on restart. Read §11 before enabling.
- **K-means and IVF-HNSW** are off by default and should remain off unless you have 40M+ embeddings. See §11.4 for the scale guidance and memory table.
- **Embedding metadata** internal keys (`_embeddings_stored_separately`, `_embedding_chunk_count`) are no longer written to node properties. If your queries filter on these keys, remove those filters — nodes are transparently upgraded on first read.
- **WAL snapshot retention** now defaults to keeping 3 snapshots. Adjust with `NORNICDB_WAL_SNAPSHOT_RETENTION_MAX_COUNT` if you want more history.

---

*Document generated from `v1.0.11..main` — 69 commits, 332 files, +34,077 / −13,473 lines.*
