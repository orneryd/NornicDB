# Hybrid ANN Plan (HNSW CandidateGen + Exact Rerank)

**Goal:** unify all vector search paths (Cypher/Bolt, HTTP search, Qdrant gRPC) behind **one** high-performance pipeline that:

1) uses **approximate candidate generation** when `N` is large  
2) uses **exact reranking** (preferably GPU) for best accuracy  
3) falls back to **exact brute-force** when `N` is small (overhead-free)

This plan assumes the following decisions have been made:
- ✅ Approx + exact rerank once `N` exceeds threshold; exact brute-force for small `N`.
- ✅ GPU rerank is on by default when GPU is available/healthy (automatic).
- ✅ Named vectors are first-class (separate indices).
- ✅ Recall quality targets should be configurable/tunable.
- ✅ ANN indexes are **not persisted** initially; they are rebuilt on start.

---

## 1) Single Pipeline (Same Semantics Everywhere)

All endpoints call the same internal vector pipeline:

1. **CandidateGen(query, k, filters, vectorName) → candidates (size C)**
2. **ExactScore(query, candidates) → exact scores**
3. **Apply payload filters (if not already applied)**
4. **Return top-k**

Notes:
- “ExactScore” means the final ranking is computed with the **true metric** (cosine/dot/euclid).
- GPU accelerates scoring; it does not change semantics.
- For Qdrant payload filters, correctness > speed: we can do “filter after candidate gen” initially, then optimize with payload-aware candidate gen later.

---

## 2) Candidate Generation Strategy (Auto)

We choose candidate generation mode based on **dataset size `N`**, dimension `D`, and whether GPU is available.

### 2.1 Modes

**Mode A: Exact brute-force (small `N`)**
- CandidateGen = all vectors
- ExactScore = CPU SIMD dot/cosine (GPU optional; generally not necessary for small N)
- Best when `N` is small enough that overhead dominates.

**Mode B: HNSW candidate gen + exact rerank (large `N`)**
- CandidateGen = HNSW top-C (approx)
- ExactScore = GPU dot/cosine (default) or CPU if GPU unavailable
- Best scalable default on CPU-only machines too.

**Mode C (optional): K-means routing + exact rerank**
- CandidateGen = pick closest clusters → candidate pool
- ExactScore = GPU dot/cosine
- Use when clustering is enabled and materially improves tail latency at very large `N`.

### 2.2 Default Thresholds (initial proposal)

These are intentionally conservative; we’ll tune from benchmarks.

- `N_small_max`: `5000` (below this, brute-force is often faster than ANN overhead)
- `candidate_multiplier`: `C = max(k * 20, 200)` capped by `max_candidates`
- `max_candidates`: `5000` (guard rail)

We should keep these as config and “auto” selection logic.

---

## 3) Recall/Quality Targets (Configurable)

Yes—this should be tunable.

### 3.1 What we tune

HNSW recall/latency is mostly controlled by:
- `efSearch` (query-time): higher = better recall, slower
- `efConstruction` (build-time): higher = better graph, slower ingest/build
- `M` (graph degree): higher = better recall, more memory, slower build

### 3.2 Config approach

Expose two layers:

1) **Simple “quality preset”**
- `NORNICDB_VECTOR_ANN_QUALITY=fast|balanced|accurate`
  - `fast`: lower efSearch, lower candidate_multiplier
  - `balanced`: reasonable defaults
  - `accurate`: higher efSearch and/or bigger candidate pool

2) **Advanced knobs**
- `NORNICDB_VECTOR_HNSW_M`
- `NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION`
- `NORNICDB_VECTOR_HNSW_EF_SEARCH`
- `NORNICDB_VECTOR_CANDIDATE_MULTIPLIER`
- `NORNICDB_VECTOR_MAX_CANDIDATES`
- `NORNICDB_VECTOR_CPU_RERANK_ENABLED` (default: `true`)
  - When `true`, CPU-only deployments rerank the ANN candidate set with exact CPU SIMD scoring for consistency/accuracy.
  - When `false`, CPU-only deployments may return ANN scores directly (faster but lower/variable recall/precision depending on tuning).
- `NORNICDB_VECTOR_ANN_REBUILD_INTERVAL` (default: `0`)
  - If `>0`, periodically rebuild ANN structures (e.g. HNSW) to handle deletions/updates and keep graph quality high.
  - If `0`, rebuild is manual / on restart only.

### 3.3 Measuring recall

Add a benchmark/test that compares:
- exact brute-force top-k
- ANN top-k (before rerank)
- ANN + exact rerank top-k

Track:
- recall@k (intersection / k)
- latency distribution under concurrency

---

## 4) GPU Exact Rerank (Default On)

### 4.1 Semantics

GPU rerank must compute the exact metric for the chosen distance:
- cosine: dot(normalized(query), normalized(candidate))
- dot: dot(query, candidate) (no normalization)
- euclid: convert to similarity consistently with existing definitions

GPU usage policy:
- GPU acceleration is **automatic** (use GPU when a supported backend is available and healthy; fall back to CPU otherwise).
- Optional “force CPU” switch can exist for debugging/benchmarking only (not required for normal operation).

### 4.2 Data path

We need a fast path to score candidate vectors on GPU without inventing a new embedding store.

What we already have (use this):
- `pkg/gpu.EmbeddingIndex` already maintains:
  - a contiguous CPU-side vector slab (`cpuVectors`)
  - a nodeID→index map (`idToIndex`)
  - GPU-resident buffers when available (Metal/CUDA/Vulkan) plus automatic fallback
  - sync semantics (`SyncToGPU`) and stats
- `pkg/gpu.ClusterIndex` embeds/uses an `EmbeddingIndex` and already supports cluster-assisted search.

So the plan is to **reuse `EmbeddingIndex` as the canonical “GPU-ready embedding store” per vector space** (per `(db, type/collection, vectorName, dims, distance)`), instead of adding another store keyed by numeric IDs.

What we need to add (phased, solid):

- **Phase 1 (must-have): CPU subset scoring**
  - Add `ScoreSubset(query []float32, ids []string)` implemented using:
    - `idToIndex` + contiguous `cpuVectors`
    - SIMD dot/cosine on CPU
  - This is required for hybrid ANN reranking even in CPU-only deployments and does not require GPU kernels.

- **Phase 2 (optional, only if benchmarks justify): GPU subset scoring**
  - Extend Metal/CUDA/Vulkan kernels to score only a subset of indices (or accept an index list).
  - Keep CPU fallback always available.

Keep nodeIDs as strings for correctness/compatibility. Only optimize ID representation after profiling shows it matters.

Guardrails:
- hard cap on candidate set size
- per-request timeouts
- cancellation support

---

## 5) Named Vectors (First-Class)

Treat `(db, type/label, vectorName, dims, distance)` as a unique **vector space** identity.

Implications:
- Qdrant “named vectors” map cleanly.
- Named vectors are a general NornicDB feature (not Qdrant-only): a node can have multiple embeddings associated with different semantic “fields” (e.g. `title`, `content`, `code`, `summary`).
- Chunked embeddings remain supported, but are a distinct concept from named vectors (chunking is “many vectors for one document”, naming is “different embedding fields”).

### 5.1 Data model (NornicDB-native)

Add a canonical representation for named vectors on `storage.Node`.

Proposed shape:
- `NamedEmbeddings map[string][]float32`
  - Key: `vectorName` (e.g. `"title"`, `"content"`, `"default"`)
  - Value: single dense embedding vector

Keep `ChunkEmbeddings [][]float32` for chunking.

Important semantic separation:
- `ChunkEmbeddings`: multiple vectors representing one node/document (typically chunked text)
- `NamedEmbeddings`: multiple “fields”/representations per node (title vs body, different models, etc.)

### 5.2 Search semantics with both chunking and naming

Define a stable scoring contract:

- Query selects a **vectorName** (explicitly or default).
- CandidateGen and ExactScore operate within that vector space.
- If the chosen vectorName is backed by `NamedEmbeddings[vectorName]`:
  - score the node with that single embedding.
- If the chosen vector space is “chunk search” (explicit opt-in):
  - score each chunk embedding and aggregate to a node score using a configurable reducer:
    - default: `max` similarity over chunks (best for “did any chunk match?” retrieval)
    - optional: `avg`, `topN-avg`, etc.
  - optionally return the winning chunk index for debugging/explanations (not required for compatibility).

This avoids ambiguous “search everything always” behavior while still supporting multi-embedding nodes.

### 5.3 Indexing model

Index registry maintains per-vector-space indices:

- For `NamedEmbeddings`:
  - one ANN index + one GPU `EmbeddingIndex` per `(db, type, vectorName, dims, distance)`
  - nodeID participates once per vector space

- For chunk search (optional, explicit):
  - index each chunk as `nodeID#chunk:<i>` under a dedicated vector space (e.g. `vectorName="chunks"`),
  - then aggregate results back to node-level (max score per node).

### 5.4 Cypher / Neo4j mapping

Neo4j’s mental model is “a vector index over a vector property”.
For NornicDB, named vectors map cleanly to “multiple vector properties/indexes”.

Proposed Cypher surface:
- `CREATE VECTOR INDEX <index_name> FOR (n:<Label>) ON (n.<vectorProperty>) OPTIONS {...}`
- `CALL db.index.vector.queryNodes('<index_name>', k, <vector|text>) ...`

Internally, `<index_name>` resolves to a vector space (including vectorName), and the query runs through the unified CandidateGen→ExactScore pipeline.

### 5.5 Qdrant gRPC mapping (thin adapter)

Map Qdrant’s vectors directly to NornicDB named vectors:

- Qdrant unnamed vector → `vectorName = "default"`
- Qdrant named vectors → `vectorName = <name>`

Upsert:
- `Vectors.Vector` → `NamedEmbeddings["default"] = vec`
- `Vectors.Vectors` (map) → `NamedEmbeddings[name] = vec` for each entry

Search:
- `SearchPoints.vector_name` (if present) selects vectorName; else default.

No separate Qdrant-only indexing layer: Qdrant gRPC should call into the shared index registry for indexing + search.

### 5.6 Migration plan (from current state)

1) Introduce `NamedEmbeddings` on `storage.Node` with backward-compat defaults:
   - If `NamedEmbeddings` is empty but `ChunkEmbeddings` has at least one vector:
     - treat `ChunkEmbeddings[0]` as `NamedEmbeddings["default"]` for default-vector search paths (temporary migration behavior).
2) Update ingestion/embedding pipeline to populate `NamedEmbeddings` directly.
3) Update Qdrant gRPC:
   - write named vectors into `NamedEmbeddings`
   - query named vectors from `NamedEmbeddings`
4) Update Cypher vector procedures to read/write the correct vector space.
5) After stability, optionally deprecate the implicit “chunk[0] as default embedding” behavior.

### 5.7 Testing requirements

- Unit tests:
  - upsert/get/search named vectors (default + multiple names)
  - mixed nodes: some only have `ChunkEmbeddings`, some only `NamedEmbeddings`, some both
  - scoring reducer correctness for chunk search (max/avg)
- Compatibility tests:
  - Qdrant clients: named vector upsert + `vector_name` search matches expected behavior
  - Cypher vector procedure behavior is unchanged for single-vector use cases

---

## 6) Index Registry (Unify All Endpoints)

Create a shared in-process registry responsible for **all** vector index lifecycle and query execution.

Important scope decision (make this deliverable):
- Start with a **per-database registry** that lives inside the existing per-db `search.Service` lifecycle.
- Do **not** introduce a global cross-db registry initially (we already have per-db search services and namespaced storage).
- If/when distributed shards arrive, the vector space key becomes a routing key and a higher-level registry can sit above per-db registries.

### 6.1 Responsibilities

- **Vector space lifecycle**
  - Create/update/delete vector spaces
  - Track configuration per space (dims, distance, ANN tuning knobs)
- **Index maintenance**
  - Upsert/remove vectors (named vectors and chunk vectors)
  - Ensure CPU ANN structure and GPU `EmbeddingIndex` stay consistent
  - Handle rebuilds (cold start, repair, tuning changes)
- **Search execution**
  - CandidateGen (brute/HNSW/k-means routing)
  - Exact rerank (GPU if available; CPU fallback)
  - Apply filters and return top-k
- **Observability**
  - Expose stats per vector space (N, dims, backend in use, efSearch, GPU used, fallbacks)
  - Provide debug hooks to print the chosen search plan (“why was this backend chosen?”)

### 6.2 Registry key (vector space identity)

Key every index by:
- `dbName` (namespace)
- `typeOrLabel` (Cypher label OR Qdrant collection)
- `vectorName` (e.g. `"default"`, `"title"`, `"content"`, `"chunks"`)
- `dims`
- `distance`

This is the canonical mapping that lets us unify:
- Cypher vector indexes (“vector property/index name” → vector space)
- NornicDB native search (type filters → vector space)
- Qdrant gRPC (collection + vectorName → vector space)

### 6.3 Registry internals (per vector space)

Each vector space holds:

- **Storage/index adapters**
  - A CPU ANN index for candidate generation:
    - brute-force for small `N`
    - HNSW for large `N`
    - optional k-means routing for very large `N` / GPU-heavy environments
  - A GPU-friendly embedding store:
    - reuse `pkg/gpu.EmbeddingIndex` (already supports Metal/CUDA/Vulkan + auto fallback)
    - one instance per vector space (dims must match)
- **Policy**
  - `N_small_max` threshold for brute-force
  - `candidate_multiplier`, `max_candidates`
  - HNSW tuning (`M`, `efConstruction`, `efSearch`)
  - optional chunk reducer strategy for chunk space

### 6.4 Update triggers (how indexes stay consistent)

**On node mutation (create/update/delete):**
- For each affected vector space:
  - Upsert/remove in ANN structure
  - Upsert/remove in `gpu.EmbeddingIndex`
  - Ensure GPU sync policy is applied (see §4.2 and existing `SyncToGPU` semantics)

**On startup / rebuild (rebuild-on-start; no persistence):**

Codified order (recommended):

1) **Rebuild vector spaces by scanning storage**
   - build/initialize all vector spaces (namespaced engine per db)
   - index `NamedEmbeddings` into their vector spaces
   - if “chunk space” is enabled, index chunk vectors into `vectorName="chunks"`
   - populate the canonical vector store for each space:
     - CPU slab + nodeID→index mapping (via existing `gpu.EmbeddingIndex`)
     - optionally sync to GPU when available/healthy (existing backend auto-fallback)
   - build the ANN candidate structure incrementally (HNSW add per vector)

2) **Run k-means clustering after ANN build (optional)**
   - clustering is an optimization/routing structure and should run *after* the embedding set exists
   - if enabled for a vector space and the space meets min-embeddings thresholds, trigger clustering

Scheduling policy (recommended):
- ANN structures are kept up to date on every mutation (upsert/remove).
- HNSW graph quality can degrade over high-churn workloads; schedule periodic rebuilds independently:
  - `NORNICDB_VECTOR_ANN_REBUILD_INTERVAL` controls this maintenance rebuild cadence.
- k-means clustering has its own trigger policy:
  - run once after bulk ingest, and/or
  - run periodically only when enabled and embeddings have changed enough to justify reclustering.

### 6.5 Endpoint usage (everything calls registry)

All endpoints call the same registry functions:
- Cypher vector procedures
- HTTP search (vector/hybrid)
- Qdrant gRPC

This eliminates endpoint-specific indexing layers and prevents dimension-mismatch fallbacks.

---

## 7) Milestones / PR Breakdown (Agent-Parallel)

This section is intentionally PR-sized. Each PR has:
- a hard rollback path (feature flag or compile-time switch)
- a benchmark gate (must not regress)
- unit/integration tests added alongside changes

### PR0 — Make current vector search scalable (Owner A)

**Goal:** deliver performance at large `N` without changing external APIs yet.

Deliverables:
- Refactor `search.Service`’s vector backend into a strategy:
  - brute-force for small `N`
  - HNSW candidate generation for large `N`
- Add CPU rerank option (exact SIMD rerank of candidate set):
  - controlled by `NORNICDB_VECTOR_CPU_RERANK_ENABLED` (default `true`)
- Add periodic rebuild support:
  - controlled by `NORNICDB_VECTOR_ANN_REBUILD_INTERVAL`
- Benchmark gates:
  - existing search benchmarks
  - `testing/benchmarks/grpc_vs_bolt`
  - `testing/benchmarks/nornic_vs_qdrant`

This PR is intentionally “no named vectors yet” so we can lock in core performance first.

### PR1 — Named vectors data model (Owner B)

**Goal:** make named vectors first-class on `storage.Node` without changing query behavior yet.

Deliverables:
- Add `NamedEmbeddings map[string][]float32` to `storage.Node`
- Define canonical default name: `"default"` (internal)
- Migration behavior (temporary):
  - if `NamedEmbeddings` is empty and `ChunkEmbeddings` has vectors, treat `ChunkEmbeddings[0]` as `"default"` for indexing/search
- Update any JSON/serialization paths if nodes are exported/imported
- Tests:
  - node copy/clone/serialization preserves `NamedEmbeddings`
  - mixed nodes (only chunks / only named / both)

**Notes:**
- This PR is purely a data model foundation; it should not change search ranking yet.

### PR2 — Per-db VectorSpace + IndexRegistry skeleton (Owner C)

**Goal:** establish the central registry abstraction and keying model.

Deliverables:
- Introduce `VectorSpaceKey` with `(db, type, vectorName, dims, distance)`
- Introduce `IndexRegistry`:
  - create/delete/get space
  - stats/introspection (counts, dims, backend choice)
- Define the “chunk vector space” convention:
  - `vectorName="chunks"` reserved internally for chunk search
- Tests:
  - key canonicalization
  - create/delete lifecycle

### PR3 — CandidateGen + ExactScore interfaces + vector-only pipeline (Owner D)

**Goal:** make “vector-only search” a single pipeline with a stable contract.

Deliverables:
- Add internal interfaces:
  - `CandidateGenerator` (`SearchCandidates(query, k, ...)`)
  - `ExactScorer` (`ScoreSubset(query, ids)` or `ScoreCandidates(query, candidates)`)
- Implement `VectorSearchCandidates(...)` in terms of:
  - `auto` strategy (brute vs HNSW based on `N_small_max`)
  - exact scoring stage (GPU when available)
- Preserve existing hybrid text search (BM25/RRF/MMR/cross-encoder) unchanged
- Tests:
  - correctness (ordering, thresholds)
  - cancellation and timeouts

### PR4 — HNSW candidate generation defaults + recall tuning (Owner E)

**Goal:** switch candidate generation to HNSW for large `N`.

Deliverables:
- Use `pkg/search/HNSWIndex` as CandidateGen for cosine spaces
- Configure/tune:
  - `NORNICDB_VECTOR_ANN_QUALITY=fast|balanced|accurate`
  - plus advanced overrides for `M`, `efConstruction`, `efSearch`
- Add recall tracking tests/benchmarks:
  - `recall@k` for ANN-only
  - `recall@k` for ANN + exact rerank
- Ensure HNSW delete/update policy is defined:
  - initial: remove+add for “update”
  - later: tombstones + rebuild if required for performance/correctness

### PR5 — Exact rerank implementation (CPU subset first; GPU subset optional) (Owner F)

**Goal:** make GPU rerank the default “exact scoring stage” when GPU is healthy.

Deliverables:
- Extend `pkg/gpu.EmbeddingIndex` with a subset scoring API:
  - `ScoreSubset(query []float32, ids []string) ([]SearchResult, error)` (name TBD)
  - GPU path scores only the subset (no global scan)
  - CPU fallback uses SIMD dot/cosine
- Define GPU sync policy for bulk ingest:
  - batch threshold (`AutoSync`/`BatchThreshold` already exist)
  - prefer “sync after bulk upsert” semantics for Qdrant ingestion
- Benchmarks:
  - verify p95/p99 improvements under concurrency for large `N`

### PR6 — K-means routing (optional) integrated as a CandidateGen (Owner G)

**Goal:** make k-means routing a swappable candidate generator for very large datasets.

Deliverables:
- Implement cluster routing CandidateGen:
  - choose top `numClustersToSearch`
  - candidate pool = IDs from those clusters
  - exact rerank by GPU subset scoring
- Trigger policy:
  - after bulk loads
  - and/or periodic clustering interval
- Benchmarks for very large `N` to justify complexity

### PR7 — Endpoint wiring: Qdrant gRPC → NamedEmbeddings + Registry (Owner H)

**Goal:** Qdrant gRPC becomes a thin adapter to the registry + named vectors data model.

Deliverables:
- Qdrant Upsert writes vectors into:
  - `NamedEmbeddings["default"]` for unnamed vectors
  - `NamedEmbeddings[name]` for named vectors
- Qdrant Search reads vectorName and calls registry search on that vector space
- Remove endpoint-specific indexing caches once stable
- Expand qdrant-client (Python) E2E script to cover named vector flows

### PR8 — Endpoint wiring: Cypher vector procedures → Registry + virtual properties (Owner I)

**Goal:** Cypher vector procedures use the same registry and can address named vector spaces.

Deliverables:
- Map “index name” to vector space:
  - existing behavior continues to work
  - new behavior: allow index definitions over named vector “properties”
- Ensure Neo4j-compatible behavior remains consistent
- Add regression tests + benchmark gates

---

## 8) Compatibility & Stability Requirements

- Must preserve Neo4j compatibility for Cypher vector procedures.
- Must preserve Qdrant gRPC contract behavior:
  - score ordering
  - vectorName selection
  - payload filter correctness
  - “managed embeddings” precondition behavior (if applicable)
- Must include:
  - microbenchmarks for scoring paths
  - end-to-end harness runs producing CSV for regression tracking

Recommended benchmark gates:
- `testing/benchmarks/grpc_vs_bolt` (ensure no regression for Bolt hot paths)
- `testing/benchmarks/nornic_vs_qdrant` (ensure Qdrant gRPC stays competitive)
- Recall benchmark for ANN + rerank (ensure quality doesn’t regress when tuning changes)

---

## 9) Open Questions (to decide during implementation)

1) For CPU-only + large `N`, should we do:
   - HNSW-only, or
   - HNSW + CPU rerank?
   - Decision: support both; default to **HNSW + CPU rerank** when `NORNICDB_VECTOR_CPU_RERANK_ENABLED=true`.
2) For deletes/updates, do we require exact HNSW correctness or is periodic rebuild acceptable?
   - (HNSW update/delete semantics can be tricky; if needed, choose “tombstones + rebuild”.)
   - Decision: periodic rebuild is acceptable and should be user-configurable via `NORNICDB_VECTOR_ANN_REBUILD_INTERVAL`.
3) Do we want per-collection persistence of ANN index, or rebuild-on-start?
   - Decision (initial): **do not persist**; rebuild-on-start.
4) Chunk space defaults:
   - do we expose chunk search explicitly (recommended) or transparently?
   - what is the default reducer (`max` is recommended)?
5) Named vectors in Cypher:
   - do we map named vectors to explicit properties, or provide a dedicated procedure surface?
   - Decision: expose named vectors as **explicit Cypher properties** (Neo4j-style), but implement them as **struct-backed virtual properties** on `storage.Node` so they can be filtered/hidden from default `RETURN n` property materialization (like we already do for embeddings). Procedures remain the primary high-performance query surface, but users can still reference the properties directly when needed.
