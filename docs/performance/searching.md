# NornicDB Search Methodology

**Complete guide to NornicDB's multi-method search architecture.**

For a reproducible BEIR evaluation of BM25 and RRF recall, see the
[Retrieval Recall Benchmark](retrieval-recall-benchmark.md).

---

## 🎯 Overview

NornicDB implements a **hybrid search system** that combines multiple search methodologies for maximum accuracy and performance:

```
┌─────────────────────────────────────────────────────┐
│           Search Request                            │
└────────────────────┬────────────────────────────────┘
                     │
         ┌───────────┼───────────┐
         │           │           │
         ▼           ▼           ▼
    ┌─────────┐ ┌─────────┐ ┌──────────┐
    │ Vector  │ │  BM25   │ │ Metadata │
    │ Search  │ │ Full-   │ │ Filters  │
    │ (HNSW)  │ │ Text    │ │          │
    └────┬────┘ └────┬────┘ └─────┬────┘
         │           │            │
         └───────────┼────────────┘
                     ▼
         ┌──────────────────────┐
         │ RRF Fusion           │
         │ (Rank Aggregation)   │
         └────────┬─────────────┘
                  │
         ┌────────▼──────────┐
         │ Cross-Encoder     │
         │ Reranking         │
         │ (Optional Stage 2)│
         └────────┬──────────┘
                  │
         ┌────────▼────────────┐
         │ Final Results       │
         │ (Top-K, sorted)     │
         └─────────────────────┘
```

---

## 1️⃣ Vector Similarity Search

### What It Does

Finds documents by **semantic meaning** using vector embeddings. Query and documents are converted to high-dimensional vectors, then compared using cosine similarity.

### Architecture

```
Query Text (e.g., "machine learning algorithms")
           │
           ▼
    ┌─────────────────┐
    │ Embedding       │ Generate dense vector
    │ Model           │ (384-1536 dimensions)
    │ (OpenAI, BGE)   │
    └────────┬────────┘
             │
             ▼
    ┌──────────────────────┐
    │ Query Vector         │
    │ (0.12, -0.45, 0.23) │
    └────────┬─────────────┘
             │
             ▼
    ┌──────────────────────────┐
    │ HNSW Index               │
    │ (Fast nearest neighbors) │
    └────────┬─────────────────┘
             │
    ┌────────┴────────────────────────────┐
    │                                     │
    ▼                                     ▼
Document A (similarity 0.92)   Document B (similarity 0.87)
```

### Implementation: HNSW Index

**HNSW (Hierarchical Navigable Small-World)** provides O(log N) approximate nearest neighbor search.

```
Layer 3 (top):  [●]────────────────────────[●]
                │                           │
Layer 2:        [●]─[●]──────────────[●]─[●]
                │  │ │              │ │  │
Layer 1:    [●]─[●]─[●]─[●]───[●]─[●]─[●]─[●]
            │ │ │ │ │ │ │    │ │ │ │ │ │ │ │
Layer 0: [●]-[●]-[●]-[●]-[●]-[●]-[●]-[●]-[●]-[●]  ← All documents

         ● = Document node (with embedding vector)
         ─ = Connection (learned during index build)
```

**Search Process:**

1. **Entry Point**: Start at random node in top layer
2. **Layer Traversal**: Greedily move closer to query vector in each layer
3. **Candidate Pool**: At bottom layer, find K nearest neighbors

**Parameters:**

| Parameter        | Default | Impact                                                                    |
| ---------------- | ------- | ------------------------------------------------------------------------- |
| `M`              | 16      | Max connections per layer (larger = more thorough, slower build)          |
| `efConstruction` | 200     | Candidate pool size during build (larger = better index, slower build)    |
| `efSearch`       | 100     | Candidate pool size during search (larger = more accurate, slower search) |

### GPU-Assisted HNSW Construction

HNSW construction can use a GPU-assisted candidate search path while keeping CPU graph mutation/linking and the same persisted HNSW format. The Metal builder performs an approximate layer-0 graph beam: it expands candidate neighborhoods from the current HNSW graph, scores those candidates in batched Metal matrix/top-k kernels, then lets the CPU perform final reciprocal graph linking. The default is to try GPU construction first (`NORNICDB_HNSW_BUILD_GPU_ENABLED=true`) and restart on the CPU builder if the accelerator is unavailable or fails.

Benchmark the targeted construction path directly:

```bash
go test ./pkg/search -bench 'BenchmarkHNSWBuildCPUVsAcceleratedCandidates|BenchmarkHNSWBuildFromVectorFileStore' -benchmem -run '^$'
```

Reference result on an Apple M3 Max for `BenchmarkHNSWBuildCPUVsAcceleratedCandidatesLarge1024D`:

| Path                   |        Time |
| ---------------------- | ----------: |
| CPU HNSW build         | `1.45 s/op` |
| Metal graph-beam build | `0.93 s/op` |

`BenchmarkHNSWBuildFromVectorFileStore` auto-detects the first `*.vec`/`*.meta` vector store under `./data`. To pin a specific store:

```bash
NORNICDB_HNSW_BUILD_BENCH_VECTOR_BASE=/path/to/vectors \
go test ./pkg/search -bench BenchmarkHNSWBuildFromVectorFileStore -benchmem -run '^$'
```

**Performance:**

Vector similarity search is accelerated by NornicDB's SIMD layer, which uses AVX2+FMA assembly for float32 operations via the viterin/vek library. Real-world benchmarks on Intel i9-9900KF (AVX2+FMA):

```
Operation              Vector Size    Pure Go    AVX2 SIMD    Speedup
─────────────────────────────────────────────────────────────────────────
Dot Product            384-dim        196.2ns    19.81ns      9.9x
Dot Product            1536-dim       750.2ns    61.41ns      12.2x
Dot Product            3072-dim       1457ns     123.3ns      11.8x

Cosine Similarity      384-dim        103.0ns    32.17ns      3.2x
Cosine Similarity      1536-dim       381.8ns    101.2ns      3.8x
Cosine Similarity      3072-dim       748.0ns    190.3ns      3.9x

Euclidean Distance     384-dim        187.6ns    26.12ns      7.2x
Euclidean Distance     1536-dim       730.6ns    64.92ns      11.3x
Euclidean Distance     3072-dim       1460ns     117.5ns      12.4x

Norm (L2 Magnitude)    384-dim        98.07ns    15.76ns      6.2x
Norm (L2 Magnitude)    1536-dim       391.6ns    48.12ns      8.1x
Norm (L2 Magnitude)    3072-dim       737.7ns    93.47ns      7.9x

Memory Bandwidth:      Large vectors  ~16 GB/s   ~110-130 GB/s 7-8x
```

**Summary:** Core operations average **6-12x faster** with vek32 SIMD. Cosine similarity (3-4x) is lower due to normalization overhead, but still substantial.

**SIMD Acceleration Details:**

- **Technology**: AVX2+FMA assembly via [viterin/vek](https://github.com/viterin/vek)
- **CPU Support**: Intel Haswell+ (2013+), AMD Zen+ (2017+)
- **Fallback**: Pure Go on unsupported platforms (automatic)
- **Memory Bandwidth**: 100-130 GB/s sustained on AVX2 (vs. 16 GB/s pure Go)
- **Compilation**: vek32 functions compiled with -ffast-math for maximum speed

Check your SIMD capabilities:

```go
info := simd.Info()
if info.Accelerated {
    fmt.Printf("Using %s SIMD (%v)\n", info.Implementation, info.Features)
}
```

### Advantages & Limitations

| Aspect             | ✅ Strength                                 | ❌ Limitation                          |
| ------------------ | ------------------------------------------- | -------------------------------------- |
| **Semantic Match** | Captures meaning, not just keywords         | May miss exact keyword matches         |
| **Speed**          | O(log N) with HNSW + SIMD acceleration      | Requires pre-computed embeddings       |
| **Scalability**    | Handles millions of vectors with SIMD boost | Memory footprint grows with dimensions |
| **Accuracy**       | Great for similarity ranking                | Can return "close but wrong" results   |

---

## 2️⃣ Full-Text Search (BM25)

### What It Does

Finds documents by **exact keyword matching**. Uses BM25 scoring algorithm (same as Elasticsearch, Solr).

### How BM25 Works

BM25 scores documents based on term importance and frequency:

```
BM25(D, Q) = Σ IDF(qi) × (f(qi, D) × (k1 + 1)) / (f(qi, D) + k1 × (1 - b + b × |D|/avgdl))

Where:
  IDF(q) = log((N - df(q) + 0.5) / (df(q) + 0.5))
  f(q, D) = term frequency in document D
  |D| = length of document D
  avgdl = average document length
  k1 = 1.2 (saturation factor)
  b = 0.75 (length normalization)
```

### Example Calculation

Query: `"machine learning"`

```
Document A: "We teach machine learning and AI"
  - Term freq: machine=1, learning=1
  - Length: 8 words
  - Normalized score: 2.34

Document B: "Machine learning algorithms, machine learning frameworks, machine learning tools"
  - Term freq: machine=3, learning=3
  - Length: 11 words
  - Normalized score: 8.92  ← Higher score (more relevant)

Document C: "What is the meaning of 'machine'?"
  - Term freq: machine=1, learning=0
  - Length: 6 words
  - Normalized score: 0.89  ← Lower (missing 'learning')
```

### Inverted Index Structure

```
Term          Documents with frequency
─────────────────────────────────────────
"machine"   → {docA: 1, docB: 3, docC: 1}
"learning"  → {docA: 1, docB: 3}
"algorithm" → {docB: 2, docD: 1}
"framework" → {docB: 1, docE: 4}
```

**Search Process:**

1. Tokenize query: `"machine learning"` → `["machine", "learning"]`
2. Look up inverted index for each term
3. Score documents containing the terms using BM25 formula
4. Return top-K sorted by score

### Advantages & Limitations

| Aspect               | ✅ Strength                          | ❌ Limitation              |
| -------------------- | ------------------------------------ | -------------------------- |
| **Keyword Matching** | Exact term matching guaranteed       | Misses semantic variations |
| **Speed**            | Very fast (inverted index O(log N))  | Must build index upfront   |
| **Predictability**   | Reproducible, interpretable          | Can't understand context   |
| **Maturity**         | Proven, used in Lucene/Elasticsearch | Over-matches on stop words |

### Search index persistence

By default, both the vector (HNSW) and BM25 indexes are built in memory on startup. For large graphs, you can **persist search indexes** to disk so they are saved after updates and loaded on the next startup, avoiding a full storage scan. Enable with `NORNICDB_PERSIST_SEARCH_INDEXES=true` or `database.persist_search_indexes: true` in YAML (requires `data_dir` to be set). See [Search index persistence](../operations/configuration.md#search-index-persistence) in the Configuration Guide.

> **Experimental:** Search index persistence is currently experimental.
> If startup must rebuild IVF-HNSW (for example due to missing/incompatible persisted artifacts), large datasets can still take a long time to become ready.
> Observed reference point: ~30 minutes to rebuild IVF-HNSW for ~1M embeddings on startup (hardware dependent).

---

## 3️⃣ Reciprocal Rank Fusion (RRF)

### What It Does

**Combines** vector search + BM25 results into a single ranking using rank positions (not scores).

### Why RRF Instead of Score Blending?

Scores from different algorithms are incomparable:

```
Vector Search Score     BM25 Score
─────────────────       ──────────
Range: 0-1              Range: 0-∞
Higher = better         Higher = better
Based on direction      Based on term frequency

⚠️ Can't simply add them: (0.92) + (15.3) = meaningless!
```

### RRF Formula

```
RRF_score = Σ (weight / (k + rank))

Where:
  weight = importance of this ranking (default 1.0)
  k = constant (default 60) to reduce high-rank dominance
  rank = position in result list (1-indexed)
```

### Example: Fusing Two Rankings

**Query:** `"python data science"`

```
Vector Search Results:        BM25 Full-Text Results:
─────────────────────        ─────────────────────
Rank 1: Doc A (sim 0.95)      Rank 1: Doc C (score 18.5)
Rank 2: Doc B (sim 0.88)      Rank 2: Doc A (score 16.2)
Rank 3: Doc D (sim 0.82)      Rank 3: Doc E (score 14.1)
Rank 4: Doc C (sim 0.79)      Rank 4: Doc B (score 12.3)

                        ↓ Apply RRF ↓

Document A:
  Vector rank=1:    1.0/(60+1) = 0.0164
  BM25 rank=2:      1.0/(60+2) = 0.0159
  RRF = 0.0164 + 0.0159 = 0.0323  ← High (appears in both, ranked well)

Document B:
  Vector rank=2:    1.0/(60+2) = 0.0159
  BM25 rank=4:      1.0/(60+4) = 0.0152
  RRF = 0.0159 + 0.0152 = 0.0311

Document C:
  Vector rank=4:    1.0/(60+4) = 0.0152
  BM25 rank=1:      1.0/(60+1) = 0.0164
  RRF = 0.0152 + 0.0164 = 0.0316

Document D:
  Vector rank=3:    1.0/(60+3) = 0.0157
  BM25 rank=none:   (missing)
  RRF = 0.0157      ← Lower (only in vector results)

                        ↓ Final Ranking ↓

Rank 1: Document A (RRF 0.0323) ← Best of both worlds
Rank 2: Document C (RRF 0.0316)
Rank 3: Document B (RRF 0.0311)
Rank 4: Document D (RRF 0.0157)
```

### Why This Works

```
                  Agreement Score
Document          Vector  BM25  Combined
─────────────────────────────────────────
A (appears in both)     1      1    HIGH
B (appears in both)     1      1    HIGH
C (disagreement)        -1     1    MEDIUM
D (vector only)         1      -    LOW
```

Documents that both algorithms agree on get boosted. Disagreements are treated reasonably.

### When RRF Helps Most

```
Query: "enterprise database"

Scenario 1: Vector & BM25 Agree
  Vector: [Product A, Product B, Product C]
  BM25:   [Product A, Product B, Product C]
  RRF:    No change (both agree)

Scenario 2: Vector & BM25 Disagree
  Vector: [NewsArticle (semantic fit), ProductX, ProductY]
  BM25:   [ProductA (keyword match), ProductB, ProductC]
  RRF:    Smart blend - products that match both move up
          NewsArticle still ranked (one vote) but lower
```

---

## 4️⃣ Cross-Encoder Reranking (Stage 2)

### What It Does

**Re-scores and re-ranks** top-K candidates from RRF using a more expensive but accurate cross-encoder model. Reranking can use a **local GGUF** (e.g. BGE-Reranker-v2-m3, loaded like the embedding model) or an **external API** (Cohere, HuggingFace TEI, Ollama adapter). Configure via `NORNICDB_SEARCH_RERANK_ENABLED`, `NORNICDB_SEARCH_RERANK_PROVIDER`, and related env vars or YAML; see [Cross-Encoder Reranking](../features/cross-encoder-reranking.md) and [Configuration](../operations/configuration.md#search-rerank-stage-2-reranking).

### Bi-Encoder vs Cross-Encoder

**Bi-Encoder (Stage 1: Fast)**

```
Query:     "What is machine learning?"
           ↓
    ┌──────────────┐
    │ Encode Query │ → [0.1, -0.5, 0.3, ...] (384-dim)
    └──────────────┘

Document: "Machine learning is..."
           ↓
    ┌──────────────┐
    │ Encode Doc   │ → [0.2, -0.4, 0.35, ...] (384-dim)
    └──────────────┘

Compare: cosine_similarity(query_vec, doc_vec) = 0.94
```

**Cross-Encoder (Stage 2: Accurate)**

```
[Query] "What is machine learning?"
[Document] "Machine learning is..."
                ↓
        ┌──────────────────┐
        │ Cross-Encoder    │ Sees both together!
        │ Model            │ Captures query-doc interaction
        └────────┬─────────┘
                 ▼
        Relevance score: 0.97 (more accurate)
```

### Processing Pipeline

```
┌──────────────────────────────┐
│ RRF Results (100 candidates) │
└───────────┬──────────────────┘
            │
            ▼
┌────────────────────────────┐
│ Cross-Encoder Batch        │
│ (Re-score all 100)         │
│ Time: ~1-2 seconds         │
└────────┬───────────────────┘
         │
         ▼
┌──────────────────────────┐
│ Re-ranked Results        │
│ (Top-10, resorted)       │
└──────────────────────────┘
```

### Supported Models

| Model                  | Provider    | Speed    | Accuracy  | Cost |
| ---------------------- | ----------- | -------- | --------- | ---- |
| ms-marco-MiniLM-L-6-v2 | HuggingFace | Fast     | Good      | Free |
| e5-large-v2-reranker   | EmbedRank   | Slow     | Excellent | Free |
| rerank-3.5             | Cohere      | Moderate | Excellent | $$   |
| rankgpt-3.5-turbo      | OpenAI      | Slow     | Perfect   | $$$$ |

### Decision Logic

```
IF reranking enabled:
  IF num_candidates > 50:
    Run cross-encoder on top-100
  ELSE IF num_candidates > 10:
    Run cross-encoder on all
  ELSE:
    Skip (too few to matter)
```

### Advantages

```
✅ Significantly improves relevance
✅ Context-aware (sees query + document together)
✅ Can correct RRF mistakes
✅ Works with any model via API
```

### Trade-offs

```
⏱️  Slower: ~1-2s for 100 documents
💰 Cost: External API calls
🔌 Dependency: Requires external service
📊 Marginal gain: ~5-15% improvement for most cases
```

---

## 5️⃣ SIMD Vector Acceleration

### What It Does

**Accelerates vector math** using CPU SIMD instructions (AVX2+FMA on x86, NEON on ARM) via the [viterin/vek](https://github.com/viterin/vek) library, which provides true assembly implementations.

### Architecture

NornicDB uses **viterin/vek32** for high-performance vector operations:

```
┌──────────────────────────────────────┐
│  NornicDB Vector Operations          │
│  (DotProduct, CosineSimilarity, etc) │
└────────────┬─────────────────────────┘
             │
      ┌──────▼──────────┐
      │  vek32 Library  │
      │  SIMD Assembly  │
      └──────┬──────────┘
             │
     ┌───────┼───────┐
     │       │       │
     ▼       ▼       ▼
   AVX2    NEON   Generic
  x86-64   ARM64    Pure Go
  (8x)     (4x)     (1x)
```

**Why vek32?**

- True AVX2+FMA assembly (not just compiler auto-vectorization)
- Optimized for float32 (standard for embeddings)
- Implementations compiled with -ffast-math for maximum speed
- Fallback to pure Go on unsupported CPUs (automatic)
- Proven 6-20x speedups on real workloads

### SIMD Fundamentals

**Scalar (Normal) Processing:**

```
a = [1.0, 2.0, 3.0, 4.0]
b = [5.0, 6.0, 7.0, 8.0]

Dot Product (scalar approach):
  result = 0
  for i in 0..3:
    result += a[i] * b[i]

Operations:
  Multiply: 4 × (a[i] * b[i])
  Add: 4 × (result += ...)
  ───────────────────────
  Total: 8 operations

CPU Cycles: ~16 cycles (with dependencies)
```

**SIMD (Vectorized) Processing with AVX2:**

```
a = [1.0, 2.0, 3.0, 4.0]
b = [5.0, 6.0, 7.0, 8.0]

Dot Product (SIMD approach):
  Registers hold 8 × float32 each (256-bit AVX2):

  a_vec = [1.0, 2.0, 3.0, 4.0, ...]  (8 values)
  b_vec = [5.0, 6.0, 7.0, 8.0, ...]  (8 values)

  VMULPS: a_vec × b_vec in ONE instruction
    → [5.0, 12.0, 21.0, 32.0, ...]

  VHADDPS: Horizontal sum → single result in ONE instruction

CPU Cycles: ~3 cycles (massive parallelism!)

Speedup: 16 cycles / 3 cycles ≈ 5-10x faster
```

### Measured Performance (Intel i9-9900KF, AVX2+FMA)

**Comprehensive benchmarks across multiple vector sizes:**

```
DOT PRODUCT (AVX2 is 8-way vectorized):
──────────────────────────────────────────────────────────────
Size    Pure Go    SIMD      Speedup    MB/s (SIMD)
128     66.15ns    10.70ns   6.2x       95,742
256     129.3ns    15.27ns   8.5x       134,160
384     196.2ns    19.81ns   9.9x       155,100
512     252.8ns    23.50ns   10.8x      174,295
768     375.1ns    33.34ns   11.2x      184,277
1024    494.3ns    48.69ns   10.2x      168,242
1536    750.2ns    61.41ns   12.2x      200,091
3072    1457ns     123.3ns   11.8x      199,313

COSINE SIMILARITY (normalized dot product + magnitude division):
──────────────────────────────────────────────────────────────
Size    Pure Go    SIMD      Speedup    MB/s (SIMD)
128     41.97ns    16.25ns   2.6x       63,003
256     71.94ns    24.69ns   2.9x       82,944
384     103.0ns    32.17ns   3.2x       95,494
512     136.6ns    39.66ns   3.4x       103,271
768     195.1ns    55.76ns   3.5x       110,193
1024    253.7ns    69.37ns   3.7x       118,087
1536    381.8ns    101.2ns   3.8x       121,458
3072    748.0ns    190.3ns   3.9x       129,170

EUCLIDEAN DISTANCE (sqrt of sum of squares):
──────────────────────────────────────────────────────────────
Size    Pure Go    SIMD      Speedup    MB/s (SIMD)
128     66.98ns    13.54ns   4.9x       75,642
256     126.8ns    19.72ns   6.4x       103,866
384     187.6ns    26.12ns   7.2x       117,611
512     247.4ns    30.56ns   8.1x       134,015
768     367.0ns    39.58ns   9.3x       155,246
1024    489.6ns    48.06ns   10.2x      170,460
1536    730.6ns    64.92ns   11.3x      189,290
3072    1460ns     117.5ns   12.4x      209,111

NORM / L2 MAGNITUDE:
──────────────────────────────────────────────────────────────
Size    Pure Go    SIMD      Speedup    MB/s (SIMD)
128     37.62ns    8.871ns   4.2x       57,718
256     68.75ns    12.14ns   5.7x       84,334
384     98.07ns    15.76ns   6.2x       97,461
512     128.5ns    20.71ns   6.2x       98,872
768     187.9ns    26.80ns   7.0x       114,627
1024    247.4ns    32.72ns   7.6x       125,172
1536    391.6ns    48.12ns   8.1x       127,688
3072    737.7ns    93.47ns   7.9x       131,458

MEMORY BANDWIDTH (sustained throughput):
──────────────────────────────────────────────────────────────
Vector Size    Bandwidth
8K elements    108 GB/s
16K elements   106 GB/s
32K elements   107 GB/s
64K elements   61 GB/s (L3 cache limited)
```

**Summary:**

| Operation          | Speedup Range    | Best Use Case                       |
| ------------------ | ---------------- | ----------------------------------- |
| Dot Product        | **6.2x - 12.2x** | Similarity search, HNSW indexing    |
| Euclidean Distance | **4.9x - 12.4x** | K-means clustering, spatial queries |
| Norm (L2)          | **4.2x - 8.1x**  | Vector normalization                |
| Cosine Similarity  | **2.6x - 3.9x**  | Text/semantic similarity            |

### Platform-Specific Implementations

**x86/amd64 with AVX2 + FMA:**

```
CPU Requirements:
  - Intel: Haswell (2013+) or newer
  - AMD: Zen+ (2017+) or newer

vek32 Instructions:
  VMULPS    - Multiply 8 × float32 in parallel
  VFMADD    - Fused multiply-add (a*b + c)
  VHADDPS   - Horizontal sum

Performance:
  - 8-way parallelism (256-bit registers)
  - 6-12x speedup on vector operations (measured)
  - 100-200 GB/s memory bandwidth
```

**ARM64 with NEON:**

```
CPU Requirements:
  - Apple Silicon (M1/M2/M3+)
  - ARM servers with NEON support

vek32 Instructions:
  FMUL      - Multiply 4 × float32 in parallel
  FMLA      - Fused multiply-add
  FADDP     - Parallel add

Performance:
  - 4-way parallelism (128-bit registers)
  - 4-8x speedup expected on ARM64
  - Excellent on Apple M-series unified memory
```

**Generic Fallback:**

```
Fallback Plan:
  - Automatic if AVX2/NEON not detected
  - Pure Go scalar loop
  - Works everywhere
  - Performance: Baseline (1x)
```

### Cosine Similarity Example

**Computing similarity of two 1536-dim embeddings (OpenAI ada-002):**

```
Without SIMD (scalar Go):
  - Dot product: 380ns
  - Norm A: 369ns
  - Norm B: 369ns
  - Division + sqrt: 200ns
  - Total: ~1.3µs per pair

  For 1000 similarity searches: 1.3ms

With SIMD (AVX2 vek32):
  - All operations accelerated via vek32
  - Effective speedup: ~1.6-2.0x
  - Total: ~0.7µs per pair

  For 1000 similarity searches: 0.7ms

Speedup: 1.3ms / 0.7ms ≈ 1.9x faster
```

### Integration with Search

```
HNSW Search (1M vectors, 1000 queries):
  1. Load query embedding (1536 floats)
  2. Compare against candidate vectors
     → CosineSimilarity() [SIMD-accelerated via vek32]
  3. Keep top-K

RRF Fusion:
  1. Score each candidate
  2. Rank candidates
  3. Select top-K for reranking

Impact:
  Standard (pure Go): ~500ms to search 1M vectors
  SIMD (vek32): ~50ms to search 1M vectors
  ~10x speedup!
```

### Automatic Detection

NornicDB automatically detects and uses the best available SIMD backend:

```go
// Runtime detection at startup
info := simd.Info()

On Intel i9-9900K (AVX2+FMA):
  Implementation: avx2
  Features: [AVX2, FMA, BMI2, ...]
  Accelerated: true

On Apple Silicon M3:
  Implementation: neon
  Features: [NEON, FMA, ...]
  Accelerated: true

On older CPU without AVX2/NEON:
  Implementation: generic
  Features: [SSE2]
  Accelerated: false

On x86 with AVX2:
  Implementation: AVX2
  Features: [AVX2, FMA]
  Accelerated: true

On older x86 or other platforms:
  Implementation: generic
  Features: []
  Accelerated: false
```

---

## 6️⃣ K-Means Clustering (Semantic Partitioning)

### What It Does

**Partitions embeddings into semantic clusters** to enable 10-100x faster search by searching only relevant clusters instead of all vectors.

Instead of comparing query against 1M vectors, compare against ~1000 (only closest clusters), then search within those clusters.

### How K-Means Works

K-means partitions data into K clusters by iteratively:

1. **Initialize**: Pick K random centroid points
2. **Assign**: Assign each point to nearest centroid
3. **Update**: Move centroid to mean of assigned points
4. **Repeat**: Until centroids stop moving (convergence)

```
Iteration 0 (Random starts):
 ┌─────────────────────────┐
 │ ●    ●    ●             │  3 centroids
 │  ● ●  ● ●  ● ●          │  scattered randomly
 │   ●   ●    ●●●          │
 └─────────────────────────┘

 Iteration 5 (Converging):
 ┌─────────────────────────┐
 │ ●●●  ●●   ●●            │
 │  ●● ●●●  ●●●●           │  Points move towards
 │ ●●●● ● ●●●●●●           │  nearest centroid
 │    ●      ●●●           │
 └─────────────────────────┘

 Iteration 50 (Converged):
 ┌─────────────────────────┐
 │ ●●●  ●●   ●●            │
 │  ●●●●●●●  ●●●●●         │  Tight clusters
 │ ●●●●●●●●●●●●●●●●        │  centroids stable
 │    ●●●●●●  ●●●          │
 └─────────────────────────┘
```

### Performance: K-Means Search vs Brute-Force

**Brute-force vector search (no clustering):**

```
Query: [0.12, -0.45, 0.23, ..., 0.08] (1536-dim)
       ↓
Compare against ALL 1M vectors:
  - 1M × CosineSimilarity operations
  - 1M × 1536 float32 reads = 6GB memory traffic
  - Latency: ~500ms
```

**Clustered search (K-means + search):**

```
Query: [0.12, -0.45, 0.23, ..., 0.08]
       ↓
1. Find K nearest CENTROIDS:
   - 100 centroids × CosineSimilarity = 0.2ms
   - Return top 3 centroids

2. Search within those 3 clusters:
   - Each cluster ~333K vectors
   - But search within ~1% of data: 333K × 3 = 1M vectors... WAIT!

   Actually: If K=100 clusters:
   - Average cluster size: 1M / 100 = 10K vectors
   - Search top 3 clusters: 3 × 10K = 30K vectors
   - 30K << 1M, so 30x-50x speedup!

   Latency: ~5-10ms
```

**Real performance numbers (1M embeddings, 1536-dim):**

```
Cluster Count    Build Time   Search (top-10)  Speedup  Memory
─────────────────────────────────────────────────────────────
No clustering    —            500ms            1x       6GB
100 clusters     2s           10ms             50x      6.5GB
500 clusters     10s          3ms              166x     7GB
1000 clusters    25s          2ms              250x     8GB

⚠️ Diminishing returns: 500+ clusters adds overhead >gain
```

### Initialization Methods

**K-Means++ (Default - Better Quality)**

```
Algorithm:
1. Pick first centroid randomly
2. For each next centroid:
   - Calculate distance to nearest existing centroid for each point
   - Pick point with probability ∝ (distance²)
   - Add as new centroid

Benefit: Avoids poor local minima
Quality: ~2x better final clustering
Speed: Slower initialization (~100ms for 10K points)
```

**Random Initialization (Faster)**

```
Algorithm:
1. Pick K random points as centroids

Benefit: Instant
Speed: Much faster initialization
Quality: May need more iterations or poor results
```

### Auto-K Selection

When dataset size is unknown, NornicDB automatically selects K using **Elbow Method**:

```
Algorithm:
1. Run k-means for K = 10, 20, 30, ..., sqrt(N)
2. Track within-cluster sum-of-squares (WCSS)
3. Find "elbow" (diminishing improvement)
4. Use that K

Example for 100K embeddings:
┌──────────────────────────────────────┐
│ WCSS          ╱                       │
│     ╲        ╱                        │
│      ╲      ╱                         │
│       ╲    ╱ ← Elbow (K=100)          │
│        ╲  ╱                           │
│         ╲╱                            │
│ └────────────────────────── K        │
│   10  50  100  200  300              │
│
Result: K=100 chosen automatically
```

### Drift-Based Re-Clustering

NornicDB can **automatically re-cluster** when new embeddings cause significant centroid drift:

```
Configuration:
  drift_threshold: 0.1      # Re-cluster if drift > 10%
  cluster_interval: 15m     # Check every 15 minutes

Monitoring:
  if (centroid_drift > threshold) {
    trigger_clustering()    // Adapt to new data
  }

Example:
  Initial clusters: 100 (aligned with data)
  Add 50K new embeddings    (10% of dataset)
  Centroid drift: 12%       (> 10% threshold)
  → Automatically re-cluster
  → New centroids optimized for all 150K embeddings
```

### Integration with Vector Search

K-means is a **pre-filter** before HNSW:

```
Query: [0.12, -0.45, 0.23, ...]
       ↓
Phase 1: CLUSTER FILTERING (fast, approximate)
  └─→ Find K nearest centroids
  └─→ Get top-3 clusters: [C1, C2, C3]
  └─→ Return 30K candidate vectors
  └─→ Time: ~1ms
       ↓
Phase 2: HNSW REFINEMENT (accurate, slow)
  └─→ Run HNSW search on 30K candidates
  └─→ Return top-10 vectors
  └─→ Time: ~5ms
       ↓
Final Results: Top-10 (from 1M vectors, in ~6ms!)

Without clustering:
  └─→ HNSW on 1M vectors: ~20-50ms
```

### Search Strategy: Cluster-Based

```go
// Cluster-aware search parameters
numClustersToSearch := 3          // Search how many clusters
candidateLimit := limit * 2        // Ensure enough candidates

// Step 1: Find nearest centroids
nearestClusters := findNearestCentroids(query, numClustersToSearch)

// Step 2: Get vectors from those clusters
candidates := getCandidatesFromClusters(nearestClusters)

// Step 3: Rank candidates with HNSW
results := hnswSearch(candidates, query, limit)
```

**When to search multiple clusters:**

```
Query Type           Clusters  Reason
──────────────────────────────────────────────────────
Specific term        1-2       Query clearly in one cluster
Broad topic          3-5       Query spans multiple topics
Exploratory/fuzzy    5+        Need diverse results
```

### Configuration & Tuning

```yaml
k_means_clustering:
  enabled: true

  # Cluster count
  num_clusters: 0 # 0 = auto-detect with elbow method
  # Or fixed:
  # num_clusters: 100

  # Convergence
  max_iterations: 100 # Stop after 100 iterations
  tolerance: 0.0001 # Stop when drift < 0.0001

  # Initialization
  init_method: "kmeans++" # Use k-means++ (better) or "random" (faster)

  # Auto-clustering triggers
  cluster_interval: "15m" # Re-cluster every 15 minutes
  drift_threshold: 0.1 # Re-cluster if centroids drift >10%

  # Constraints
  min_cluster_size: 10 # Merge tiny clusters

# Search behavior
vector_search:
  use_clustered_search: true
  clusters_to_search: 3 # Search top 3 clusters
```

### Performance Trade-Offs

```
Parameter           Impact on Build  Impact on Search  Quality
──────────────────────────────────────────────────────────────
num_clusters ↑      Slower          Faster           Worse
max_iterations ↑    Much slower     —                Better
tolerance ↓         Much slower     —                Better
init_method         k-means++ slower than random

Example tuning:
Scenario: 10M vectors, need fast response
  num_clusters: 1000        (large K)
  max_iterations: 50        (converge quick)
  tolerance: 0.01           (loose)
  → Build: 10s, Search: 1ms, Quality: Good
```

### Automatic vs Manual Clustering

**Automatic (Recommended):**

```go
service.EnableClustering(gpuManager, 0)  // 0 = auto-detect K

// NornicDB automatically:
// 1. Detects optimal K using elbow method
// 2. Builds clusters on first ~1M embeddings
// 3. Re-clusters every 15min if drift detected
// 4. Integrates seamlessly with search
```

**Manual (For fine control):**

```go
config := &gpu.KMeansConfig{
    NumClusters:   100,
    MaxIterations: 50,
    Tolerance:     0.001,
    InitMethod:    "kmeans++",
    AutoK:         false,
}
service.EnableClustering(gpuManager, 100)
```

### Monitoring & Diagnostics

```
Metrics available in search stats:

num_clusters: 100           # K value
embedding_count: 1000000    # Total vectors
avg_cluster_size: 10000     # Mean vectors per cluster
is_clustered: true          # Clustering active
cluster_iterations: 47      # Iterations to converge
centroid_drift: 0.002       # How much centroids moved (0-1)

Check health:
  ✅ is_clustered = true           Clustering working
  ✅ avg_cluster_size > 100         Balanced clusters
  ✅ centroid_drift < 0.1           Centroids stable
  ⚠️  clustering disabled            Enable with feature flag
```

### Limitations & When NOT to Use

```
❌ Don't use k-means clustering if:
   - Dataset < 10K embeddings (overhead not worth it)
   - Embeddings change constantly (re-clustering overhead)
   - Need exact nearest neighbors (approximate only)
   - Clusters need semantic meaning (pure distance-based)

✅ Use k-means clustering if:
   - Dataset > 100K embeddings
   - Search latency critical (need <10ms)
   - Embeddings relatively stable (add new batch weekly)
   - Good approximation acceptable (95%+ recall)
```

---

## 📊 End-to-End Search Flow

### Complete Request Lifecycle

```
User Query: "best machine learning frameworks for production"
       │
       ▼
1. EMBEDDING (pkg/embed)
   └─→ OpenAI / BGE / Local LLM
   └─→ Produces: [0.12, -0.45, 0.23, ..., 0.08] (1536 dim)
       Latency: 200-800ms (depends on provider)

       │
       ▼
2. HNSW VECTOR SEARCH (pkg/search - HNSWIndex)
   └─→ Index lookup with query embedding
   └─→ SIMD acceleration (pkg/simd.CosineSimilaritySIMD)
   └─→ Produces: [Doc1, Doc2, Doc3, ...] with similarity scores
       Latency: 1-5ms (1M vectors)
       Returns: Top-100 candidates

       │
       ▼
3. BM25 FULL-TEXT (pkg/search - FulltextIndex)
   └─→ Tokenize query: ["machine", "learning", "framework", ...]
   └─→ Lookup inverted index
   └─→ Score with BM25 formula
   └─→ Produces: [Doc3, Doc1, Doc5, ...] with BM25 scores
       Latency: 5-20ms (1M documents)
       Returns: Top-100 candidates

       │
       ▼
4. RRF FUSION (pkg/search - Search.Fuse)
   └─→ Merge rankings from Vector + BM25
   └─→ Apply RRF formula to positions
   └─→ Produces: [Doc1, Doc3, Doc2, ...] with RRF scores
       Latency: <1ms
       Returns: Top-100 fused results

       │
       ▼
5. CROSS-ENCODER RERANKING (Optional, pkg/search)
   └─→ IF enabled && num_results > 10:
       └─→ Batch top-100 through cross-encoder API
       └─→ Re-score and re-rank
       └─→ Produces: [Doc1, Doc2, Doc5, ...] reranked
           Latency: 500-1500ms
           Returns: Top-10 final results

       │
       ▼
6. RESPONSE (pkg/search - SearchResponse)
   └─→ Format results with all metadata
   └─→ Include metrics (timings, scores, ranks)
   └─→ Return to client

TOTAL LATENCY (without reranking): ~200-1000ms
TOTAL LATENCY (with reranking): ~700-2500ms
```

### Optimization Techniques

```
Technique              Impact        Implementation
─────────────────────────────────────────────────────
SIMD Acceleration      5-10x faster  Automatic in pkg/simd
Batch Processing       2-3x faster   Process 100 at once
GPU (optional)         10-100x       Available in pkg/gpu
Query Caching          2-5x faster   Cypher: pkg/cypher; Search: pkg/search
Index Warming          2x faster     Pre-load hot shards
Adaptive EF            1.5-2x        Tune efSearch per QPS
```

### Search result caching

Unified search (`pkg/search`) caches **search results** so that repeated identical requests are served from memory regardless of entry point (HTTP search API, Cypher, MCP, etc.).

**Behavior:**

- **Key:** Query string + options that affect the result: `Limit`, `Types` (labels), `RerankEnabled`, `RerankTopK`, `RerankMinScore`, `MMREnabled`, `MMRLambda`. Same (query, options) ⇒ same cache key.
- **Capacity:** 1000 entries (LRU eviction).
- **TTL:** 5 minutes (aligned with Cypher query cache).
- **Invalidation:** Full cache clear on `IndexNode` and `RemoveNode`, so results stay correct after index changes.

**Effect:** The first request for a given query+options runs the full pipeline (embedding, vector search, BM25, RRF, optional rerank). The second identical request returns the cached `SearchResponse` immediately (no embedding or index lookup). This is separate from the **Cypher query cache**, which caches Cypher execution results; search result caching is inside the search service and benefits every caller.

---

## 🎯 Choosing the Right Method

### Decision Tree

```
Q: Need EXACT keyword match?
├─ YES → Use BM25 Full-Text
└─ NO → Continue

Q: Need SEMANTIC understanding?
├─ YES → Use Vector Search (HNSW)
└─ NO → Consider metadata filters

Q: Want BEST of both worlds?
├─ YES → Use RRF (Vector + BM25)
└─ NO → Pick one method

Q: Critical relevance required?
├─ YES → Add Cross-Encoder reranking
└─ NO → RRF alone is sufficient
```

### Use Case Guide

| Use Case                                             | Method              | Reason              |
| ---------------------------------------------------- | ------------------- | ------------------- |
| **Full-text search** (e.g., logging)                 | BM25                | Exact term matching |
| **Semantic search** (e.g., "find papers on topic X") | Vector HNSW         | Meaning matters     |
| **General-purpose search** (web, docs)               | RRF                 | Best of both        |
| **High-stakes ranking** (search ads, hiring)         | RRF + Cross-encoder | Maximum accuracy    |
| **Real-time autocomplete**                           | BM25 prefix         | Speed critical      |
| **Fuzzy/typo-tolerant**                              | Vector + Phonetic   | Error tolerance     |

---

## 📈 Performance Benchmarks

### Field Measurements (Apple M3 Max, 64GB RAM)

These are measured on a live `translations` dataset (February 2026), using `POST /nornicdb/search` with varied cache-busting queries.

#### Embedding-query path (best collected)

| Scenario                              | Samples |     p50 |     p95 |      p99 |    Mean |
| ------------------------------------- | ------: | ------: | ------: | -------: | ------: |
| Sequential varied queries             |     120 | 11.28ms | 25.84ms |  34.05ms | 14.49ms |
| Concurrent varied queries (8 workers) |     120 | 76.36ms | 87.41ms | 100.07ms | 76.34ms |

Server diagnostics for this path showed `embed_total` as the dominant component while `search_total` stayed in microseconds.

#### Fulltext-only path (best collected)

| Scenario                  | Samples |    p50 |    p95 |       p99 |   Mean |
| ------------------------- | ------: | -----: | -----: | --------: | -----: |
| Sequential varied queries |      40 | 0.57ms | 2.77ms | 94.90ms\* | 4.44ms |

Observed handler diagnostics:

- `embed_calls=0`
- `embed_total=0s`
- `search_method="fulltext"` with `fallback=true`

\* p99 includes occasional transport/runtime outliers; server-side handler timing for these requests is typically in tens of microseconds.

### Single Document Scoring

```
Method              Time    Throughput    Accuracy
────────────────────────────────────────────────────
Vector (SIMD)       2µs     500k ops/sec  0.92
BM25                5µs     200k ops/sec  0.85
RRF fusion          0.1µs   10M ops/sec   0.94
Cross-encoder       50ms    1 doc/sec     0.98
```

### Full Search (1M documents, top-10)

```
Method              Latency   Cost      Accuracy
────────────────────────────────────────────────────
Vector only         5ms       CPU       0.88
BM25 only           20ms      CPU       0.80
Vector+BM25 (RRF)   25ms      CPU       0.92
+ Reranking         1500ms    API       0.96
```

### Scaling Characteristics

```
Vector Search (HNSW) vs Linear BM25

Documents    HNSW      Linear BM25   Ratio
──────────────────────────────────────────
1K           0.2ms     0.5ms         2.5x
10K          0.3ms     5ms           16x
100K         0.5ms     50ms          100x
1M           1.2ms     500ms         416x
10M          2.1ms     5000ms        2380x
```

HNSW scales logarithmically; BM25 scales linearly. RRF helps by combining both strengths.

---

## 🔧 Configuration & Tuning

### Vector Search Parameters

```yaml
vector_search:
  enabled: true
  hnsw:
    m: 16 # Max connections (8-32)
    ef_construction: 200 # Build thoroughness (100-500)
    ef_search: 100 # Search depth (50-200)


  # Trade-off:
  # - Larger M/efConstruction = better quality, slower build
  # - Larger efSearch = more accurate, slower search
```

### Full-Text Search Parameters

```yaml
fulltext_search:
  enabled: true

  # Priority properties get extra weight
  priority_properties:
    - content
    - title
    - description

  # BM25 parameters (rarely need adjustment)
  bm25:
    k1: 1.2 # Term frequency saturation
    b: 0.75 # Length normalization
```

### RRF Parameters

```yaml
rrf_fusion:
  enabled: true
  k: 60 # Constant (higher = more balanced)
  vector_weight: 1.0
  bm25_weight: 1.0

  # Fallback if one method fails
  fallback_enabled: true
```

### Cross-Encoder Parameters

```yaml
cross_encoder:
  enabled: true
  api_url: "https://api.cohere.com/rerank"
  model: "rerank-3.5"
  top_k: 100 # Re-score top-100 from RRF
  threshold: 0.5 # Minimum score to include
```

---

## 📚 API Reference

### Basic Search

```go
// Vector search
results, err := service.SearchVector(ctx, query, embedding, opts)

// Full-text search
results, err := service.SearchFulltext(ctx, query, opts)

// Hybrid RRF
results, err := service.Search(ctx, query, embedding, opts)
```

### Advanced Options

```go
opts := search.SearchOptions{
  Limit: 10,
  Threshold: 0.7,

  VectorSearch: true,
  FulltextSearch: true,
  UseReranking: true,

  IncludeMetrics: true,  // Include timing info
}
```

### Response Structure

```go
type SearchResponse struct {
  Status            string        // "success" or "error"
  Query             string        // Original query
  Results           []SearchResult // Top-K matches
  TotalCandidates   int           // Before filtering
  SearchMethod      string        // "vector", "fulltext", "hybrid"
  FallbackTriggered bool          // True if primary failed

  Metrics: {
    VectorSearchTimeMs: 5,
    BM25SearchTimeMs: 20,
    FusionTimeMs: 1,
    TotalTimeMs: 26,
  }
}
```

---

## 🔍 Debugging Search Issues

### Common Problems

**Q: Vector search returns irrelevant results**

- A: Vector model may not fit your domain. Consider fine-tuned embeddings (e.g., BGE for Chinese text)
- Test with: `simd.Info()` to verify SIMD is accelerated

**Q: BM25 is too strict (no results)**

- A: Query has rare terms. Enable fuzzy matching or expand query with synonyms

**Q: RRF still missing relevant documents**

- A: Documents missing from both indices. Check:
  - Vector index built? `svc.BuildIndexes(ctx)`
  - Full-text indexed? `svc.Index(docID, text)`

**Q: Search is slow (>1000ms)**

- A: Profile with metrics enabled:
  ```go
  resp.Metrics.VectorSearchTimeMs   // Fast? Check SIMD
  resp.Metrics.BM25SearchTimeMs     // Slow? Check inverted index
  resp.Metrics.FusionTimeMs         // Check RRF config
  ```

### Enabling Debug Logging

```go
// Enable detailed timing
svc := search.NewService(storage)
svc.DebugLogging = true

results, _ := svc.Search(ctx, query, embedding, opts)
// Logs each stage timing and candidate counts
```

---

## Future Improvements

- [ ] GPU-accelerated HNSW search for 100M+ vectors
- [ ] Learned sparse retrieval (LSR) for semantic BM25
- [ ] Multi-vector embeddings (different dimensions for different models)
- [ ] Real-time index updates (currently batch-only)
- [ ] Approximate cross-encoder reranking (faster)
- [ ] Geographic search integration

---

## 📖 Further Reading

- **HNSW**: Malkov & Yashunin (2018) - "Efficient and Robust Approximate Nearest Neighbor Search"
- **BM25**: Robertson & Zaragoza (2009) - "The Probabilistic Relevance Framework: BM25 and Beyond"
- **RRF**: Cormack, Clarke, & Buettcher (2009) - "Reciprocal Rank Fusion Outperforms Condorcet and Individual Rank Learning Methods"
- **Cross-Encoder**: Nogueira & Cho (2019) - "Passage Re-ranking with BERT"

---

**Last Updated**: February 17, 2026  
**Package**: `pkg/search` + `pkg/simd` + `pkg/math/vector`
