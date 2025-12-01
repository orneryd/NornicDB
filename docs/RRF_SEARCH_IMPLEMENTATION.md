# NornicDB Search Implementation

## Overview

The search package (`pkg/search/`) implements a **hybrid search system** combining:

1. **Vector Search** - Cosine similarity on 1024-dimensional embeddings
2. **BM25 Full-Text Search** - Keyword search with TF-IDF scoring
3. **RRF (Reciprocal Rank Fusion)** - Industry-standard algorithm to merge rankings

This is the same approach used by:
- Azure AI Search
- Elasticsearch
- Weaviate
- Google Cloud Search

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

## Full-Text Search Properties

The BM25 search indexes these node properties (matching Mimir's Neo4j `node_search` index):

| Property       | Description                    |
|----------------|--------------------------------|
| `content`      | Main content field             |
| `text`         | Text content (file chunks)     |
| `title`        | Node titles                    |
| `name`         | Node names                     |
| `description`  | Node descriptions              |
| `path`         | File paths                     |
| `workerRole`   | Agent worker roles             |
| `requirements` | Task requirements              |

**All properties are concatenated and indexed together** - a search for "docker configuration" will match nodes where any of these fields contain those terms.

## RRF Algorithm

**Formula**: `RRF_score(doc) = Σ (weight_i / (k + rank_i))`

Where:
- `k` = constant (default: 60)
- `rank_i` = rank of document in result set i (1-indexed)
- `weight_i` = importance weight for result set i

### Adaptive Weights

The system automatically adjusts weights based on query characteristics:

| Query Type | Words | Vector Weight | BM25 Weight | Rationale |
|------------|-------|---------------|-------------|-----------|
| Short      | 1-2   | 0.5           | 1.5         | Exact keyword matching |
| Medium     | 3-5   | 1.0           | 1.0         | Balanced |
| Long       | 6+    | 1.5           | 0.5         | Semantic understanding |

## Usage

```go
import "github.com/orneryd/nornicdb/pkg/search"

// Create search service
engine := storage.NewMemoryEngine()
svc := search.NewService(engine)

// Build indexes from storage
ctx := context.Background()
svc.BuildIndexes(ctx)

// Hybrid RRF search
opts := search.DefaultSearchOptions()
opts.Limit = 10
opts.MinSimilarity = 0.5

response, err := svc.Search(ctx, "authentication security", embedding, opts)

// Results include RRF metadata
for _, r := range response.Results {
    fmt.Printf("%s: rrf=%.4f vectorRank=%d bm25Rank=%d\n",
        r.ID, r.RRFScore, r.VectorRank, r.BM25Rank)
}
```

### Search Options

```go
type SearchOptions struct {
    Limit         int       // Max results (default: 50)
    MinSimilarity float64   // Vector threshold (default: 0.5)
    Types         []string  // Filter by node labels
    
    // RRF configuration
    RRFK         float64   // RRF constant (default: 60)
    VectorWeight float64   // Vector weight (default: 1.0)
    BM25Weight   float64   // BM25 weight (default: 1.0)
    MinRRFScore  float64   // Min RRF score (default: 0.01)
}
```

## Fallback Chain

The search automatically falls back when needed:

1. **RRF Hybrid** (if embedding provided)
2. **Vector Only** (if BM25 returns no results)
3. **Full-Text Only** (if no embedding or vector search fails)

## Performance (Apple M3 Max)

| Operation | Scale | Time |
|-----------|-------|------|
| Vector Search | 10K vectors | ~8.5ms |
| BM25 Search | 10K documents | ~255µs |
| RRF Fusion | 100 candidates | ~27µs |
| Index Build | 38K nodes | ~5.4s |

## Test Coverage

```bash
# Run all search tests
go test -v ./pkg/search/...

# Run with real Neo4j data
go test -v ./pkg/search/... -run RealData

# Run benchmarks
go test -bench=. ./pkg/search/...
```

## Files

- `search.go` - Main service with RRF fusion
- `vector_index.go` - Cosine similarity search
- `fulltext_index.go` - BM25 inverted index
- `search_test.go` - Comprehensive tests

## Future Improvements

1. **HNSW Index** - O(log n) vector search vs current O(n)
2. **GPU Acceleration** - Metal/CUDA for vector operations
3. **Stemming** - Better text normalization
4. **Field Boosting** - Weight title matches higher
5. **Phrase Search** - Exact phrase matching
