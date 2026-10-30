# K-Means Clustering Testing Guide

Quick reference for testing k-means clustering with NornicDB.

## Prerequisites

```bash
cd nornicdb
```

## 1. Generate Test Data

### Option A: Movie Dataset (Best for Semantic Testing)
```bash
# Generate 2000 movies with genre-specific content (will cluster by genre)
go run cmd/kmeans-test-data/main.go -mode movies -count 2000 -db ./data/movies-test
```

### Option B: Pre-clustered Embeddings (Best for K-Means Validation)
```bash
# Generate 5000 embeddings with 50 known clusters (ground truth)
go run cmd/kmeans-test-data/main.go -mode clusters -count 5000 -clusters 50 -db ./data/cluster-test
```

### Option C: Large Dataset (Stress Testing)
```bash
# Generate 10000 embeddings 
go run cmd/kmeans-test-data/main.go -mode clusters -count 10000 -clusters 100 -db ./data/stress-test
```

## 2. Start NornicDB with K-Means Enabled

```bash
# Enable k-means clustering
export NORNICDB_GPU_CLUSTERING_ENABLED=true

# For movie data (needs embedder to generate embeddings)
export OLLAMA_BASE_URL=http://localhost:11434
go run cmd/nornicdb/main.go -data ./data/movies-test

# For pre-clustered data (has embeddings already)
go run cmd/nornicdb/main.go -data ./data/cluster-test
```

## 3. Watch the Logs

You should see:
```
🔬 K-means clustering enabled for accelerated semantic search
✅ Search indexes built from existing data
[K-MEANS] ✅ Clustering ENABLED | mode=CPU clusters=100 max_iter=50 init=kmeans++
[K-MEANS] 🔄 STARTING | embeddings=5000
[K-MEANS] ✅ COMPLETE | clusters=100 embeddings=5000 iterations=12 duration=234ms
```

For movie data with embedder:
```
🧠 Embed worker started
🔄 Processing node movie-00001 for embedding...
[K-MEANS] 🔬 Embedding batch complete (2000 processed), triggering k-means clustering...
```

## 4. Test Search

### Via HTTP API
```bash
# Semantic search (uses cluster-accelerated path if available)
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Content-Type: application/json" \
  -d '{"query": "space exploration aliens", "limit": 10}'

# Should see in logs:
# [K-MEANS] 🔍 SEARCH | mode=clustered clusters_searched=3 candidates=20 duration=1.2ms
```

### Via Cypher
```cypher
// Full-text search
CALL db.index.fulltext.queryNodes('default', 'horror scary') YIELD node, score
RETURN node.title, node.genre, score LIMIT 10

// Vector similarity search (if embeddings exist)
CALL db.index.vector.queryNodes('default', 10, 'romantic love story')
YIELD node, score RETURN node.title, score
```

## 5. Verify Clustering is Working

Check these log messages:

| Log | Meaning |
|-----|---------|
| `mode=clustered` | ✅ Using k-means accelerated search |
| `mode=brute_force` | ❌ Falling back to brute force |
| `mode=brute_force_fallback` | ⚠️ Cluster search failed, using fallback |
| `reason=not_yet_clustered` | ⏳ Clustering hasn't run yet |
| `reason=too_few_embeddings` | Need 1000+ embeddings |

## 6. Minimum Requirements

- **1000+ embeddings** required for k-means to trigger
- Fewer embeddings = brute force is faster anyway

## Quick Command Reference

```bash
# Generate + import movies
go run cmd/kmeans-test-data/main.go -mode movies -count 2000 -db ./data/test

# Generate + import clustered embeddings  
go run cmd/kmeans-test-data/main.go -mode clusters -count 5000 -db ./data/test

# Just save to JSON (no import)
go run cmd/kmeans-test-data/main.go -mode movies -count 2000 -output ./data/export

# Run NornicDB with k-means
NORNICDB_GPU_CLUSTERING_ENABLED=true go run cmd/nornicdb/main.go -data ./data/test

# Test search
curl -X POST localhost:7474/nornicdb/search -d '{"query":"test","limit":10}'
```

## Cleanup

```bash
rm -rf ./data/movies-test ./data/cluster-test ./data/stress-test
```
