# K-Means Clustering

**K-Means clustering for vector embeddings (not database clustering).**

## 📚 Documentation

- **[K-Means Algorithm](kmeans-algorithm.md)** - Algorithm details and how clustering speeds up vector search
- **[Metal Optimizations](metal-optimizations.md)** - Apple Silicon-specific kernel notes

## 🎯 What is K-Means Clustering?

K-Means clustering groups similar vectors together. NornicDB uses it as an inverted-file approximate nearest-neighbor (IVF) candidate generator: queries first identify the most relevant clusters, then perform exact similarity scoring within those clusters. This is faster than full brute-force search on large datasets.

## Quick Start

K-means clustering is **off by default**. Enable it through the feature flag:

```bash
export NORNICDB_KMEANS_CLUSTERING_ENABLED=true
./nornicdb serve
```

The cluster index is built automatically once the dataset reaches `NORNICDB_KMEANS_MIN_EMBEDDINGS` (default `1000`). Below that threshold, brute-force search is used because it is faster on small data. The number of clusters is chosen automatically as `K ≈ √(N/2)` (capped at 8192) unless you override it via `NORNICDB_KMEANS_NUM_CLUSTERS`.

For the user-facing tuning surface, see [Vector Search → Performance Tuning](../../user-guides/vector-search.md#performance-tuning).

## 📖 Learn More

- **[K-Means Algorithm](kmeans-algorithm.md)** — How K-Means works in NornicDB's IVF candidate generator
- **[Metal Optimizations](metal-optimizations.md)** — Atomic-float workaround for the Metal kernel
- **[Vector Search](../../user-guides/vector-search.md)** — User-facing vector search guide that consumes the cluster index
- **[GPU Acceleration](../../features/gpu-acceleration.md)** — Backend selection (Metal/CUDA/Vulkan) used by the kernel

---

**Get started** → **[K-Means Algorithm](kmeans-algorithm.md)**
