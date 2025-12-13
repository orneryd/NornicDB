# GPU Acceleration

**10-100x speedup for vector operations with GPU acceleration.**

## Overview

NornicDB leverages GPU acceleration for:
- Vector similarity search
- K-means clustering
- Embedding generation
- Matrix operations

## Supported Backends

| Backend | Platform | Performance |
|---------|----------|-------------|
| Metal | macOS (Apple Silicon) | Excellent |
| CUDA | NVIDIA GPUs | Excellent |
| Vulkan | Cross-platform | Good |
| CPU | Fallback | Baseline |

## Automatic Detection

GPU acceleration is automatically detected and enabled:

```
🟢 GPU Acceleration: Available (backend: metal)
```

If no GPU is available:
```
🟡 GPU Acceleration: Unavailable (using CPU fallback)
```

## Configuration

### Backend selection environment variable

You can explicitly request which GPU backend NornicDB should use by setting the `NORNICDB_GPU_BACKEND` environment variable. When unset the server will auto-detect an available backend.

Accepted values:
- `auto` (default) — automatically detect the best available backend
- `vulkan` — use the Vulkan backend
- `cuda` — use the CUDA backend (NVIDIA GPUs)
- `metal` — use Apple's Metal backend (macOS/Apple Silicon)
- `cpu` — force CPU-only fallback

If a requested backend is unavailable at runtime, NornicDB will fall back to a compatible backend or to CPU processing.

Examples

PowerShell (temporary for current process):

```powershell
$env:NORNICDB_GPU_BACKEND = 'vulkan'
.\bin\nornicdb.exe serve
```

Unix shell:

```bash
export NORNICDB_GPU_BACKEND=vulkan
./bin/nornicdb serve
```

Docker run example:

```bash
docker run -e NORNICDB_GPU_BACKEND=vulkan \
  -p 7474:7474 -p 7687:7687 \
  --device /dev/dri:/dev/dri \
  timothyswt/nornicdb-amd64-vulkan:latest
```

docker-compose snippet:

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-amd64-vulkan:latest
    environment:
      - NORNICDB_GPU_BACKEND=vulkan
    devices:
      - /dev/dri:/dev/dri
```

Continue to the Docker section for platform-specific build/deploy examples.



### Docker with GPU

**Apple Silicon (Metal):**
```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    # Metal is automatically enabled
```

**NVIDIA (CUDA):**
```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-amd64-cuda:latest
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
```

## Performance Benchmarks

### Vector Search (1M vectors, 1024 dimensions)

| Backend | Latency | Throughput |
|---------|---------|------------|
| Metal (M2) | 2ms | 500 qps |
| CUDA (A100) | 1ms | 1000 qps |
| CPU (AVX2) | 50ms | 20 qps |

### K-Means Clustering (100K points)

| Backend | Time | Speedup |
|---------|------|---------|
| Metal | 120ms | 50x |
| CUDA | 80ms | 75x |
| CPU | 6000ms | 1x |

## GPU Memory Management

### Memory Limits

```go
// GPU manager respects memory limits
gpuManager := gpu.NewManager(&gpu.Config{
    Backend:      gpu.BackendMetal,
    MaxMemoryMB:  2048,  // Limit to 2GB
    BatchSize:    1000,  // Process in batches
})
```

### Automatic Batching

For large datasets, operations are automatically batched:

```go
// Search 10M vectors in batches
results, err := searchService.VectorSearch(ctx, query, 10000000, 10)
// Automatically batched to fit GPU memory
```

## Supported Operations

### Vector Search

```go
// GPU-accelerated cosine similarity search
results, err := db.HybridSearch(ctx, 
    "query text",
    queryEmbedding,  // GPU computes similarity
    nil,
    100,
)
```

### K-Means Clustering

```go
// GPU-accelerated clustering
clusterer := kmeans.NewGPUClusterer(&kmeans.Config{
    K:          10,
    MaxIters:   100,
    Tolerance:  0.0001,
    UseGPU:     true,
})

centroids, assignments, err := clusterer.Cluster(vectors)
```

### Embedding Generation

```go
// GPU-accelerated local embeddings
embedder := embed.NewLocalEmbedder(&embed.Config{
    ModelPath:  "/models/mxbai-embed-large.gguf",
    GPULayers:  -1,  // Auto-detect
})
```

## Troubleshooting

### GPU Not Detected

```bash
# Check GPU status
curl http://localhost:7474/status | jq .gpu
```

### Out of Memory

```bash
# Reduce batch size
export NORNICDB_GPU_BATCH_SIZE=500

# Limit GPU memory
export NORNICDB_GPU_MAX_MEMORY_MB=1024
```

### CUDA Errors

```bash
# Check NVIDIA driver
nvidia-smi

# Check CUDA version
nvcc --version

# Ensure container has GPU access
docker run --gpus all nvidia/cuda:11.8-base nvidia-smi
```

### Metal Errors (macOS)

```bash
# Check Metal support
system_profiler SPDisplaysDataType | grep Metal

# Ensure running on Apple Silicon
uname -m  # Should show arm64
```

## Fallback Behavior

If GPU acceleration fails, NornicDB automatically falls back to CPU:

```go
// Automatic fallback
result, err := gpuManager.VectorSearch(vectors, query, k)
if err == gpu.ErrGPUUnavailable {
    // Falls back to CPU automatically
    result = cpuSearch(vectors, query, k)
}
```

## See Also

- **[Vector Search](../user-guides/vector-search.md)** - Search guide
- **[Memory Decay](memory-decay.md)** - Time-based scoring
- **[Performance](../performance/)** - Benchmarks

