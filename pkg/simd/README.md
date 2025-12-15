# SIMD Vector Operations

High-performance SIMD-accelerated vector operations for NornicDB embeddings.

## Features

- **Auto-detection**: Automatically uses best available backend
- **Metal GPU**: Auto-enabled on macOS with CGO (Apple Silicon optimized)
- **CPU SIMD**: Uses [vek](https://github.com/viterin/vek) for AVX2/NEON acceleration
- **Graceful fallback**: Always works, even without GPU

## Backend Selection

| Platform | CGO | Backend | Batch Operations |
|----------|-----|---------|------------------|
| macOS (darwin) | Yes | Metal GPU + CPU SIMD | 2-3x faster |
| macOS (darwin) | No | Pure Go | Baseline |
| Linux arm64 | Yes | CPU SIMD (NEON) | 4-10x faster |
| Linux amd64 | Yes | CPU SIMD (AVX2) | 6-12x faster |
| Any | No | Pure Go | Baseline |

## Usage

```go
import "github.com/orneryd/nornicdb/pkg/simd"

// Single vector operations (auto-selects best backend)
similarity := simd.CosineSimilarity(vecA, vecB)
distance := simd.EuclideanDistance(vecA, vecB)
dot := simd.DotProduct(vecA, vecB)

// Batch operations (Metal GPU on macOS, CPU fallback elsewhere)
scores := make([]float32, numVectors)
err := simd.BatchCosineSimilarityMetal(embeddings, query, scores)
if err != nil {
    // Metal not available, use CPU loop
    for i := 0; i < numVectors; i++ {
        start := i * dimensions
        end := start + dimensions
        scores[i] = simd.CosineSimilarity(embeddings[start:end], query)
    }
}

// Check what's available
if simd.MetalAvailable() {
    fmt.Println("Metal GPU acceleration enabled")
}
info := simd.Info()
fmt.Printf("Using: %s (accelerated=%v)\n", info.Implementation, info.Accelerated)
```

## Build Tags

Metal is **auto-enabled** on macOS with CGO. Use build tags to control:

```bash
# Default build (Metal auto-enabled on macOS)
go build ./...

# Disable Metal (CPU-only)
go build -tags nometal ./...
```

## Benchmarks

### CPU SIMD (vek)

```bash
go test ./pkg/simd -bench=Benchmark -benchmem -run=^$
```

### Metal GPU vs CPU

```bash
go test ./pkg/simd -bench=BenchmarkMetal -benchmem -run=^$
```

### Sample Results (Apple M2 Max)

| Operation | Batch Size | Metal GPU | CPU SIMD | Speedup |
|-----------|------------|-----------|----------|---------|
| Cosine Similarity | 1K × 768 | 679μs | 892μs | **1.3x** |
| Cosine Similarity | 10K × 768 | 4.2ms | 8.7ms | **2.1x** |
| Cosine Similarity | 100K × 768 | 37.8ms | 87.0ms | **2.3x** |
| Cosine Similarity | 10K × 1536 | 7.7ms | 17.9ms | **2.3x** |

**Key insight**: Metal shines for batch operations (1K+ vectors). For single vectors, CPU SIMD is faster due to GPU dispatch overhead.

## API Reference

### Single Vector Operations

```go
// Cosine similarity (-1 to 1, higher = more similar)
func CosineSimilarity(a, b []float32) float32

// Euclidean distance (0 = identical)
func EuclideanDistance(a, b []float32) float32

// Dot product
func DotProduct(a, b []float32) float32

// Vector norm (magnitude)
func Norm(v []float32) float32

// Normalize in-place
func NormalizeInPlace(v []float32)
```

### Batch Operations (Metal GPU)

```go
// Batch cosine similarity (query vs N embeddings)
func BatchCosineSimilarityMetal(embeddings, query, scores []float32) error

// Batch dot product
func BatchDotProductMetal(embeddings, query, results []float32) error

// Batch Euclidean distance
func BatchEuclideanDistanceMetal(embeddings, query, distances []float32) error

// Batch normalize
func BatchNormalizeMetal(vectors []float32, numVectors, dimensions int) error
```

### Utilities

```go
// Check if Metal GPU is available
func MetalAvailable() bool

// Get runtime info (implementation, features, accelerated)
func Info() RuntimeInfo
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Public API (simd.go)                 │
│  CosineSimilarity, DotProduct, BatchCosineSimilarity... │
└─────────────────────────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│  simd_arm64   │  │  simd_amd64   │  │ simd_generic  │
│   (NEON/vek)  │  │   (AVX2/vek)  │  │   (pure Go)   │
└───────────────┘  └───────────────┘  └───────────────┘
            │               │
            └───────┬───────┘
                    ▼
┌─────────────────────────────────────────────────────────┐
│              simd_metal_darwin.go (macOS only)          │
│         Metal GPU kernels for batch operations          │
│    Auto-fallback to CPU SIMD if Metal unavailable       │
└─────────────────────────────────────────────────────────┘
```