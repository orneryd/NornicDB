package search

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type failingHNSWBuildAccelerator struct{}

func (f failingHNSWBuildAccelerator) Prepare(int, int) error { return nil }
func (f failingHNSWBuildAccelerator) CandidateSearch(context.Context, [][]float32, [][]float32, int) ([][]int, [][]float32, error) {
	return nil, nil, errors.New("forced accelerator failure")
}
func (f failingHNSWBuildAccelerator) Close() error { return nil }

func TestHNSWBuildGPUConfigFromEnv(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_BATCH_SIZE", "64")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_CANDIDATE_K", "32")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_DISTANCE_PRECISION", "fp32")

	config := HNSWConfigFromEnv()
	assert.False(t, config.UseGPUBuild)
	assert.Equal(t, 64, config.GPUBuildBatchSize)
	assert.Equal(t, 32, config.GPUBuildCandidateK)
	assert.Equal(t, "fp32", config.GPUBuildDistancePrecision)
}

func TestHNSWIndexSupportsGPUBuild(t *testing.T) {
	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	assert.True(t, idx.SupportsGPUBuild())
	assert.False(t, (*HNSWIndex)(nil).SupportsGPUBuild())
}

func TestCPUHNSWBuildAcceleratorCandidateSearchStable(t *testing.T) {
	accel := NewCPUHNSWBuildAccelerator()
	require.NoError(t, accel.Prepare(2, 4))

	frontier := [][]float32{
		{1, 0},
		{1, 0},
		{0, 1},
	}
	for _, vec := range frontier {
		vector.NormalizeInPlace(vec)
	}
	indices, distances, err := accel.CandidateSearch(context.Background(), [][]float32{{1, 0}}, frontier, 2)
	require.NoError(t, err)
	require.Len(t, indices, 1)
	require.Equal(t, []int{0, 1}, indices[0], "equal distances should retain stable index ordering")
	require.Len(t, distances[0], 2)
	assert.InDelta(t, 0, distances[0][0], 1e-6)
	assert.InDelta(t, 0, distances[0][1], 1e-6)
}

func TestBuildHNSWWithOptionalGPUFallback(t *testing.T) {
	pairs := deterministicHNSWBuildPairs(128, 8)
	config := DefaultHNSWConfig()
	config.UseGPUBuild = true
	config.GPUBuildBatchSize = 16
	config.GPUBuildCandidateK = 8

	idx, stats, err := buildHNSWWithOptionalGPU(context.Background(), 8, config, nil, len(pairs), hnswPairSliceIterator(pairs), failingHNSWBuildAccelerator{})
	require.NoError(t, err)
	require.NotNil(t, idx)
	assert.Equal(t, len(pairs), idx.Size())
	assert.True(t, stats.Fallback)
	assert.Equal(t, "batch_parallel", stats.Strategy)
	assert.Equal(t, "*search.CPUHNSWBuildAccelerator", stats.Backend)
	assert.GreaterOrEqual(t, stats.KernelErrors, 1)

	results, err := idx.Search(context.Background(), pairs[0].vec, 1, 0)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, pairs[0].id, results[0].ID)
}

func TestBuildHNSWAcceleratedCPUShim(t *testing.T) {
	pairs := deterministicHNSWBuildPairs(256, 16)
	config := DefaultHNSWConfig()
	config.UseGPUBuild = true
	config.GPUBuildBatchSize = 32
	config.GPUBuildCandidateK = 16

	idx, stats, err := buildHNSWWithOptionalGPU(context.Background(), 16, config, nil, len(pairs), hnswPairSliceIterator(pairs), NewCPUHNSWBuildAccelerator())
	require.NoError(t, err)
	require.NotNil(t, idx)
	assert.Equal(t, len(pairs), idx.Size())
	assert.Equal(t, "batch_parallel", stats.Strategy)
	assert.NotZero(t, stats.Batches)

	for _, query := range []hnswBuildPair{pairs[0], pairs[17], pairs[99]} {
		results, err := idx.Search(context.Background(), query.vec, 5, 0)
		require.NoError(t, err)
		require.NotEmpty(t, results)
		assert.Equal(t, query.id, results[0].ID)
	}
}

func BenchmarkHNSWBuildCPUVsAcceleratedCandidates(b *testing.B) {
	pairs := deterministicHNSWBuildPairs(1500, 64)
	baseConfig := DefaultHNSWConfig()
	baseConfig.UseGPUBuild = false
	gpuConfig := baseConfig
	gpuConfig.UseGPUBuild = true
	gpuConfig.GPUBuildBatchSize = 512
	gpuConfig.GPUBuildCandidateK = 64

	b.Run("CPU", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx, _, err := buildHNSWSequential(64, baseConfig, pairs)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedCPUShim", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 64, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), NewCPUHNSWBuildAccelerator())
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedMetal", func(b *testing.B) {
		if _, err := NewMetalHNSWBuildAccelerator(); err != nil {
			b.Skipf("Metal unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewMetalHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 64, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedCUDA", func(b *testing.B) {
		if _, err := NewCudaHNSWBuildAccelerator(); err != nil {
			b.Skipf("CUDA unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewCudaHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 64, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedVulkan", func(b *testing.B) {
		if _, err := NewVulkanHNSWBuildAccelerator(); err != nil {
			b.Skipf("Vulkan unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewVulkanHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 64, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
}

func BenchmarkHNSWBuildCPUVsAcceleratedCandidates1024D(b *testing.B) {
	pairs := deterministicHNSWBuildPairs(1500, 1024)
	baseConfig := DefaultHNSWConfig()
	baseConfig.UseGPUBuild = false
	gpuConfig := baseConfig
	gpuConfig.UseGPUBuild = true
	gpuConfig.GPUBuildBatchSize = 512
	gpuConfig.GPUBuildCandidateK = 128

	b.Run("CPU", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx, _, err := buildHNSWSequential(1024, baseConfig, pairs)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedMetal", func(b *testing.B) {
		if _, err := NewMetalHNSWBuildAccelerator(); err != nil {
			b.Skipf("Metal unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewMetalHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 1024, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedCUDA", func(b *testing.B) {
		if _, err := NewCudaHNSWBuildAccelerator(); err != nil {
			b.Skipf("CUDA unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewCudaHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 1024, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedVulkan", func(b *testing.B) {
		if _, err := NewVulkanHNSWBuildAccelerator(); err != nil {
			b.Skipf("Vulkan unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewVulkanHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 1024, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
}

func BenchmarkHNSWBuildCPUVsAcceleratedCandidatesLarge1024D(b *testing.B) {
	pairs := deterministicHNSWBuildPairs(8000, 1024)
	baseConfig := DefaultHNSWConfig()
	baseConfig.UseGPUBuild = false
	gpuConfig := baseConfig
	gpuConfig.UseGPUBuild = true
	gpuConfig.GPUBuildBatchSize = 2048
	gpuConfig.GPUBuildCandidateK = 128

	b.Run("CPU", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx, _, err := buildHNSWSequential(1024, baseConfig, pairs)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedMetal", func(b *testing.B) {
		if _, err := NewMetalHNSWBuildAccelerator(); err != nil {
			b.Skipf("Metal unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewMetalHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 1024, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedCUDA", func(b *testing.B) {
		if _, err := NewCudaHNSWBuildAccelerator(); err != nil {
			b.Skipf("CUDA unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewCudaHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 1024, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
	b.Run("AcceleratedVulkan", func(b *testing.B) {
		if _, err := NewVulkanHNSWBuildAccelerator(); err != nil {
			b.Skipf("Vulkan unavailable: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			accel, err := NewVulkanHNSWBuildAccelerator()
			if err != nil {
				b.Fatal(err)
			}
			idx, _, err := buildHNSWWithOptionalGPU(context.Background(), 1024, gpuConfig, nil, len(pairs), hnswPairSliceIterator(pairs), accel)
			if err != nil {
				b.Fatal(err)
			}
			if idx.Size() != len(pairs) {
				b.Fatalf("size mismatch: got %d want %d", idx.Size(), len(pairs))
			}
		}
	})
}

func BenchmarkHNSWBuildFromVectorFileStore(b *testing.B) {
	base, dim, ok := findHNSWBuildBenchVectorStore(b)
	if !ok {
		b.Skip("no vector store found; set NORNICDB_HNSW_BUILD_BENCH_VECTOR_BASE and NORNICDB_HNSW_BUILD_BENCH_DIMENSIONS or place data under ./data")
	}
	vfs, err := NewVectorFileStore(base, dim)
	require.NoError(b, err)
	defer vfs.Close()
	require.NoError(b, vfs.Load())
	total := vfs.Count()
	require.Greater(b, total, 0)

	config := HNSWConfigFromEnv()
	config.UseGPUBuild = false
	iter := hnswVectorFileStoreIterator(vfs, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, _, err := buildHNSWWithOptionalGPU(context.Background(), dim, config, nil, total, iter, nil)
		if err != nil {
			b.Fatal(err)
		}
		if idx.Size() != total {
			b.Fatalf("size mismatch: got %d want %d", idx.Size(), total)
		}
	}
}

func deterministicHNSWBuildPairs(n, dim int) []hnswBuildPair {
	rng := rand.New(rand.NewSource(42))
	pairs := make([]hnswBuildPair, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		cluster := i % 16
		vec[cluster%dim] = 2
		for d := range vec {
			vec[d] += float32(rng.NormFloat64() * 0.01)
		}
		pairs[i] = hnswBuildPair{
			id:  fmt.Sprintf("vec-%05d", i),
			vec: vec,
		}
	}
	return pairs
}

func findHNSWBuildBenchVectorStore(tb testing.TB) (string, int, bool) {
	if base := os.Getenv("NORNICDB_HNSW_BUILD_BENCH_VECTOR_BASE"); base != "" {
		dim := 0
		if raw := os.Getenv("NORNICDB_HNSW_BUILD_BENCH_DIMENSIONS"); raw != "" {
			_, _ = fmt.Sscanf(raw, "%d", &dim)
		}
		if dim <= 0 {
			var err error
			dim, err = readVectorStoreMetaDimensions(base + ".meta")
			require.NoError(tb, err)
		}
		return base, dim, true
	}
	root := "./data"
	if _, err := os.Stat(root); err != nil {
		return "", 0, false
	}
	var foundBase string
	var foundDim int
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || foundBase != "" || !strings.HasSuffix(path, ".meta") {
			return nil
		}
		base := strings.TrimSuffix(path, ".meta")
		if _, statErr := os.Stat(base + ".vec"); statErr != nil {
			return nil
		}
		dim, metaErr := readVectorStoreMetaDimensions(path)
		if metaErr != nil || dim <= 0 {
			return nil
		}
		foundBase = base
		foundDim = dim
		return nil
	})
	return foundBase, foundDim, foundBase != ""
}

func readVectorStoreMetaDimensions(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var meta VectorFileStoreMeta
	if err := msgpack.NewDecoder(f).Decode(&meta); err != nil {
		return 0, err
	}
	return meta.Dimensions, nil
}
