package search

import (
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/math/vector"
)

func BenchmarkVectorSearchStrategyBreakEven(b *testing.B) {
	dims := 1024
	sizes := []int{5_000, 20_000, 100_000}

	makeData := func(n int) (ids []string, vecs [][]float32, query []float32) {
		rng := rand.New(rand.NewSource(1))
		ids = make([]string, n)
		vecs = make([][]float32, n)
		for i := 0; i < n; i++ {
			ids[i] = "id-" + itoa(i)
			v := make([]float32, dims)
			for j := 0; j < dims; j++ {
				v[j] = rng.Float32()
			}
			vecs[i] = vector.Normalize(v)
		}
		q := make([]float32, dims)
		for j := 0; j < dims; j++ {
			q[j] = rng.Float32()
		}
		query = vector.Normalize(q)
		return ids, vecs, query
	}

	for _, n := range sizes {
		b.Run("n="+itoa(n), func(b *testing.B) {
			ids, vecs, query := makeData(n)
			k := 20

			// CPU brute-force
			cpuIndex := NewVectorIndex(dims)
			for i := range ids {
				_ = cpuIndex.Add(ids[i], vecs[i])
			}

			// HNSW (ANN)
			hnsw := NewHNSWIndex(dims, HNSWConfigFromEnv())
			for i := range ids {
				_ = hnsw.Add(ids[i], vecs[i])
			}

			// GPU brute-force (if available)
			var gpuIndex *gpu.EmbeddingIndex
			{
				cfg := gpu.DefaultConfig()
				cfg.Enabled = true
				cfg.FallbackOnError = true
				manager, err := gpu.NewManager(cfg)
				if err == nil && manager != nil && manager.IsEnabled() {
					icfg := gpu.DefaultEmbeddingIndexConfig(dims)
					icfg.GPUEnabled = true
					icfg.AutoSync = false
					gpuIndex = gpu.NewEmbeddingIndex(manager, icfg)
					_ = gpuIndex.AddBatch(ids, vecs)
					_ = gpuIndex.SyncToGPU()
				}
			}

			b.Run("cpu_brute", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = cpuIndex.Search(b.Context(), query, k, 0.0)
				}
			})

			b.Run("hnsw", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = hnsw.Search(b.Context(), query, k, 0.0)
				}
			})

			b.Run("gpu_brute", func(b *testing.B) {
				if gpuIndex == nil {
					b.Skip("gpu index unavailable")
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = gpuIndex.Search(query, k)
				}
			})
		})
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [32]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
