package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

var quantizedDotBenchSink float32

func BenchmarkHNSWQuantizedDot(b *testing.B) {
	const dim = 1024
	rng := rand.New(rand.NewSource(42))
	v := make([]float32, dim)
	query := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
		query[i] = rng.Float32()*2 - 1
	}
	v = vector.Normalize(v)
	query = vector.Normalize(query)
	b.Run("float32", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			quantizedDotBenchSink = vector.DotProductSIMD(query, v)
		}
	})
	for _, mode := range []HNSWQuantization{HNSWQuantizationScalar, HNSWQuantizationBinary, HNSWQuantizationProduct} {
		q, err := newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: mode, PQCompression: 16}, dim, [][]float32{v})
		if err != nil {
			b.Fatal(err)
		}
		q.set(0, v)
		scorer := q.query(query)
		b.Run(fmt.Sprint(mode), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				quantizedDotBenchSink = scorer.dot(0)
			}
		})
	}
}

// BenchmarkHNSWQuantizedQueryEncode includes the per-query lookup-table work
// omitted by the score-only microbenchmark. PQ has 256 codewords per subspace.
func BenchmarkHNSWQuantizedQueryEncode(b *testing.B) {
	const dim = 1024
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(i%17 - 8)
	}
	v = vector.Normalize(v)
	for _, cfg := range []HNSWQuantizationConfig{{Mode: HNSWQuantizationScalar}, {Mode: HNSWQuantizationBinary}, {Mode: HNSWQuantizationBinary, BinaryQueryBits: 4}, {Mode: HNSWQuantizationBinary, BinaryQueryBits: 8}, {Mode: HNSWQuantizationProduct, PQCompression: 16}} {
		cfg, _ = normalizeHNSWQuantizationConfig(cfg)
		q := &hnswQuantizedVectors{config: cfg, dimensions: dim, stride: dim}
		if cfg.Mode == HNSWQuantizationBinary {
			q.stride = (dim + 63) / 64
		}
		if cfg.Mode == HNSWQuantizationProduct {
			q.stride = dim / 4
			q.books = make([]ivfpqCodebook, q.stride)
			for seg := range q.books {
				q.books[seg] = ivfpqCodebook{SubDim: 4, Codeword: make([][]float32, 256)}
				for code := range q.books[seg].Codeword {
					q.books[seg].Codeword[code] = v[seg*4 : seg*4+4]
				}
			}
		}
		b.Run(fmt.Sprintf("%s/query%d", cfg.Mode, cfg.BinaryQueryBits), func(b *testing.B) {
			warm := q.query(v)
			warm.release()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scorer := q.query(v)
				quantizedDotBenchSink = scorer.scale
				scorer.release()
			}
		})
	}
}
