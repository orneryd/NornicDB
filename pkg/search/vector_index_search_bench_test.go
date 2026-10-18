package search

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkVectorIndexSearch measures the exact (brute-force) scan the CPU
// brute-force strategy runs: 50,000 × 1,024-d vectors, the top 200 for a query.
func BenchmarkVectorIndexSearch(b *testing.B) {
	const n, dims, limit = 50000, 1024, 200
	rng := rand.New(rand.NewSource(1))
	vi := NewVectorIndex(dims)
	for i := 0; i < n; i++ {
		v := make([]float32, dims)
		for d := range v {
			v[d] = float32(rng.NormFloat64())
		}
		if err := vi.Add(fmt.Sprintf("v%d", i), v); err != nil {
			b.Fatal(err)
		}
	}
	query := make([]float32, dims)
	for d := range query {
		query[d] = float32(rng.NormFloat64())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := vi.Search(context.Background(), query, limit, -1); err != nil {
			b.Fatal(err)
		}
	}
}

// The exact scan returns exactly the best limit vectors by cosine, best first,
// on one goroutine (small index) and across workers (large index); it honours
// minSimilarity and stops on a canceled context.
func TestVectorIndexSearchExactTopK(t *testing.T) {
	for _, n := range []int{500, 3 * vectorIndexScanChunk * 4} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(n)))
			const dims = 32
			vi := NewVectorIndex(dims)
			for i := 0; i < n; i++ {
				v := make([]float32, dims)
				for d := range v {
					v[d] = float32(rng.NormFloat64())
				}
				if err := vi.Add(fmt.Sprintf("v%d", i), v); err != nil {
					t.Fatal(err)
				}
			}
			query := make([]float32, dims)
			for d := range query {
				query[d] = float32(rng.NormFloat64())
			}
			all, err := vi.Search(context.Background(), query, n, -1)
			if err != nil || len(all) != n {
				t.Fatalf("full scan: %v, %d results", err, len(all))
			}
			for i := 1; i < len(all); i++ {
				if all[i].Score > all[i-1].Score {
					t.Fatalf("not best first at %d", i)
				}
			}
			top, err := vi.Search(context.Background(), query, 25, -1)
			if err != nil || len(top) != 25 {
				t.Fatalf("top 25: %v, %d results", err, len(top))
			}
			for i := range top {
				if top[i].Score != all[i].Score {
					t.Fatalf("top[%d] = %v, want score %v", i, top[i], all[i].Score)
				}
			}
			threshold := all[9].Score
			above, err := vi.Search(context.Background(), query, n, threshold)
			if err != nil || len(above) < 10 || above[len(above)-1].Score < threshold {
				t.Fatalf("minSimilarity: %v, %d results", err, len(above))
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := vi.Search(canceled, query, 25, -1); err == nil {
				t.Fatal("a canceled scan must fail")
			}
		})
	}
}
