package search

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"testing"
	"time"
)

// BenchmarkHNSWChurn measures an index under delete and re-insert churn:
// remove and insert latency, search latency and recall@10 once churn has left
// tombstones, search latency and recall@10 next to a large removed region,
// Load of a churned graph, and heap per node. It uses only the public API so
// the same file runs on both sides of a paired A/B.
func BenchmarkHNSWChurn(b *testing.B) {
	const (
		n    = 5000
		dims = 64
	)
	build := func(b *testing.B) (*HNSWIndex, [][]float32) {
		b.Helper()
		rng := rand.New(rand.NewSource(42))
		vectors := make([][]float32, n)
		index := NewHNSWIndex(dims, DefaultHNSWConfig())
		for i := range vectors {
			vectors[i] = make([]float32, dims)
			for j := range vectors[i] {
				vectors[i][j] = float32(rng.NormFloat64())
			}
			if err := index.Add(fmt.Sprintf("v-%d", i), vectors[i]); err != nil {
				b.Fatal(err)
			}
		}
		return index, vectors
	}
	query := func(rng *rand.Rand) []float32 {
		q := make([]float32, dims)
		for j := range q {
			q[j] = float32(rng.NormFloat64())
		}
		return q
	}

	b.Run("remove_reinsert", func(b *testing.B) {
		index, vectors := build(b)
		rng := rand.New(rand.NewSource(7))
		var removeTime, insertTime time.Duration
		b.ResetTimer()
		pprof.SetGoroutineLabels(pprof.WithLabels(b.Context(), pprof.Labels("phase", "measured-churn")))
		defer pprof.SetGoroutineLabels(b.Context())
		for i := 0; i < b.N; i++ {
			at := rng.Intn(n)
			id := fmt.Sprintf("v-%d", at)
			started := time.Now()
			index.Remove(id)
			removed := time.Now()
			if err := index.Add(id, vectors[at]); err != nil {
				b.Fatal(err)
			}
			removeTime += removed.Sub(started)
			insertTime += time.Since(removed)
		}
		b.ReportMetric(float64(removeTime.Nanoseconds())/float64(b.N), "remove-ns/op")
		b.ReportMetric(float64(insertTime.Nanoseconds())/float64(b.N), "insert-ns/op")
	})

	churn := func(b *testing.B, index *HNSWIndex, vectors [][]float32, rng *rand.Rand) {
		b.Helper()
		for i := 0; i < n*3/10; i++ { // 30% of the population churned once
			at := rng.Intn(n)
			id := fmt.Sprintf("v-%d", at)
			index.Remove(id)
			if err := index.Add(id, vectors[at]); err != nil {
				b.Fatal(err)
			}
		}
	}
	cosine := func(a, c []float32) float64 {
		var dot, na, nc float64
		for j := range a {
			dot += float64(a[j] * c[j])
			na += float64(a[j] * a[j])
			nc += float64(c[j] * c[j])
		}
		return dot / (math.Sqrt(na*nc) + 1e-12)
	}
	// top10 is the exact top 10 of the given population for q.
	top10 := func(q []float32, vectors [][]float32, population []int) map[string]bool {
		order := slices.Clone(population)
		sort.Slice(order, func(a, c int) bool {
			return cosine(q, vectors[order[a]]) > cosine(q, vectors[order[c]])
		})
		truth := map[string]bool{}
		for _, i := range order[:10] {
			truth[fmt.Sprintf("v-%d", i)] = true
		}
		return truth
	}

	b.Run("search_after_churn", func(b *testing.B) {
		index, vectors := build(b)
		rng := rand.New(rand.NewSource(9))
		churn(b, index, vectors, rng)
		queries := make([][]float32, 64)
		for i := range queries {
			queries[i] = query(rng)
		}
		b.ResetTimer()
		pprof.SetGoroutineLabels(pprof.WithLabels(b.Context(), pprof.Labels("phase", "measured-churn")))
		defer pprof.SetGoroutineLabels(b.Context())
		for i := 0; i < b.N; i++ {
			if _, err := index.Search(context.Background(), queries[i%len(queries)], 10, -1); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		all := make([]int, n)
		for i := range all {
			all[i] = i
		}
		hits := 0
		for _, q := range queries { // every vector is live after churn
			truth := top10(q, vectors, all)
			results, err := index.Search(context.Background(), q, 10, -1)
			if err != nil {
				b.Fatal(err)
			}
			for _, result := range results {
				if truth[result.ID] {
					hits++
				}
			}
		}
		b.ReportMetric(float64(hits)/float64(10*len(queries)), "recall@10")
	})

	b.Run("load_after_churn", func(b *testing.B) {
		index, vectors := build(b)
		churn(b, index, vectors, rand.New(rand.NewSource(9)))
		path := filepath.Join(b.TempDir(), "hnsw")
		if err := index.Save(path); err != nil {
			b.Fatal(err)
		}
		byID := make(map[string][]float32, n)
		for i, v := range vectors {
			byID[fmt.Sprintf("v-%d", i)] = v
		}
		lookup := func(id string) ([]float32, bool) { v, ok := byID[id]; return v, ok }
		b.ResetTimer()
		pprof.SetGoroutineLabels(pprof.WithLabels(b.Context(), pprof.Labels("phase", "measured-churn")))
		defer pprof.SetGoroutineLabels(b.Context())
		for i := 0; i < b.N; i++ {
			loaded, err := LoadHNSWIndex(path, lookup)
			if err != nil || loaded == nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("search_near_removed_region", func(b *testing.B) {
		index, vectors := build(b)
		center := vectors[0]
		removed := 0
		for i, v := range vectors { // about a third: the vectors most aligned with center
			var dot float32
			for j := range v {
				dot += v[j] * center[j]
			}
			if dot > 3.5 {
				index.Remove(fmt.Sprintf("v-%d", i))
				removed++
			}
		}
		b.ReportMetric(float64(removed), "removed")
		b.ResetTimer()
		pprof.SetGoroutineLabels(pprof.WithLabels(b.Context(), pprof.Labels("phase", "measured-churn")))
		defer pprof.SetGoroutineLabels(b.Context())
		for i := 0; i < b.N; i++ {
			if _, err := index.Search(context.Background(), center, 1, -1); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("recall_near_removed_region", func(b *testing.B) {
		index, vectors := build(b)
		center := vectors[0]
		var live []int
		for i, v := range vectors { // as above: remove about a third
			var dot float32
			for j := range v {
				dot += v[j] * center[j]
			}
			if dot > 3.5 {
				index.Remove(fmt.Sprintf("v-%d", i))
			} else {
				live = append(live, i)
			}
		}
		rng := rand.New(rand.NewSource(11))
		queries := make([][]float32, 32)
		truth := make([]map[string]bool, len(queries))
		for q := range queries {
			queries[q] = make([]float32, dims)
			for j := range queries[q] {
				queries[q][j] = center[j] + 0.5*float32(rng.NormFloat64())
			}
			truth[q] = top10(queries[q], vectors, live)
		}
		hits, total := 0, 0
		b.ResetTimer()
		pprof.SetGoroutineLabels(pprof.WithLabels(b.Context(), pprof.Labels("phase", "measured-churn")))
		defer pprof.SetGoroutineLabels(b.Context())
		for i := 0; i < b.N; i++ {
			q := i % len(queries)
			results, err := index.Search(context.Background(), queries[q], 10, -1)
			if err != nil {
				b.Fatal(err)
			}
			for _, result := range results {
				if truth[q][result.ID] {
					hits++
				}
			}
			total += 10
		}
		b.ReportMetric(float64(hits)/float64(total), "recall@10")
	})

	b.Run("heap_per_node", func(b *testing.B) {
		var before, after runtime.MemStats
		for i := 0; i < b.N; i++ {
			runtime.GC()
			runtime.ReadMemStats(&before)
			index, vectors := build(b)
			runtime.GC()
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/n, "heap-B/node")
			runtime.KeepAlive(index)
			runtime.KeepAlive(vectors)
		}
	})
}
