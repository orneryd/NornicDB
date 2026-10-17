package search

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

// BenchmarkHNSWChurn measures an index under delete and re-insert churn:
// remove and insert latency, search latency once churn has left tombstones,
// search next to a large removed region, and heap per node. It uses only the
// public API so the same file runs on both sides of a paired A/B.
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

	b.Run("search_after_churn", func(b *testing.B) {
		index, vectors := build(b)
		rng := rand.New(rand.NewSource(9))
		for i := 0; i < n*3/10; i++ { // 30% of the population churned once
			at := rng.Intn(n)
			id := fmt.Sprintf("v-%d", at)
			index.Remove(id)
			if err := index.Add(id, vectors[at]); err != nil {
				b.Fatal(err)
			}
		}
		queries := make([][]float32, 64)
		for i := range queries {
			queries[i] = query(rng)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := index.Search(context.Background(), queries[i%len(queries)], 10, -1); err != nil {
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
		for i := 0; i < b.N; i++ {
			if _, err := index.Search(context.Background(), center, 1, -1); err != nil {
				b.Fatal(err)
			}
		}
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
