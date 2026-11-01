package cypher

// apoc_algorithm_cancel_bench_test.go measures the healthy (uncancelled) public
// path of the five whole-graph algorithms that observe caller cancellation
// (apoc.algo.pageRank, betweenness, closeness, labelPropagation, wcc) on a small
// and a large adjacency population. The same fixture and statements run before
// and after the checkpoints were added, so the cost of the observations is
// visible in the same workload it interrupts.

import (
	"context"
	"testing"
)

// apocAlgorithmBenchmarkSink keeps a benchmarked statement's row count from
// being optimized away.
var apocAlgorithmBenchmarkSink int

func BenchmarkApocAlgorithmHealthy(b *testing.B) {
	for _, population := range []struct {
		name  string
		nodes int
	}{
		{"small", 32},
		{"large", 512},
	} {
		fixture := newAlgorithmCancelFixture(b, population.nodes)
		for _, algorithm := range apocAlgorithmCases() {
			b.Run(population.name+"/"+algorithm.name, func(b *testing.B) {
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := fixture.exec.Execute(ctx, algorithm.query, nil)
					if err != nil {
						b.Fatal(err)
					}
					if want := population.nodes; len(result.Rows) != want {
						b.Fatalf("%s returned %d rows, want %d", algorithm.name, len(result.Rows), want)
					}
					apocAlgorithmBenchmarkSink = len(result.Rows)
				}
			})
		}
	}
}
