package search

import (
	"context"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
)

func BenchmarkCPUHNSWBuildCandidateSearch(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	frontier := make([][]float32, 2048)
	for i := range frontier {
		v := make([]float32, 1024)
		for d := range v {
			v[d] = rng.Float32()*2 - 1
		}
		frontier[i] = vector.Normalize(v)
	}
	accelerator := NewCPUHNSWBuildAccelerator()
	if err := accelerator.Prepare(1024, 2048); err != nil {
		b.Fatal(err)
	}
	defer accelerator.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := accelerator.CandidateSearch(context.Background(), frontier[:32], frontier, 128)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestCPUHNSWBuildScratchDoesNotAliasReturnedCandidates(t *testing.T) {
	a := NewCPUHNSWBuildAccelerator()
	if err := a.Prepare(2, 3); err != nil {
		t.Fatal(err)
	}
	frontier := [][]float32{{1, 0}, {0, 1}, {-1, 0}}
	ids, dist, err := a.CandidateSearch(context.Background(), [][]float32{{1, 0}}, frontier, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		_, _, err = a.CandidateSearch(context.Background(), [][]float32{{0, 1}, {-1, 0}}, frontier, 3)
		if err != nil {
			t.Fatal(err)
		}
	}
	if ids[0][0] != 0 || ids[0][1] != 1 || ids[0][2] != 2 || dist[0][0] != 0 || dist[0][1] != 1 || dist[0][2] != 2 {
		t.Fatalf("pooled scratch changed an owned result: %v %v", ids, dist)
	}
}
