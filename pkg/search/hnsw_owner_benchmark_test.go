package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// This identical public-API fixture runs on the release base and the port.
func lifecycleBenchmarkFixture(b *testing.B, indexed bool) (*Service, *storage.NamespacedEngine, []float32) {
	b.Helper()
	engine := storage.NewNamespacedEngine(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "hnsw-bench")
	svc := NewService(engine, 64, nil)
	svc.SetRuntimeStrategyTransitionsEnabled(false)
	rng := rand.New(rand.NewSource(86))
	var query []float32
	for i := 0; i < 512; i++ {
		vector := make([]float32, 64)
		for j := range vector {
			vector[j] = rng.Float32()*2 - 1
		}
		if i == 42 {
			query = append([]float32(nil), vector...)
		}
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("node-%04d", i)), ChunkEmbeddings: [][]float32{vector}}
		if _, err := engine.CreateNode(node); err != nil {
			b.Fatal(err)
		}
		if indexed {
			if err := svc.IndexNode(node); err != nil {
				b.Fatal(err)
			}
		}
	}
	return svc, engine, query
}

func BenchmarkHNSWLifecycleRequest(b *testing.B) {
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	b.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	b.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	minimum := -1.0
	options := &SearchOptions{Limit: 10, MinSimilarity: &minimum}
	b.Run("first", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			svc, engine, query := lifecycleBenchmarkFixture(b, true)
			b.StartTimer()
			response, err := svc.Search(context.Background(), "", query, options)
			b.StopTimer()
			if err != nil || response == nil || len(response.Results) == 0 || response.Results[0].NodeID != "node-0042" {
				b.Fatalf("search: %v", err)
			}
			if err := svc.Close(); err != nil {
				b.Fatal(err)
			}
			_ = engine.Close()
		}
	})
	b.Run("ready", func(b *testing.B) {
		svc, engine, query := lifecycleBenchmarkFixture(b, false)
		defer engine.Close()
		defer svc.Close()
		if err := svc.BuildIndexes(context.Background()); err != nil {
			b.Fatal(err)
		}
		if svc.CurrentStrategy() != "HNSW" {
			b.Fatalf("build did not publish HNSW: %s", svc.CurrentStrategy())
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			response, err := svc.Search(context.Background(), "", query, options)
			if err != nil || len(response.Results) == 0 || response.Results[0].NodeID != "node-0042" {
				b.Fatalf("search: %v", err)
			}
		}
	})
}

func BenchmarkHNSWLifecycleBuild(b *testing.B) {
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	b.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		svc, engine, _ := lifecycleBenchmarkFixture(b, false)
		b.StartTimer()
		err := svc.BuildIndexes(context.Background())
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if svc.CurrentStrategy() != "HNSW" {
			b.Fatalf("build did not publish HNSW: %s", svc.CurrentStrategy())
		}
		if err := svc.Close(); err != nil {
			b.Fatal(err)
		}
		_ = engine.Close()
	}
}
