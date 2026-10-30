package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const populationBenchDims = 64

func populationBenchNode(r *rand.Rand, i int) *storage.Node {
	vec := make([]float32, populationBenchDims)
	for j := range vec {
		vec[j] = r.Float32()*2 - 1
	}
	return &storage.Node{
		ID:              storage.NodeID(fmt.Sprintf("nornic:n%d", i)),
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": fmt.Sprintf("document %d about topic%d and term%d", i, i%97, i%13)},
		ChunkEmbeddings: [][]float32{vec},
	}
}

func populationBenchEngine(b *testing.B, nodes int) *storage.MemoryEngine {
	b.Helper()
	engine := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = engine.Close() })
	r := rand.New(rand.NewSource(1))
	for i := range nodes {
		_, err := engine.CreateNode(populationBenchNode(r, i))
		require.NoError(b, err)
	}
	return engine
}

func populationBenchService(b *testing.B, engine storage.Engine, dir string) *Service {
	b.Helper()
	svc := NewService(engine, populationBenchDims, nil)
	svc.FixVectorDimensions()
	if dir != "" {
		svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
		svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
		svc.SetPersistenceEnabled(true)
	}
	return svc
}

// BenchmarkBuildIndexesFromStorage measures an explicit build that populates
// BM25 and vectors from storage, then publishes them, in memory and with
// persistence (base save and final save).
func BenchmarkBuildIndexesFromStorage(b *testing.B) {
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000000")
	b.Setenv("NORNICDB_SEARCH_BUILD_PROGRESS_LOG_SEC", "0")
	for _, persisted := range []bool{false, true} {
		b.Run(fmt.Sprintf("persisted=%t", persisted), func(b *testing.B) {
			engine := populationBenchEngine(b, 5000)
			b.ResetTimer()
			for range b.N {
				dir := ""
				if persisted {
					dir = b.TempDir()
				}
				svc := populationBenchService(b, engine, dir)
				require.NoError(b, svc.BuildIndexes(context.Background()))
				b.StopTimer()
				require.NoError(b, svc.Close())
				b.StartTimer()
			}
		})
	}
}

// BenchmarkLiveNodeWrite measures IndexNode on a ready service: an update of
// a node's text and vector, the vector write path every commit takes.
func BenchmarkLiveNodeWrite(b *testing.B) {
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000000")
	b.Setenv("NORNICDB_SEARCH_BUILD_PROGRESS_LOG_SEC", "0")
	engine := populationBenchEngine(b, 2000)
	svc := populationBenchService(b, engine, "")
	b.Cleanup(func() { _ = svc.Close() })
	require.NoError(b, svc.BuildIndexes(context.Background()))
	r := rand.New(rand.NewSource(2))
	// Two versions of 256 nodes alternate, so every write changes the node.
	updates := make([]*storage.Node, 512)
	for i := range updates {
		updates[i] = populationBenchNode(r, i%256)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := svc.IndexNode(updates[i%len(updates)]); err != nil {
			b.Fatal(err)
		}
	}
}
