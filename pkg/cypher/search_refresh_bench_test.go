package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func newSearchRefreshBenchExecutor(b *testing.B) *StorageExecutor {
	b.Helper()
	base := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	if _, err := store.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}}); err != nil {
		b.Fatal(err)
	}
	svc := search.NewService(store, 2, nil)
	b.Cleanup(func() { _ = svc.Close() })
	if err := svc.BuildIndexes(context.Background()); err != nil {
		b.Fatal(err)
	}
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	return exec
}

// A committed SET publishes the node to search (notifyNodeMutated).
func BenchmarkSearchRefreshCypherSet(b *testing.B) {
	exec := newSearchRefreshBenchExecutor(b)
	ctx := context.Background()
	words := []string{"alpha", "bravo"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := exec.Execute(ctx, "MATCH (n:Doc) SET n.content = $word", map[string]any{"word": words[i%2]}); err != nil {
			b.Fatal(err)
		}
	}
}

// Creating and deleting a relationship with a vector publishes it to search
// and, where deletes reach search, removes it.
func BenchmarkSearchRefreshCypherRelationshipCreateDelete(b *testing.B) {
	exec := newSearchRefreshBenchExecutor(b)
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "CREATE (:End {name: 'b'})", nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := exec.Execute(ctx, "MATCH (a:Doc), (b:End) CREATE (a)-[:CITES {vec: [1.0, 0.0]}]->(b)", nil); err != nil {
			b.Fatal(err)
		}
		if _, err := exec.Execute(ctx, "MATCH ()-[r:CITES]->() DELETE r", nil); err != nil {
			b.Fatal(err)
		}
	}
}
