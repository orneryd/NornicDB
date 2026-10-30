package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkBoundAnchorMultiHopTraversal(b *testing.B) {
	base := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "bound-anchor-multihop-bench")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := context.Background()

	for index := 0; index < 100; index++ {
		repository := &storage.Node{ID: storage.NodeID(fmt.Sprintf("repo-%d", index)), Labels: []string{"Repository"}, Properties: map[string]interface{}{"id": fmt.Sprintf("repo-%d", index)}}
		file := &storage.Node{ID: storage.NodeID(fmt.Sprintf("file-%d", index)), Labels: []string{"File"}, Properties: map[string]interface{}{"relative_path": fmt.Sprintf("file-%d.go", index)}}
		function := &storage.Node{ID: storage.NodeID(fmt.Sprintf("function-%d", index)), Labels: []string{"Function"}, Properties: map[string]interface{}{"active": true}}
		for _, node := range []*storage.Node{repository, file, function} {
			if _, err := store.CreateNode(node); err != nil {
				b.Fatal(err)
			}
		}
		if err := store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("repo-file-%d", index)), Type: "REPO_CONTAINS", StartNode: repository.ID, EndNode: file.ID}); err != nil {
			b.Fatal(err)
		}
		if err := store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("file-function-%d", index)), Type: "CONTAINS", StartNode: file.ID, EndNode: function.ID}); err != nil {
			b.Fatal(err)
		}
	}

	query := `
		MATCH (e:Function)
		MATCH (e)<-[:CONTAINS]-(f:File)<-[:REPO_CONTAINS]-(r:Repository)
		RETURN f.relative_path AS path, r.id AS repository
	`
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("expected 100 rows, got %d", len(result.Rows))
		}
	}
}
