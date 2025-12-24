package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkMatchRelationships_CountAll(b *testing.B) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Seed a moderate number of nodes/edges so the traversal path would be expensive
	// if it were to materialize paths.
	const nodes = 1000
	for i := 0; i < nodes; i++ {
		_, _ = store.CreateNode(&storage.Node{ID: storage.NodeID("n" + itoa(i))})
	}
	for i := 0; i < nodes-1; i++ {
		_ = store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID("e" + itoa(i)),
			StartNode: storage.NodeID("n" + itoa(i)),
			EndNode:   storage.NodeID("n" + itoa(i+1)),
			Type:      "KNOWS",
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) as count", nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [32]byte
	n := 0
	for i > 0 {
		buf[n] = byte('0' + i%10)
		n++
		i /= 10
	}
	if neg {
		buf[n] = '-'
		n++
	}
	// reverse
	for j := 0; j < n/2; j++ {
		buf[j], buf[n-1-j] = buf[n-1-j], buf[j]
	}
	return string(buf[:n])
}
