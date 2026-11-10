package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkLabelScanShapes runs the scans a labelled MATCH without an index
// asks the store for, with the result cache off, over a label of 1,000 nodes
// among 20,000: a property in braces or in WHERE, returning a property, the
// node or a count, and the label alone. The other nodes carry the same
// property values, so a scan that reads every node pays for them.
func BenchmarkLabelScanShapes(b *testing.B) {
	const total, labelled = 20000, 1000
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
	ctx := context.Background()
	for i := 0; i < total-labelled; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("o%d", i)), Labels: []string{"Other"}, Properties: map[string]interface{}{"id": fmt.Sprintf("o%d", i), "survivor": fmt.Sprintf("s%d", i%10), "body": "payload stored on an unrelated node"}}
		if _, err := store.CreateNode(ctx, node); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < labelled; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("p%d", i)), Labels: []string{"Identity"}, Properties: map[string]interface{}{"id": fmt.Sprintf("p%d", i), "survivor": fmt.Sprintf("s%d", i%10), "name": "identity"}}
		if _, err := store.CreateNode(ctx, node); err != nil {
			b.Fatal(err)
		}
	}
	executor := NewStorageExecutor(store)
	executor.cache = nil
	for _, shape := range []struct {
		name, query string
		rows        int
	}{
		{"braces_property", "MATCH (n:Identity {survivor: $v}) RETURN n.id", labelled / 10},
		{"braces_node", "MATCH (n:Identity {survivor: $v}) RETURN n", labelled / 10},
		{"braces_count", "MATCH (n:Identity {survivor: $v}) RETURN count(n)", 1},
		{"where_property", "MATCH (n:Identity) WHERE n.survivor = $v RETURN n.id", labelled / 10},
		{"label_property", "MATCH (n:Identity) RETURN n.id", labelled},
	} {
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := executor.Execute(ctx, shape.query, map[string]interface{}{"v": fmt.Sprintf("s%d", i%10)})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != shape.rows {
					b.Fatalf("%d rows, want %d", len(result.Rows), shape.rows)
				}
			}
		})
	}
}
