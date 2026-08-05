package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkRelationshipMergePropertyIdentityExisting(b *testing.B) {
	for _, fanout := range []int{2, 32, 256} {
		b.Run(fmt.Sprintf("fanout=%d", fanout), func(b *testing.B) {
			exec, ctx := newRelationshipMergeIdentityBenchmark(b, fanout)
			params := map[string]interface{}{
				"scope_id":        "scope-001",
				"evidence_source": "source-001",
			}
			query := `
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
MERGE (source)-[rel:ASSERTS {
  scope_id: $scope_id,
  evidence_source: $evidence_source
}]->(target)
SET rel.last_seen = 'benchmark'`

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, query, params); err != nil {
					b.Fatalf("execute relationship MERGE: %v", err)
				}
			}
		})
	}
}

func BenchmarkUnwindRelationshipMergePropertyIdentityExisting(b *testing.B) {
	for _, fanout := range []int{2, 32, 256} {
		b.Run(fmt.Sprintf("fanout=%d", fanout), func(b *testing.B) {
			exec, ctx := newRelationshipMergeIdentityBenchmark(b, fanout)
			rows := make([]map[string]interface{}, 0, fanout)
			for i := 0; i < fanout; i++ {
				rows = append(rows, map[string]interface{}{
					"scope_id":        fmt.Sprintf("scope-%03d", i),
					"evidence_source": fmt.Sprintf("source-%03d", i),
				})
			}
			query := `UNWIND $rows AS row
MATCH (source:Source {id: 'source'})
MATCH (target:Target {id: 'target'})
MERGE (source)-[rel:ASSERTS {
  scope_id: row.scope_id,
  evidence_source: row.evidence_source
}]->(target)
SET rel.last_seen = 'benchmark'`
			params := map[string]interface{}{"rows": rows}
			if _, err := exec.Execute(ctx, query, params); err != nil {
				b.Fatalf("warm relationship MERGE batch: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, query, params); err != nil {
					b.Fatalf("execute relationship MERGE batch: %v", err)
				}
			}
		})
	}
}

func newRelationshipMergeIdentityBenchmark(b *testing.B, fanout int) (*StorageExecutor, context.Context) {
	b.Helper()
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "bench")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	for _, node := range []*storage.Node{
		{ID: "source", Labels: []string{"Source"}, Properties: map[string]interface{}{"id": "source"}},
		{ID: "target", Labels: []string{"Target"}, Properties: map[string]interface{}{"id": "target"}},
	} {
		if _, err := store.CreateNode(node); err != nil {
			b.Fatalf("seed relationship endpoint: %v", err)
		}
	}
	for i := 0; i < fanout; i++ {
		props := map[string]interface{}{
			"scope_id":        fmt.Sprintf("scope-%03d", i),
			"evidence_source": fmt.Sprintf("source-%03d", i),
		}
		edge := &storage.Edge{
			ID:         deterministicRelationshipMergeEdgeID("source", "target", "ASSERTS", props, 0),
			Type:       "ASSERTS",
			StartNode:  "source",
			EndNode:    "target",
			Properties: props,
		}
		if err := store.CreateEdge(edge); err != nil {
			b.Fatalf("seed relationship %d: %v", i, err)
		}
	}
	return exec, ctx
}
