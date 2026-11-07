package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func TestRebaseNOR56IssuedTransactionBatch(t *testing.T) {
	for _, traced := range []bool{false, true} {
		name := "plain"
		if traced {
			name = "traced"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			tenant := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor56")
			_, err := tenant.CreateNode(ctx, &storage.Node{ID: "existing", Labels: []string{"Fixture"}, Properties: map[string]interface{}{"value": "before"}})
			if err != nil {
				t.Fatal(err)
			}
			var view storage.Engine = tenant
			if traced {
				view = storage.NewTracedEngine(view)
			}
			executor := NewStorageExecutor(view)
			if _, err = executor.Execute(ctx, "BEGIN", nil); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			if _, err = executor.Execute(ctx, "MATCH (n:Fixture) SET n.value='pending'", nil); err != nil {
				t.Fatal(err)
			}
			issued, err := executor.FunctionStorage("")
			if err != nil {
				t.Fatal(err)
			}
			point, err := issued.GetNode(ctx, "existing")
			if err != nil {
				t.Fatal(err)
			}
			if point.Properties["value"] != "pending" {
				t.Fatalf("point did not observe pending value: %v", point.Properties)
			}
			batch := executor.BatchGetNodes(ctx, []storage.NodeID{"existing"})
			if batch["existing"] == nil || batch["existing"].Properties["value"] != "pending" {
				t.Fatalf("batch did not preserve transaction view: %v", batch)
			}
		})
	}
}
