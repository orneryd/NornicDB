package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type batchPreparationEngine struct {
	storage.Engine
	calls int
}

func (*batchPreparationEngine) Namespace() string { return "test" }
func (e *batchPreparationEngine) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	e.calls++
	return e.Engine.BatchGetNodes(ids)
}
func (e *batchPreparationEngine) BatchGetNodesWithContext(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	e.calls++
	if reader, ok := e.Engine.(interface {
		BatchGetNodesWithContext(context.Context, []storage.NodeID) (map[storage.NodeID]*storage.Node, error)
	}); ok {
		return reader.BatchGetNodesWithContext(ctx, ids)
	}
	return e.Engine.BatchGetNodes(ids)
}

func TestBatchPathPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"materializedIDsWithContext", "readNodeBatch.func1", "materializedKeyedWithContext", "copyNodeWithoutEmbeddingsWithContext", "native-copy"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			ns := storage.NewNamespacedEngine(b, "test")
			for i := 0; i <= 12; i++ {
				properties := map[string]interface{}{"k": int64(i)}
				if owner == "copyNodeWithoutEmbeddingsWithContext" || owner == "native-copy" {
					for p := 0; p < 20; p++ {
						properties[fmt.Sprintf("property%d", p)] = int64(p)
					}
				}
				_, err := ns.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("p%d", i)), Labels: []string{"P"}, Properties: properties})
				require.NoError(t, err)
			}
			for i := 0; i < 12; i++ {
				require.NoError(t, ns.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: storage.NodeID(fmt.Sprintf("p%d", i)), EndNode: storage.NodeID(fmt.Sprintf("p%d", i+1)), Type: "NEXT"}))
			}
			probe := &batchPreparationEngine{Engine: ns}
			e := NewStorageExecutor(probe)
			query := "MATCH (a:P {k:0}), (b:P {k:12}) WITH a,b MATCH p = ANY SHORTEST (a)(()-[:NEXT]->())+(b) RETURN p AS path"
			run := func() {
				t.Helper()
				r, err := e.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Len(t, r.Rows, 1)
				require.Positive(t, probe.calls)
			}
			run()
			e.ClearQueryCaches()
			probe.calls = 0
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			if owner == "copyNodeWithoutEmbeddingsWithContext" {
				ctx.via, ctx.after = ".toUserNodeBatch", 3
			}
			if owner == "native-copy" {
				ctx.owner, ctx.via, ctx.after = "copyNodeWithoutEmbeddingsWithContext", ".(*BadgerEngine).readNodeBatch", 3
			}
			_, err = e.Execute(ctx, query, nil)
			require.Positive(t, probe.calls, "the cancellation execution must enter the batch reader")
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			e.ClearQueryCaches()
			run()
		})
	}
}
