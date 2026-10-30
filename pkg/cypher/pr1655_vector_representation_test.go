package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPR1655NoopVectorKeepsTypedResultWithoutMutation(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		name := "node"
		if relationship {
			name = "relationship"
		}
		t.Run(name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			vector := []float64{0.12345678901234567, 0.9876543210987654}
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"embedding": []any{vector[0], vector[1]}}})
				require.NoError(t, err)
			}
			counting := &graphitiCopyCountingEngine{Engine: store}
			exec := NewStorageExecutor(counting)
			var entity any
			var original map[string]any
			if relationship {
				require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"embedding": []any{vector[0], vector[1]}}}))
				edge, err := store.GetEdge("r")
				require.NoError(t, err)
				entity = edge
				original = edge.Properties
			} else {
				node, err := store.GetNode("a")
				require.NoError(t, err)
				entity = node
				original = node.Properties
			}
			// Pipeline rows can carry generic lists even when the parameter is typed.
			original["embedding"] = []any{vector[0], vector[1]}
			_, err := exec.callSetVectorProperty(context.Background(), []any{entity, "embedding", vector}, relationship)
			require.NoError(t, err)
			var result any
			if relationship {
				result = entity.(*storage.Edge).Properties["embedding"]
			} else {
				result = entity.(*storage.Node).Properties["embedding"]
			}
			require.Equal(t, vector, result, "the row keeps the precise typed vector")
			require.IsType(t, []any{}, original["embedding"], "normalization must not mutate a retained observation map")
			require.Equal(t, int64(0), counting.NodeUpdateCount())
			require.Equal(t, int64(0), counting.EdgeUpdateCount())
			require.Equal(t, int64(0), counting.NodeGetCount())
			require.Equal(t, int64(0), counting.edgeGets)
		})
	}
}
