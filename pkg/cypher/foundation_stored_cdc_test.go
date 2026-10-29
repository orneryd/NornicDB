// Author: Lane #1644 (Codex). CDC endpoint identity describes stored
// entities, even when the visible query view hides those endpoints.
package cypher_test

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFoundationPublicStoredCDC(t *testing.T) {
	for _, withKeys := range []bool{false, true} {
		name := "labels"
		if withKeys {
			name = "selected-keys"
		}
		t.Run(name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			tenant := storage.NewNamespacedEngine(base, "tenant")
			ctx := context.Background()
			if withKeys {
				require.NoError(t, base.GetSchemaForNamespace("tenant").AddConstraint(storage.Constraint{Name: "doc_key", Type: storage.ConstraintNodeKey, Label: "Doc", Properties: []string{"k"}}))
			}
			base.SetChangeCapture("tenant", storage.ChangeCaptureFull)
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := tenant.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": string(id), "unselected": "retained payload"}, ChunkEmbeddings: [][]float32{{1, 2, 3, 4}}})
				require.NoError(t, err)
			}
			require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"v": int64(1)}}))
			for _, id := range []string{"tenant:a", "tenant:b"} {
				require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, id, true))
				_, err := base.GetNode(storage.NodeID(id))
				require.ErrorIs(t, err, storage.ErrNotFound)
			}
			executor := cypher.NewStorageExecutor(tenant)
			cursor, err := executor.Execute(ctx, "CALL db.cdc.current() YIELD id RETURN id", nil)
			require.NoError(t, err)
			require.Len(t, cursor.Rows, 1)
			edge, err := base.GetStoredEdge("tenant:ab")
			require.NoError(t, err)
			edge.Properties["v"] = int64(2)
			require.NoError(t, base.UpdateEdge(edge))
			result, err := executor.Execute(ctx, "CALL db.cdc.query($from) YIELD event RETURN event", map[string]any{"from": cursor.Rows[0][0]})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			event := result.Rows[0][0].(map[string]any)
			for _, side := range []string{"start", "end"} {
				endpoint := event[side].(map[string]any)
				require.Equal(t, []any{"Doc"}, endpoint["labels"], "stored endpoint labels must not disappear behind query suppression")
				if withKeys {
					want := "a"
					if side == "end" {
						want = "b"
					}
					require.Equal(t, map[string]any{"Doc": []any{map[string]any{"k": want}}}, endpoint["keys"])
				}
			}
		})
	}
}
