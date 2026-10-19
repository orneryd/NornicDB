package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A reserved write superseded before admission changes nothing: neither the
// empty index's vector dimensions nor the node's index entries (bot thread on
// #1489: "Check reservations before mutation-admission side effects").
func TestSupersededReservedWriteHasNoAdmissionSideEffects(t *testing.T) {
	for _, edge := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "relationship"}[edge], func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = base.Close() })
			svc := NewServiceWithDimensions(storage.NewNamespacedEngine(base, "test"), 2)
			t.Cleanup(func() { _ = svc.Close() })
			require.Equal(t, 2, svc.VectorIndexDimensions())
			if edge {
				stale := svc.ReserveEdgeMutation("r")
				_ = svc.ReserveEdgeMutation("r") // newer, still pending
				require.NoError(t, svc.indexEdgeWithMutation(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"v": []float32{1, 0, 0}}}, stale))
			} else {
				stale := svc.ReserveNodeMutation("a")
				_ = svc.ReserveNodeMutation("a") // newer, still pending
				require.NoError(t, svc.IndexNodeWithMutation(&storage.Node{ID: "a", ChunkEmbeddings: [][]float32{{1, 0, 0}}}, stale))
			}
			require.Equal(t, 2, svc.VectorIndexDimensions(), "a superseded write replaced the index's dimensions")
			require.Zero(t, svc.EmbeddingCount())
		})
	}
}
