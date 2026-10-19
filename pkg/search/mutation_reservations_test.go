package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Only live overlapping work may retain predecessor links. Completed work
// cannot become live again when a later reservation is abandoned.
func TestReservationOwnerOutstandingWorkIsBounded(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			engine, svc, fault := xhighNestedService(t)
			_, err := engine.CreateNode(&storage.Node{ID: "a", ChunkEmbeddings: [][]float32{{1, 0}}})
			require.NoError(t, err)
			_, err = engine.CreateNode(&storage.Node{ID: "b"})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": []float32{1, 0}}}))
			reserve := func() RefreshRetry {
				if kind == "node" {
					return RefreshRetry{Node: svc.ReserveNodeMutation("a")}
				}
				return RefreshRetry{Edge: svc.ReserveEdgeMutation("r")}
			}
			refresh := func(work RefreshRetry) error {
				if kind == "node" {
					return svc.RefreshNodeWithMutation(work.Node)
				}
				return svc.RefreshEdgeWithMutation(work.Edge)
			}
			linkCount := func() int {
				svc.nodeMutationMu.Lock()
				defer svc.nodeMutationMu.Unlock()
				return len(svc.mutationReservations.links)
			}
			older := reserve()
			current := reserve()
			for i := 0; i < 256; i++ {
				newer := reserve()
				current.Discard()
				current = newer
				require.Equal(t, 2, linkCount(), "abandoned noncurrent work retained a link")
			}
			current.Discard()
			require.Zero(t, linkCount(), "one surviving reservation needs no predecessor state")
			require.NoError(t, refresh(older))
			if kind == "node" {
				require.Equal(t, 1, svc.EmbeddingCount())
			} else {
				require.True(t, svc.HasRelationshipVectorEntries("AFTER", "vec"))
			}
			require.Zero(t, linkCount())
			later := reserve()
			fault.nodes.Store(kind == "node")
			fault.edges.Store(kind == "relationship")
			require.NoError(t, refresh(older), "a completed generation read storage again behind a fresh reservation")
			later.Discard()
			svc.nodeMutationMu.Lock()
			nodes := len(svc.pendingNodeMutations)
			edges := len(svc.pendingEdgeMutations)
			links := len(svc.mutationReservations.links)
			svc.nodeMutationMu.Unlock()
			require.Zero(t, nodes)
			require.Zero(t, edges)
			require.Zero(t, links)
		})
	}
}

func TestReservationOwnerLegacyRetryNotification(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			engine, svc, fault := xhighNestedService(t)
			_, err := engine.CreateNode(&storage.Node{ID: "a", ChunkEmbeddings: [][]float32{{1, 0}}})
			require.NoError(t, err)
			_, err = engine.CreateNode(&storage.Node{ID: "b"})
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": []float32{1, 0}}}))
			var older RefreshRetry
			if kind == "node" {
				older = RefreshRetry{Node: svc.ReserveNodeMutation("a")}
			} else {
				older = RefreshRetry{Edge: svc.ReserveEdgeMutation("r")}
			}
			calls := 0
			svc.SetRefreshRetry(func(node storage.NodeID, edge storage.EdgeID, err error) {
				calls++
				require.ErrorIs(t, err, xhighNestedReadFailure)
				if kind == "node" {
					require.Equal(t, storage.NodeID("a"), node)
					require.Empty(t, edge)
				} else {
					require.Equal(t, storage.EdgeID("r"), edge)
					require.Empty(t, node)
				}
			})
			fault.nodes.Store(kind == "node")
			fault.edges.Store(kind == "relationship")
			if kind == "node" {
				require.ErrorIs(t, svc.RefreshNode("a"), xhighNestedReadFailure)
			} else {
				require.ErrorIs(t, svc.RefreshEdge("r"), xhighNestedReadFailure)
			}
			require.Equal(t, 1, calls)
			require.True(t, older.Current(), "notification changed the original discard contract")
			fault.nodes.Store(false)
			fault.edges.Store(false)
			if kind == "node" {
				require.NoError(t, svc.RefreshNodeWithMutation(older.Node))
				require.Equal(t, 1, svc.EmbeddingCount())
			} else {
				require.NoError(t, svc.RefreshEdgeWithMutation(older.Edge))
				require.True(t, svc.HasRelationshipVectorEntries("AFTER", "vec"))
			}
		})
	}
}
