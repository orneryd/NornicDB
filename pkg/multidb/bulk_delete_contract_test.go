package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedBulkMissingDeleteRejectsAtomically(t *testing.T) {
	for _, entity := range []string{"node", "edge"} {
		t.Run(entity, func(t *testing.T) {
			manager, database := setupTestManager(t)
			view, err := manager.GetStorage(database)
			require.NoError(t, err)
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err = view.CreateNode(t.Context(), &storage.Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, view.CreateEdge(t.Context(), &storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "R"}))
			before, _, _ := manager.GetStorageSize(database)
			node, err := storage.GetStoredNode(t.Context(), view, "a")
			require.NoError(t, err)
			edge, err := storage.GetStoredEdge(t.Context(), view, "e")
			require.NoError(t, err)
			if entity == "node" {
				err = view.BulkDeleteNodes(t.Context(), []storage.NodeID{"a", "missing"})
			} else {
				err = view.BulkDeleteEdges(t.Context(), []storage.EdgeID{"e", "missing"})
			}
			require.ErrorIs(t, err, storage.ErrNotFound)
			afterNode, err := storage.GetStoredNode(t.Context(), view, "a")
			require.NoError(t, err)
			require.Equal(t, node, afterNode)
			afterEdge, err := storage.GetStoredEdge(t.Context(), view, "e")
			require.NoError(t, err)
			require.Equal(t, edge, afterEdge)
			nodes, err := view.NodeCount(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 2, nodes)
			edges, err := view.EdgeCount(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 1, edges)
			after, _, _ := manager.GetStorageSize(database)
			require.Equal(t, before, after)
		})
	}
}
