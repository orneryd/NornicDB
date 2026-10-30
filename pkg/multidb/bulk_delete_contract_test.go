package multidb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestManagedBulkMissingDeleteRejectsAtomically(t *testing.T) {
	for _, entity := range []string{"node", "edge"} {
		t.Run(entity, func(t *testing.T) {
			manager, database := setupTestManager(t)
			view, err := manager.GetStorage(database)
			require.NoError(t, err)
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err = view.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, view.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "R"}))
			before, _, _ := manager.GetStorageSize(context.Background(), database)
			node, err := storage.GetStoredNode(view, "a")
			require.NoError(t, err)
			edge, err := storage.GetStoredEdge(view, "e")
			require.NoError(t, err)
			if entity == "node" {
				err = view.BulkDeleteNodes([]storage.NodeID{"a", "missing"})
			} else {
				err = view.BulkDeleteEdges([]storage.EdgeID{"e", "missing"})
			}
			require.ErrorIs(t, err, storage.ErrNotFound)
			afterNode, err := storage.GetStoredNode(view, "a")
			require.NoError(t, err)
			require.Equal(t, node, afterNode)
			afterEdge, err := storage.GetStoredEdge(view, "e")
			require.NoError(t, err)
			require.Equal(t, edge, afterEdge)
			nodes, err := view.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 2, nodes)
			edges, err := view.EdgeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, edges)
			after, _, _ := manager.GetStorageSize(context.Background(), database)
			require.Equal(t, before, after)
		})
	}
}
