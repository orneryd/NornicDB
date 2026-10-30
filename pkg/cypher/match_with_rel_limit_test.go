package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchRelationshipWithLimitReturnsBoundRows(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	for _, id := range []storage.NodeID{"a", "b", "c", "d"} {
		_, err := ns.CreateNode(&storage.Node{ID: id, Labels: []string{"T"}})
		require.NoError(t, err)
	}

	for i, edge := range []*storage.Edge{
		{ID: "r1", StartNode: "a", EndNode: "b", Type: "REL", Properties: map[string]interface{}{"group_id": "old"}},
		{ID: "r2", StartNode: "b", EndNode: "c", Type: "REL", Properties: map[string]interface{}{"group_id": "old"}},
		{ID: "r3", StartNode: "c", EndNode: "d", Type: "REL", Properties: map[string]interface{}{"group_id": "old"}},
	} {
		require.NoError(t, ns.CreateEdge(edge), "edge %d", i)
	}

	res, err := exec.Execute(ctx, "MATCH ()-[r:REL]->() WHERE r.group_id='old' WITH r LIMIT 1 RETURN r", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
}
