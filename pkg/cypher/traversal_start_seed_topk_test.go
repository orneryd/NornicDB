package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSelectTopKNodesByOrder_MultiKey(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	nodes := []*storage.Node{
		{ID: "n1", Properties: map[string]interface{}{"a": int64(3), "b": "a", "c": int64(1)}},
		{ID: "n2", Properties: map[string]interface{}{"a": int64(3), "b": "a", "c": int64(2)}},
		{ID: "n3", Properties: map[string]interface{}{"a": int64(3), "b": "b", "c": int64(1)}},
		{ID: "n4", Properties: map[string]interface{}{"a": int64(2), "b": "a", "c": int64(5)}},
		{ID: "n5", Properties: map[string]interface{}{"a": int64(1), "b": "a", "c": int64(9)}},
	}

	topK, ok, err := exec.selectTopKNodesByOrder(context.Background(), nodes, "n", "n.a DESC, n.b ASC, n.c DESC", 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, topK, 3)
	require.Equal(t, "n2", string(topK[0].ID))
	require.Equal(t, "n1", string(topK[1].ID))
	require.Equal(t, "n3", string(topK[2].ID))
}

func TestDrawerFeedTraversal_UsesStartSeedTopK(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := &allNodesForbiddenEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{ID: "test:wing-1", Labels: []string{"Wing"}, Properties: map[string]interface{}{"name": "Main Wing"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&storage.Node{ID: "test:room-1", Labels: []string{"Room"}, Properties: map[string]interface{}{"name": "Root Room"}})
	require.NoError(t, err)
	require.NoError(t, eng.CreateEdge(&storage.Edge{ID: "test:edge-room-wing", Type: "IN_WING", StartNode: "test:room-1", EndNode: "test:wing-1"}))

	drawers := []struct {
		id       string
		filedAt  int64
		content  string
		metadata string
	}{
		{id: "drawer-99", filedAt: 400, content: "content-99", metadata: "meta-99"},
		{id: "drawer-01", filedAt: 300, content: "content-01", metadata: "meta-01"},
		{id: "drawer-03", filedAt: 300, content: "content-03", metadata: "meta-03"},
		{id: "drawer-05", filedAt: 300, content: "content-05", metadata: "meta-05"},
		{id: "drawer-02", filedAt: 200, content: "content-02", metadata: "meta-02"},
	}

	for _, drawer := range drawers {
		drawerNodeID := storage.NodeID("test:" + drawer.id)
		_, err := eng.CreateNode(&storage.Node{
			ID:     drawerNodeID,
			Labels: []string{"Drawer"},
			Properties: map[string]interface{}{
				"drawer_id":     drawer.id,
				"content":       drawer.content,
				"metadata_json": drawer.metadata,
				"filed_at":      drawer.filedAt,
			},
		})
		require.NoError(t, err)
		require.NoError(t, eng.CreateEdge(&storage.Edge{ID: storage.EdgeID("test:" + drawer.id + "-room"), Type: "IN_ROOM", StartNode: drawerNodeID, EndNode: "test:room-1"}))
	}

	_, err = exec.Execute(ctx, "CREATE INDEX idx_drawer_filed_at FOR (d:Drawer) ON (d.filed_at)", nil)
	require.NoError(t, err)
	eng.forbidScan = true

	result, err := exec.Execute(ctx, `
MATCH (d:Drawer)-[:IN_ROOM]->(r:Room)-[:IN_WING]->(w:Wing)
WHERE d.filed_at IS NOT NULL
RETURN d.drawer_id, d.content, d.metadata_json, w.name, r.name
ORDER BY d.filed_at DESC, d.drawer_id ASC
LIMIT 3
`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"d.drawer_id", "d.content", "d.metadata_json", "w.name", "r.name"}, result.Columns)
	require.Len(t, result.Rows, 3)
	require.Equal(t, "drawer-99", result.Rows[0][0])
	require.Equal(t, "drawer-01", result.Rows[1][0])
	require.Equal(t, "drawer-03", result.Rows[2][0])
	require.True(t, exec.LastHotPathTrace().TraversalStartSeedTopK)
}
