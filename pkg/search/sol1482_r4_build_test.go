package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSol1482R4BuildSkipsSupersededRefresh(t *testing.T) {
	engine := newSnapshotGateEngine(t)
	svc := populationOrderService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	_, err := engine.CreateNode(solR4Node("nornic:a", "snapshotword", []float32{1, 0, 0}))
	require.NoError(t, err)
	_, err = engine.CreateNode(solR4Node("nornic:b", "endword", []float32{0, 1, 0}))
	require.NoError(t, err)
	edge := &storage.Edge{ID: "nornic:r", Type: "REL", StartNode: "nornic:a", EndNode: "nornic:b", Properties: map[string]any{"v": []float32{1, 0, 0}}}
	require.NoError(t, engine.CreateEdge(edge))
	built := startGatedBuild(t, engine, svc)
	require.NoError(t, engine.UpdateNode(solR4Node("nornic:a", "staleword", []float32{0, 0, 1})))
	requireReturnsWhileBlocked(t, "node refresh", func() error { return svc.RefreshNode("nornic:a") })
	require.NoError(t, engine.DeleteEdge("nornic:r"))
	requireReturnsWhileBlocked(t, "edge removal", func() error { return svc.RefreshEdge("nornic:r") })
	newerNode := svc.ReserveNodeMutation("nornic:a")
	newerEdge := svc.ReserveEdgeMutation("nornic:r")
	// The newer refresh has reserved but has not read yet. A skipped queued
	// refresh must not mark its ID written and remove the build's own copy.
	require.NoError(t, engine.CreateEdge(edge))
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:a"}, searchIDs(t, svc, "snapshotword"))
	require.Empty(t, searchIDs(t, svc, "staleword"))
	require.True(t, svc.HasRelationshipVectorEntries("REL", "v"), "a skipped edge removal was marked written")
	require.NoError(t, engine.UpdateNode(solR4Node("nornic:a", "finalword", []float32{0, 1, 0})))
	require.NoError(t, svc.RefreshNodeWithMutation(newerNode))
	require.NoError(t, engine.DeleteEdge("nornic:r"))
	require.NoError(t, svc.RefreshEdgeWithMutation(newerEdge))
	require.Equal(t, []string{"nornic:a"}, searchIDs(t, svc, "finalword"))
	require.False(t, svc.HasRelationshipVectorEntries("REL", "v"))
	_, err = svc.Search(context.Background(), "finalword", nil, nil)
	require.NoError(t, err)
}

func solR4Node(id, text string, v []float32) *storage.Node {
	return admissionNode(id, text, v, "named")
}
