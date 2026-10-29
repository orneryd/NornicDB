package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestConstraintFactsReachContinuationProjection(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer base.Close()
	engine := storage.NewNamespacedEngine(base, "test")
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err = engine.CreateNode(&storage.Node{ID: "d", Labels: []string{"D"}, Properties: map[string]any{"key": "k", "from": start, "to": start.Add(time.Hour), "extra": "full"}})
	require.NoError(t, err)
	schema := engine.GetSchema()
	require.NoError(t, schema.AddConstraint(storage.Constraint{Name: "window", Type: storage.ConstraintTemporal, Label: "D", Properties: []string{"key", "from", "to"}}))
	projected := &projectedContinuationEngine{Engine: engine}
	service := &Service{engine: projected}
	count := 0
	err = service.streamCompleteContinuationNodes(context.Background(), &SearchOptions{}, SearchContinuationRequest{}, func(node *storage.Node) error {
		count++
		require.Contains(t, node.Properties, "key")
		require.Contains(t, node.Properties, "from")
		require.Contains(t, node.Properties, "to")
		require.NotContains(t, node.Properties, "extra")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, 1, projected.calls)
	require.Equal(t, []string{"from", "key", "to"}, projected.properties)
	projected.properties[0] = "changed"
	require.Equal(t, []string{"key", "from", "to"}, schema.GetAllConstraints()[0].Properties)
	// A row-dependent Where must keep the full-node fallback. The projection
	// reader must not be selected simply because a schema view is available.
	count = 0
	err = service.streamCompleteContinuationNodes(context.Background(), &SearchOptions{Where: func(*storage.Node) bool { return true }}, SearchContinuationRequest{}, func(node *storage.Node) error {
		count++
		require.Equal(t, "full", node.Properties["extra"])
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, 1, projected.calls)
}
