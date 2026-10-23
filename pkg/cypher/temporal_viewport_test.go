package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type temporalViewportProbeEngine struct {
	storage.Engine
	visibleIDs map[string]bool
	calls      int
}

func (e *temporalViewportProbeEngine) IsCurrentTemporalNode(node *storage.Node, asOf time.Time) (bool, error) {
	e.calls++
	return e.visibleIDs[string(node.ID)], nil
}

func TestCollectNodesWithStreaming_RespectsTemporalViewport(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	_, err := base.CreateNode(&storage.Node{ID: "nornic:visible", Labels: []string{"Doc"}, Properties: map[string]any{"idx": 1}})
	require.NoError(t, err)
	_, err = base.CreateNode(&storage.Node{ID: "nornic:hidden", Labels: []string{"Doc"}, Properties: map[string]any{"idx": 2}})
	require.NoError(t, err)

	probe := &temporalViewportProbeEngine{
		Engine:     base,
		visibleIDs: map[string]bool{"nornic:visible": true, "nornic:hidden": false},
	}
	exec := NewStorageExecutor(probe)
	ctx := WithTemporalViewport(context.Background(), AsOfTemporalViewport(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))

	nodes, err := exec.collectNodesWithStreaming(ctx, []string{"Doc"}, nil, "n", "", -1)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, "nornic:visible", string(nodes[0].ID))
	assert.GreaterOrEqual(t, probe.calls, 1)
}

func TestTemporalViewportDisabledKeepsNodes(t *testing.T) {
	viewport := CurrentTemporalViewport()
	checker := &temporalViewportProbeEngine{visibleIDs: map[string]bool{"nornic:visible": true}}
	nodes := []*storage.Node{{ID: "nornic:visible"}, {ID: "nornic:hidden"}}

	filtered, err := filterNodesByTemporalViewport(nodes, viewport, checker)
	require.NoError(t, err)
	assert.Len(t, filtered, 2)
	assert.Equal(t, 0, checker.calls)
	assert.False(t, viewport.Enabled())
}
