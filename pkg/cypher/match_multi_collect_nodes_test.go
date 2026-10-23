package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type collectNodesLabelProbeEngine struct {
	storage.Engine
	streamCalls         int
	labelCalls          int
	labelLookupCalls    int
	projectedLabelCalls int
	projectedVisits     int
	labelIDs            []storage.NodeID
	projectedNodes      []*storage.Node
	projectedProperties []string
}

type collectNodesIDProbeEngine struct {
	storage.Engine
	labelLookupCalls int
	labelIDs         []storage.NodeID
	getNodeErr       error
}

func (e *collectNodesIDProbeEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.getNodeErr != nil {
		return nil, e.getNodeErr
	}
	return e.Engine.GetNode(id)
}

func (e *collectNodesIDProbeEngine) ForEachNodeIDByLabel(_ string, visit func(storage.NodeID) bool) error {
	e.labelLookupCalls++
	for _, id := range e.labelIDs {
		if !visit(id) {
			return nil
		}
	}
	return nil
}

func (e *collectNodesLabelProbeEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.labelCalls++
	return nil, assert.AnError
}

func (e *collectNodesLabelProbeEngine) StreamNodes(_ context.Context, _ func(node *storage.Node) error) error {
	e.streamCalls++
	return assert.AnError
}

func (e *collectNodesLabelProbeEngine) StreamEdges(_ context.Context, _ func(edge *storage.Edge) error) error {
	return nil
}

func (e *collectNodesLabelProbeEngine) StreamNodeChunks(_ context.Context, _ int, _ func(nodes []*storage.Node) error) error {
	return nil
}

func (e *collectNodesLabelProbeEngine) ForEachNodeIDByLabel(label string, visit func(storage.NodeID) bool) error {
	e.labelLookupCalls++
	for _, id := range e.labelIDs {
		if !visit(id) {
			return nil
		}
	}
	return nil
}

func (e *collectNodesLabelProbeEngine) StreamNodesByLabelProjected(_ string, properties []string, visit func(*storage.Node) error) error {
	if e.projectedNodes == nil {
		return storage.ErrNotImplemented
	}
	e.projectedLabelCalls++
	e.projectedProperties = append([]string(nil), properties...)
	for _, node := range e.projectedNodes {
		e.projectedVisits++
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

func TestPipelineFilteredCountReducesProjectedLabelStream(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{
		Engine: base,
		projectedNodes: []*storage.Node{
			{ID: "person-1", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(20)}},
			{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(40)}},
			{ID: "person-3", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(50)}},
		},
	}
	exec := NewStorageExecutor(probe)
	probe.labelCalls = 0

	result, err := exec.Execute(context.Background(),
		"MATCH (person:Person) WHERE person.age > 30 RETURN count(person) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	require.Equal(t, 1, probe.projectedLabelCalls)
	require.Equal(t, []string{"age"}, probe.projectedProperties)
	require.Equal(t, 0, probe.streamCalls)
	require.Equal(t, 0, probe.labelCalls)
}

func TestPipelineFilteredCount_PropagatesProjectedReaderError(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{Engine: base}
	result, err := NewStorageExecutor(probe).Execute(context.Background(),
		"MATCH (person:Person) WHERE person.age > 30 RETURN count(person) AS count", nil)
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	require.Nil(t, result)
}

func TestCollectNodesWithStreaming_LabelLimitPrefersLabelLookup(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	for i := 0; i < 100; i++ {
		label := "Other"
		if i%20 == 0 {
			label = "SystemPrompt"
		}
		id := storage.NodeID(fmt.Sprintf("nornic:n-%d", i))
		_, err := base.CreateNode(&storage.Node{
			ID:     id,
			Labels: []string{label},
			Properties: map[string]any{
				"idx": i,
			},
		})
		require.NoError(t, err)
	}

	probe := &collectNodesIDProbeEngine{
		Engine: base,
		labelIDs: []storage.NodeID{
			"nornic:n-0", "nornic:n-20", "nornic:n-40", "nornic:n-60", "nornic:n-80",
		},
	}
	exec := NewStorageExecutor(probe)

	nodes, err := exec.collectNodesWithStreaming(context.Background(), []string{"SystemPrompt"}, nil, "n", "", 3)
	require.NoError(t, err)
	require.Len(t, nodes, 3)
	assert.GreaterOrEqual(t, probe.labelLookupCalls, 1, "label-id lookup path must be used")
}

func TestCollectNodesWithStreaming_UsesLabelIndexedStreamForResidualFilters(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{
		Engine: base,
		projectedNodes: []*storage.Node{
			{ID: "person-1", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(20)}},
			{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(40)}},
		},
	}
	exec := NewStorageExecutor(probe)
	probe.labelCalls = 0

	nodes, err := exec.collectNodesWithStreaming(
		context.Background(), []string{"Person"}, nil, "n", "n.age > 30", -1,
	)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, storage.NodeID("person-2"), nodes[0].ID)
	assert.Equal(t, 1, probe.projectedLabelCalls, "label-indexed streaming must supply the scan")
	assert.Equal(t, 0, probe.streamCalls, "the converged collector must not scan unrelated labels")
	assert.Equal(t, 0, probe.labelCalls, "the collector must not materialize the complete label population")
}

func TestMatchLimitStopsProjectedLabelStreamAfterRequestedRows(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	projectedNodes := make([]*storage.Node, 40)
	for i := range projectedNodes {
		projectedNodes[i] = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("person-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": fmt.Sprintf("person-%d", i)},
		}
	}
	probe := &collectNodesLabelProbeEngine{Engine: base, projectedNodes: projectedNodes}
	result, err := NewStorageExecutor(probe).Execute(context.Background(),
		"MATCH (person:Person) RETURN person.name LIMIT 10 /* cache_bust */", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 10)
	require.Equal(t, 1, probe.projectedLabelCalls)
	require.Equal(t, 10, probe.projectedVisits, "LIMIT must stop the storage iterator, not just truncate the result")
}

func TestCollectNodesWithStreaming_PropagatesProjectedReaderError(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{Engine: base, labelIDs: []storage.NodeID{"nornic:one"}}
	exec := NewStorageExecutor(probe)
	nodes, err := exec.collectNodesWithStreaming(context.Background(), []string{"Person"}, nil, "n", "", 1)
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	require.Nil(t, nodes)
	require.Equal(t, 0, probe.labelLookupCalls)
}

func TestCollectNodesWithStreaming_PropagatesLabelLookupReadError(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesIDProbeEngine{
		Engine: base, labelIDs: []storage.NodeID{"nornic:missing"}, getNodeErr: assert.AnError,
	}
	nodes, err := NewStorageExecutor(probe).collectNodesWithStreaming(
		context.Background(), []string{"Person"}, nil, "n", "", 1,
	)
	require.ErrorIs(t, err, assert.AnError)
	require.Nil(t, nodes)
}
