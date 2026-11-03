package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// labelIDProbe is the label-id lookup both collect probes expose: it records
// the lookup and visits the probe's ids in order, stopping when the collector
// declines another id. Embedding it leaves each probe's optional projected
// method set its own.
type labelIDProbe struct {
	storage.Engine
	labelLookupCalls int
	labelIDs         []storage.NodeID
}

func (p *labelIDProbe) ForEachNodeIDByLabel(_ context.Context, _ string, visit func(storage.NodeID) bool) error {
	p.labelLookupCalls++
	for _, id := range p.labelIDs {
		if !visit(id) {
			return nil
		}
	}
	return nil
}

type collectNodesLabelProbeEngine struct {
	labelIDProbe
	streamCalls         int
	labelCalls          int
	projectedLabelCalls int
	projectedVisits     int
	projectedNodes      []*storage.Node
	projectedProperties []string
}

type collectNodesIDProbeEngine struct {
	labelIDProbe
	getNodeErr error
}

func (e *collectNodesIDProbeEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if e.getNodeErr != nil {
		return nil, e.getNodeErr
	}
	return e.Engine.GetNode(ctx, id)
}

func (e *collectNodesLabelProbeEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
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

func (e *collectNodesLabelProbeEngine) StreamNodesByLabelProjected(ctx context.Context, _ string, properties []string, visit func(*storage.Node) error) error {
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
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{
		labelIDProbe: labelIDProbe{Engine: base},
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
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{labelIDProbe: labelIDProbe{Engine: base}}
	result, err := NewStorageExecutor(probe).Execute(context.Background(),
		"MATCH (person:Person) WHERE person.age > 30 RETURN count(person) AS count", nil)
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	require.Nil(t, result)
}

func TestCollectNodesWithStreaming_LabelLimitPrefersLabelLookup(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })

	for i := 0; i < 100; i++ {
		label := "Other"
		if i%20 == 0 {
			label = "SystemPrompt"
		}
		id := storage.NodeID(fmt.Sprintf("nornic:n-%d", i))
		_, err := base.CreateNode(t.Context(), &storage.Node{
			ID:     id,
			Labels: []string{label},
			Properties: map[string]any{
				"idx": i,
			},
		})
		require.NoError(t, err)
	}

	probe := &collectNodesIDProbeEngine{labelIDProbe: labelIDProbe{
		Engine: base,
		labelIDs: []storage.NodeID{
			"nornic:n-0", "nornic:n-20", "nornic:n-40", "nornic:n-60", "nornic:n-80",
		},
	}}
	exec := NewStorageExecutor(probe)

	nodes, err := exec.collectNodesWithStreamingProjection(
		context.Background(), []string{"SystemPrompt"}, nil, "n", "", 3, nil,
	)
	require.NoError(t, err)
	require.Len(t, nodes, 3)
	assert.GreaterOrEqual(t, probe.labelLookupCalls, 1, "label-id lookup path must be used")
}

func TestCollectNodesWithStreaming_UsesLabelIndexedStreamForResidualFilters(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{
		labelIDProbe: labelIDProbe{Engine: base},
		projectedNodes: []*storage.Node{
			{ID: "person-1", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(20)}},
			{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]any{"age": int64(40)}},
		},
	}
	exec := NewStorageExecutor(probe)
	probe.labelCalls = 0

	nodes, err := exec.collectNodesWithStreamingProjection(
		context.Background(), []string{"Person"}, nil, "n", "n.age > 30", -1, nil,
	)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, storage.NodeID("person-2"), nodes[0].ID)
	assert.Equal(t, 1, probe.projectedLabelCalls, "label-indexed streaming must supply the scan")
	assert.Equal(t, 0, probe.streamCalls, "the converged collector must not scan unrelated labels")
	assert.Equal(t, 0, probe.labelCalls, "the collector must not materialize the complete label population")
}

func TestMatchLimitStopsProjectedLabelStreamAfterRequestedRows(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	projectedNodes := make([]*storage.Node, 40)
	for i := range projectedNodes {
		projectedNodes[i] = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("person-%d", i)),
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": fmt.Sprintf("person-%d", i)},
		}
	}
	probe := &collectNodesLabelProbeEngine{labelIDProbe: labelIDProbe{Engine: base}, projectedNodes: projectedNodes}
	result, err := NewStorageExecutor(probe).Execute(context.Background(),
		"MATCH (person:Person) RETURN person.name LIMIT 10 /* cache_bust */", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 10)
	require.Equal(t, 1, probe.projectedLabelCalls)
	require.Equal(t, 10, probe.projectedVisits, "LIMIT must stop the storage iterator, not just truncate the result")
}

// assertCollectNodesProjectionError drives one label-projected collect through
// the streaming owner and asserts the shared read-error contract: the reader
// error is reported as-is and no partial population is returned. The two
// failing readers keep their own capability method sets.
func assertCollectNodesProjectionError(t *testing.T, engine storage.Engine, wantErr error) {
	t.Helper()
	nodes, err := NewStorageExecutor(engine).collectNodesWithStreamingProjection(
		context.Background(), []string{"Person"}, nil, "n", "", 1, nil,
	)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, nodes)
}

func TestCollectNodesWithStreaming_PropagatesProjectedReaderError(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesLabelProbeEngine{labelIDProbe: labelIDProbe{Engine: base, labelIDs: []storage.NodeID{"nornic:one"}}}
	assertCollectNodesProjectionError(t, probe, storage.ErrNotImplemented)
	require.Equal(t, 0, probe.labelLookupCalls)
}

func TestCollectNodesWithStreaming_PropagatesLabelLookupReadError(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	probe := &collectNodesIDProbeEngine{labelIDProbe: labelIDProbe{
		Engine: base, labelIDs: []storage.NodeID{"nornic:missing"},
	}, getNodeErr: assert.AnError}
	assertCollectNodesProjectionError(t, probe, assert.AnError)
}

// cancelDuringProjectedScanEngine is a store whose label-projected stream
// cancels the statement's context while the scan is running, as a client
// disconnect or an elapsed deadline does.
type cancelDuringProjectedScanEngine struct {
	storage.Engine
	nodes  []*storage.Node
	cancel func()
	at     int
	visits int
}

// Pin the optional capability: a signature drift must fail compilation.
var _ storage.ProjectedLabelNodeReader = (*cancelDuringProjectedScanEngine)(nil)

func (e *cancelDuringProjectedScanEngine) StreamNodesByLabelProjected(ctx context.Context, _ string, _ []string, visit func(*storage.Node) error) error {
	for _, node := range e.nodes {
		e.visits++
		if e.visits == e.at {
			e.cancel()
		}
		if err := visit(node); err != nil {
			return err
		}
	}
	return nil
}

// TestCollectNodesWithStreamingProjectionOwnerContract keeps the one collect
// owner's contract after the forwarding tier was retired: a bounded collect
// preallocates its result and stops the scan at the limit-th match, and a
// caller cancellation raised during the scan ends it with that error and no
// partial result rather than reading the rest of the population.
func TestCollectNodesWithStreamingProjectionOwnerContract(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	projectedNodes := make([]*storage.Node, 20)
	for index := range projectedNodes {
		projectedNodes[index] = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("item-%02d", index)),
			Labels:     []string{"Item"},
			Properties: map[string]any{"rank": int64(index)},
		}
	}

	bounded := &collectNodesLabelProbeEngine{labelIDProbe: labelIDProbe{Engine: base}, projectedNodes: projectedNodes}
	nodes, err := NewStorageExecutor(bounded).collectNodesWithStreamingProjection(
		context.Background(), []string{"Item"}, nil, "n", "", 5, nil,
	)
	require.NoError(t, err)
	require.Len(t, nodes, 5)
	require.Equal(t, 5, cap(nodes), "a bounded collect preallocates its result")
	require.Equal(t, 5, bounded.projectedVisits, "LIMIT stops the scan, not just the result")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := &cancelDuringProjectedScanEngine{Engine: base, nodes: projectedNodes, cancel: cancel, at: 3}
	nodes, err = NewStorageExecutor(cancelled).collectNodesWithStreamingProjection(
		ctx, []string{"Item"}, nil, "n", "", -1, nil,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, nodes, "a canceled scan returns no partial population")
	require.Equal(t, 3, cancelled.visits, "the owner stops reading as soon as the caller cancels")
}
