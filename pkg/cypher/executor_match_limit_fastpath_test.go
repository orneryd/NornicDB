package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type countingStreamingEngine struct {
	storage.Engine
	streamNodesCalls int
	allNodesCalls    int
	allEdgesCalls    int
	labelCalls       int
}

func (c *countingStreamingEngine) StreamNodes(ctx context.Context, fn func(node *storage.Node) error) error {
	c.streamNodesCalls++
	if streamer, ok := c.Engine.(storage.StreamingEngine); ok {
		return streamer.StreamNodes(ctx, fn)
	}
	return fmt.Errorf("inner engine does not implement StreamingEngine")
}

func (c *countingStreamingEngine) StreamEdges(ctx context.Context, fn func(edge *storage.Edge) error) error {
	if streamer, ok := c.Engine.(storage.StreamingEngine); ok {
		return streamer.StreamEdges(ctx, fn)
	}
	return fmt.Errorf("inner engine does not implement StreamingEngine")
}

func (c *countingStreamingEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*storage.Node) error) error {
	if streamer, ok := c.Engine.(storage.StreamingEngine); ok {
		return streamer.StreamNodeChunks(ctx, chunkSize, fn)
	}
	return fmt.Errorf("inner engine does not implement StreamingEngine")
}

func (c *countingStreamingEngine) AllNodes() ([]*storage.Node, error) {
	c.allNodesCalls++
	return c.Engine.AllNodes()
}

func (c *countingStreamingEngine) AllEdges() ([]*storage.Edge, error) {
	c.allEdgesCalls++
	return c.Engine.AllEdges()
}

func (c *countingStreamingEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	c.labelCalls++
	return c.Engine.GetNodesByLabel(label)
}

func TestSimpleMatchLimitFastPath_UsesStreamingOnly(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:Thing {id:%d})", i), nil)
		require.NoError(t, err)
	}

	fastResult, handled := exec.tryFastPathSimpleMatchReturnLimit(ctx, "MATCH (n) RETURN n LIMIT 25 /* cache_bust_a */", "MATCH (N) RETURN N LIMIT 25 /* CACHE_BUST_A */")
	require.True(t, handled, "fast-path parser should handle simple shape")
	require.NotNil(t, fastResult)
	require.Equal(t, 25, len(fastResult.Rows))

	result, err := exec.Execute(ctx, "MATCH (n) RETURN n LIMIT 25 /* cache_bust_a */", nil)
	require.NoError(t, err)
	require.Equal(t, 25, len(result.Rows))
	require.Equal(t, []string{"n"}, result.Columns)
	require.Greater(t, counting.streamNodesCalls, 0, "fast path must stream nodes")
	require.Equal(t, 0, counting.allNodesCalls, "fast path must not load all nodes")

	trace := exec.LastHotPathTrace()
	require.True(t, trace.SimpleMatchLimitFastPath, "simple match-limit fast path trace must be set")
}

func TestSimpleMatchLimitFastPath_LabelAndAlias(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	for i := 0; i < 30; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:Thing {id:%d})", i), nil)
		require.NoError(t, err)
	}

	fastResult, handled := exec.tryFastPathSimpleMatchReturnLimit(ctx, "MATCH (n:Thing) RETURN n AS node LIMIT 10 /* cache_bust_b */", "MATCH (N:THING) RETURN N AS NODE LIMIT 10 /* CACHE_BUST_B */")
	require.True(t, handled)
	require.NotNil(t, fastResult)
	require.Equal(t, 10, len(fastResult.Rows))

	result, err := exec.Execute(ctx, "MATCH (n:Thing) RETURN n AS node LIMIT 10 /* cache_bust_b */", nil)
	require.NoError(t, err)
	require.Equal(t, 10, len(result.Rows))
	require.Equal(t, []string{"node"}, result.Columns)
	require.Greater(t, counting.labelCalls, 0)
	require.Equal(t, 0, counting.allNodesCalls)
}

func TestSimpleMatchLimitFastPath_DoesNotCaptureWhereShape(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	for i := 0; i < 40; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:Thing {id:%d})", i), nil)
		require.NoError(t, err)
	}

	result, err := exec.Execute(ctx, "MATCH (n) WHERE n.id >= 0 RETURN n LIMIT 5", nil)
	require.NoError(t, err)
	require.Equal(t, 5, len(result.Rows))
	// Generic WHERE path should not set simple match-limit trace.
	trace := exec.LastHotPathTrace()
	require.False(t, trace.SimpleMatchLimitFastPath)
}

func TestConvergedMatchPipelineStreamsUnboundedCandidates(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (n:Thing {id:%d})", i), nil)
		require.NoError(t, err)
	}
	counting.streamNodesCalls = 0
	counting.allNodesCalls = 0
	counting.labelCalls = 0

	result, err := exec.Execute(ctx, "MATCH (n:Thing) WHERE n.id >= 0 RETURN n.id ORDER BY n.id", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 8)
	require.Greater(t, counting.streamNodesCalls, 0, "the row pipeline must use the shared streaming scan")
	require.Equal(t, 0, counting.allNodesCalls, "the row pipeline must not materialize the complete store through AllNodes")
	require.Equal(t, 0, counting.labelCalls, "the row pipeline must not materialize a complete label population")
}
