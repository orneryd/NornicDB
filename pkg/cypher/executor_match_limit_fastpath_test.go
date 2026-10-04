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

func TestGh713SimpleMatchLimitUsesSharedProjectionAndPagination(t *testing.T) {
	for _, route := range []string{"direct", "autocommit", "explicit transaction"} {
		for _, test := range []struct {
			projection string
			limit      string
			column     string
			rows       int
		}{
			{"n", "2", "n", 2},
			{"n", "1 + 1", "n", 2},
			{"n", "$limit", "n", 2},
			{"n AS `node value`", "2", "node value", 2},
			{"n AS `a``b`", "2", "a`b", 2},
			{"n AS `node value`", "0", "node value", 0},
		} {
			t.Run(route+"/"+test.projection+"/"+test.limit, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, "CREATE (:LimitNode {id: 'a'}), (:LimitNode {id: 'b'}), (:LimitNode {id: 'c'})", nil)
				require.NoError(t, err)
				query := "MATCH (n:LimitNode) RETURN " + test.projection + " LIMIT " + test.limit
				params := map[string]interface{}{"limit": int64(2)}
				var result *ExecuteResult
				if route == "direct" {
					var handled bool
					result, handled = exec.tryFastPathSimpleMatchReturnLimit(withQueryParams(ctx, params), query, upperASCII(query))
					require.True(t, handled)
				} else {
					if route == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					result, err = exec.Execute(ctx, query, params)
					require.NoError(t, err)
					if route == "explicit transaction" {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				}
				require.Equal(t, []string{test.column}, result.Columns)
				require.Len(t, result.Rows, test.rows)
				seen := make(map[storage.NodeID]bool)
				for _, row := range result.Rows {
					require.Len(t, row, 1)
					node, ok := row[0].(*storage.Node)
					require.True(t, ok)
					require.Contains(t, []string{"a", "b", "c"}, node.Properties["id"])
					require.False(t, seen[node.ID])
					seen[node.ID] = true
				}
				stored, err := exec.Execute(ctx, "MATCH (n:LimitNode) RETURN count(n) AS total", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(3)}}, stored.Rows)
			})
		}
	}
}

func TestGh713SimpleNodeCompilerDeclinesUnsupportedPlans(t *testing.T) {
	for _, projection := range []string{
		"", "*", "DISTINCT n", "count(n)", "n, n AS other",
		"n LIMIT 1", "n AS", "n.id", "N", "m AS node",
	} {
		t.Run(projection, func(t *testing.T) {
			column, handled := parseSimpleReturnVariable(projection, "n")
			require.False(t, handled)
			require.Empty(t, column)
		})
	}
}

func TestGh713SimpleMatchLimitDeclinesInvalidCompleteWindows(t *testing.T) {
	for _, limit := range []string{"-1", "1.5", "$missing", "1 garbage"} {
		t.Run(limit, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			query := "MATCH (n) RETURN n LIMIT " + limit
			result, handled := exec.tryFastPathSimpleMatchReturnLimit(ctx, query, upperASCII(query))
			require.False(t, handled)
			require.Nil(t, result)
		})
	}
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
