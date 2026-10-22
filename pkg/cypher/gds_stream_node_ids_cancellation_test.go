package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The GDS stream renderer converts the algorithm's materialized rows after
// the procedure returns; it stops there when the statement is canceled.
func TestGDSStreamNodeIDRenderingExecuteCancellation(t *testing.T) {
	project := func(t *testing.T, executor *StorageExecutor) {
		t.Helper()
		// The projection catalog is shared: each run projects its own copy.
		_, _ = executor.Execute(context.Background(), "CALL gds.graph.drop('render_cancel', false) YIELD graphName RETURN graphName", nil)
		t.Cleanup(func() {
			_, _ = executor.Execute(context.Background(), "CALL gds.graph.drop('render_cancel', false) YIELD graphName RETURN graphName", nil)
		})
		_, err := executor.Execute(context.Background(), "UNWIND range(1, $n) AS i WITH gds.graph.project('render_cancel', i, i % $n + 1) AS g RETURN g.nodeCount AS nodes", map[string]interface{}{"n": int64(4096)})
		require.NoError(t, err)
	}
	cases := []entityPopulationCase{{
		name:     "gds_stream_render",
		query:    "CALL gds.louvain.stream('render_cancel') YIELD nodeId, communityId RETURN nodeId, communityId",
		consumer: "renderNodeIDs",
		after:    1,
		rows:     4096,
		setup:    project,
		fixture: func(t *testing.T) *StorageExecutor {
			return NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
		},
	}}
	runEntityPopulationCases(t, cases, map[string]interface{}{"n": int64(4096)})
}
