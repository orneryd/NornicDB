package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cancellation is an error from the canonical candidate path, never an index
// miss that authorizes another scan or a creating MERGE.
func TestIndexCandidateCollectorsCancellationAndRetry(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "index_context")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	for _, query := range []string{
		"CREATE (:P {k: 1, v: 2})",
		"CREATE INDEX p_k FOR (n:P) ON (n.k)",
		"CREATE INDEX p_v FOR (n:P) ON (n.v)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	pattern := nodePatternInfo{variable: "n", labels: []string{"P"}}
	inline := nodePatternInfo{variable: "n", labels: []string{"P"}, properties: map[string]interface{}{"k": int64(1)}}
	collectors := map[string]func(context.Context) ([]*storage.Node, bool, error){
		"equality": func(ctx context.Context) ([]*storage.Node, bool, error) {
			seek, planned, err := exec.planPropertyIndexEquality(ctx, pattern, "n.k = 1", nil)
			return planCandidateSeekNodes(exec, ctx, seek, planned, err)
		},
		"parameter IN": func(ctx context.Context) ([]*storage.Node, bool, error) {
			seek, planned, err := exec.planPropertyIndexInParam(ctx, pattern, "n.k IN $keys", map[string]interface{}{"keys": []interface{}{int64(1)}}, nil)
			return planCandidateSeekNodes(exec, ctx, seek, planned, err)
		},
		"literal IN": func(ctx context.Context) ([]*storage.Node, bool, error) {
			seek, planned, err := exec.planPropertyIndexInLiteral(ctx, pattern, "n.k IN [1]", nil)
			return planCandidateSeekNodes(exec, ctx, seek, planned, err)
		},
		"parameter OR": func(ctx context.Context) ([]*storage.Node, bool, error) {
			seek, planned, err := exec.planPropertyIndexInOrParam(ctx, pattern, "n.k IN $keys OR n.v IN $keys", map[string]interface{}{"keys": []interface{}{int64(1), int64(2)}})
			return planCandidateSeekNodes(exec, ctx, seek, planned, err)
		},
		"equality OR": func(ctx context.Context) ([]*storage.Node, bool, error) {
			seek, planned, err := exec.planPropertyIndexOrEquality(ctx, pattern, "n.k = 1 OR n.v = 2")
			return planCandidateSeekNodes(exec, ctx, seek, planned, err)
		},
		"range": func(ctx context.Context) ([]*storage.Node, bool, error) {
			ids, used, err := exec.resolvePropertyIndexRangeCandidates(ctx, pattern, "n.k >= 1", nil)
			return readResolvedCandidateNodes(exec, ctx, pattern, nodeCandidateIndexedProperty, ids, used, err)
		},
		"non-null": func(ctx context.Context) ([]*storage.Node, bool, error) {
			ids, used, err := exec.resolvePropertyIndexNotNullCandidates(ctx, pattern, "n.k IS NOT NULL")
			return readResolvedCandidateNodes(exec, ctx, pattern, nodeCandidateIndexedProperty, ids, used, err)
		},
		"inline": func(ctx context.Context) ([]*storage.Node, bool, error) {
			nodes, _, used, err := exec.collectPipelineIndexedNodeCandidates(ctx, inline, "", pipelineMatchPhysicalHint{})
			return nodes, used, err
		},
		"MERGE candidate IDs": func(ctx context.Context) ([]*storage.Node, bool, error) {
			return exec.findMergeNodesScanned(ctx, store, inline.labels, inline.properties)
		},
	}
	for name, collect := range collectors {
		t.Run(name, func(t *testing.T) {
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			nodes, _, err := collect(canceled)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, nodes)
			nodes, _, err = collect(ctx)
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			require.Equal(t, int64(1), nodes[0].Properties["k"])
		})
	}
}
