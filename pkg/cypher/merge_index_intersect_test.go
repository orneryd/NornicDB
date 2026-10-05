package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The candidate sets of a two-property index lookup are intersected starting
// from the smaller set. The properties come from a map, so the smaller set is
// visited first or second at random; repeating the lookup exercises both.
func TestLookupPatternCandidatesUsingPropertyIndex_IntersectsSets(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_intersect_idx")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:P {id:'both', a:1, b:2}), (:P {id:'a1', a:1, b:3}),
		(:P {id:'a2', a:1, b:4}), (:P {id:'b-only', a:9, b:2})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX idx_p_a IF NOT EXISTS FOR (n:P) ON (n.a)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX idx_p_b IF NOT EXISTS FOR (n:P) ON (n.b)", nil)
	require.NoError(t, err)

	pattern := nodePatternInfo{variable: "n", labels: []string{"P"}, properties: map[string]interface{}{"a": int64(1), "b": int64(2)}}
	for i := 0; i < 32; i++ {
		nodes, ok := exec.lookupPatternCandidatesUsingPropertyIndex(pattern, store)
		require.True(t, ok)
		require.Len(t, nodes, 1)
		require.Equal(t, "both", nodes[0].Properties["id"])
	}
}
