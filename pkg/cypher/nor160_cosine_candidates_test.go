//go:build nornic_copycount

package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The cosine scan under the predicate contract (NOR-160): rejected candidates
// and candidates that leave the top-k heap cost no copy, each node returned is
// copied exactly once, and none is the candidate the transaction keeps.
func TestNOR160CosineScanCopiesOnlyWhatItReturns(t *testing.T) {
	ctx := context.Background()
	engine := newTestMemoryEngine(t)
	const count = 8
	for index := 0; index < count; index++ {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:n%d", index)), Labels: []string{"P"}, Properties: map[string]any{
			"vec":  []float64{1, float64(index)},
			"keep": index%2 == 0,
		}})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))
	var reader storage.ProjectedLabelNodeReader = tx
	_, isReadOnly := reader.(storage.ReadOnlyLabelNodeReader)
	require.True(t, isReadOnly, "the transaction must take the read-only path")

	exec := NewStorageExecutor(engine)
	seen := map[storage.NodeID]*storage.Node{}
	filter := func(node *storage.Node) bool {
		seen[node.ID] = node
		return node.Properties["keep"] == true
	}
	projection := []string{"vec", "keep"}
	scan := func() []vectorNodeScore {
		scores, ok := exec.fetchCosineNodeScoresProjectedLabel(ctx, reader, "P", "vec", 2, []float32{1, 0}, true, projection, filter)
		require.True(t, ok)
		require.Len(t, scores, 2)
		return scores
	}
	scan() // fills the transaction's projected label cache

	var scores []vectorNodeScore
	nodes, _ := storage.CountStructuralCopies(func() { scores = scan() })
	require.Len(t, seen, count, "every candidate reaches the filter")
	require.Len(t, nodes, 2, "only the nodes returned are copied: %v", nodes)
	for _, score := range scores {
		require.Equal(t, 1, nodes[score.node.ID], score.node.ID)
		require.NotSame(t, seen[score.node.ID], score.node, "a returned node must not be the shared candidate")
	}

	// What is returned is the caller's: changing it reaches no later read.
	id := scores[0].node.ID
	scores[0].node.Properties["keep"] = "changed"
	again := scan()
	require.Equal(t, true, seen[id].Properties["keep"])
	for _, score := range again {
		require.Equal(t, true, score.node.Properties["keep"])
	}
}
