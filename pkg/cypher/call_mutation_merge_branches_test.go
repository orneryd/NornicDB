package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestApplyRemoveAndMergeWhereContext_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "remove_merge_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	n := &storage.Node{ID: "n1", Labels: []string{"Person", "Tmp"}, Properties: map[string]interface{}{"name": "A", "drop": int64(1)}}
	_, err := store.CreateNode(n)
	require.NoError(t, err)

	out := &ExecuteResult{Stats: &QueryStats{}}
	require.NoError(t, exec.pipelineApplyRemove(ctx, []pipelineRow{{"n": n}, {"n": "not-node"}}, originalBraceView("REMOVE n.drop, n:Tmp"), out))
	require.Equal(t, 1, out.Stats.PropertiesSet)

	nAfter, err := store.GetNode("n1")
	require.NoError(t, err)
	_, hasDrop := nAfter.Properties["drop"]
	require.False(t, hasDrop)
	require.Equal(t, []string{"Person"}, nAfter.Labels)
}
