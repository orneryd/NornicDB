package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTryCollectNodesFromPropertyIndexOrderLimit_Branches(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &allNodesForbiddenEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	for i := 0; i < 250; i++ {
		active := i < 10 // only low ranks active
		_, err := eng.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("nornic:p-%03d", i)),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"rank":   int64(i),
				"active": active,
				"score":  nil,
			},
		})
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE INDEX idx_person_rank FOR (n:Person) ON (n.rank)", nil)
	require.NoError(t, err)

	nodes, ok, err := exec.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePatternInfo{variable: "n", labels: []string{"Person"}}, "n.rank IS NOT NULL AND n.active = true", "n.rank DESC", 5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, nodes, 5)
	for i, node := range nodes {
		require.EqualValues(t, 9-i, node.Properties["rank"])
	}

	nodes, ok, err = exec.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePatternInfo{variable: "n", labels: []string{"Person"}}, "", "n.rank DESC", 3)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, nodes)

	nodes, ok, err = exec.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePatternInfo{variable: "n", labels: []string{"Person"}}, "", "n.rank DESC", 0)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, nodes)
}

func TestTryCollectNodesFromPropertyIndexNotNullPaths(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "seek_notnull_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE INDEX idx_item_score FOR (n:Item) ON (n.score)", nil)
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		props := map[string]interface{}{"id": i}
		if i%2 == 0 {
			props["score"] = int64(10 + i)
		}
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (:Item {id:%d%s})", i, mapSuffixForScore(props)), nil)
		require.NoError(t, err)
	}

	nodes, ok, err := exec.tryCollectNodesFromPropertyIndexNotNullOrderLimit(context.Background(), nodePatternInfo{variable: "n", labels: []string{"Item"}}, "n.score IS NOT NULL", "n.score DESC", 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, nodes, 2)
	require.EqualValues(t, 14, nodes[0].Properties["score"])
	require.EqualValues(t, 12, nodes[1].Properties["score"])

	nodes, ok, err = exec.tryCollectNodesFromPropertyIndexNotNull(nodePatternInfo{variable: "n", labels: []string{"Item"}}, "n.score IS NOT NULL")
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, nodes, 3)

	nodes, ok, err = exec.tryCollectNodesFromPropertyIndexNotNullOrderLimit(context.Background(), nodePatternInfo{variable: "n", labels: []string{"Item"}}, "n.score IS NOT NULL", "n.id DESC", 2)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, nodes)
}

func mapSuffixForScore(props map[string]interface{}) string {
	if score, ok := props["score"]; ok {
		return fmt.Sprintf(", score:%v", score)
	}
	return ""
}
