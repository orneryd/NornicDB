package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type nonStreamingStorageEngine struct {
	storage.Engine
}

func TestContainsMultiWordWithLimitFiltersNonStreamingStorage(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "nornic")
	for i := 0; i < 6; i++ {
		_, err := store.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("unrelated-%d", i)),
			Labels: []string{"Document"},
			Properties: map[string]interface{}{
				"content": fmt.Sprintf("gate3-stress-w2-op%d", i),
			},
		})
		require.NoError(t, err)
	}
	_, err := store.CreateNode(&storage.Node{
		ID:     "exact-match",
		Labels: []string{"Document"},
		Properties: map[string]interface{}{
			"content": "this node is safe to delete after verification",
		},
	})
	require.NoError(t, err)

	exec := NewStorageExecutor(&nonStreamingStorageEngine{Engine: store})
	result, err := exec.Execute(context.Background(), `
		MATCH (n)
		WHERE n.content CONTAINS "safe to delete"
		RETURN n.content
		LIMIT 5
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"this node is safe to delete after verification"}}, result.Rows)

	result, err = exec.Execute(context.Background(), `
		MATCH (n)
		WHERE n.content CONTAINS "some phrase with common words"
		RETURN n.content
		LIMIT 5
	`, nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}
