package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Property filters reach past any number of nearer vectors they reject: 514
// rejected nodes rank above the 14 that pass and the query text matches no
// word, so only the vector side can find them. Each limit returns that many
// passing nodes, up to all 14, from both procedures and with paging.
func TestDbRetrieveFiltersReachPastNearerRejectedVectors(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	for i := 0; i < 514; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("x%d", i)), Labels: []string{"S"}, Properties: map[string]interface{}{"state": "retired"}, ChunkEmbeddings: [][]float32{{1, 0.1 + float32(i)*0.0002, 0, 0}}})
		require.NoError(t, err)
	}
	for j := 0; j < 14; j++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("e%d", j)), Labels: []string{"S"}, Properties: map[string]interface{}{"state": "current"}, ChunkEmbeddings: [][]float32{{1, 0.45 + float32(j)*0.01, 0, 0}}})
		require.NoError(t, err)
	}
	service := search.NewService(store, 4, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)
	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		for _, paged := range []bool{false, true} {
			for limit, want := range map[int64]int64{1: 1, 10: 10, 14: 14, 20: 14} {
				request := map[string]interface{}{"query": "zzqq", "embedding": []interface{}{1.0, 0.0, 0.0, 0.0}, "types": []interface{}{"S"}, "limit": limit, "filters": map[string]interface{}{"state": "current"}}
				query := "CALL " + procedure + "($request) YIELD node WHERE node.state = 'current' RETURN count(node)"
				if paged {
					request["n"], request["max_results"] = limit, limit
					query = "CALL " + procedure + "($request) YIELD page RETURN size(page.results)"
				}
				result, err := exec.Execute(ctx, query, map[string]interface{}{"request": request})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{want}}, result.Rows, "%s paged=%v limit %d", procedure, paged, limit)
			}
		}
	}
}
