package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// rejectedVectorReachFixture is the canonical owner of the retrieval filter
// population: 514 rejected nodes rank above the 14 that pass and the query
// text matches no word, so only the vector side can find the passing nodes.
// Property filters and where both reach past the nearer rejected vectors.
type rejectedVectorReachFixture struct {
	ctx   context.Context
	store storage.Engine
	exec  *StorageExecutor
}

func newRejectedVectorReachFixture(t *testing.T) *rejectedVectorReachFixture {
	t.Helper()
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
	return &rejectedVectorReachFixture{ctx: ctx, store: store, exec: exec}
}

// TestDbRetrieveFilteredVectorResultsReachPastNearerRejectedVectors owns the
// documented contract that a filtered retrieval reaches past any number of
// nearer vectors it rejects. Each case counts the passing nodes a limit
// returns, up to all 14, from both procedures and with paging: "filters"
// selects the property-filter request key, "where" selects a Cypher predicate
// on each candidate bound as node.
func TestDbRetrieveFilteredVectorResultsReachPastNearerRejectedVectors(t *testing.T) {
	selection := []struct {
		name      string
		parameter string
		value     interface{}
	}{
		{name: "filters", parameter: "filters", value: map[string]interface{}{"state": "current"}},
		{name: "where", parameter: "where", value: "node.state = 'current'"},
	}

	for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
		for _, selectBy := range selection {
			for _, paged := range []bool{false, true} {
				t.Run(procedure+"/"+selectBy.name+fmt.Sprintf("/paged=%v", paged), func(t *testing.T) {
					fixture := newRejectedVectorReachFixture(t)
					for _, limit := range []int64{1, 10, 14, 20} {
						want := limit
						if want > 14 {
							want = 14
						}
						request := map[string]interface{}{"query": "zzqq", "embedding": []interface{}{1.0, 0.0, 0.0, 0.0}, "types": []interface{}{"S"}, "limit": limit, selectBy.parameter: selectBy.value}
						query := "CALL " + procedure + "($request) YIELD node WHERE node.state = 'current' RETURN count(node)"
						if paged {
							request["n"], request["max_results"] = limit, limit
							query = "CALL " + procedure + "($request) YIELD page RETURN size(page.results)"
						}
						result, err := fixture.exec.Execute(fixture.ctx, query, map[string]interface{}{"request": request})
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{want}}, result.Rows, "limit %d", limit)
					}
				})
			}
		}
	}
}

// retrievedIDs runs a db.retrieve request against fixture and returns the
// ids of the admitted nodes in ascending order.

// retrievedRelationshipNumbers runs a db.retrieveRelationships statement and
// returns the admitted relationship.n values in ascending order.
