package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestApocDijkstraHonorsAcceptedDefaultWeight(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "default_weight"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (a:WeightAudit {name:'a'}),(b:WeightAudit {name:'b'}),(c:WeightAudit {name:'c'}), (a)-[:R]->(b),(a)-[:R {cost:2.0}]->(c),(c)-[:R {cost:2.0}]->(b)`, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		fallback interface{}
		want     float64
	}{{float64(10), 4}, {int64(10), 4}, {float64(1), 1}, {float64(3), 3}, {float64(0), 0}} {
		result, err := exec.Execute(ctx, `MATCH(a:WeightAudit {name:'a'}),(b:WeightAudit {name:'b'}) CALL apoc.algo.dijkstra(a,b,'R>','cost',$fallback) YIELD path,weight RETURN weight`, map[string]interface{}{"fallback": tc.fallback})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, tc.want, result.Rows[0][0])
	}
	_, err = exec.Execute(ctx, `MATCH(a:WeightAudit {name:'a'}),(b:WeightAudit {name:'b'}) CALL apoc.algo.dijkstra(a,b,'R>','cost',$fallback) YIELD path,weight RETURN weight`, map[string]interface{}{"fallback": "invalid"})
	require.ErrorContains(t, err, "defaultWeight")
	// Handler validation remains fail-closed for callers that already hold
	// evaluated arguments, independently of literal signature validation.
	_, err = exec.callApocAlgoDijkstra(ctx, []interface{}{"default_weight:a", "default_weight:b", "R>", "cost", "invalid"})
	require.ErrorContains(t, err, "defaultWeight")
}
