package cypher

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestApocDijkstraHonorsAcceptedDefaultWeight(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "default_weight"))
		ctx := t.Context()
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
		const query = `MATCH(a:WeightAudit {name:'a'}),(b:WeightAudit {name:'b'}) CALL apoc.algo.dijkstra(a,b,'R>','cost',$fallback) YIELD path,weight RETURN weight`
		for _, tc := range []struct{ name, query string }{
			{"bound string", query},
			{"literal string", `MATCH(a:WeightAudit {name:'a'}),(b:WeightAudit {name:'b'}) CALL apoc.algo.dijkstra(a,b,'R>','cost','invalid') YIELD path,weight RETURN weight`},
			{"WITH-bound string", `WITH $fallback AS fallback MATCH(a:WeightAudit {name:'a'}),(b:WeightAudit {name:'b'}) CALL apoc.algo.dijkstra(a,b,'R>','cost',fallback) YIELD path,weight RETURN weight`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := exec.Execute(ctx, tc.query, map[string]interface{}{"fallback": "invalid"})
				// Proven argument types reject at admission, before the handler.
				var semanticError *SemanticError
				require.ErrorAs(t, err, &semanticError)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
				require.Contains(t, err.Error(), "Type mismatch: expected Float but was String")
			})
		}
		t.Run("accepted after rejected cached call", func(t *testing.T) {
			result, err := exec.Execute(ctx, query, map[string]interface{}{"fallback": float64(1)})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{float64(1)}}, result.Rows)
		})
		t.Run("evaluated handler argument", func(t *testing.T) {
			// Evaluated arguments retain the named runtime diagnostic, independently
			// of the public call's static signature admission.
			_, err := exec.callApocAlgoDijkstra(ctx, []interface{}{"default_weight:a", "default_weight:b", "R>", "cost", "invalid"})
			require.ErrorContains(t, err, "defaultWeight")
			var runtimeError *classifiedCypherError
			require.ErrorAs(t, err, &runtimeError)
			require.Equal(t, "Neo.ClientError.Statement.TypeError", runtimeError.BoltErrorCode())
			require.Equal(t, "InvalidArgumentType", runtimeError.BoltErrorDetail())
		})
	})
}
