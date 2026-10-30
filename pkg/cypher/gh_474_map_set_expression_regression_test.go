package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH474_SetMapEvaluatesScopedExpressionValues(t *testing.T) {
	rows := []interface{}{
		map[string]interface{}{"id": int64(1), "name": "n1"},
		map[string]interface{}{"id": int64(2), "name": "n2"},
	}
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh474")
			for index := int64(1); index <= 4; index++ {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(string(rune('0' + index))), Labels: []string{"A"},
					Properties: map[string]interface{}{"id": index, "name": "n" + string(rune('0'+index))},
				})
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			checkRows := func(query string, params map[string]interface{}, want [][]interface{}) {
				t.Helper()
				result, err := exec.Execute(ctx, query, params)
				require.NoError(t, err, query)
				require.ElementsMatch(t, want, result.Rows, query)
			}
			checkRows("UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name} RETURN a.id AS id, a.name AS n", map[string]interface{}{"rows": rows}, [][]interface{}{{int64(1), "n1"}, {int64(2), "n2"}})
			checkRows("UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a += {name: r.name + 'x'} RETURN a.id AS id, a.name AS n", map[string]interface{}{"rows": rows}, [][]interface{}{{int64(1), "n1x"}, {int64(2), "n2x"}})
			checkRows("MATCH (a:A {id: 3}) SET a += {name: a.name + 'y', id2: a.id * 2} RETURN a.name AS n, a.id2 AS id2", nil, [][]interface{}{{"n3y", int64(6)}})
			checkRows("MATCH (a:A {id: 4}) SET a += {name: toUpper(a.name), n2: 1 + 2} RETURN a.name AS n, a.n2 AS n2", nil, [][]interface{}{{"N4", int64(3)}})
			checkRows("UNWIND $rows AS r MERGE (a:A {id: r.id}) SET a.name = r.name + 'z' RETURN a.id AS id, a.name AS n", map[string]interface{}{"rows": rows}, [][]interface{}{{int64(1), "n1z"}, {int64(2), "n2z"}})

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
