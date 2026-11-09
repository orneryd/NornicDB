package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAPOCNilListsRemainLists(t *testing.T) {
	for _, bundled := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "bundled"}[bundled], func(t *testing.T) {
			if bundled {
				withPluginFunctions(t, bundledAPOCPluginFunctions(t))
			}
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nil_lists"))
			for _, list := range []interface{}{[]interface{}(nil), []string(nil), []int64(nil)} {
				for _, query := range []string{
					"RETURN apoc.map.fromPairs($items) AS value",
					"RETURN apoc.map.fromLists($items,$items) AS value",
					"RETURN apoc.map.clean({},$items,$items) AS value",
					"RETURN apoc.map.removeKeys({},$items) AS value",
				} {
					result, err := e.Execute(context.Background(), query, map[string]interface{}{"items": list})
					require.NoError(t, err, "%T: %s", list, query)
					require.Equal(t, map[string]interface{}{}, result.Rows[0][0])
				}
				_, err := e.Execute(context.Background(), "CREATE (:Keep {key:1})", nil)
				require.NoError(t, err)
				result, err := e.Execute(context.Background(), "MATCH (n:Keep) SET n += apoc.map.fromPairs($items) RETURN n.key", map[string]interface{}{"items": list})
				require.NoError(t, err)
				for _, row := range result.Rows {
					require.Equal(t, int64(1), row[0])
				}
				result, err = e.Execute(context.Background(), "MATCH (n:Keep) SET n = apoc.map.fromPairs($items) RETURN properties(n)", map[string]interface{}{"items": list})
				require.NoError(t, err)
				for _, row := range result.Rows {
					require.Equal(t, map[string]interface{}{}, row[0])
				}
				_, err = e.Execute(context.Background(), "MATCH (n:Keep) DELETE n", nil)
				require.NoError(t, err)
			}
		})
	}
}
