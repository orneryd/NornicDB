package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestKeywordProjectionAndNestedSubqueryOrder(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        interface{}
	}{
		{"empty_keyword_projection", "WITH {a:1} AS exists RETURN exists {} AS v", map[string]interface{}{}},
		{"null_keyword_projection", "WITH null AS exists RETURN exists {.a} AS v", nil},
		{"keyword_selector", "WITH {a:1} AS exists, 5 AS b RETURN exists {b} AS v", map[string]interface{}{"b": int64(5)}},
		{"where_keyword_projection", "WITH {a:1} AS count MATCH (q:Q) WHERE q.id = size(keys(count {.*})) RETURN q.id AS v", int64(1)},
		{"optional_where_keyword_projection", "WITH {a:1} AS count OPTIONAL MATCH (q:Q) WHERE q.id = size(keys(count {.*})) RETURN q.id AS v", int64(1)},
		{"pattern_value_keyword_projection", "WITH {a:1} AS count MATCH (q:Q {id:size(keys(count {.*}))}) RETURN q.id AS v", int64(1)},
		{"grouped_count_order", "RETURN (COUNT { UNWIND [2,1] AS x ORDER BY x LIMIT 1 RETURN x }) AS v", int64(1)},
		{"map_count_order", "RETURN {n: COUNT { UNWIND [2,1] AS x ORDER BY x LIMIT 1 RETURN x }} AS v", map[string]interface{}{"n": int64(1)}},
		{"list_count_order", "RETURN [COUNT { UNWIND [2,1] AS x ORDER BY x LIMIT 1 RETURN x }] AS v", []interface{}{int64(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "keyword_projection"))
			_, err := e.Execute(context.Background(), "CREATE (:Q {id:1})", nil)
			require.NoError(t, err)
			result, err := e.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
		})
	}
}
