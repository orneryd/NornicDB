package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR73PublicRows(t *testing.T) {
	cases := []struct {
		name, query string
		rows        [][]interface{}
	}{
		{"with_function_unwind", "MATCH (n:ReviewItem) WITH labels(n) AS xs UNWIND xs AS x RETURN x", [][]interface{}{{"ReviewItem"}}},
		{"property_list_unwind", "MATCH (n:ReviewItem) UNWIND [n.k] AS x RETURN x", [][]interface{}{{int64(2)}}},
		{"literal_unwind", "MATCH (n:ReviewItem) UNWIND [1] AS x RETURN x", [][]interface{}{{int64(1)}}},
		{"unwind_scalar", "MATCH (n:ReviewItem) WITH n.k AS xs UNWIND xs AS x RETURN x", [][]interface{}{{int64(2)}}},
		{"nested_unwind", "UNWIND [1] AS i WITH i AS x UNWIND [x] AS y RETURN y", [][]interface{}{{int64(1)}}},
		{"aggregate_size", "MATCH (n:ReviewItem) RETURN sum(size(n.arr)) AS total", [][]interface{}{{int64(2)}}},
		{"with_aggregate_size", "MATCH (n:ReviewItem) WITH sum(size(n.arr)) AS total RETURN total", [][]interface{}{{int64(2)}}},
		{"order_by", "MATCH (n:ReviewItem) UNWIND [3,1,2] AS x RETURN x ORDER BY x", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
		{"optional_unwind", "OPTIONAL MATCH (n:ReviewItem) WITH labels(n) AS xs UNWIND xs AS x RETURN x", [][]interface{}{{"ReviewItem"}}},
		{"call_unwind", "CALL { MATCH (n:ReviewItem) WITH labels(n) AS xs UNWIND xs AS x RETURN x } RETURN x", [][]interface{}{{"ReviewItem"}}},
		{"union_unwind", "MATCH (n:ReviewItem) WITH labels(n) AS xs UNWIND xs AS x RETURN x UNION ALL RETURN 'second' AS x", [][]interface{}{{"ReviewItem"}, {"second"}}},
		{"profile_unwind", "PROFILE MATCH (n:ReviewItem) WITH labels(n) AS xs UNWIND xs AS x RETURN x", [][]interface{}{{"ReviewItem"}}},
	}
	for _, explicit := range []bool{false, true} {
		for _, c := range cases {
			t.Run(c.name+map[bool]string{false: "/auto", true: "/explicit"}[explicit], func(t *testing.T) {
				ctx := context.Background()
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_r73"))
				_, err := e.Execute(ctx, "CREATE (:ReviewItem {k:2, arr:[1,2]})", nil)
				require.NoError(t, err)
				if explicit {
					_, err = e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer e.Execute(ctx, "ROLLBACK", nil)
				}
				result, err := e.Execute(ctx, c.query, nil)
				require.NoError(t, err, "query=%s", c.query)
				require.Equal(t, c.rows, result.Rows, "query=%s", c.query)
			})
		}
	}
}
