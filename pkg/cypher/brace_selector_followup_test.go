package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSubqueryBraceSelectorFollowups(t *testing.T) {
	for _, receiver := range []string{"exists", "count", "collect"} {
		for _, selector := range []string{"optional", "match", "return", "with", "unwind", "call", "limit", "skip", "offset", "order"} {
			t.Run(receiver+"_"+selector, func(t *testing.T) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace"))
				r, err := e.Execute(context.Background(), fmt.Sprintf("WITH {a:1} AS %s, 5 AS %s RETURN %s {%s} AS v", receiver, selector, receiver, selector), nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{map[string]interface{}{selector: int64(5)}}}, r.Rows)
			})
		}
	}
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN (COUNT { LIMIT 1 RETURN 1 AS x }) AS v", int64(1)},
		{"RETURN [COLLECT { LIMIT 1 RETURN 1 AS x }] AS v", []interface{}{[]interface{}{int64(1)}}},
		{"RETURN (COUNT { SKIP 0 RETURN 1 AS x }) AS v", int64(1)},
		{"RETURN (COUNT { OFFSET 0 RETURN 1 AS x }) AS v", int64(1)},
		{"RETURN [COLLECT { UNWIND [3,1,2] AS x ORDER BY x LIMIT 2 RETURN x }] AS v", []interface{}{[]interface{}{int64(1), int64(2)}}},
		{"WITH {a:1} AS exists, 5 AS optional RETURN exists {optional /* selector */} AS v", map[string]interface{}{"optional": int64(5)}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "brace"))
			r, err := e.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
		})
	}
}
