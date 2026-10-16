package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmptyQuotedIterator(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  interface{}
		bad   bool
	}{
		{"RETURN [`` IN [2] | 1] AS v", []interface{}{int64(1)}, false},
		{"RETURN [`` IN [2] | ``] AS v", []interface{}{int64(2)}, false},
		{"RETURN [`` IN [1,2] WHERE `` > 1 | `` + 1] AS v", []interface{}{int64(3)}, false},
		{"RETURN [`` IN [] | ``] AS v", []interface{}{}, false},
		{"RETURN [`` IN null | ``] AS v", nil, false},
		{"WITH 9 AS `` RETURN [`` IN [2] | ``] AS v", []interface{}{int64(2)}, false},
		{"RETURN [`x x` IN [2] | `x x`] AS v", []interface{}{int64(2)}, false},
		{"RETURN [ IN [2] | 1] AS v", nil, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "empty_iterator"))
			r, err := e.Execute(context.Background(), tc.query, nil)
			if tc.bad {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
		})
	}
}
