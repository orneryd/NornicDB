package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeEvaluatesStatementArgumentsAtRuntime(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			query string
			want  [][]interface{}
		}{
			{"RETURN range(COUNT { RETURN 1 UNION ALL RETURN 1 }, 3) AS value", [][]interface{}{{[]interface{}{int64(2), int64(3)}}}},
			{"RETURN range(1, COUNT { RETURN 1 UNION ALL RETURN 1 }) AS value", [][]interface{}{{[]interface{}{int64(1), int64(2)}}}},
			{"RETURN range(1, 4, COUNT { RETURN 1 UNION ALL RETURN 1 }) AS value", [][]interface{}{{[]interface{}{int64(1), int64(3)}}}},
			{"WITH range(1, COUNT { RETURN 1 UNION ALL RETURN 1 }) AS value RETURN value", [][]interface{}{{[]interface{}{int64(1), int64(2)}}}},
			{"UNWIND [1,2] AS x RETURN range(count(*), 3) AS value", [][]interface{}{{[]interface{}{int64(2), int64(3)}}}},
			{"MATCH (n:Absent) RETURN range(1, COUNT { RETURN 1 UNION ALL RETURN 1 }) AS value", [][]interface{}{}},
		} {
			t.Run(tc.query, func(t *testing.T) {
				exec := NewStorageExecutor(newTestMemoryEngine(t))
				result, err := exec.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, result.Rows)
			})
		}
	})
}
