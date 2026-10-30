package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReleaseClauseReaderRegressions(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
	}{
		{"keyword map key", "WITH {a: 1} AS count RETURN count {return: 2} AS m", [][]interface{}{{map[string]interface{}{"return": int64(2)}}}},
		{"count map projection", "WITH {a: 1} AS count RETURN count {.a} AS m", [][]interface{}{{map[string]interface{}{"a": int64(1)}}}},
		{"map projection in WHERE function", "WITH {a:1} AS m WHERE size(keys(m {.a})) = 1 RETURN 1 AS v", [][]interface{}{{int64(1)}}},
		{"CALL standalone ORDER after WHERE", "CALL () { UNWIND [1, 2] AS x WITH x WHERE x > 0 ORDER BY x RETURN count(*) AS n } RETURN n", [][]interface{}{{int64(2)}}},
		{"commented COUNT body", "RETURN COUNT { /* comment */ RETURN 1 AS x } AS n", [][]interface{}{{int64(1)}}},
		{"CALL standalone ORDER after UNWIND", "CALL () { UNWIND [2, 1] AS x ORDER BY x RETURN collect(x) AS n } RETURN n", [][]interface{}{{[]interface{}{int64(1), int64(2)}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Rows)
		})
	}
}

func TestReleaseClauseReaderRejectsMalformedPatternMaps(t *testing.T) {
	for _, query := range []string{"MATCH (n {.a}) RETURN n", "MATCH (n {bad}) RETURN n", "RETURN {.a} AS m"} {
		t.Run(query, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
		})
	}
}
