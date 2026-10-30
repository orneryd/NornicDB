package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQuantifierFold: Cypher's three-valued rules for all / any / none /
// single over per-element predicate results.
func TestQuantifierFold(t *testing.T) {
	fold := func(function string, results ...interface{}) interface{} {
		q := quantifierFold{function: function}
		for _, result := range results {
			if value, decided := q.add(result); decided {
				return value
			}
		}
		return q.result()
	}
	for _, tc := range []struct {
		function string
		results  []interface{}
		want     interface{}
	}{
		{"all", nil, true},
		{"all", []interface{}{true, true}, true},
		{"all", []interface{}{true, nil}, nil},
		{"all", []interface{}{nil, false}, false},
		{"any", nil, false},
		{"any", []interface{}{false, nil}, nil},
		{"any", []interface{}{nil, true}, true},
		{"none", nil, true},
		{"none", []interface{}{false, nil}, nil},
		{"none", []interface{}{nil, true}, false},
		{"single", nil, false},
		{"single", []interface{}{true, false}, true},
		{"single", []interface{}{true, nil}, nil},
		{"single", []interface{}{true, nil, true}, false},
	} {
		require.Equal(t, tc.want, fold(tc.function, tc.results...), "%s %v", tc.function, tc.results)
	}
}

// TestNullArgumentsEvaluateToNull: a null argument makes these functions
// null, and a list predicate over a null list is null (#736), on the
// pipeline's projections and in WHERE (Neo4j's results).
func TestNullArgumentsEvaluateToNull(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {l: [1, 2]}), (:Q)", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"WITH null AS m RETURN keys(m), keys(null)", [][]interface{}{{nil, nil}}},
		{"WITH null AS m RETURN abs(m) AS v", [][]interface{}{{nil}}},
		{"WITH null AS m RETURN all(x IN m WHERE x > 0) AS a, any(x IN m WHERE x > 0) AS b, none(x IN m WHERE x > 0) AS c, single(x IN m WHERE x > 0) AS d",
			[][]interface{}{{nil, nil, nil, nil}}},
		{"RETURN all(x IN null WHERE x > 0) AS v", [][]interface{}{{nil}}},
		{"RETURN none(x IN null WHERE true) AS v", [][]interface{}{{nil}}},
		{"RETURN any(x IN [1, null] WHERE x > 0) AS v", [][]interface{}{{true}}},
		{"RETURN all(x IN [1, null] WHERE x > 5) AS v", [][]interface{}{{false}}},
		{"RETURN single(x IN [1, 2, null] WHERE x > 0) AS v", [][]interface{}{{false}}},
		{"RETURN none(x IN [null] WHERE x > 0) AS v", [][]interface{}{{nil}}},
		{"MATCH (n:Q) RETURN none(x IN n.l WHERE x > 1) AS v ORDER BY v", [][]interface{}{{false}, {nil}}},
		{"MATCH (n:Q) WHERE NOT any(x IN n.l WHERE x > 5) RETURN count(n) AS c", [][]interface{}{{int64(1)}}},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}
}
