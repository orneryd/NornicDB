package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A CASE inside a reduce, quantifier or comprehension reads the scope's
// variables, so it is evaluated with the scope, in a predicate as in a
// projection (it was evaluated for the row first, where they don't exist).
// Recorded on Neo4j 5.26.30.
func TestCaseInsideVariableScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_in_scope"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:ZN {name: 'Acme  Holdings'})", nil)
	require.NoError(t, err)
	normalise := func(value string) string {
		return "reduce(t = '', w IN [x IN split(toLower(trim(" + value + ")), ' ') WHERE x <> ''] | CASE WHEN t = '' THEN w ELSE t + ' ' + w END)"
	}
	for query, want := range map[string]interface{}{
		"WITH ['Acme  Holdings'] AS names RETURN any(name IN names WHERE " + normalise("name") + " = 'acme holdings') AS v":                                              true,
		"MATCH (n:ZN) WHERE " + normalise("n.name") + " = 'acme holdings' RETURN n.name AS v":                                                                            "Acme  Holdings",
		"WITH 'Acme  Holdings' AS name RETURN reduce(t = '', w IN [x IN split(name, ' ') WHERE x <> ''] | CASE WHEN t = '' THEN w ELSE t + w END) = 'AcmeHoldings' AS v": true,
		"WITH [1, 2] AS l WHERE [x IN l | CASE WHEN x > 1 THEN 'big' ELSE 'small' END] = ['small', 'big'] RETURN 1 AS v":                                                 int64(1),
		"WITH [1, 2] AS l WHERE all(x IN l WHERE CASE WHEN x > 0 THEN true ELSE false END) RETURN 1 AS v":                                                                int64(1),
		"UNWIND [1, 2] AS i WITH i WHERE CASE WHEN i > 1 THEN true ELSE false END AND reduce(s = 0, x IN [i] | s + CASE WHEN x > 1 THEN x ELSE 0 END) = 2 RETURN i AS v": int64(2),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}

// caseBlockSpans returns only the CASE blocks outside a scope that binds
// names.
func TestCaseBlockSpansSkipScopes(t *testing.T) {
	for expr, want := range map[string]int{
		"CASE WHEN a THEN 1 END = 1":                                   1,
		"reduce(s = 0, x IN l | s + CASE WHEN x THEN 1 ELSE 0 END)":    0,
		"[x IN l | CASE WHEN x THEN 1 END]":                            0,
		"[(a)-->(b) | CASE WHEN b.x THEN 1 END]":                       0,
		"any(x IN l WHERE CASE WHEN x THEN true END)":                  0,
		"EXISTS { MATCH (n) WHERE CASE WHEN n.x THEN true END }":       0,
		"size([1, CASE WHEN a THEN 2 END]) = 2":                        1,
		"n.any(CASE WHEN a THEN 1 END)":                                1,
		"coalesce(CASE WHEN a THEN 1 END, 2) = CASE WHEN b THEN 2 END": 2,
		"{k: CASE WHEN a THEN 1 END}.k = 1":                            1,
		"([x IN l | x])[0] = CASE WHEN a THEN 1 END":                   1,
	} {
		require.Len(t, caseBlockSpans(expr), want, expr)
	}
}
