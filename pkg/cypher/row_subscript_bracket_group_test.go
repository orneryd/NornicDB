package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSubscriptValidationReadsBracketGroups: the subscript check reads the
// bracket group that closes an item, so a list comprehension or literal that
// is the whole item is not taken for a subscript of its inner list (which
// evaluated text such as `1,2,3] WHERE x > 1 | x * 2`), while a real
// subscript is still checked.
func TestSubscriptValidationReadsBracketGroups(t *testing.T) {
	for expression, want := range map[string]int{
		"[x IN [1,2,3] | x]":    0,
		"l[0]":                  1,
		"[x IN [1,2,3] | x][0]": 18,
		"m['a[b]']":             1,
		"n.list[1..2]":          6,
		"[1, [2]]":              0,
		"x]":                    -1,
		"'[a]'":                 -1,
	} {
		require.Equal(t, want, closingBracketGroupStart(expression), expression)
	}

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subscript"))
	ctx := context.Background()
	for statement, want := range map[string][][]interface{}{
		"RETURN [x IN [1,2,3] WHERE x > 1 | x * 2] AS r":     {{[]interface{}{int64(4), int64(6)}}},
		"UNWIND [1] AS one RETURN [x IN [1,2] | x * 2] AS r": {{[]interface{}{int64(2), int64(4)}}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
	_, err := exec.Execute(ctx, "WITH [1, 2] AS l RETURN l['a'] AS r", nil)
	require.Error(t, err)
}

// TestRowSubscriptKeyHoldsBrackets: a subscript whose key holds brackets of
// its own (m[r[k]], m[l[0].id]) is the bracket group that closes the
// expression, read by the row evaluator wherever it is (a projection, a
// comprehension, a quantifier, a WHERE), a missing key giving null.
// Recorded on Neo4j 5.26.30 (#1211).
func TestRowSubscriptKeyHoldsBrackets(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subscript_key_brackets"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {id:'a'})-[:R {id:'r1'}]->(:N {id:'b'})", nil)
	require.NoError(t, err)
	const head = "WITH {x: true} AS m MATCH (x:N {id: 'a'})-[r:R]->() "
	for statement, want := range map[string][][]interface{}{
		"WITH {x: true} AS m, [{id: 'r1'}] AS r RETURN m[r[0].id] AS v":      {{nil}},
		"WITH {x: true} AS m, [{id: 'x'}] AS r RETURN m[r[0].id] AS v":       {{true}},
		head + "RETURN [k IN keys(r) | m[r[k]]] AS v":                        {{[]interface{}{nil}}},
		head + "RETURN any(k IN keys(r) WHERE m[r[k]] IS NULL) AS v":         {{true}},
		head + "RETURN none(k IN keys(x) WHERE m[x[k]] IS NOT NULL) AS v":    {{true}},
		head + "WHERE any(k IN keys(r) WHERE m[r[k]] IS NULL) RETURN 1":      {{int64(1)}},
		head + "WITH m, [r] AS r RETURN m[r[0]['id']] AS v, m[r[0].id] AS w": {{nil, nil}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
}
