package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A value that isn't a list in a list position is a list of that one value
// at runtime, as in Neo4j 5.26 (answers taken from neo4j:5.26.30); a literal
// of another type there is a compile-time type error.
func TestListPositionReadsAValueAsAOneElementList(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:QP {s: 5, t: 'ab', b: true})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		{"MATCH (n:QP) RETURN all(x IN n.s WHERE x > 1) AS r", true},
		{"MATCH (n:QP) RETURN none(x IN n.s WHERE x > 1) AS r", false},
		{"MATCH (n:QP) RETURN any(x IN n.t WHERE x = 'ab') AS r", true},
		{"MATCH (n:QP) RETURN single(x IN n.b WHERE x) AS r", true},
		{"MATCH (n:QP) WITH n.s AS v RETURN all(x IN v WHERE x > 1) AS r", true},
		{"MATCH (n:QP) WITH {m: n.s} AS v RETURN any(x IN v.m WHERE x = 5) AS r", true},
		{"MATCH (n:QP) RETURN [x IN n.s WHERE x > 1] AS r", []interface{}{int64(5)}},
		{"MATCH (n:QP) RETURN [x IN n.s | x + 1] AS r", []interface{}{int64(6)}},
		{"MATCH (n:QP) RETURN reduce(a = 0, x IN n.s | a + x) AS r", int64(5)},
		{"MATCH (n:QP) RETURN 5 IN n.s AS r", true},
		{"MATCH (n:QP) RETURN any(x IN n.missing WHERE x > 1) AS r", nil},
		{"MATCH (n:QP) WHERE any(x IN n.s WHERE x = 5) RETURN n.s AS r", int64(5)},
		{"MATCH (n:QP) WHERE all(x IN n.s WHERE x > 1) RETURN n.s AS r", int64(5)},
		{"UNWIND 5 AS x RETURN x AS r", int64(5)},
		{"MATCH (n:QP) UNWIND n.s AS x RETURN x AS r", int64(5)},
		{"MATCH (n:QP) WITH n.s AS v UNWIND v AS x RETURN x AS r", int64(5)},
		{"MATCH (n:QP) UNWIND n AS x RETURN count(x) AS r", int64(1)},
		{"MATCH (n:QP) UNWIND n.missing AS x RETURN count(x) AS r", int64(0)},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Len(t, result.Rows, 1, tc.query)
		assert.Equal(t, tc.want, result.Rows[0][0], tc.query)
	}
	for _, query := range []string{
		"RETURN all(x IN 5 WHERE x > 1) AS r",
		"RETURN any(x IN 'ab' WHERE true) AS r",
		"RETURN [x IN {a: 1} | x] AS r",
		"RETURN reduce(a = 0, x IN 5 | a + x) AS r",
		"RETURN 1 IN 5 AS r",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		assert.Contains(t, err.Error(), "Type mismatch: expected List<T> but was", query)
	}
}

func TestTraversableList(t *testing.T) {
	assert.Nil(t, traversableList(nil))
	assert.Equal(t, []interface{}{int64(5)}, traversableList(int64(5)))
	assert.Equal(t, []interface{}{"ab"}, traversableList("ab"))
	assert.Equal(t, []interface{}{int64(1), int64(2)}, traversableList([]int64{1, 2}))
	assert.Equal(t, []interface{}{}, traversableList([]interface{}{}))
}

// The shared evaluator's all / any / none / single use the same argument
// parser and list rule as the row evaluator.
func TestSharedEvaluatorQuantifierListRule(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	node := &storage.Node{ID: "n1", Properties: map[string]interface{}{"s": int64(5), "l": []interface{}{int64(1), int64(2)}}}
	nodes := map[string]*storage.Node{"n": node}
	eval := func(function, inner string) interface{} {
		return exec.evaluateQuantifierWithContext(ctx, function, inner, nodes, nil, nil, nil, nil, 0)
	}
	assert.Equal(t, true, eval("all", "x IN n.s WHERE x > 1"))
	assert.Equal(t, false, eval("none", "x IN n.s WHERE x > 1"))
	assert.Equal(t, true, eval("single", "x IN n.l WHERE x = 2"))
	assert.Nil(t, eval("any", "x IN n.missing WHERE x > 1"))
	// Shapes semantic validation rejects are null here.
	assert.Nil(t, eval("none", "x WHERE x > 1"))
	assert.Nil(t, eval("none", "x IN n.l"))
	assert.Nil(t, eval("all", "1x IN n.l WHERE true"))
	// A list expression this evaluator returns as its own text is no list.
	assert.Nil(t, eval("none", "x IN nosuch.prop WHERE true"))
}

func TestParseQuantifierArguments(t *testing.T) {
	variable, list, predicate, ok := parseQuantifierArguments("x IN n.list WHERE x > 1")
	require.True(t, ok)
	assert.Equal(t, []string{"x", "n.list", "x > 1"}, []string{variable, list, predicate})
	for _, inner := range []string{"x n.list WHERE x", "x IN n.list", " IN n.list WHERE x", "x IN  WHERE x", "x IN n.list WHERE "} {
		_, _, _, ok := parseQuantifierArguments(inner)
		assert.False(t, ok, inner)
	}
}
