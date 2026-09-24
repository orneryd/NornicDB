package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// labels() / type() on a non-node / non-relationship is an error whether or not
// the call is wrapped in parentheses (#516).
func TestLabelsTypeArgumentCheckIgnoresParentheses(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for _, q := range []string{
		"WITH 'x' AS s RETURN labels(s) AS l",
		"WITH 'x' AS s RETURN (labels(s)) AS l",
		"WITH 1 AS x RETURN (labels(x)) AS l",
		"WITH 1 AS x RETURN ((labels(x))) AS l",
		"WITH 'x' AS s RETURN (type(s)) AS t",
		"WITH 'x' AS s WITH (labels(s)) AS l RETURN l",
		"RETURN (labels('x')) AS l",
	} {
		_, err := exec.Execute(ctx, q, nil)
		assert.Error(t, err, q)
	}
	_, err := exec.Execute(ctx, "CREATE (:A)-[:R]->(:B)", nil)
	require.NoError(t, err)
	res, err := exec.Execute(ctx, "MATCH (a:A)-[r:R]->() RETURN (labels(a)) AS l, (type(r)) AS t", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{[]interface{}{"A"}, "R"}}, res.Rows)
}

// A literal argument is a compile-time type error with Neo4j's message on every
// route, including a standalone RETURN (#516); labels(null) stays null.
func TestLabelsTypeLiteralArgumentIsTypeMismatch(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for q, want := range map[string]string{
		"RETURN labels('x') AS l":               "expected Node but was String",
		"RETURN (labels('x')) AS l":             "expected Node but was String",
		"RETURN ((labels(1))) AS l":             "expected Node but was Integer",
		"RETURN (type('x')) AS t":               "expected Relationship but was String",
		"RETURN type([1]) AS t":                 "expected Relationship but was List",
		"RETURN labels({a: 1}) AS l":            "expected Node but was Map",
		"WITH 1 AS a RETURN (labels('x')) AS l": "expected Node but was String",
		"MATCH (n) RETURN labels(true) AS l":    "expected Node but was Boolean",
	} {
		_, err := exec.Execute(ctx, q, nil)
		if assert.Error(t, err, q) {
			assert.Contains(t, err.Error(), "Type mismatch: "+want, q)
		}
	}
	res, err := exec.Execute(ctx, "RETURN labels(null) AS l", nil)
	require.NoError(t, err)
	assert.Nil(t, res.Rows[0][0])
	_, err = exec.Execute(ctx, "CREATE (:A)", nil)
	require.NoError(t, err)
	res, err = exec.Execute(ctx, "MATCH (a:A) RETURN (labels(a)) AS l", nil)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{"A"}, res.Rows[0][0])
}
