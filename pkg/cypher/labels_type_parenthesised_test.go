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
