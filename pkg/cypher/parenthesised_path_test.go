package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A parenthesised path without a quantifier is the path, with its WHERE a
// filter, as in Neo4j 5.26 and 2026.09; written next to another element it
// is Neo4j's juxtaposition error (#907).
func TestParenthesisedPaths(t *testing.T) {
	exec, ctx := newPathSelectorExecutor(t)
	l := func(values ...interface{}) []interface{} { return values }
	for query, want := range map[string][][]interface{}{
		"MATCH p = ((a:SP {id: 1})-->(b)) RETURN count(p) AS c":                                        {l(int64(2))},
		"MATCH ((a:SP {id: 1})-->(b)) RETURN count(*) AS c":                                            {l(int64(2))},
		"MATCH ((a:SP {id: 1})-->(b) WHERE b.id > 2) RETURN b.id AS b":                                 {l(int64(3))},
		"MATCH p = ((a:SP {id: 1})-->(b) WHERE b.id > 2) RETURN length(p) AS l":                        {l(int64(1))},
		"MATCH ((a:SP {id: 1})-->(b)), ((c:SP {id: 2})-->(d)) RETURN count(*) AS c":                    {l(int64(2))},
		"MATCH ((a:SP {id: 1})-->(b) WHERE b.id = 2), (c) RETURN count(*) AS c":                        {l(int64(5))},
		"OPTIONAL MATCH ((a:SP {id: 9})-->(b)) RETURN a":                                               {l(nil)},
		"MATCH (((a:SP {id: 1})-->(b))) RETURN count(*) AS c":                                          {l(int64(2))},
		"MATCH ((a:SP {id: 1})-->(b)) WHERE b.id > 2 RETURN count(*) AS c":                             {l(int64(1))},
		"MATCH ((a:SP {id: 1})-->(b:SP|X) WHERE b.id > 2 OR b.id < 0) WHERE b.id < 4 RETURN b.id AS b": {l(int64(3))},
		"MATCH (a:SP {id: 1}) WHERE ((a.id > 0) AND (a.id < 2)) RETURN a.id AS a":                      {l(int64(1))},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	for _, query := range []string{
		"MATCH (a:SP {id: 1}) ((a)-->(b)) RETURN count(*) AS c",
		"MATCH (x:SP {id: 1})((a)-->(b) WHERE b.id = 2)(y) RETURN count(*) AS c",
		"MATCH ((a:SP {id: 1})-->(b) WHERE b.id > 2)-->(c) RETURN count(*) AS c",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "Juxtaposition is currently only supported for quantified path patterns.", query)
	}
	_, err := exec.Execute(ctx, "MATCH ((a)-->(b) WHERE a:A:B|C) RETURN 1", nil)
	require.ErrorContains(t, err, "Mixing label expression symbols")

	require.True(t, mayUseParenthesisedPath("MATCH ( (a)-[r]->(b)) RETURN 1"))
	require.False(t, mayUseParenthesisedPath("MATCH (a) WHERE ((a.x > 1) AND (a.y < 2)) RETURN count((a))"))
	require.False(t, mayUseParenthesisedPath("RETURN 1"))
	rewrite := pathPrefixRewrite{}
	text := "(a)-['x']->(b {k: [1]})"
	require.NoError(t, (&labelExpressionRewriter{query: text}).plainParenthesisedPath(0, len(text), &rewrite))
	text = "((a)-->(b)"
	require.NoError(t, (&labelExpressionRewriter{query: text}).plainParenthesisedPath(0, len(text), &rewrite))
	require.Empty(t, rewrite.pathWheres)
}
