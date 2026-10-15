package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReleasePatternSemanticsRegressions(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
		errorText   string
	}{
		{name: "COUNT extra block", query: "CYPHER 25 RETURN COUNT { { RETURN 1 AS x } } AS c", want: [][]interface{}{{int64(1)}}},
		{name: "CALL extra block", query: "CYPHER 25 CALL () { { RETURN 1 AS x } } RETURN x", want: [][]interface{}{{int64(1)}}},
		{name: "inline dynamic predicate empty rows", query: "CYPHER 25 WITH 1 AS l MATCH (x:Missing WHERE x:$(l)) RETURN count(x)", want: [][]interface{}{{int64(0)}}},
		{name: "dynamic any predicate empty rows", query: "CYPHER 25 WITH 1 AS l MATCH (x:Missing) WHERE x:$any(l) RETURN count(x)", want: [][]interface{}{{int64(0)}}},
		{name: "dynamic predicate reached", query: "CYPHER 25 WITH 1 AS l MATCH (x:Q) WHERE x:$(l) RETURN count(x)", errorText: "TypeError"},
		{name: "dynamic pattern compile check", query: "CYPHER 25 WITH 1 AS l MATCH (x:Missing:$(l)) RETURN count(x)", errorText: "SyntaxError"},
		{name: "relationship dynamic predicate", query: "CYPHER 25 WITH 1 AS l MATCH (x:Missing)-[r WHERE r:$(l)]->() RETURN count(x)", errorText: "TypeError"},
		{name: "dynamic predicate empty rows", query: "CYPHER 25 WITH 1 AS l MATCH (x:Missing) WHERE x:$(l) RETURN count(x)", want: [][]interface{}{{int64(0)}}},
		{name: "node as relationship comprehension", query: "MATCH (x:Q {id:1})-[e:R]->(y) RETURN [(w)-[x]->() | w.id]", errorText: "SyntaxError"},
		{name: "valid node comprehension", query: "MATCH (n:Q {id:1}) RETURN [(n)-->(w) | w.id]", want: [][]interface{}{{[]interface{}{int64(2)}}}},
		{name: "shadowing local node comprehension", query: "WITH 1 AS x MATCH (n:Q {id:1}) RETURN [x IN [n] | [(x)-->(w) | w.id]]", want: [][]interface{}{{[]interface{}{[]interface{}{int64(2)}}}}},
		{name: "pattern in keyword map projection", query: "WITH 1 AS e, {a: 1} AS count RETURN count {items: [(e)-->(w) | w.id]}", errorText: "SyntaxError"},
		{name: "relationship as node predicate", query: "MATCH (x:Q {id:1})-[e:R]->(y) WHERE (e)-->() RETURN count(*)", errorText: "SyntaxError"},
		{name: "relationship as node comprehension", query: "MATCH (x:Q {id:1})-[e:R]->(y) RETURN [(e)-->(w) | w.id]", errorText: "SyntaxError"},
		{name: "plain value as node comprehension", query: "WITH 1 AS e RETURN [(e)-->(w) | w.id]", errorText: "SyntaxError"},
		{name: "property as local iterator", query: "MATCH (n:Q {id:1}) RETURN [(n)-->(z) WHERE z.id > 0 | [z.id IN [2] | z]] AS v", errorText: "SyntaxError"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, "CREATE (:Q {id:1})-[:R]->(:Q {id:2})-[:R]->(:Q {id:3})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, tc.query, nil)
			if tc.errorText != "" {
				require.Error(t, err)
				requireStatusCode(t, err, "Neo.ClientError.Statement."+tc.errorText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Rows)
		})
	}
}
