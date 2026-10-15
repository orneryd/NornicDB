package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// graphReadingExpression is the one reader of graph-reading expressions: it
// finds them at any depth (inside function calls, lists and maps), tells
// their kinds apart, and skips strings and comments.
func TestGraphReadingExpression(t *testing.T) {
	for expression, want := range map[string]string{
		"RETURN COUNT { (n) } AS c":                          "COUNT",
		"RETURN toString(COUNT { (n) }) AS c":                "COUNT",
		"RETURN [x IN [1] | EXISTS { MATCH (n) }] AS e":      "EXISTS",
		"RETURN {c: size(COLLECT { MATCH (n) RETURN n })}":   "COLLECT",
		"RETURN exists((a)-->(b)) AS e":                      graphReadingPattern,
		"RETURN shortestPath((a)-[*]->(b)) AS p":             graphReadingPattern,
		"RETURN size([(a)-->(b) | b]) AS c":                  graphReadingPatternComprehension,
		"RETURN [(n) | n.x] AS xs":                           graphReadingPatternComprehension,
		"WITH 1 AS x WHERE ()-->() RETURN x":                 graphReadingPatternPredicate,
		"WITH 1 AS x WHERE (a)<-[:R]-() RETURN x":            graphReadingPatternPredicate,
		"RETURN count(n) AS c":                               "",
		"RETURN 'shortestPath' AS hint":                      "",
		"RETURN 'allShortestPaths' AS hint":                  "",
		"RETURN [(1)] AS xs":                                 "",
		"RETURN [((1)), (2)] AS xs":                          "",
		"CREATE (a)-[:R]->(b)":                               "",
		"WITH 1 AS x WHERE x > 0 CREATE (a)-[:R]->(b)":       "",
		"RETURN 'COUNT { (n) }' AS text // EXISTS { (n) }":   "",
		"RETURN [x IN [1, 2] | x] AS xs":                     "",
		"RETURN list[(1)] AS x":                              "",
		"RETURN (1) - 1 AS x, (2) <-1 AS y":                  "",
		"RETURN exists(n.prop) AS e":                         "",
		"RETURN [(a)-->(b) | b] AS xs, COUNT { (n) } AS c":   "COUNT",
		"WITH 1 AS x WHERE ()-->() RETURN [(n) | 1] AS xs":   graphReadingPatternComprehension,
		"RETURN {count: 1, exists: true, collect: [1]} AS m": "",
	} {
		require.Equal(t, want, graphReadingExpression(expression), expression)
		require.Equal(t, want != "", expressionReadsGraphText(expression), expression)
	}
}
