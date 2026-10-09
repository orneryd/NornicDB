package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A pattern expression tests whether its pattern exists, and is valid
// wherever a boolean is expected: FILTER, exists(), NOT / AND / OR, a CASE
// WHEN condition, a list comprehension's WHERE. As a value of its own it is a
// SyntaxError. Results are Neo4j 5.26.30's and 2026.09's (#907).
func TestPatternPredicatesInBooleanPositions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_positions"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:SQ {v: 3})-[:R]->(:SQ {v: 1})", nil)
	require.NoError(t, err)
	for query, rows := range map[string][][]interface{}{
		"CYPHER 25 MATCH (n:SQ) FILTER (n)-[:R]->() RETURN count(*) AS c":                                 {{int64(1)}},
		"CYPHER 25 MATCH (n:SQ) FILTER NOT (n)-[:R]->() RETURN count(*) AS c":                             {{int64(1)}},
		"CYPHER 25 MATCH (n:SQ) FILTER n:SQ AND (n)<-[:R]-() RETURN n.v AS v":                             {{int64(1)}},
		"MATCH (n:SQ) WITH n, exists((n)-[:R]->()) AS e RETURN n.v AS v, e ORDER BY v":                    {{int64(1), false}, {int64(3), true}},
		"MATCH (n:SQ) RETURN n.v AS v, exists((n)-[:R]->()) AS e ORDER BY v":                              {{int64(1), false}, {int64(3), true}},
		"MATCH (n:SQ) RETURN n.v AS v, NOT (n)-[:R]->() AS e ORDER BY v":                                  {{int64(1), true}, {int64(3), false}},
		"MATCH (n:SQ) RETURN n.v AS v, (n)-[:R]->() AND true AS e ORDER BY v":                             {{int64(1), false}, {int64(3), true}},
		"MATCH (n:SQ) RETURN n.v AS v, CASE WHEN (n)-[:R]->() THEN 'out' ELSE 'none' END AS e ORDER BY v": {{int64(1), "none"}, {int64(3), "out"}},
		"MATCH (n:SQ) RETURN n.v AS v, [x IN [1] WHERE (n)-[:R]->() | x] AS e ORDER BY v":                 {{int64(1), []interface{}{}}, {int64(3), []interface{}{int64(1)}}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, rows, result.Rows, query)
	}
	for _, query := range []string{
		"MATCH (n:SQ) RETURN n.v AS v, (n)-[:R]->() AS e",
		"MATCH (n:SQ) RETURN size((n)-[:R]->()) AS e",
		"MATCH (n:SQ) RETURN ((n)-[:R]->()) AS e",
		"CYPHER 25 MATCH (n:SQ) FILTER (n)-[:R]->(m) RETURN count(*) AS c",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
	// A runtime operand that isn't a boolean is a TypeError, as in Neo4j
	// ("Don't know how to treat that as a predicate").
	for _, query := range []string{
		"MATCH (n:SQ) RETURN n.v AS v, (n)-[:R]->() OR n.v AS e ORDER BY v",
		"MATCH (n:SQ) RETURN n.v AS v, NOT (n)-[:R]->() AND n.v AS e ORDER BY v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", code, query)
	}
	// An operand's evaluation error is the statement's.
	for _, query := range []string{
		"MATCH (n:SQ) RETURN (n)-[:R]->() AND 1 / 0 = 1 AS e",
		"MATCH (n:SQ) RETURN [x IN [1] WHERE (n)-[:R]->() AND x / 0 = 1 | x] AS e",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "/ by zero", query)
	}
}

func TestPatternInBooleanPosition(t *testing.T) {
	positioned := func(expression string) bool {
		start := len(expression) - len(trimLeadingToPattern(expression))
		end, ok := relationshipChainEnd(expression, start, len(expression))
		require.True(t, ok, expression)
		return patternInBooleanPosition(expression, start, end)
	}
	for expression, want := range map[string]bool{
		"(n)-->()":             false,
		"(n)-->() AND x":       true,
		"x OR (n)-->()":        true,
		"NOT ((n)-->())":       true,
		"exists((n)-->())":     true,
		"size((n)-->())":       false,
		"x = (n)-->()":         false,
		"WHEN (n)-->() THEN 1": true,
		"WHERE (n)-->() | x":   true,
		"NOT (n)-->() = true":  false,
	} {
		require.Equal(t, want, positioned(expression), expression)
	}
	require.Equal(t, "MATCH (n) RETURN x", maskPredicatePatterns("MATCH (n) RETURN x"))
	require.Equal(t, "[(n)-->() | 1]", maskPredicatePatterns("[(n)-->() | 1]"))
	require.Equal(t, "'(n)-->()' AND x", maskPredicatePatterns("'(n)-->()' AND x"))
}

// trimLeadingToPattern is expression from its first pattern node on.
func trimLeadingToPattern(expression string) string {
	for index := range expression {
		if expression[index] == '(' {
			if _, ok := relationshipChainEnd(expression, index, len(expression)); ok {
				return expression[index:]
			}
		}
	}
	return ""
}
