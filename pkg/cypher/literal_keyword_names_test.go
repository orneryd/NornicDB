package cypher

import (
	"context"
	"math"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestLiteralKeywordsAreNeverVariables: true, false and null are literals in
// every expression, even where a variable of that name was declared, and
// f(all) is f's ALL modifier without an argument, as in Neo4j 5.26.30
// (#907). Expected results are Neo4j's.
func TestLiteralKeywordsAreNeverVariables(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R]->(:Q {id: 2})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query       string
		rows        [][]interface{}
		syntaxError bool
	}{
		{"WITH 1 AS all RETURN count(all) AS v", nil, true},
		{"MATCH (a:Q {id: 1})-[all:R]->(b) RETURN type(all) AS v", nil, true},
		{"WITH 1 AS ALL RETURN count(ALL) AS v", nil, true},
		{"MATCH (a:Q {id: 1})-[ALL:R]->(b) RETURN type(ALL) AS v", nil, true},
		{"WITH 1 AS false RETURN false AS v", [][]interface{}{{false}}, false},
		{"WITH 1 AS false WITH false WHERE false = 1 RETURN false AS v", nil, true},
		{"WITH 1 AS false, 2 AS y RETURN y, false ORDER BY false", [][]interface{}{{int64(2), false}}, false},
		{"WITH 1 AS false RETURN [y IN [1] | false] AS v", [][]interface{}{{[]interface{}{false}}}, false},
		{"WITH 1 AS FALSE RETURN FALSE AS v", [][]interface{}{{false}}, false},
		{"WITH 1 AS FALSE WITH FALSE WHERE FALSE = 1 RETURN FALSE AS v", nil, true},
		{"WITH 1 AS FALSE, 2 AS y RETURN y, FALSE ORDER BY FALSE", [][]interface{}{{int64(2), false}}, false},
		{"WITH 1 AS FALSE RETURN [y IN [1] | FALSE] AS v", [][]interface{}{{[]interface{}{false}}}, false},
		{"MATCH (null:Q {id: 1}) RETURN null.id AS v", [][]interface{}{{nil}}, false},
		{"WITH 1 AS null RETURN null AS v", [][]interface{}{{nil}}, false},
		{"UNWIND [1] AS null RETURN null + 1 AS v", [][]interface{}{{nil}}, false},
		{"WITH 1 AS null WITH null WHERE null = 1 RETURN null AS v", nil, true},
		{"WITH 1 AS null, 2 AS y RETURN y, null ORDER BY null", [][]interface{}{{int64(2), nil}}, false},
		{"WITH [1] AS null RETURN null[0] AS v", [][]interface{}{{nil}}, false},
		{"WITH {a: 1} AS null RETURN null.a AS v", [][]interface{}{{nil}}, false},
		{"WITH 1 AS null RETURN count(null) AS v", [][]interface{}{{int64(0)}}, false},
		{"WITH 1 AS null RETURN [y IN [1] | null] AS v", [][]interface{}{{[]interface{}{nil}}}, false},
		{"MATCH (a:Q {id: 1})-[null:R]->(b) RETURN type(null) AS v", [][]interface{}{{nil}}, false},
		{"MATCH (NULL:Q {id: 1}) RETURN NULL.id AS v", [][]interface{}{{nil}}, false},
		{"WITH 1 AS NULL RETURN NULL AS v", [][]interface{}{{nil}}, false},
		{"UNWIND [1] AS NULL RETURN NULL + 1 AS v", [][]interface{}{{nil}}, false},
		{"WITH 1 AS NULL WITH NULL WHERE NULL = 1 RETURN NULL AS v", nil, true},
		{"WITH 1 AS NULL, 2 AS y RETURN y, NULL ORDER BY NULL", [][]interface{}{{int64(2), nil}}, false},
		{"WITH [1] AS NULL RETURN NULL[0] AS v", [][]interface{}{{nil}}, false},
		{"WITH {a: 1} AS NULL RETURN NULL.a AS v", [][]interface{}{{nil}}, false},
		{"WITH 1 AS NULL RETURN count(NULL) AS v", [][]interface{}{{int64(0)}}, false},
		{"WITH 1 AS NULL RETURN [y IN [1] | NULL] AS v", [][]interface{}{{[]interface{}{nil}}}, false},
		{"MATCH (a:Q {id: 1})-[NULL:R]->(b) RETURN type(NULL) AS v", [][]interface{}{{nil}}, false},
		{"WITH 1 AS true RETURN true AS v", [][]interface{}{{true}}, false},
		{"WITH 1 AS true WITH true WHERE true = 1 RETURN true AS v", nil, true},
		{"WITH 1 AS true, 2 AS y RETURN y, true ORDER BY true", [][]interface{}{{int64(2), true}}, false},
		{"WITH 1 AS true RETURN [y IN [1] | true] AS v", [][]interface{}{{[]interface{}{true}}}, false},
		{"WITH 1 AS TRUE RETURN TRUE AS v", [][]interface{}{{true}}, false},
		{"WITH 1 AS TRUE WITH TRUE WHERE TRUE = 1 RETURN TRUE AS v", nil, true},
		{"WITH 1 AS TRUE, 2 AS y RETURN y, TRUE ORDER BY TRUE", [][]interface{}{{int64(2), true}}, false},
		{"WITH 1 AS TRUE RETURN [y IN [1] | TRUE] AS v", [][]interface{}{{[]interface{}{true}}}, false},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		if tc.syntaxError {
			require.Error(t, err, tc.query)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, tc.query)
			continue
		}
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}

	// A backtick-quoted `null` is a variable, not the literal.
	result, err := exec.Execute(ctx, "WITH 1 AS `null` RETURN `null` AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// TestNaNAndInfinityLiterals: NaN and Infinity are float literals in any
// case, and stay literals where a variable of that name was declared, as in
// Neo4j 5.26.30 (#907).
func TestNaNAndInfinityLiterals(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R]->(:Q {id: 2}), (:Q {id: 3})", nil)
	require.NoError(t, err)
	value := func(query string) interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows[0][0]
	}
	for _, query := range []string{"RETURN NaN AS v", "RETURN nan AS v", "RETURN NAN AS v", "WITH 1 AS NaN RETURN NaN AS v", "RETURN -NaN AS v"} {
		require.True(t, math.IsNaN(value(query).(float64)), query)
	}
	for _, query := range []string{"RETURN Infinity AS v", "RETURN infinity AS v", "WITH 1 AS Infinity RETURN Infinity AS v"} {
		require.Equal(t, math.Inf(1), value(query), query)
	}
	require.Equal(t, math.Inf(-1), value("RETURN -Infinity AS v"))
	require.Equal(t, false, value("RETURN NaN = NaN AS v"))
	require.Equal(t, true, value("RETURN Infinity > 1e308 AS v"))
	require.Equal(t, "Infinity", value("RETURN toString(Infinity) AS v"))
	require.Equal(t, "NaN", value("RETURN toString(NaN) AS v"))
	require.Equal(t, int64(3), value("MATCH (NaN) RETURN count(*) AS v"))
	list := value("RETURN [NaN, Infinity] AS v").([]interface{})
	require.True(t, math.IsNaN(list[0].(float64)))
	require.Equal(t, math.Inf(1), list[1])
}
