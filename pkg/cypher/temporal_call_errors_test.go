package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestTemporalFunctionCallErrors pins Neo4j's run-time errors of X.truncate,
// the duration.between family and datetime.fromepoch(millis): an argument of
// a type the function doesn't take is ProcedureCallFailed, and the values it
// can't use fail with Neo4j's own codes; null to duration.between is null.
func TestTemporalFunctionCallErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:TruncFields {m: 'x'})", nil)
	require.NoError(t, err)
	for query, code := range map[string]string{
		"RETURN date.truncate('day', 'x') AS v":                                           "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN date.truncate('day', duration('P1D')) AS v":                               "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN localtime.truncate('day', null) AS v":                                     "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN date.truncate(null, date('2020-01-02')) AS v":                             "Neo.ClientError.Procedure.ProcedureCallFailed",
		// A property is never a map: Neo4j 5.26 rejects it as it compiles.
		"MATCH (f:TruncFields) RETURN date.truncate('day', date('2020-01-02'), f.m) AS v": "Neo.ClientError.Statement.SyntaxError",
		"RETURN date.truncate('zz', date('2020-01-02')) AS v":                             "Neo.DatabaseError.Statement.ExecutionFailed",
		"RETURN date.truncate('day', time('03:04:05Z')) AS v":                             "Neo.ClientError.Statement.TypeError",
		"RETURN time.truncate('day', date('2020-01-02')) AS v":                            "Neo.ClientError.Statement.TypeError",
		"RETURN datetime.truncate('day', datetime('2020-01-02T03:04:05Z'), {a: 1}) AS v":  "Neo.ClientError.Statement.ArgumentError",
		"RETURN duration.between('x', date('2020-01-02')) AS v":                           "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN duration.inDays(date('2020-01-02'), 1) AS v":                              "Neo.ClientError.Procedure.ProcedureCallFailed",
		"WITH 'x' AS s RETURN duration.inSeconds(s, s) AS v":                              "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN datetime.fromepoch(1.5, 2) AS v":                                          "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN datetime.fromepoch(null, 1) AS v":                                         "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN datetime.fromepoch(-3, -3) AS v":                                          "Neo.ClientError.Statement.ArgumentError",
		"RETURN datetime.fromepoch(1, 1000000000) AS v":                                   "Neo.ClientError.Statement.ArgumentError",
		"RETURN datetime.fromepochmillis(1.5) AS v":                                       "Neo.ClientError.Procedure.ProcedureCallFailed",
		"RETURN datetime.fromepochmillis(null) AS v":                                      "Neo.ClientError.Procedure.ProcedureCallFailed",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, statusText(err), code, query)
	}
	for query, want := range map[string]interface{}{
		"RETURN duration.between(null, date('2020-01-02')) AS v":                                    nil,
		"RETURN duration.inDays(date('2020-01-02'), null) AS v":                                     nil,
		"RETURN datetime.fromepoch(1, 5) = datetime('1970-01-01T00:00:01.000000005Z') AS v":         true,
		"RETURN datetime.fromepochmillis(1000) = datetime('1970-01-01T00:00:01Z') AS v":             true,
		"RETURN date.truncate('month', datetime('2020-05-17T03:04:05Z')) = date('2020-05-01') AS v": true,
		"RETURN datetime.truncate('day', datetime('2020-01-02T03:04:05Z'), {day: 5}).day AS v":      int64(5),
		"WITH date('2020-01-02') AS d RETURN duration.inDays(d, d).days AS v":                       int64(0),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}

	// The context evaluator reports the same error through the statement's
	// failure slot.
	failureCtx := withExpressionFailureSlot(context.Background())
	require.Nil(t, exec.evaluateExpressionWithContext(failureCtx, "date.truncate('day', 'x')", nil, nil))
	require.Contains(t, statusText(getExpressionFailure(failureCtx)), "Neo.ClientError.Procedure.ProcedureCallFailed")

	require.Equal(t, "NO_VALUE", neo4jProvidedValue(nil))
	require.Equal(t, `String("x")`, neo4jProvidedValue("x"))
	require.Equal(t, "Boolean('true')", neo4jProvidedValue(true))
	require.Equal(t, "Double(1.5)", neo4jProvidedValue(1.5))
	require.Equal(t, "Long(7)", neo4jProvidedValue(int64(7)))
	require.Equal(t, "P1D", neo4jProvidedValue(stringerValue("P1D")))
	require.True(t, isTemporalInstant(time.Now()))
	require.False(t, isTemporalInstant("2020-01-02"))
}

type stringerValue string

func (value stringerValue) String() string { return string(value) }

// TestSplitWithDelimiterList: split(text, [delimiters]) cuts the text at any
// of the delimiters, as Neo4j does.
func TestSplitWithDelimiterList(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN split('a,b;c', [',', ';']) AS v": []interface{}{"a", "b", "c"},
		"RETURN split('Ab c', ['a', 'b']) AS v":  []interface{}{"A", " c"},
		"RETURN split('ab', ['a', 'b']) AS v":    []interface{}{"", "", ""},
		"RETURN split('abc', ['']) AS v":         []interface{}{"a", "b", "c"},
		"RETURN split('aXYb', ['XY', 'X']) AS v": []interface{}{"a", "b"},
		"RETURN split('aXYb', ['X', 'XY']) AS v": []interface{}{"a", "Yb"},
		"RETURN split('', [',']) AS v":           []interface{}{""},
		"RETURN split('abc', []) AS v":           []interface{}{"abc"},
		"RETURN split('a,,b', [',']) AS v":       []interface{}{"a", "", "b"},
		"RETURN split('abc', ['b', null]) AS v":  nil,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	_, err := exec.Execute(ctx, "WITH 1 AS one RETURN split('a1b', ['1', one]) AS v", nil)
	require.Error(t, err)
}

// TestTemporalCallDefensiveReturns: calls the statement-level checks already
// reject (argument counts) or that have no value (an unusable time zone)
// are null, not errors.
func TestTemporalCallDefensiveReturns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	eval := func(expression string) interface{} {
		value, _ := parseLiteralValueFromComputedRow(expression)
		return value
	}
	for _, expression := range []string{
		"duration.between(1)",
		"date.truncate()",
		"datetime.fromepoch(1)",
		"date.realtime(1)",
		"date.realtime('Not/AZone')",
	} {
		value, handled, err := exec.evaluateTemporalConstructor(context.Background(), eval, expression)
		require.True(t, handled, expression)
		require.NoError(t, err, expression)
		require.Nil(t, value, expression)
	}
	instant := CypherDateTime{Time: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)}
	for _, fields := range []map[string]interface{}{{"timezone": 1}, {"timezone": "Not/AZone"}} {
		value, handled, err := truncateTemporalValue("datetime", "day", instant, fields)
		require.True(t, handled)
		require.NoError(t, err)
		require.Nil(t, value)
	}
	value, handled, err := truncateTemporalValue("nokind", "day", instant, nil)
	require.False(t, handled)
	require.NoError(t, err)
	require.Nil(t, value)
}

// TestFunctionArgumentsWithMapLiterals: a map literal with several keys is
// one function argument (splitFunctionArgs), so a call with one isn't read
// as having too many arguments and null.
func TestFunctionArgumentsWithMapLiterals(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	require.Equal(t, []string{"'day'", "d", "{year: 2020, month: 2}"}, exec.splitFunctionArgs("'day', d, {year: 2020, month: 2}"))
	require.Equal(t, []string{"`a,b`", "[1, 2]", "f(x, y)"}, exec.splitFunctionArgs("`a,b`, [1, 2], f(x, y)"))
	require.Equal(t, []string{"a", "", "b"}, exec.splitFunctionArgs("a,,b"))
	require.Nil(t, exec.splitFunctionArgs("  "))

	ctx := context.Background()
	_, err := exec.Execute(ctx, "RETURN date.truncate('Ab c', 1, {year: 2020, month: 2}) AS v", nil)
	require.Contains(t, statusText(err), "Neo.ClientError.Procedure.ProcedureCallFailed")
	result, err := exec.Execute(ctx, "RETURN date.truncate('day', date('2020-01-02'), {year: 2020, day: 3}) IS NOT NULL AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}
