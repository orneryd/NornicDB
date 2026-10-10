package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Parameters of a type a Cypher 25 function's signature excludes, and
// string.regexReplace replacements Java rejects, with Neo4j 2026.09's codes.
func TestCypher25FunctionArgumentErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "c25_function_errors"))
	ctx := context.Background()
	for _, tc := range []struct {
		query  string
		params map[string]interface{}
		code   string
	}{
		{"RETURN coll.flatten($l) AS v", map[string]interface{}{"l": "x"}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN coll.flatten([1], $d) AS v", map[string]interface{}{"d": "x"}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN coll.insert($l, 0, 1) AS v", map[string]interface{}{"l": "x"}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN coll.insert([1], $i, 1) AS v", map[string]interface{}{"i": "x"}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN string.join($l, ',') AS v", map[string]interface{}{"l": "x"}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN string.join(['a'], $s) AS v", map[string]interface{}{"s": int64(1)}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN string.indexOf('a', $s) AS v", map[string]interface{}{"s": int64(1)}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN cardinality($l) AS v", map[string]interface{}{"l": "x"}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN replace('a', 'a', 'b', $l) AS v", map[string]interface{}{"l": "x"}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN coll.sort($l) AS v", map[string]interface{}{"l": int64(1)}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN date('2020', $p) AS v", map[string]interface{}{"p": int64(1)}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN format(date('2020-01-01'), $p) AS v", map[string]interface{}{"p": int64(1)}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN format($d, 'yyyy') AS v", map[string]interface{}{"d": int64(1)}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN allReduce(a = 0, x IN [1] | a + x, $p) AS v", map[string]interface{}{"p": int64(2)}, "Neo.ClientError.Statement.SyntaxError"},
		{"RETURN string.regexReplace('a1', '(\\\\d)', '<$x>') AS v", nil, "Neo.DatabaseError.Statement.ExecutionFailed"},
		{"RETURN string.regexReplace('a1', '(\\\\d)', 'x\\\\') AS v", nil, "Neo.DatabaseError.Statement.ExecutionFailed"},
		{"RETURN string.regexReplace('a1', '(\\\\d)', '$') AS v", nil, "Neo.DatabaseError.Statement.ExecutionFailed"},
		{"RETURN string.regexReplace('a1', '(\\\\d)', '<${x') AS v", nil, "Neo.DatabaseError.Statement.ExecutionFailed"},
		{"RETURN string.regexReplace('a1', '(\\\\d)', '<$9>') AS v", nil, "Neo.DatabaseError.Statement.ExecutionFailed"},
		{"RETURN string.regexReplace('a1', '(?<d>\\\\d)', '<${e}>') AS v", nil, "Neo.DatabaseError.Statement.ExecutionFailed"},
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+tc.query, tc.params)
		require.Error(t, err, tc.query)
		requireStatusCode(t, err, tc.code)
	}
	// A parameter's type is checked when the statement compiles, as Neo4j
	// 2026.09 does, in Cypher 5 and 25; a Float parameter, a list's elements
	// and a row value only when the function runs (TypeError).
	for _, tc := range []struct {
		query   string
		params  map[string]interface{}
		code    string
		message string
	}{
		{"CYPHER 25 RETURN coll.sort($l) AS v", map[string]interface{}{"l": int64(1)}, "Neo.ClientError.Statement.SyntaxError", "Type mismatch for parameter 'l': expected List<T> but was Integer"},
		{"CYPHER 25 RETURN coll.sort($l) AS v", map[string]interface{}{"l": true}, "Neo.ClientError.Statement.SyntaxError", "Type mismatch for parameter 'l': expected List<T> but was Boolean"},
		{"CYPHER 25 RETURN coll.sort($l) AS v", map[string]interface{}{"l": map[string]interface{}{"a": int64(1)}}, "Neo.ClientError.Statement.SyntaxError", "Type mismatch for parameter 'l': expected List<T> but was Map, Node or Relationship"},
		{"CYPHER 25 RETURN coll.insert([1], $i, 1) AS v", map[string]interface{}{"i": "x"}, "Neo.ClientError.Statement.SyntaxError", "Type mismatch for parameter 'i': expected Integer but was String"},
		{"RETURN toUpper( $s ) AS v", map[string]interface{}{"s": int64(1)}, "Neo.ClientError.Statement.SyntaxError", "Type mismatch for parameter 's': expected String but was Integer"},
		{"CYPHER 25 RETURN coll.insert([1], $i, 1) AS v", map[string]interface{}{"i": 1.5}, "Neo.ClientError.Statement.TypeError", "coll.insert"},
		{"CYPHER 25 UNWIND [1, [2]] AS x RETURN coll.sort(x) AS v", nil, "Neo.ClientError.Statement.TypeError", "coll.sort"},
	} {
		_, err := exec.Execute(ctx, tc.query, tc.params)
		require.ErrorContains(t, err, tc.message, tc.query)
		requireStatusCode(t, err, tc.code)
	}
	result, err := exec.Execute(ctx, "CYPHER 25 RETURN coll.sort($l) AS v, toUpper($s) AS u", map[string]interface{}{"l": []interface{}{int64(2), int64(1)}, "s": "a"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{int64(1), int64(2)}, "A"}}, result.Rows)
	require.False(t, parameterMayBeFunctionArgument("RETURN $value AS value"))
	require.False(t, parameterMayBeFunctionArgument("RETURN 1"))
	require.True(t, parameterMayBeFunctionArgument("RETURN f(1, $p)"))
	require.True(t, parameterMayBeFunctionArgument("RETURN $a + f($b)"))
	require.False(t, parameterMayBeFunctionArgument("RETURN $a + $b"))
	// Only a bare parameter is typed here: $m.k is a property access, and a
	// parameter the statement doesn't pass has no type.
	require.NoError(t, validateStaticFunctionParameters("RETURN toUpper($m.k), toUpper($missing), toUpper($s)", map[string]interface{}{"m": map[string]interface{}{"k": "a"}, "s": "b"}, false))

	value := func(query string, params map[string]interface{}) interface{} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, params)
		require.NoError(t, err, query)
		return result.Rows[0][0]
	}
	for query, want := range map[string]interface{}{
		"RETURN string.regexReplace('a1', '(?<d>\\\\d)', '<${d}>') AS v":    "a<1>",
		"RETURN string.regexReplace('a12', '(\\\\d)(\\\\d)', '<$12>') AS v": "a<12>",
		"RETURN string.regexReplace('a1', 'z', '$9') AS v":                  "a1",
		"RETURN string.regexReplace('a1', '(\\\\d)', '\\\\$1') AS v":        "a$1",
		"RETURN string.regexReplace('a1', '(\\\\d)', '$0$1') AS v":          "a11",
	} {
		require.Equal(t, want, value(query, nil), query)
	}
	require.Nil(t, value("RETURN allReduce(a = 0, x IN [1] | a + x, $p) AS v", map[string]interface{}{"p": nil}))
	_, err = exec.Execute(ctx, "CYPHER 25 RETURN string.regexReplace('a1', '(?<d>\\\\d)', '<${e}>') AS v", nil)
	require.ErrorContains(t, err, "No group with name {e}")
}

// The guards the compile-time checks leave to the evaluators: argument
// counts, argument errors and run-time types, met when a function is called
// from another route.
func TestCypher25FunctionGuards(t *testing.T) {
	failing := errors.New("argument failed")
	call := func(name string, values ...interface{}) (interface{}, error) {
		args := make([]string, len(values))
		for index := range values {
			args[index] = string(rune('a' + index))
		}
		value, found, err := cypherfn.EvaluateFunction(name, args, cypherfn.Context{Eval: func(expression string) (interface{}, error) {
			argument := values[expression[0]-'a']
			if argument == failing {
				return nil, failing
			}
			return argument, nil
		}})
		require.True(t, found, name)
		return value, err
	}
	node := &storage.Node{Properties: map[string]interface{}{"s": "a"}}
	for name, arguments := range map[string][]interface{}{
		"coll.distinct": {}, "coll.flatten": {}, "coll.insert": {[]interface{}{}}, "coll.sort": {}, "string.join": {},
		"string.indexOf": {"a"}, "cardinality": {}, "property_exists": {node}, "format": {},
	} {
		_, err := call(name, arguments...)
		require.Error(t, err, name)
	}
	for name, arguments := range map[string][]interface{}{
		"coll.distinct": {failing}, "coll.flatten": {failing}, "string.indexOf": {failing, "a"}, "property_exists": {failing, "s"},
		"format": {failing}, "coll.max": {failing},
	} {
		_, err := call(name, arguments...)
		require.ErrorIs(t, err, failing, name)
	}
	for name, arguments := range map[string][]interface{}{
		"coll.flatten": {"x"}, "coll.insert": {"x", int64(0), int64(1)}, "string.join": {"x", ","},
		"cardinality": {"x"}, "property_exists": {map[string]interface{}{}, "s"}, "coll.indexOf": {[]interface{}{1}, int64(1), int64(2)},
	} {
		_, err := call(name, arguments...)
		require.Error(t, err, name)
	}
	_, err := call("coll.flatten", []interface{}{int64(1)}, "x")
	require.Error(t, err)
	_, err = call("string.join", []interface{}{"a"}, int64(1))
	require.Error(t, err)
	_, err = call("property_exists", node, int64(1))
	require.Error(t, err)
	exists, err := call("property_exists", node, nil)
	require.NoError(t, err)
	require.Nil(t, exists)
	_, err = call("coll.insert", []interface{}{}, "x", int64(1))
	require.Error(t, err)
	_, err = call("replace", "a", "a", "b", "x")
	require.Error(t, err)
	_, err = call("format", date(t, "2020-01-01"), "yyyy", int64(1))
	require.Error(t, err)
	printed, err := call("format", time.Date(2020, 1, 2, 3, 4, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, "2020-01-02T03:04:00Z", printed)
	printed, err = call("format", time.Date(2020, 1, 2, 3, 4, 0, 0, time.UTC), "yyyy z")
	require.NoError(t, err)
	require.Equal(t, "2020 Z", printed)
	printed, err = call("format", CypherDateTime{Time: time.Date(2020, 1, 2, 3, 4, 0, 0, time.FixedZone("", 3600)), ZoneID: "Nowhere/Zone"}, "z zzzz")
	require.NoError(t, err)
	require.Equal(t, "GMT+01:00 GMT+01:00", printed, "a zone without English names prints its offset")
	printed, err = call("format", durationFromGroups(0, 0, 1, 500_000_000), "SSSSSSSSSSS")
	require.NoError(t, err)
	require.Equal(t, "50000000000", printed, "Neo4j fails past nine digits (an internal error); NornicDB pads")
	_, err = constructTemporalWithPattern("date", "2020", int64(1))
	require.Error(t, err)
}

func date(t *testing.T, text string) CypherDate {
	value, ok := parseTemporalText("date", text)
	require.True(t, ok)
	return value.(CypherDate)
}

// The reduce forms' parser and fold, the zone helpers and the call scanners,
// on input the statements in other tests don't reach.
func TestReduceFormAndPatternHelpers(t *testing.T) {
	for _, arguments := range []string{"a = 0", "a, x IN l | x", "a = 0, 1 IN l | x", "a = 0, x l | x", "a = 0, x IN l x",
		"a = , x IN l | x", "a = 0, x IN l | ", "`` = 0, x IN l | x"} {
		_, ok := parseReduceForm("reduce", arguments)
		require.False(t, ok, arguments)
	}
	_, ok := parseReduceForm("allReduce", "a = 0, x IN l | x, ")
	require.False(t, ok)
	form, ok := parseReduceForm("allReduce", "a = 0, x IN l | a + x, a < 3")
	require.True(t, ok)
	failing := errors.New("failed")
	identity := func(accumulator, item interface{}) (interface{}, error) { return item, nil }
	fail := func(accumulator, item interface{}) (interface{}, error) { return nil, failing }
	_, err := runReduceForm(form, 0, []interface{}{1}, fail, identity)
	require.ErrorIs(t, err, failing)
	_, err = runReduceForm(form, 0, []interface{}{1}, identity, fail)
	require.ErrorIs(t, err, failing)
	_, err = runReduceForm(form, 0, []interface{}{1}, identity, identity)
	require.Error(t, err, "a predicate that isn't a boolean")

	for text, want := range map[string]string{"UTC+01:00": "UTC+01:00", "GMT-05:30:15x": "GMT-05:30:15", "UT+00:00": "UT"} {
		zoneID, _, length, ok := prefixedOffsetZone(text)
		require.True(t, ok, text)
		require.Equal(t, want, zoneID)
		require.Equal(t, len(want)+map[bool]int{true: 6, false: 0}[want == "UT"], length, text)
	}
	for _, text := range []string{"UTC+1", "UTC+19:00", "GMT+0x:00", "UTC"} {
		_, _, _, ok := prefixedOffsetZone(text)
		require.False(t, ok, text)
	}
	location, ok := loadTemporalLocation("GMT-05:30")
	require.True(t, ok)
	_, offset := time.Date(2020, 1, 1, 0, 0, 0, 0, location).Zone()
	require.Equal(t, -19_800, offset)

	require.Empty(t, quantifiedExpressionBindings("reduce + 1"))
	require.Empty(t, quantifiedExpressionBindings("reduce(s = 0"))
	require.NoError(t, checkReduceForm("allReduce", "allReduce(a", 9, nil))
	rewritten, rewrite := canonicalizeFunctionAliases("WITH 1 AS ln RETURN ln, PROPERTY_EXISTS(n.s, s), property_exists(n s), property_exists(n, s t)")
	require.Nil(t, rewrite)
	require.Equal(t, "WITH 1 AS ln RETURN ln, PROPERTY_EXISTS(n.s, s), property_exists(n s), property_exists(n, s t)", rewritten)
}
