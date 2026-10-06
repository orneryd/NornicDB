package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStaticFunctionArgumentsFromCatalog(t *testing.T) {
	expected := func(name string) []string {
		arguments, ok := lookupStaticFunctionArguments(name)
		require.True(t, ok, name)
		out := make([]string, len(arguments))
		for index, argument := range arguments {
			out[index] = argument.expected
		}
		return out
	}
	require.Equal(t, []string{"Boolean, Float, Integer or String"}, expected("toInteger"))
	require.Equal(t, []string{"Float, Integer or Duration"}, expected("sum"))
	require.Equal(t, []string{"Map, Node, Relationship, String or List<T>"}, expected("isEmpty"))
	require.Equal(t, []string{"String", "", "Map, Node or Relationship"}, expected("date.truncate"), "a temporal input isn't checked at compile time")
	require.Equal(t, []string{"List<Float>, List<Integer> or List<Number>", "List<Float>, List<Integer> or List<Number>"}, expected("vector.similarity.cosine"))
	require.Equal(t, []string{"String", "String or List<String>"}, expected("split"))
	require.Equal(t, []string{"String", "String", "String"}, expected("TRIM"))
	require.Equal(t, []string{"Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime"}, expected("toString"))
	for _, name := range []string{"point.distance", "point.withinBBox", "coalesce", "reduce", "any", "cosh", "nosuch", "x234567890123456789012345678901234567890123456789012345678901234567890"} {
		_, ok := lookupStaticFunctionArguments(name)
		require.False(t, ok, name)
	}

	toInteger, _ := lookupStaticFunctionArguments("tointeger")
	require.True(t, toInteger[0].accepts("List<Integer>"), "toInteger accepts a list at compile time")
	cosine, _ := lookupStaticFunctionArguments("vector.similarity.cosine")
	require.True(t, cosine[0].accepts("List<T>"), "an empty list's element type is unknown")
	require.True(t, cosine[0].accepts("List<Integer>"))
	require.False(t, cosine[0].accepts("String"))
	require.True(t, staticArgumentType{}.accepts("String"), "a position without options isn't checked")

	options, known := staticCatalogTypeOptions("INTEGER | FLOAT | DURATION")
	require.True(t, known)
	require.Equal(t, []string{"Integer", "Float", "Duration"}, options)
	_, known = staticCatalogTypeOptions("POINT")
	require.False(t, known)
	_, known = staticCatalogTypeOptions("STRING | POINT")
	require.False(t, known)
	require.Equal(t, "", joinTypeNames(nil))
	require.Equal(t, "Float", joinTypeNames([]string{"Float"}))
	require.Equal(t, "Float or Integer", joinTypeNames([]string{"Float", "Integer"}))

	require.Equal(t, "Map, Node or Relationship", unwindStaticValueType("UNWIND [{k: 1}, {k: 2}] AS x"))
	require.Equal(t, "Float, Integer or Number", unwindStaticValueType("UNWIND [1, 2.5] AS x"))
	require.Equal(t, "Integer", unwindStaticValueType("UNWIND [1, 2] AS x"))
	require.Equal(t, "", unwindStaticValueType("UNWIND [1, 'a'] AS x"))
	require.Equal(t, "", unwindStaticValueType("UNWIND $list AS x"))
}

// TestStaticFunctionArgumentTypesThroughExecute: an argument of a type no
// signature accepts is Neo4j's compile-time SyntaxError, for every function
// the catalog types, with Neo4j's message; arguments Neo4j doesn't check at
// compile time still run.
func TestStaticFunctionArgumentTypesThroughExecute(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for query, message := range map[string]string{
		"RETURN vector.similarity.cosine('zz', [1.0]) AS v":     "Type mismatch: expected List<Float>, List<Integer> or List<Number> but was String",
		"RETURN date.truncate(1, date('2020-01-02')) AS v":      "Type mismatch: expected String but was Integer",
		"RETURN date.truncate('day', date('2020-01-02'), 'zz')": "Type mismatch: expected Map, Node or Relationship but was String",
		"RETURN datetime.fromepoch('zz', 1) AS v":               "Type mismatch: expected Float or Integer but was String",
		"RETURN point('zz') AS v":                               "Type mismatch: expected Map, Node or Relationship but was String",
		"RETURN isEmpty(1) AS v":                                "Type mismatch: expected Map, Node, Relationship, String or List<T> but was Integer",
		"RETURN range('a', 1) AS v":                             "Type mismatch: expected Integer but was String",
		"RETURN trim(1, 'a', 'b') AS v":                         "Type mismatch: expected String but was Integer",
		"UNWIND [{k: 1}, {k: 2}] AS x RETURN sum(x) AS v":       "Type mismatch: expected Float, Integer or Duration but was Map, Node or Relationship",
		"UNWIND [1, 2.5] AS x RETURN toUpper(x) AS v":           "Type mismatch: expected String but was Float, Integer or Number",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}
	for _, query := range []string{
		"UNWIND [1, 2.5] AS x RETURN abs(x) AS v",
		"UNWIND [{k: 1}] AS x RETURN keys(x) AS v",
		"RETURN split('a,b', [',']) IS NOT NULL AS v",
		"RETURN date.truncate('month', date('2020-01-02')) AS v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

// TestStaticLiteralArgumentValues: a percentile literal outside 0.0..1.0 and
// a point() map literal without coordinates fail when the statement
// compiles, as in Neo4j, even when no row would call the function; computed
// values fail when they run, with an ArgumentError.
func TestStaticLiteralArgumentValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for query, code := range map[string]string{
		"MATCH (n:Nothing) RETURN percentileCont(n.x, 1.5) AS v":  "Neo.ClientError.Statement.SyntaxError",
		"MATCH (n:Nothing) RETURN percentileDisc(n.x, -0.1) AS v": "Neo.ClientError.Statement.SyntaxError",
		"UNWIND [1] AS x RETURN percentileDisc(x, 0.5 + 1) AS v":  "Neo.ClientError.Statement.ArgumentError",
		"MATCH (n:Nothing) RETURN point({a: 1}) AS v":             "Neo.ClientError.Statement.SyntaxError",
		"RETURN point({x: 1}) AS v":                               "Neo.ClientError.Statement.SyntaxError",
		"WITH {x: 1} AS m RETURN point(m) AS v":                   "Neo.ClientError.Statement.ArgumentError",
		"RETURN point({x: 1, y: 2, crs: 'zz'}) AS v":              "Neo.ClientError.Statement.ArgumentError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, statusText(err), code, query)
	}
	_, err := exec.Execute(ctx, "RETURN point({a: 1, b: 2}) AS v", nil)
	require.Contains(t, statusText(err), "A map with keys 'a', 'b' is not describing a valid point")
	for _, query := range []string{
		"UNWIND [1, 2] AS x RETURN percentileCont(x, 1) AS v",
		"UNWIND [1, 2] AS x RETURN percentileCont(x, -0) AS v",
		"RETURN point({x: 1, y: 2}) AS v",
		"RETURN point({`latitude`: 1, `longitude`: 2}) AS v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}

	require.NoError(t, checkStaticLiteralArguments("percentileCont", []string{"x"}))
	require.NoError(t, checkStaticLiteralArguments("point", []string{"m", "n"}))
	keys, isMap := staticMapLiteralKeys("{`a``b`: 1, c: 2}")
	require.True(t, isMap)
	require.Equal(t, map[string]bool{"a`b": true, "c": true}, keys)
	_, isMap = staticMapLiteralKeys("{a}")
	require.False(t, isMap)
	_, isMap = staticMapLiteralKeys("m{.a}")
	require.False(t, isMap)
}
