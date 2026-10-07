package cypher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// neo5FunctionContext is a function context whose Eval reads expression
// values from a table; "boom" fails.
func neo5FunctionContext(values map[string]interface{}) cypherfn.Context {
	return cypherfn.Context{
		Eval: func(expr string) (interface{}, error) {
			if expr == "boom" {
				return nil, errors.New("boom")
			}
			value, ok := values[expr]
			if !ok {
				return nil, fmt.Errorf("unknown expression %q", expr)
			}
			return value, nil
		},
	}
}

// TestNeo4j5FunctionsArgumentErrorsFailTheStatement: an error evaluating an
// argument of a Neo4j 5 function fails the statement, as in Neo4j.
func TestNeo4j5FunctionsArgumentErrorsFailTheStatement(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo5cov"))
	ctx := context.Background()
	for _, query := range []string{
		"RETURN radians(1 % 0) AS v",
		"RETURN char_length(toString(1 % 0)) AS v",
		"RETURN upper(toString(1 % 0)) AS v",
		"RETURN btrim(toString(1 % 0)) AS v",
		"RETURN trim(toString(1 % 0)) AS v",
		"RETURN normalize(toString(1 % 0)) AS v",
		"RETURN toIntegerList([1 % 0]) AS v",
		"RETURN nullIf(1 % 0, 1) AS v",
		"RETURN valueType(1 % 0) AS v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "/ by zero", query)
	}
}

// TestNeo4j5FunctionsMoreNeo4jResults pins more of the #698 functions'
// results to Neo4j 5.26's.
func TestNeo4j5FunctionsMoreNeo4jResults(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo5cov2"))
	ctx := context.Background()
	for query, want := range map[string]interface{}{
		"RETURN radians(null) AS v":                      nil,
		"RETURN normalize('a', NFC) AS v":                "a",
		"RETURN normalize('ﬁ', NFKC) AS v":               "fi",
		"RETURN normalize('ﬁ', NFKD) AS v":               "fi",
		"RETURN toIntegerList([false, '1.5']) AS v":      []interface{}{int64(0), int64(1)},
		"RETURN toBooleanList([1.5, 'false']) AS v":      []interface{}{nil, false},
		"RETURN toStringList([[1], {a: 1}]) AS v":        []interface{}{nil, nil},
		"RETURN toStringList([date('2020-01-02')]) AS v": []interface{}{"2020-01-02"},
		"RETURN upper(null) AS v":                        nil,
		"RETURN ltrim(null, 'x') AS v":                   nil,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, want, result.Rows[0][0], query)
	}
	_, err := exec.Execute(ctx, "RETURN normalize('a', NFX) AS v", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "normal form")
}

// TestNeo4j5FunctionImplementations calls the #698 function implementations
// directly: argument counts, argument evaluation errors and value kinds the
// Cypher front end doesn't produce.
func TestNeo4j5FunctionImplementations(t *testing.T) {
	ctx := neo5FunctionContext(map[string]interface{}{
		"s":     "xax",
		"nan32": float32(math.NaN()),
		"f32":   float32(1.5),
		"one":   int64(1),
		"x":     "x",
		"null":  nil,
	})
	oneArgument := map[string]cypherfn.Func{
		"radians":       fnRadians,
		"isNaN":         fnIsNaN,
		"char_length":   fnCharLength,
		"upper":         fnStringCase(func(s string) string { return s }, "upper"),
		"toIntegerList": fnListConversion("toIntegerList", convertToIntegerOrNull),
		"valueType":     fnValueType,
	}
	for name, function := range oneArgument {
		_, err := function(ctx, []string{"s", "s"})
		require.Error(t, err, name)
		require.Contains(t, err.Error(), "argument(s), got 2", name)
	}
	// trim takes its FROM form or one to three positional arguments
	// (trimSpecificationForm).
	_, trimErr := fnTrim(ctx, []string{"s", "s", "s", "s"})
	require.Error(t, trimErr)
	require.Contains(t, trimErr.Error(), "argument(s), got 4")
	for name, function := range map[string]cypherfn.Func{
		"btrim":     fnTrimFunction("btrim", true, true),
		"normalize": fnNormalize,
	} {
		_, err := function(ctx, nil)
		require.Error(t, err, name)
		require.Contains(t, err.Error(), "expects 1 or 2 argument(s), got 0", name)
	}
	_, err := fnNullIf(ctx, []string{"s"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nullIf() expects 2 argument(s), got 1")

	// An argument that fails to evaluate fails the call.
	for name, call := range map[string]func() (interface{}, error){
		"radians":     func() (interface{}, error) { return fnRadians(ctx, []string{"boom"}) },
		"isNaN":       func() (interface{}, error) { return fnIsNaN(ctx, []string{"boom"}) },
		"char_length": func() (interface{}, error) { return fnCharLength(ctx, []string{"boom"}) },
		"upper": func() (interface{}, error) {
			return fnStringCase(func(s string) string { return s }, "upper")(ctx, []string{"boom"})
		},
		"btrim":     func() (interface{}, error) { return fnTrimFunction("btrim", true, true)(ctx, []string{"boom"}) },
		"trim text": func() (interface{}, error) { return fnTrim(ctx, []string{"boom"}) },
		"trim char": func() (interface{}, error) { return fnTrim(ctx, []string{"boom FROM s"}) },
		"normalize": func() (interface{}, error) { return fnNormalize(ctx, []string{"boom"}) },
		"nullIf":    func() (interface{}, error) { return fnNullIf(ctx, []string{"s", "boom"}) },
		"valueType": func() (interface{}, error) { return fnValueType(ctx, []string{"boom"}) },
	} {
		_, err := call()
		require.EqualError(t, err, "boom", name)
	}

	// A float32 argument is a float.
	value, err := fnIsNaN(ctx, []string{"nan32"})
	require.NoError(t, err)
	require.Equal(t, true, value)

	// trim's character must be a string.
	_, err = fnTrim(ctx, []string{"LEADING one FROM s"})
	var mismatch *cypherfn.TypeMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "String", mismatch.Expected)
	value, err = fnTrim(ctx, []string{"LEADING x FROM s"})
	require.NoError(t, err)
	require.Equal(t, "ax", value)
}

// TestNeo4j5ListConversionValues: the list conversions' item conversions.
func TestNeo4j5ListConversionValues(t *testing.T) {
	require.Equal(t, int64(0), convertToIntegerOrNull(false))
	require.Equal(t, int64(2), convertToIntegerOrNull("2.9"))
	require.Nil(t, convertToIntegerOrNull("NaN"))
	require.Equal(t, "1.5", convertToStringOrNull(float32(1.5)))
	require.Nil(t, convertToStringOrNull([]interface{}{int64(1)}))
	require.Nil(t, convertToStringOrNull(&storage.Node{}))
	require.Nil(t, convertToStringOrNull(struct{}{}))
	require.Equal(t, "P1D", convertToStringOrNull(&CypherDuration{Days: 1}))
	require.Nil(t, convertToBooleanOrNull(float32(1)))
	require.Nil(t, convertToBooleanOrNull(1.0))
}

// TestValueTypeUnionOrdering: the element-type union helpers of valueType().
func TestValueTypeUnionOrdering(t *testing.T) {
	integer, text := namedValueType("INTEGER"), namedValueType("STRING")
	listOf := func(elements ...valueType) valueType {
		list := namedValueType("LIST")
		list.elements = elements
		return list
	}
	// Different numbers of element types are different element sets.
	require.False(t, sameElementNames([]valueType{integer}, []valueType{integer, text}))
	// Lists holding different element types differ.
	require.False(t, sameElementNames([]valueType{listOf(integer)}, []valueType{listOf(text)}))
	require.True(t, sameElementNames([]valueType{listOf(integer)}, []valueType{listOf(integer)}))

	// Lists order by their element types, then by how many they hold.
	require.True(t, lessValueType(listOf(text), listOf(integer)))
	require.False(t, lessValueType(listOf(integer), listOf(text)))
	require.True(t, lessValueType(listOf(integer), listOf(integer, text)))
	require.False(t, lessValueType(listOf(integer, text), listOf(integer)))
}
