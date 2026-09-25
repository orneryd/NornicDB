package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNeo4jValueTypeNameBranches pins the type names Neo4j's runtime errors
// use for every value kind (#657).
func TestNeo4jValueTypeNameBranches(t *testing.T) {
	for _, tc := range []struct {
		value interface{}
		want  string
	}{
		{int64(1), "Long"},
		{1.5, "Double"},
		{"a", "String"},
		{true, "Boolean"},
		{&storage.Node{ID: "n"}, "NodeIdReference"},
		{&storage.Edge{ID: "r"}, "RelationshipReference"},
		{&PathResult{}, "Path"},
		{PathResult{}, "Path"},
		{CypherDate{}, "Date"},
		{&CypherDate{}, "Date"},
		{CypherTime{}, "Time"},
		{CypherLocalTime{}, "LocalTime"},
		{CypherLocalDateTime{}, "LocalDateTime"},
		{CypherDateTime{}, "DateTime"},
		{CypherDuration{}, "Duration"},
		{map[string]interface{}{"_pathResult": true}, "Path"},
		{map[string]interface{}{"a": 1}, "Map"},
		{nil, "NoValue"},
		{map[string]int{"a": 1}, "Map"},
		{[]interface{}{int64(1), "a"}, "List"},
		{[]interface{}{}, "List"},
		{[]interface{}{map[string]interface{}{}}, "List"},
		{[]int64{1, 2}, "LongArray"},
		{[]string{"a"}, "StringArray"},
		{struct{}{}, "struct {}"},
	} {
		require.Equal(t, tc.want, neo4jValueTypeName(tc.value), "%#v", tc.value)
	}
}

// TestNeo4jValueReprBranches pins how runtime errors show a value.
func TestNeo4jValueReprBranches(t *testing.T) {
	for _, tc := range []struct {
		value interface{}
		want  string
	}{
		{int64(1), "Long(1)"},
		{"x", `String("x")`},
		{true, "Boolean('true')"},
		{&storage.Node{ID: "n1"}, "(n1)"},
		{&storage.Edge{ID: "r1"}, "-[r1]-"},
		{(*storage.Node)(nil), "NodeIdReference"},
		{(*storage.Edge)(nil), "RelationshipReference"},
		{nil, "NO_VALUE"},
		{[]interface{}{int64(1), "a"}, `List{Long(1), String("a")}`},
		{[]int64{1, 2}, "LongArray[1, 2]"},
		{map[string]interface{}{"a": 1}, "Map"},
	} {
		require.Equal(t, tc.want, neo4jValueRepr(tc.value), "%#v", tc.value)
	}
}

// TestRuntimeTypeErrorNilBranches covers the operands the runtime type
// checks leave alone: null, a string next to a temporal value, an operator
// they don't check, and values that have properties.
func TestRuntimeTypeErrorNilBranches(t *testing.T) {
	require.NoError(t, runtimeArithmeticTypeError('+', nil, int64(1)))
	require.NoError(t, runtimeArithmeticTypeError('+', "2020-01-01", CypherDuration{}))
	require.NoError(t, runtimeArithmeticTypeError('&', int64(1), int64(2)))
	require.Error(t, runtimeArithmeticTypeError('^', "a", int64(2)))
	require.NoError(t, unaryMinusTypeError(nil))
	require.NoError(t, unaryMinusTypeError(CypherDuration{}))
	require.Error(t, unaryMinusTypeError("a"))
	require.NoError(t, propertyAccessTypeError(nil))
	require.NoError(t, propertyAccessTypeError(CypherDate{}))
	require.NoError(t, propertyAccessTypeError(map[interface{}]interface{}{"a": 1}))
	require.Error(t, propertyAccessTypeError(int64(1)))
	require.False(t, isRuntimeList(nil))
	require.False(t, isRuntimeList("a"))
	require.True(t, isRuntimeList([]int64{1}))
}

// TestRecordRowOperatorFailureBranches covers how the row evaluator finds the
// failing operator of an expression it couldn't resolve: inside the right
// operand, under a unary minus, in a function argument and in the list,
// predicate or projection of a comprehension (#657).
func TestRecordRowOperatorFailureBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rowfail"))
	row := pipelineRow{"z": int64(0), "one": int64(1), "s": "a", "l": []interface{}{int64(1)}}
	for _, tc := range []struct {
		expr     string
		recorded bool
		code     string
	}{
		{"one + (one / z)", true, "/ by zero"},
		{"-(one / z)", true, "/ by zero"},
		{"-one", false, ""},
		{"-s", true, "TypeError"},
		{"abs(one / z)", true, "/ by zero"},
		{"abs(one)", false, ""},
		{"[x IN range(1, one / z) | x]", true, "/ by zero"},
		{"[x IN [1, 2] WHERE x > 1 | x / z]", true, "/ by zero"},
		{"[x IN [1, 2] WHERE x / z > 1 | x]", true, "/ by zero"},
		{"[x IN [1, 2] | x]", false, ""},
		{"s.name", true, "TypeError"},
		{"n {a: 1}", false, ""},
		{"(a)-[:R]->(b)", false, ""},
	} {
		ctx := context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
		recorded := exec.recordRowOperatorFailure(ctx, tc.expr, row)
		require.Equal(t, tc.recorded, recorded, tc.expr)
		if tc.recorded {
			err := getExpressionFailure(ctx)
			require.Error(t, err, tc.expr)
			require.Contains(t, err.Error(), tc.code, tc.expr)
		}
	}
	require.True(t, isOperandExpressionText("-x"))
	require.False(t, isOperandExpressionText(""))
	require.True(t, isOperatorExpressionText("-[1]"))
}
