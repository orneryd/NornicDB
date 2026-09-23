package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBooleanExpressionsUseCypherOperatorPrecedence(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "boolean_precedence"))
	result, err := executor.Execute(context.Background(), `
		RETURN true XOR false AND false AS conjunction,
		       NOT false OR true AS negation,
		       false = true IS NULL AS nullPredicate,
		       false = true IN [true, false] AS listPredicate
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"conjunction", "negation", "nullPredicate", "listPredicate"}, result.Columns)
	require.Equal(t, [][]interface{}{{true, true, true, false}}, result.Rows)
}

func TestPostfixPredicatesBindMoreTightlyThanNegation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "postfix_predicate_precedence"))
	result, err := executor.Execute(context.Background(), `
		UNWIND [true, false, null] AS value
		RETURN value, NOT value IS NULL AS nullResult, NOT value IN [true] AS membershipResult
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"value", "nullResult", "membershipResult"}, result.Columns)
	require.Equal(t, [][]interface{}{
		{true, true, false},
		{false, true, true},
		{nil, false, nil},
	}, result.Rows)
}

func TestStringPredicatesBindMoreTightlyThanBooleanOperatorsAndPropagateNull(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "string_predicate_precedence"))
	result, err := executor.Execute(context.Background(), `
		RETURN ('abc' STARTS WITH null OR true) = (('abc' STARTS WITH null) OR true) AS equivalent,
		       ('abc' STARTS WITH null OR true) <> ('abc' STARTS WITH (null OR true)) AS distinct
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"equivalent", "distinct"}, result.Columns)
	require.Equal(t, [][]interface{}{{true, nil}}, result.Rows)
}

func TestListContainmentBindsMoreTightlyThanComparison(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_containment_precedence"))
	result, err := executor.Execute(context.Background(), `
		RETURN [1, 2] = [3, 4] IN [[3, 4], false] AS equality,
		       [1, 2] <> [3, 4] IN [[3, 4], false] AS inequality,
		       [1, 2] < [3, 4] IN [[3, 4], false] AS ordering
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{false, true, nil}}, result.Rows)
}

func TestBooleanOrderingFollowsCypherTruthValueOrder(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "boolean_ordering"))
	result, err := executor.Execute(context.Background(), `
		RETURN false < true AS ascending,
		       true >= false AS descending,
		       NOT false >= false AS comparisonBeforeNegation,
		       false < null AS unknown
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true, true, false, nil}}, result.Rows)
}

func TestMembershipEvaluatesSliceOnItsRightOperand(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "membership_slice_precedence"))
	result, err := executor.Execute(context.Background(), `
		WITH [1, 2, 3] AS values
		RETURN 3 IN values[0..1] AS member
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{false}}, result.Rows)
}
