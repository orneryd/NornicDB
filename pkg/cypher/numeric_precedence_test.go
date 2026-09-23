package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNumericOperatorsFollowCypherPrecedenceAndAssociativity(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "numeric_precedence"))
	result, err := executor.Execute(context.Background(), `
		RETURN 8 + 6 / 3 - 4 AS additive,
		       24 / 3 * 2 % 5 AS multiplicative,
		       4 ^ 3 * 2 ^ 3 AS exponent,
		       4 ^ 3 % 2 ^ 3 AS floatingModulo,
		       4 ^ (3 * 2) ^ 3 AS leftAssociativeExponent,
		       -3 ^ 2 AS negativeBase,
		       -(3 ^ 2) AS negatedPower
	`, nil)
	require.NoError(t, err)
	require.Equal(t,
		[][]interface{}{{int64(6), int64(1), float64(512), float64(0), float64(68719476736), float64(9), float64(-9)}},
		result.Rows,
	)
}

func TestFloatingPointNaNIsNotEqualToAnyValue(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nan_equality"))
	result, err := executor.Execute(context.Background(), `
		RETURN 0.0 / 0.0 = 0.0 / 0.0 AS equal,
		       0.0 / 0.0 <> 0.0 / 0.0 AS unequal,
		       0.0 / 0.0 = 1 AS equalToNumber,
		       0.0 / 0.0 = 'value' AS equalToString
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{false, true, false, false}}, result.Rows)
}
