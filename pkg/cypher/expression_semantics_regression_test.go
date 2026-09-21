package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListComprehensionFiltersBeforeProjection(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "RETURN [x IN range(1,5) WHERE x % 2 = 1 | x * 10] AS values", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{int64(10), int64(30), int64(50)}}}, result.Rows)
}

func TestNumericDivisionAndPowerPreserveCypherTypes(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "RETURN 7 / 2 AS integerDivision, 7.0 / 2 AS floatDivision, 2 ^ 3 AS power", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3), float64(3.5), float64(8)}}, result.Rows)

	result, err = exec.Execute(ctx, "WITH 7 AS a, 2 AS b RETURN a / b AS integerDivision", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
}

func TestReduceUsesBindingsCarriedThroughWith(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "WITH [1,2,3] AS values RETURN reduce(total = 0, value IN values | total + value) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6)}}, result.Rows)
}

func TestNestedMapAccessUsesTypedValues(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "WITH {a:1, b:{c:2}} AS m RETURN m.b.c AS nested", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}
