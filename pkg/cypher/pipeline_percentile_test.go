package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelinePercentileAggregates(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE ({price: 10.0}), ({price: 20.0}), ({price: 30.0}), ({price: 40.0})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH (n)
		RETURN percentileDisc(n.price, 0.25) AS discrete,
		       percentileCont(n.price, 0.25) AS continuous
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{float64(10), float64(17.5)}}, result.Rows)
}

func TestPipelinePercentileRejectsOutOfRangePercentile(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, "CREATE ({price: 10.0})", nil)
	require.NoError(t, err)

	_, err = executor.Execute(ctx, "MATCH (n) RETURN percentileDisc(n.price, $percentile)", map[string]interface{}{"percentile": 1.1})
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.ArgumentError", semanticError.Code)
	require.Equal(t, "NumberOutOfRange", semanticError.Detail)
}

func TestPipelinePercentileValidatesRowDependentPercentile(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	_, err := executor.Execute(ctx, `
		UNWIND range(0, 10) AS i
		CREATE (s:S)
		WITH s, i
		UNWIND range(0, i) AS j
		CREATE (s)-[:REL]->()
	`, nil)
	require.NoError(t, err)

	query := `
		MATCH (n:S)
		WITH n, size([(n)-->() | 1]) AS deg
		WHERE deg > 2
		WITH deg
		LIMIT 100
		RETURN percentileDisc(0.90, deg), deg
	`
	_, err = executor.Execute(ctx, query, nil)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.ArgumentError", semanticError.Code)
	require.Equal(t, "NumberOutOfRange", semanticError.Detail)
}
