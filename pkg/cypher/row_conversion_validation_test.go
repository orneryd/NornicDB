package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConversionRejectsInvalidTypedValuesInsideListComprehension(t *testing.T) {
	executor := &StorageExecutor{}
	row := pipelineRow{
		"values": []interface{}{true, []interface{}{}},
	}

	err := executor.validateRowConversionArguments(
		"[value IN values | toBoolean(value)]",
		row,
	)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.TypeError", semanticError.Code)
	require.Equal(t, "InvalidArgumentValue", semanticError.Detail)
}

func TestConversionRetainsNullForUnparseableStrings(t *testing.T) {
	executor := &StorageExecutor{}
	row := pipelineRow{
		"values": []interface{}{"true", "not a boolean"},
	}

	require.NoError(t, executor.validateRowConversionArguments(
		"[value IN values | toBoolean(value)]",
		row,
	))
	result, ok := executor.evaluateRowExpression("[value IN values | toBoolean(value)]", row)
	require.True(t, ok)
	require.Equal(t, []interface{}{true, nil}, result)
}

func TestIntegerConversionTruncatesNumericStrings(t *testing.T) {
	executor := &StorageExecutor{}
	result, ok := executor.evaluateRowExpression("toInteger('2.9')", pipelineRow{})
	require.True(t, ok)
	require.Equal(t, int64(2), result)
}
