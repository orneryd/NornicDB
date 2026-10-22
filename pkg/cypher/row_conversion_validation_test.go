package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConversionRejectsInvalidTypedValuesInsideListComprehension(t *testing.T) {
	executor := &StorageExecutor{}
	row := pipelineRow{
		"values": []interface{}{true, []interface{}{}},
	}

	fixtureAdmission835Context, fixtureAdmission835 := expressionTestRow(context.Background(), "[value IN values | toBoolean(value)]", row)
	_ = fixtureAdmission835
	_, _, err := executor.evaluateRowValue(fixtureAdmission835,
		"[value IN values | toBoolean(value)]",
		fixtureAdmission835Context,
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

	result, ok := rowValue(t, executor, "[value IN values | toBoolean(value)]", row)
	require.True(t, ok)
	require.Equal(t, []interface{}{true, nil}, result)
}

func TestIntegerConversionTruncatesNumericStrings(t *testing.T) {
	executor := &StorageExecutor{}
	result, ok := rowValue(t, executor, "toInteger('2.9')", pipelineRow{})
	require.True(t, ok)
	require.Equal(t, int64(2), result)
}
