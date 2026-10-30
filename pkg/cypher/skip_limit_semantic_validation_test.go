package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaginationRejectsRowDependentAndInvalidLiteralArguments(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	tests := []struct {
		query  string
		detail string
	}{
		{"MATCH (n) RETURN n SKIP n.count", "NonConstantExpression"},
		{"MATCH (n) RETURN n SKIP -1", "NegativeIntegerArgument"},
		{"MATCH (n) RETURN n SKIP 1.5", "InvalidArgumentType"},
		{"MATCH (n) RETURN n LIMIT n.count", "NonConstantExpression"},
	}
	for _, test := range tests {
		_, err := exec.Execute(ctx, test.query, nil)
		requireSemanticPaginationError(t, err, "Neo.ClientError.Statement.SyntaxError", test.detail)
	}
}

func TestPaginationRejectsInvalidParameterArgumentsAtRuntime(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	_, err := exec.Execute(ctx, "RETURN 1 SKIP $amount", map[string]interface{}{"amount": int64(-1)})
	requireSemanticPaginationError(t, err, "Neo.ClientError.Statement.ArgumentError", "NegativeIntegerArgument")

	_, err = exec.Execute(ctx, "RETURN 1 LIMIT $amount", map[string]interface{}{"amount": 1.5})
	requireSemanticPaginationError(t, err, "Neo.ClientError.Statement.ArgumentError", "InvalidArgumentType")
}

func requireSemanticPaginationError(t *testing.T, err error, code, detail string) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, code, semanticError.Code)
	require.Equal(t, detail, semanticError.Detail)
}
