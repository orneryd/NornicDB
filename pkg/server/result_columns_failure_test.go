package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A failed statement with an unresolved schema must not be reported as a
// successful empty-column result. The original execution failure stays terminal.
func TestStatementFailureUnresolvedColumns(t *testing.T) {
	server, _ := setupTestServer(t)
	executor, err := server.newExecutorForDatabase("nornic")
	require.NoError(t, err)
	response := &TransactionResponse{}
	failure := statementFailure(context.Background(), response, executor,
		"CALL missing.resultSchema()", errors.New("Neo.ClientError.Statement.ArithmeticError: execution failed"), nil)
	require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", failure.Code)
	require.Contains(t, failure.Message, "execution failed")
	require.Empty(t, response.Results)

	// A real missing-procedure execution retains the procedure owner's
	// compile error too; metadata never replaces either execution outcome.
	_, err = executor.Execute(context.Background(), "CALL missing.resultSchema()", nil)
	require.Error(t, err)
	response = &TransactionResponse{}
	failure = statementFailure(context.Background(), response, executor,
		"CALL missing.resultSchema()", err, nil)
	require.Equal(t, "Neo.ClientError.Procedure.ProcedureNotFound", failure.Code)
	require.Empty(t, response.Results)
}
