package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A failed statement with an unresolved schema must not be reported as a
// successful empty-column result. The schema owner's error stays terminal.
func TestStatementFailureUnresolvedColumns(t *testing.T) {
	server, _ := setupTestServer(t)
	executor, err := server.newExecutorForDatabase("nornic")
	require.NoError(t, err)
	response := &TransactionResponse{}
	failure := statementFailure(context.Background(), response, executor,
		"CALL missing.resultSchema()", errors.New("Neo.ClientError.Statement.ArithmeticError: execution failed"), nil)
	require.Equal(t, "Neo.ClientError.Procedure.ProcedureNotFound", failure.Code)
	require.Empty(t, response.Results)
}
