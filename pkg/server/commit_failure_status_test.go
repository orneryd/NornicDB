package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The shared commit mapper also handles unclassified failures. Closing the
// real store after BEGIN exercises that existing caller without synthesizing
// a status wrapper or changing the transaction executor.
func TestHTTPCommitFailureDatabaseStatus(t *testing.T) {
	srv, _ := setupTestServer(t)
	ctx := context.Background()
	session, err := srv.txSessions.Open(ctx, srv.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	require.NoError(t, srv.db.Close())
	response := &TransactionResponse{}
	srv.commitRequestTransaction(ctx, session, response)
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", response.Errors[0].Code)
	require.Contains(t, response.Errors[0].Message, "closed")
}
