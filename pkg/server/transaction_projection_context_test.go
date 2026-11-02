package server

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPProjectionNativeFailureDoesNotPublishPartialRow(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "projection_leaf"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "projection_root", []multidb.ConstituentRef{{Alias: "leaf", DatabaseName: "projection_leaf", Type: "local", AccessMode: "read_write"}}))
	_, err = manager.GetStorage("projection_root")
	require.NoError(t, err)
	require.NoError(t, base.Close())
	server := &Server{dbManager: manager}
	receipt := map[string]any{"id": "source-result-receipt"}
	result := &cypher.ExecuteResult{Columns: []string{"scalar", "node"}, Rows: [][]any{{int64(7), &storage.Node{ID: "entity"}}}, Metadata: map[string]any{"receipt": receipt, "optimistic": "source-result-optimistic", "eventual": true}}
	prior := QueryResult{Columns: []string{"previous"}}
	response := &TransactionResponse{Results: []QueryResult{prior}}
	err = server.appendStatementResult(t.Context(), response, result, "projection_root", false, []string{"row", "graph", "rest"})
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	require.Equal(t, []QueryResult{prior}, response.Results, "first scalar conversion must not publish part of the failed result")
	require.Equal(t, receipt, response.Receipt)
	require.Equal(t, "source-result-optimistic", response.Optimistic)
	require.True(t, response.Eventual, "projection failure cannot erase supplied native outcome metadata")
}

func TestHTTPProjectionCancellationKeepsOriginalExecutionError(t *testing.T) {
	server, users := setupTestServer(t)
	claims, err := users.ValidateToken(getAuthToken(t, users, "admin"))
	require.NoError(t, err)
	original := errors.New("Neo.ClientError.Statement.ArithmeticError: original failure")
	ctx, cancel := context.WithCancel(t.Context())
	response := &TransactionResponse{}
	failed := server.runRequestStatements(ctx, "", claims, "nornic", []StatementRequest{{Statement: "RETURN 1 AS value"}}, func(context.Context, string, string, map[string]any) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
		cancel()
		return &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]any{{int64(7)}}}, nil, original
	}, func(message localization.Message) string { return "unused" }, response)
	require.True(t, failed)
	require.Empty(t, response.Results)
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", response.Errors[0].Code)
}
