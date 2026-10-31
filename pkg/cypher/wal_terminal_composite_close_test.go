// Author: Codex, NOR-180 committed completion includes remote resource close, 2026-11-01.
package cypher

import (
	"context"
	"errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type terminalCloseEngine struct {
	storage.Engine
	failure error
}

func (e *terminalCloseEngine) QueryCypher(context.Context, string, map[string]interface{}, storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	return []string{"value"}, [][]interface{}{{int64(1)}}, nil
}
func (e *terminalCloseEngine) BeginCypherTx(context.Context, storage.RemoteAccessMode) (storage.RemoteCypherTx, error) {
	return nil, errors.New("unused transaction open")
}
func (e *terminalCloseEngine) Close() error { return e.failure }

func TestWALTerminalCommitKeepsRemoteCloseFailure(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:TerminalClose)", nil)
	require.NoError(t, err)
	// Seed the public remote executor cache at its transport/factory boundary.
	// The active transaction owns this executor, exactly as composite routing does.
	remote := fabric.NewRemoteFragmentExecutor()
	transport := &terminalCloseEngine{Engine: base, failure: storage.ErrStorageRestoring}
	_, err = remote.Execute(ctx, &fabric.LocationRemote{DBName: "remote", URI: "http://remote.invalid", ResolveEngine: func(string) (storage.Engine, error) { return transport, nil }}, "RETURN 1", nil, "", storage.RemoteAccessRead)
	require.NoError(t, err)
	exec.txContext.fabricRemoteExe = remote
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.ErrorIs(t, err, storage.ErrStorageRestoring)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
	require.False(t, (&db.Neo4jError{Code: code}).IsRetriable())
	actual, readErr := NewStorageExecutor(store).Execute(ctx, "MATCH (n:TerminalClose) RETURN count(n)", nil)
	require.NoError(t, readErr)
	require.Equal(t, [][]interface{}{{int64(1)}}, actual.Rows)
	require.False(t, exec.HasActiveTransaction())
}
