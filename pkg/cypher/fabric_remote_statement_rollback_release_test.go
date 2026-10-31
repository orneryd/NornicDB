package cypher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type failingCompositeRemote struct {
	storage.Engine
	queryErr, commitErr, rollbackErr error
	applyBeforeError                 bool
	applied                          atomic.Bool
	queries, commits, rollbacks      atomic.Int32
}

func (remote *failingCompositeRemote) QueryCypher(context.Context, string, map[string]interface{}, storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	return nil, nil, errors.New("composite work escaped its remote transaction")
}
func (remote *failingCompositeRemote) BeginCypherTx(_ context.Context, mode storage.RemoteAccessMode) (storage.RemoteCypherTx, error) {
	return &failingCompositeRemoteTx{remote: remote, mode: mode}, nil
}
func (remote *failingCompositeRemote) Close() error { return nil } // embedded fixture owns its engine

type failingCompositeRemoteTx struct {
	remote *failingCompositeRemote
	mode   storage.RemoteAccessMode
}

func (tx *failingCompositeRemoteTx) QueryCypher(context.Context, string, map[string]interface{}) ([]string, [][]interface{}, error) {
	tx.remote.queries.Add(1)
	if tx.remote.queryErr != nil {
		return nil, nil, tx.remote.queryErr
	}
	return []string{"r"}, [][]interface{}{{int64(1)}}, nil
}
func (tx *failingCompositeRemoteTx) ResolvedWriteMode() (bool, bool) {
	return tx.mode == storage.RemoteAccessWrite, true
}
func (tx *failingCompositeRemoteTx) Commit(context.Context) error {
	tx.remote.commits.Add(1)
	if tx.remote.applyBeforeError {
		tx.remote.applied.Store(true)
	}
	return tx.remote.commitErr
}
func (tx *failingCompositeRemoteTx) Rollback(context.Context) error {
	tx.remote.rollbacks.Add(1)
	return tx.remote.rollbackErr
}

func newCompositeFailureExecutor(t *testing.T, remote *failingCompositeRemote) (*StorageExecutor, *multidb.DatabaseManager) {
	t.Helper()
	remote.Engine = newTestMemoryEngine(t)
	cfg := multidb.DefaultConfig()
	cfg.RemoteEngineFactory = func(multidb.ConstituentRef, string) (storage.Engine, error) { return remote, nil }
	manager := fixtureowner.Own(t, newCompositeNativeManager(t, cfg))
	ctx := context.Background()
	require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "one"}))
	require.NoError(t, manager.CreateCompositeDatabase(ctx, "comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: "http://remote.invalid:7474"},
	}))
	composite, err := manager.GetStorage("comp")
	require.NoError(t, err)
	executor := NewStorageExecutor(composite)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	return executor, manager
}

func TestCompositeRemoteFailureRollsBackLocalWrites(t *testing.T) {
	for _, phase := range []string{"query", "query-and-cleanup", "read-close"} {
		t.Run(phase, func(t *testing.T) {
			cause := errors.New("remote " + phase + " rejected")
			cleanup := errors.New("remote rollback rejected")
			remote := &failingCompositeRemote{}
			if phase == "read-close" {
				remote.rollbackErr = cause
			} else {
				remote.queryErr = cause
			}
			if phase == "query-and-cleanup" {
				remote.rollbackErr = cleanup
			}
			executor, manager := newCompositeFailureExecutor(t, remote)
			ctx := context.Background()
			_, err := executor.Execute(ctx, `CALL { USE comp.one CREATE (:RemoteFailure {run:$run}) RETURN 1 AS v } CALL { WITH v USE comp.remote RETURN v AS r } RETURN v,r`, map[string]interface{}{"run": phase})
			require.ErrorIs(t, err, cause)
			if phase == "query-and-cleanup" {
				require.ErrorIs(t, err, cleanup)
			}
			require.Equal(t, int32(1), remote.queries.Load(), "must reach the remote after producing the local write's row")
			require.Equal(t, int32(1), remote.rollbacks.Load())
			require.Zero(t, remote.commits.Load(), "a READ participant ends by rollback")
			physical, err := manager.GetStorage("one")
			require.NoError(t, err)
			result, err := NewStorageExecutor(physical).Execute(ctx, `MATCH (n:RemoteFailure {run:$run}) RETURN count(n) AS count`, map[string]interface{}{"run": phase})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "a later remote failure cannot publish the local write")
		})
	}
}

func TestCompositeLostRemoteWriteAcknowledgementRemainsUnknown(t *testing.T) {
	cause := errors.New("remote commit acknowledgement lost")
	remote := &failingCompositeRemote{commitErr: cause, applyBeforeError: true}
	executor, _ := newCompositeFailureExecutor(t, remote)
	_, err := executor.Execute(context.Background(), `CALL { USE comp.one RETURN 1 AS v } CALL { WITH v USE comp.remote CREATE (:RemoteFailure) RETURN v AS r } RETURN v,r`, nil)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, fabric.ErrCommitOutcomeUnknown)
	require.False(t, nornicerrors.IsCommitRolledBack(err), "cleanup cannot prove a lost write acknowledgement rolled back")
	require.True(t, remote.applied.Load(), "control simulates a commit that applied before its acknowledgement was lost")
	require.Equal(t, int32(1), remote.commits.Load(), "never replay uncertain COMMIT")
	require.Equal(t, int32(1), remote.rollbacks.Load(), "one cleanup attempt; it cannot undo an applied commit")
}
