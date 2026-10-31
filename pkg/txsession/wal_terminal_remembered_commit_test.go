// Author: Claude, NOR-180 remembered terminal error at COMMIT, 2026-10-30.
package txsession

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A statement's terminal failure that the session remembers is delivered again
// at COMMIT. The COMMIT reply keeps that statement's classification: the
// commit status applies only to the commit's own unclassified failure. This is
// the HTTP COMMIT path: commitRequestTransaction renders CommitAndDelete's
// error with Neo4jStatus.
func TestWALTerminalRememberedErrorAtCommitKeepsClassification(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	controller := &lifecycleControllerStub{}
	engine.SetLifecycleController(controller)
	mgr := NewManager(time.Second, nil)
	session, err := mgr.OpenWithExecutorForOwner(context.Background(), "neo4j", cypher.NewStorageExecutor(engine), "")
	require.NoError(t, err)

	controller.mu.Lock()
	controller.gracefulExpire = true
	controller.mu.Unlock()
	_, err = mgr.ExecuteInSession(context.Background(), session, "CREATE (n:Remembered)", nil)
	require.ErrorIs(t, err, storage.ErrMVCCSnapshotGracefulCancel)
	statementCode, _ := nornicerrors.Neo4jStatus(err)

	_, err = mgr.CommitAndDelete(context.Background(), session)
	require.ErrorIs(t, err, storage.ErrMVCCSnapshotGracefulCancel, "the remembered failure is delivered at COMMIT")
	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, statementCode, code, "the statement's classification is kept")
	require.NotEqual(t, nornicerrors.DatabaseTransactionCommitFailed, code)
	require.Contains(t, message, storage.ErrMVCCSnapshotGracefulCancel.Error())
}
