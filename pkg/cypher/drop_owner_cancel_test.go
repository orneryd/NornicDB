package cypher

import (
	"context"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-31. Public Cypher DROP DATABASE whose
// statement is cancelled while the data purge runs (a client deadline or a
// disconnect). The client gets the accepted outcome, which is not retryable;
// the DROP owner still finishes the drop, so CREATE DATABASE of the same name
// succeeds afterwards with no further DROP and no restart.
func TestCypherDropFinishesWhenStatementCancelledDuringPurge(t *testing.T) {
	f := openDropOwnerCypherFixture(t)
	syncsBefore := f.faults.syncCalls.Load()
	f.faults.purgeEntered = make(chan struct{}, 1)
	f.faults.purgePermit = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := f.executor.Execute(ctx, "DROP DATABASE victim", nil)
		result <- err
	}()
	select {
	case <-f.faults.purgeEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the DROP did not reach its data purge")
	}
	cancel()
	close(f.faults.purgePermit)
	var err error
	select {
	case err = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled DROP statement did not return")
	}
	if err != nil {
		var accepted *storage.AcceptedCommitError
		require.ErrorAs(t, err, &accepted, "a statement cancelled after acceptance reports the accepted outcome")
		require.False(t, nornicerrors.IsTransientTransactionError(err), "an accepted DROP outcome invites no automatic retry")
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, nornicerrors.DatabaseTransactionCommitFailed, code)
	}
	// The owner syncs once after the intent and once after the final removal.
	require.Eventually(t, func() bool {
		return f.faults.syncCalls.Load() >= syncsBefore+2 && !f.manager.Exists("victim") && !f.stored("old")
	}, 10*time.Second, 10*time.Millisecond, "the DROP owner finishes the accepted drop")
	require.NoError(t, f.exec(t, "CREATE DATABASE victim"), "the name is free without another DROP")
	view, err := f.manager.GetStorage("victim")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "fresh"})
	require.NoError(t, err)
}
