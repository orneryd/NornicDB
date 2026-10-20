package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type failedCommittedDropEngine struct{ *storage.MemoryEngine }

func (e *failedCommittedDropEngine) GetInnerEngine() storage.Engine { return e.MemoryEngine }

func (e *failedCommittedDropEngine) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	op := storage.NamespaceDropFromContext(ctx)
	op.SetJournal(func() (bool, error) { return true, errors.New("admitted lifecycle journal I/O failure") })
	return e.MemoryEngine.DeleteByPrefixContext(ctx, prefix)
}

func TestNamespaceFailedCommittedDropRejectsPublicRecreate(t *testing.T) {
	base := &failedCommittedDropEngine{MemoryEngine: storage.NewMemoryEngine()}
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase("failed_a"))
	require.NoError(t, manager.CreateDatabase("failed_b"))
	executor := func(name string) *StorageExecutor {
		view, err := manager.GetStorage(name)
		require.NoError(t, err)
		e := NewStorageExecutor(view)
		e.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
		return e
	}
	admin, old, peer := executor("system"), executor("failed_a"), executor("failed_b")
	typedCountRun(t, old, "CREATE (:Old)")
	_, err = admin.Execute(context.Background(), "DROP DATABASE failed_a", nil)
	var outcome *storage.NamespaceDropError
	require.ErrorAs(t, err, &outcome)
	require.True(t, outcome.Committed)
	_, err = admin.Execute(context.Background(), "CREATE DATABASE failed_a", nil)
	require.Error(t, err, "a failed committed DROP must keep same-name CREATE unavailable")
	_, err = old.Execute(context.Background(), "CREATE (:Rejected)", nil)
	require.Error(t, err, "an existing namespaced view must not acknowledge a fresh write")
	typedCountRun(t, peer, "CREATE (:Peer)")
	typedCountWant(t, peer, "MATCH (n:Peer) RETURN count(n) AS n", int64(1))
}
