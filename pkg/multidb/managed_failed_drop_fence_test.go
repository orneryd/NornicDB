package multidb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type failedNamespaceCompletion struct {
	storage.Engine
	failure error
}

func (f *failedNamespaceCompletion) GetInnerEngine() storage.Engine { return f.Engine }
func (f *failedNamespaceCompletion) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	// A journal can admit the operation and still fail its acknowledgement.
	// Intrinsic completion consequently cannot checkpoint this entry.
	storage.NamespaceDropFromContext(ctx).SetJournal(func() (bool, error) { return true, f.failure })
	return storage.DeletePrefixContext(ctx, f.Engine, prefix)
}

func TestManagedFailedNamespaceCompletionFencesRecreate(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint("buffered=", buffered), func(t *testing.T) {
			dir := t.TempDir()
			open := func() (*storage.BadgerEngine, *storage.WAL, *storage.WALEngine) {
				base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				wal, err := storage.NewWAL(dir+"/wal", storage.DefaultWALConfig())
				require.NoError(t, err)
				return base, wal, storage.NewWALEngine(base, wal)
			}
			base, _, engine := open()
			fault := errors.New("admitted lifecycle completion failed")
			manager, err := NewDatabaseManager(&failedNamespaceCompletion{Engine: engine, failure: fault}, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, manager.Close()) }()
			require.NoError(t, manager.CreateDatabase("failed_drop"))
			view, err := manager.GetStorage("failed_drop")
			require.NoError(t, err)
			_, err = view.CreateNode(&storage.Node{ID: "old", Labels: []string{"Doc"}})
			require.NoError(t, err)
			err = manager.DropDatabase("failed_drop")
			require.ErrorIs(t, err, fault)
			require.True(t, storage.MutationApplied(err))
			require.False(t, manager.Exists("failed_drop"))
			require.ErrorIs(t, manager.CreateDatabase("failed_drop"), storage.ErrConflict,
				"a committed entry without an intrinsic completion checkpoint must fence reuse")
			_, err = base.CreateNode(&storage.Node{ID: "failed_drop:ghost"})
			require.ErrorIs(t, err, storage.ErrConflict)
			_, err = base.CreateNode(&storage.Node{ID: "peer:available"})
			require.NoError(t, err, "the failure must fence only its namespace")
			require.NoError(t, manager.Close())
			base, wal, reopened := open()
			_, err = storage.RecoverBufferedCommits(base, wal)
			require.NoError(t, err)
			manager, err = NewDatabaseManager(reopened, nil)
			require.NoError(t, err)
			require.NoError(t, manager.CreateDatabase("failed_drop"))
			view, err = manager.GetStorage("failed_drop")
			require.NoError(t, err)
			count, err := view.NodeCount()
			require.NoError(t, err)
			require.Zero(t, count, "recovery retires the old lifetime before reuse")
		})
	}
}
