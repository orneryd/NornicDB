package cypher

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Close-phase check of the Cypher
// accepted-work consumer: DROP DATABASE through the executor and the DROP
// owner, with engine Close before, during (the accepted catalog command held in
// flight) and after the statement. Every phase returns, an acknowledged drop
// is durable, and the reopened catalog is coherent.
func TestCypherDropClosePhases(t *testing.T) {
	for _, phase := range []string{"before", "during", "after"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *storage.BadgerEngine {
				t.Helper()
				native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				return native
			}
			hold := storagefault.NewCloseHold(open(), "system")
			t.Cleanup(hold.Release)
			manager, err := multidb.NewDatabaseManager(t.Context(), hold, nil)
			require.NoError(t, err)
			executor := NewStorageExecutor(storage.NewNamespacedEngine(hold, "nornic"))
			executor.SetDatabaseManager(&catalogRequestContextAdapter{optionsTestManager: optionsTestManager{&testDatabaseManagerAdapter{manager: manager}}})
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "target"}))
			view, err := manager.GetStorage("target")
			require.NoError(t, err)
			_, err = view.CreateNode(&storage.Node{ID: "old"})
			require.NoError(t, err)
			drop := func(out chan<- error) {
				_, err := executor.Execute(context.Background(), "DROP DATABASE target", nil)
				out <- err
			}

			dropped := make(chan error, 1)
			closed := make(chan error, 1)
			var dropErr error
			switch phase {
			case "before":
				require.NoError(t, manager.Close())
				go drop(dropped)
				require.Error(t, cypherClosePhaseWithin(t, "DROP DATABASE on a closed owner", dropped))
			case "during":
				entered := hold.Arm()
				go drop(dropped)
				cypherClosePhaseWithin(t, "the held catalog command", entered)
				go func() { closed <- manager.Close() }()
				cypherClosePhaseWithin(t, "native Close", hold.CloseEntered())
				hold.Release()
				require.NoError(t, cypherClosePhaseWithin(t, "Close with DROP DATABASE in flight", closed))
				dropErr = cypherClosePhaseWithin(t, "DROP DATABASE across Close", dropped)
			case "after":
				go drop(dropped)
				require.NoError(t, cypherClosePhaseWithin(t, "DROP DATABASE", dropped))
				go func() { closed <- manager.Close() }()
				require.NoError(t, cypherClosePhaseWithin(t, "Close after DROP DATABASE", closed))
			}

			again, err := multidb.NewDatabaseManager(t.Context(), open(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = again.Close() })
			if again.Exists("target") {
				require.NotEqual(t, "after", phase, "an acknowledged drop is durable")
				require.False(t, phase == "during" && dropErr == nil, "a drop acknowledged across Close is durable")
				live, err := again.GetStorage("target")
				require.NoError(t, err)
				_, err = live.GetNode("old")
				require.NoError(t, err)
				return
			}
			require.NotEqual(t, "before", phase, "a refused drop removed nothing")
			require.NoError(t, again.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "target"}))
		})
	}
}

func cypherClosePhaseWithin[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
	}
	var zero T
	return zero
}
