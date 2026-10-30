package multidb

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Close-phase check of the multidb
// accepted-work consumer: DROP DATABASE through the DROP owner with engine
// Close before, during (its accepted catalog command held in flight) and after
// the drop. Every phase returns (Close never waits on work it cannot end), a
// drop is never acknowledged and then lost, and the reopened catalog is
// coherent: the name is a live database whose data reads back, or absent and
// reusable.
func TestAcceptedDropClosePhases(t *testing.T) {
	for _, phase := range []string{"before", "during", "after"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *storage.BadgerEngine {
				t.Helper()
				native, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				fixtureowner.Own(t, native)
				return native
			}
			hold := storagefault.NewCloseHold(open(), "system")
			manager, err := NewDatabaseManager(t.Context(), hold, nil)
			require.NoError(t, err)
			t.Cleanup(hold.Release)
			require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}))
			view, err := manager.GetStorage("target")
			require.NoError(t, err)
			_, err = view.CreateNode(&storage.Node{ID: "old"})
			require.NoError(t, err)

			dropped := make(chan error, 1)
			closed := make(chan error, 1)
			var dropErr error
			switch phase {
			case "before":
				require.NoError(t, manager.Close())
				go func() { dropped <- manager.DropDatabase(context.Background(), "target") }()
				require.Error(t, closePhaseWithin(t, "DROP on a closed owner", dropped), "a closed owner cannot acknowledge a drop")
			case "during":
				entered := hold.Arm()
				go func() { dropped <- manager.DropDatabase(context.Background(), "target") }()
				closePhaseWithin(t, "the held catalog command", entered)
				go func() { closed <- manager.Close() }()
				closePhaseWithin(t, "native Close", hold.CloseEntered())
				hold.Release()
				require.NoError(t, closePhaseWithin(t, "Close with a drop in flight", closed))
				dropErr = closePhaseWithin(t, "DROP across Close", dropped)
			case "after":
				go func() { dropped <- manager.DropDatabase(context.Background(), "target") }()
				require.NoError(t, closePhaseWithin(t, "DROP", dropped))
				go func() { closed <- manager.Close() }()
				require.NoError(t, closePhaseWithin(t, "Close after a drop", closed))
			}

			again, err := NewDatabaseManager(t.Context(), open(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = again.Close() })
			if again.Exists("target") {
				require.NotEqual(t, "after", phase, "an acknowledged drop is durable")
				require.False(t, phase == "during" && dropErr == nil, "a drop acknowledged across Close is durable")
				live, err := again.GetStorage("target")
				require.NoError(t, err)
				_, err = live.GetNode("old")
				require.NoError(t, err, "a database that survived Close keeps its data")
				return
			}
			require.NotEqual(t, "before", phase, "a drop refused by a closed owner removed nothing")
			require.NoError(t, again.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}), "a finished drop leaves the name reusable")
		})
	}
}

func closePhaseWithin[T any](t *testing.T, what string, ch <-chan T) T {
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
