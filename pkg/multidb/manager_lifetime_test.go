package multidb

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The manager's shared scans (storage sizes, leaked system nodes) run under
// its own lifetime, rooted at the startup/process context it was given: Close
// ends it, and so does the end of that process lifetime.
func TestDatabaseManagerLifetimeEndsWithCloseAndProcess(t *testing.T) {
	wait := func(t *testing.T, ctx context.Context, what string) {
		t.Helper()
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not end the manager lifetime", what)
		}
	}
	t.Run("close", func(t *testing.T) {
		inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		manager, err := NewDatabaseManager(t.Context(), inner, nil)
		require.NoError(t, err)
		ctx := manager.lifetime().Context()
		require.NoError(t, ctx.Err())
		require.NoError(t, manager.Close())
		wait(t, ctx, "Close")
	})
	t.Run("process", func(t *testing.T) {
		process, stop := context.WithCancel(t.Context())
		inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
		manager, err := NewDatabaseManager(process, inner, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = manager.Close() })
		ctx := manager.lifetime().Context()
		stop()
		wait(t, ctx, "the process lifetime")
	})
	t.Run("literal", func(t *testing.T) {
		manager := &DatabaseManager{}
		ctx := manager.lifetime().Context()
		manager.lifetime().CloseAdmission()
		manager.lifetime().Cancel()
		wait(t, ctx, "the literal manager's own")
	})
}
