package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An application wrapper may forward catalog publication while exposing only
// Engine for deletion and hiding optional callback/unwrapping capabilities.
type legacyLifecycleEngine struct{ storage.Engine }

func (e *legacyLifecycleEngine) PublishNamespaceCatalogContext(ctx context.Context, mutation storage.NamespaceCatalogMutation) error {
	return storage.PublishNamespaceCatalogContext(ctx, e.Engine, mutation)
}

func TestNamespaceLegacyDropCompletesWithoutRetirementCallback(t *testing.T) {
	for _, composite := range []bool{false, true} {
		t.Run(map[bool]string{false: "standard", true: "composite"}[composite], func(t *testing.T) {
			base := &legacyLifecycleEngine{Engine: storage.NewMemoryEngine()}
			manager, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			require.NoError(t, manager.CreateDatabase("legacy_peer"))
			if composite {
				require.NoError(t, manager.CreateCompositeDatabase("legacy_target", []multidb.ConstituentRef{{Alias: "peer", DatabaseName: "legacy_peer", Type: "local", AccessMode: "read_write"}}))
			} else {
				require.NoError(t, manager.CreateDatabase("legacy_target"))
				_, err = base.CreateNode(&storage.Node{ID: "legacy_target:old"})
				require.NoError(t, err)
			}
			_, err = base.CreateNode(&storage.Node{ID: "legacy_peer:kept"})
			require.NoError(t, err)
			adminView, err := manager.GetStorage("system")
			require.NoError(t, err)
			admin := NewStorageExecutor(adminView)
			admin.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			// Completion must arrive on its own. Cancellation only cleans up a
			// failed old implementation; it must never decide a successful DROP.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := admin.Execute(ctx, "DROP DATABASE legacy_target", nil); done <- err }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Second):
				cancel()
				err := <-done
				t.Fatalf("DROP awaited an unavailable callback: %v", err)
			}
			require.False(t, manager.Exists("legacy_target"))
			_, err = base.GetNode("legacy_target:old")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = base.GetNode("legacy_peer:kept")
			require.NoError(t, err)
			catalog, err := base.GetNode("system:databases:metadata")
			require.NoError(t, err)
			require.NotContains(t, catalog.Properties["data"], "legacy_target")
			require.NoError(t, manager.CreateDatabase("legacy_target"))
			view, err := manager.GetStorage("legacy_target")
			require.NoError(t, err)
			count, err := view.NodeCount()
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}
