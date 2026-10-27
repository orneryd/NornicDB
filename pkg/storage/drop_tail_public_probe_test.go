package storage_test

import (
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A DROP that physically removes entities but cannot finish its intrinsic
// cleanup must fence new tenant writes. The unrelated namespace stays usable.
func TestReview1406FailedDropTailRetainsNamespaceFence(t *testing.T) {
	for _, route := range []string{"native-prefix", "managed-drop"} {
		t.Run(route, func(t *testing.T) {
			base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant"}))
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "peer"}))
			tenant, err := manager.GetStorage("tenant")
			require.NoError(t, err)
			peer, err := manager.GetStorage("peer")
			require.NoError(t, err)
			require.NoError(t, tenant.GetSchema().AddUniqueConstraint("tenant_uid", "Person", "uid"))
			_, err = tenant.CreateNode(&storage.Node{ID: "old-a", Labels: []string{"Person"}, Properties: map[string]any{"uid": "old-a"}})
			require.NoError(t, err)
			_, err = tenant.CreateNode(&storage.Node{ID: "old-b", Labels: []string{"Person"}, Properties: map[string]any{"uid": "old-b"}})
			require.NoError(t, err)
			require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "old-edge", StartNode: "old-a", EndNode: "old-b", Type: "REL"}))
			_, err = peer.CreateNode(&storage.Node{ID: "keep"})
			require.NoError(t, err)
			require.NoError(t, base.FlushWriteBehind())
			injected := errors.New("intrinsic DROP cleanup unavailable")
			calls := 0
			storage.ReviewSetDropTailFailure(base, func(prefix string) error {
				require.Equal(t, "tenant:", prefix)
				calls++
				return injected
			})
			if route == "managed-drop" {
				err = manager.DropDatabase(t.Context(), "tenant")
			} else {
				_, _, err = base.DeleteByPrefix("tenant:")
			}
			require.ErrorIs(t, err, injected)
			require.Equal(t, 1, calls)
			for _, entity := range []struct {
				kind storage.StoredEntityKind
				id   string
			}{{storage.StoredEntityNodeKind, "tenant:old-a"}, {storage.StoredEntityNodeKind, "tenant:old-b"}, {storage.StoredEntityEdgeKind, "tenant:old-edge"}} {
				exists, err := storage.ReviewRawEntityExists(base, entity.kind, entity.id)
				require.NoError(t, err)
				require.False(t, exists, "initial physical deletion must precede injected failure: %s", entity.id)
			}
			exists, err := storage.ReviewRawEntityExists(base, storage.StoredEntityNodeKind, "peer:keep")
			require.NoError(t, err)
			require.True(t, exists)
			require.ErrorIs(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "tenant"}), multidb.ErrDatabaseExists, "failed DROP retains original catalog entry")
			_, err = peer.CreateNode(&storage.Node{ID: "after"})
			require.NoError(t, err, "peer namespace must remain writable")
			// These are NEW transactions after the failure, not stale open snapshots.
			_, nativeErr := base.CreateNode(&storage.Node{ID: "tenant:new-native"})
			_, managedErr := tenant.CreateNode(&storage.Node{ID: "new-managed"})
			assert.Error(t, nativeErr, "postphysical cleanup failure must fence new native tenant writes")
			assert.Error(t, managedErr, "postphysical cleanup failure must fence new managed tenant writes")
		})
	}
}
