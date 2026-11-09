package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newObservationScopeStore returns the default database's public storage from
// DatabaseManager.GetStorage, on a plain or a write-behind store, optionally
// with storage limits so writes take the managed (size-tracking) path.
func newObservationScopeStore(t *testing.T, writeBehind, limited bool) (storage.Engine, func()) {
	t.Helper()
	ctx := t.Context()
	base, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	manager, err := NewDatabaseManager(ctx, base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	name := manager.DefaultDatabaseName()
	store, err := manager.GetStorage(name)
	require.NoError(t, err)
	enableLimits := func() {
		if limited {
			require.NoError(t, manager.SetDatabaseLimits(ctx, name, &Limits{Storage: StorageLimits{MaxNodes: 100, MaxEdges: 10}}))
		}
	}
	return store, enableLimits
}

// A fresh observed update through the public database storage succeeds with
// and without storage limits, and a stale observation is still rejected. With
// limits the update runs through the managed preparation path, which must
// scope the observation exactly as the namespaced engine does (NOR-418).
func TestManagedObservedUpdatesKeepNamespaceScope(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, limited := range []bool{false, true} {
			t.Run(fmt.Sprintf("write-behind=%t/limited=%t", writeBehind, limited), func(t *testing.T) {
				t.Run("relationship", func(t *testing.T) {
					ctx := t.Context()
					store, enableLimits := newObservationScopeStore(t, writeBehind, limited)
					for _, id := range []storage.NodeID{"a", "b"} {
						_, err := store.CreateNode(ctx, &storage.Node{ID: id})
						require.NoError(t, err)
					}
					require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "COUNT", Properties: map[string]any{"n": int64(0)}}))
					enableLimits()

					before, err := store.GetEdge(ctx, "r")
					require.NoError(t, err)
					after := storage.CopyEdge(before)
					after.Properties["n"] = int64(1)
					require.NoError(t, store.UpdateEdge(ctx, storage.EdgeUpdateRequest{Before: before, After: after}), "a fresh observed update without a peer must succeed")
					current, err := store.GetEdge(ctx, "r")
					require.NoError(t, err)
					require.Equal(t, int64(1), current.Properties["n"])
					require.Equal(t, storage.NodeID("a"), current.StartNode)
					require.Equal(t, storage.NodeID("b"), current.EndNode)

					stale := storage.CopyEdge(after)
					stale.Properties["n"] = int64(5)
					require.ErrorIs(t, store.UpdateEdge(ctx, storage.EdgeUpdateRequest{Before: before, After: stale}), storage.ErrConflict, "an observation older than the stored relationship must still conflict")
					current, err = store.GetEdge(ctx, "r")
					require.NoError(t, err)
					require.Equal(t, int64(1), current.Properties["n"])
				})
				for _, constrained := range []bool{false, true} {
					t.Run(fmt.Sprintf("node/unique=%t", constrained), func(t *testing.T) {
						ctx := t.Context()
						store, enableLimits := newObservationScopeStore(t, writeBehind, limited)
						if constrained {
							require.NoError(t, store.GetSchema().AddUniqueConstraint("item_key", "Item", "key"))
						}
						_, err := store.CreateNode(ctx, &storage.Node{ID: "n", Labels: []string{"Item"}, Properties: map[string]any{"key": "k0", "n": int64(0)}})
						require.NoError(t, err)
						enableLimits()

						before, err := store.GetNode(ctx, "n")
						require.NoError(t, err)
						after := storage.CopyNode(before)
						after.Properties["key"] = "k1"
						after.Properties["n"] = int64(1)
						require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{Before: before, After: after}), "a fresh observed node update without a peer must succeed")
						current, err := store.GetNode(ctx, "n")
						require.NoError(t, err)
						require.Equal(t, int64(1), current.Properties["n"])
						require.Equal(t, "k1", current.Properties["key"])

						if writeBehind || constrained {
							// Validation reads whole bodies with write-behind and the
							// claim key with a unique constraint; the observed key k0 is
							// no longer stored, so both reject this stale image.
							stale := storage.CopyNode(before)
							stale.Properties["n"] = int64(5)
							require.ErrorIs(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{Before: before, After: stale}), storage.ErrConflict, "an observation older than the stored node must still conflict")
							current, err = store.GetNode(ctx, "n")
							require.NoError(t, err)
							require.Equal(t, int64(1), current.Properties["n"])
						}
					})
				}
			})
		}
	}
}
