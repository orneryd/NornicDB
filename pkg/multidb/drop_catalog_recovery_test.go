package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestDropCatalogFailureAndRecovery(t *testing.T) {
	for _, phase := range []string{"intent-rejected", "intent-applied-ack-error", "physical-rejected", "catalog-completion-rejected"} {
		for _, recovery := range []string{"retry", "reopen", "restore"} {
			t.Run(phase+"/"+recovery, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "store")
				open := func() (*storage.BadgerEngine, *managedCatalogFailureEngine, *DatabaseManager) {
					base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
					require.NoError(t, err)
					fixtureowner.Own(t, base)
					e := &managedCatalogFailureEngine{Engine: base}
					m, err := NewDatabaseManager(e, nil)
					require.NoError(t, err)
					t.Cleanup(func() { _ = m.Close() })
					return base, e, m
				}
				base, e, m := open()
				var victimView storage.Engine
				for _, name := range []string{"victim", "peer"} {
					require.NoError(t, m.CreateDatabase(name))
					view, err := m.GetStorage(name)
					require.NoError(t, err)
					if name == "victim" {
						victimView = view
					}
					_, err = view.CreateNode(&storage.Node{ID: "keep", Properties: map[string]interface{}{"value": name}})
					require.NoError(t, err)
				}
				e.updates = 0
				switch phase {
				case "intent-applied-ack-error":
					e.acceptedError = true
				case "intent-rejected":
					e.rejectAt = 1
				case "physical-rejected":
					e.rejectDelete = true
				case "catalog-completion-rejected":
					e.rejectAt = 2
				}
				require.ErrorIs(t, m.DropDatabase("victim"), storage.ErrConflict)
				require.True(t, m.Exists("victim"))
				if phase == "intent-rejected" {
					require.False(t, m.pendingDrops["victim"])
					view, err := m.GetStorage("victim")
					require.NoError(t, err)
					node, err := view.GetNode("keep")
					require.NoError(t, err)
					require.Equal(t, "victim", node.Properties["value"])
				} else {
					require.True(t, m.pendingDrops["victim"])
					_, err := m.GetStorage("victim")
					require.ErrorIs(t, err, ErrDatabaseOffline)
					info, err := m.GetDatabase("victim")
					require.NoError(t, err)
					require.Equal(t, "offline", info.Status)
					if recovery == "retry" {
						require.NoError(t, m.SetDatabaseStatus("victim", "online"))
						_, err = m.GetStorage("victim")
						require.ErrorIs(t, err, ErrDatabaseOffline, "status changes cannot override an unfinished DROP")
					}
				}
				if phase != "intent-rejected" {
					// A standby loads the durable admission without applying DROP,
					// initializing a live view, or publishing another catalog write.
					updates := e.updates
					before, err := base.NodeCountByPrefix("victim:")
					require.NoError(t, err)
					standby, err := NewDatabaseManager(&identityReadOnlyEngine{Engine: e}, nil)
					require.NoError(t, err)
					t.Cleanup(func() { _ = standby.Close() })
					require.True(t, standby.pendingDrops["victim"])
					_, err = standby.GetStorage("victim")
					require.ErrorIs(t, err, ErrDatabaseOffline)
					require.NotContains(t, standby.engines, "victim")
					require.Equal(t, updates, e.updates)
					after, err := base.NodeCountByPrefix("victim:")
					require.NoError(t, err)
					require.Equal(t, before, after)
				}
				e.acceptedError = false
				e.rejectAt = 0
				e.rejectDelete = false
				if recovery == "restore" {
					backup := filepath.Join(t.TempDir(), "pending-catalog.bak")
					require.NoError(t, base.Backup(backup))
					require.NoError(t, base.Restore(backup))
					if phase == "intent-rejected" {
						kept, err := victimView.GetNode("keep")
						require.NoError(t, err)
						require.Equal(t, "victim", kept.Properties["value"])
					} else {
						_, err := m.GetStorage("victim")
						require.ErrorIs(t, err, ErrDatabaseOffline, "Restore must not unmask an admitted catalog DROP")
						_, err = victimView.CreateNode(&storage.Node{ID: "must-not-appear"})
						require.Error(t, err, "the pre-DROP handle cannot resume writes after restoring a pending catalog intent")
					}
				}
				if recovery == "reopen" {
					require.NoError(t, m.Close())
					base, e, m = open()
					if phase == "intent-rejected" {
						require.True(t, m.Exists("victim"))
						node, err := base.GetNode("victim:keep")
						require.NoError(t, err)
						require.Equal(t, "victim", node.Properties["value"])
						require.NoError(t, m.DropDatabase("victim"))
					}
				} else {
					require.NoError(t, m.DropDatabase("victim"))
				}
				require.False(t, m.Exists("victim"))
				_, err := base.GetNode("victim:keep")
				require.ErrorIs(t, err, storage.ErrNotFound)
				peer, err := base.GetNode("peer:keep")
				require.NoError(t, err)
				require.Equal(t, "peer", peer.Properties["value"])
				require.NoError(t, m.CreateDatabase("victim"))
				view, err := m.GetStorage("victim")
				require.NoError(t, err)
				count, err := view.NodeCount()
				require.NoError(t, err)
				require.Zero(t, count)
				_, err = view.CreateNode(&storage.Node{ID: "fresh", Properties: map[string]interface{}{"value": "new"}})
				require.NoError(t, err)
				require.NoError(t, m.Close())
				base, _, m = open()
				fresh, err := base.GetNode("victim:fresh")
				require.NoError(t, err)
				require.Equal(t, "new", fresh.Properties["value"])
				require.False(t, m.pendingDrops["victim"])
			})
		}
	}
}
