package multidb

import (
	"encoding/json"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatalogRegistrationSnapshotReopenConsistency(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		for _, unrelated := range []bool{false, true} {
			t.Run(fmt.Sprintf("reopen=%v/unrelated=%v", reopen, unrelated), func(t *testing.T) {
				dir := t.TempDir()
				open := func() *DatabaseManager {
					native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
					require.NoError(t, err)
					manager, err := NewDatabaseManager(t.Context(), native, nil)
					require.NoError(t, err)
					t.Cleanup(func() { _ = manager.Close() })
					return manager
				}
				manager := open()
				for _, database := range []string{"db1", "db2"} {
					require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: database}))
					engine, err := manager.GetStorage(database)
					require.NoError(t, err)
					_, err = engine.CreateNode(&storage.Node{ID: "marker", Properties: map[string]interface{}{"owner": database}})
					require.NoError(t, err)
				}
				admitted := []ConstituentRef{{Alias: "Winner", Type: "local", AccessMode: "read_write", DatabaseName: "db1"}, {Alias: "winner", Type: "local", AccessMode: "read_write", DatabaseName: "db2"}}
				require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "cmp", admitted))
				stored := func() []ConstituentRef {
					system, err := manager.GetStorage("system")
					require.NoError(t, err)
					node, err := system.GetNode(metadataNodeID)
					require.NoError(t, err)
					var catalog map[string]*DatabaseInfo
					require.NoError(t, json.Unmarshal([]byte(node.Properties["data"].(string)), &catalog))
					return catalog["cmp"].Constituents
				}
				assert.Equal(t, admitted, stored(), "durable snapshot retains exact admitted bytes and registration order")
				visible, err := manager.GetCompositeConstituents("cmp")
				require.NoError(t, err)
				require.Equal(t, admitted[1:], visible)
				info, err := manager.GetDatabase("cmp")
				require.NoError(t, err)
				require.Equal(t, admitted[1:], info.Constituents)
				for _, listed := range [][]*DatabaseInfo{manager.ListDatabases(), manager.ListCompositeDatabases()} {
					found := false
					for _, item := range listed {
						if item.Name == "cmp" {
							found = true
							require.Equal(t, admitted[1:], item.Constituents)
						}
					}
					require.True(t, found)
				}
				info.Constituents[0].DatabaseName = "caller-mutation"
				require.Equal(t, admitted, stored(), "presentation snapshots cannot change durable registrations")
				if unrelated {
					require.NoError(t, manager.CreateAlias(t.Context(), "unrelated", "db1"))
					assert.Equal(t, admitted, stored(), "another catalog mutation must not drop hidden registrations")
				}
				if reopen {
					require.NoError(t, manager.Close())
					manager = open()
					assert.Equal(t, admitted, stored(), "reopening retains full stored state")
				}
				view, err := manager.GetStorage("cmp.winner")
				require.NoError(t, err)
				node, err := view.GetNode("marker")
				require.NoError(t, err)
				require.Equal(t, "db2", node.Properties["owner"])
				require.NoError(t, manager.RemoveConstituent(t.Context(), "cmp", "winner"))
				visible, err = manager.GetCompositeConstituents("cmp")
				require.NoError(t, err)
				assert.Equal(t, admitted[:1], visible, "removing winner exposes the earlier admitted registration")
				assert.Equal(t, admitted[:1], stored(), "ordinary removal persists exactly the remaining registration")
				view, err = manager.GetStorage("cmp.Winner")
				if assert.NoError(t, err) {
					node, err = view.GetNode("marker")
					require.NoError(t, err)
					assert.Equal(t, "db1", node.Properties["owner"])
				}
			})
		}
	}
}
