package nornicdb

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOpenRecoversNamespaceLifecycleWithoutWriteBehind(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	configuration := func(mode string) *Config {
		cfg := DefaultConfig()
		cfg.Database.AsyncWritesEnabled = mode != "sync"
		cfg.Database.StrictDurability = mode == "strict"
		cfg.Database.WALAutoCompactionEnabled = false
		cfg.Memory.DecayEnabled = false
		return cfg
	}
	if dir := os.Getenv("NORNIC_TEST_NAMESPACE_CRASH_DIR"); dir != "" {
		db, err := Open(dir, configuration(os.Getenv("NORNIC_TEST_NAMESPACE_CRASH_MODE")))
		require.NoError(t, err)
		base := db.GetBaseStorageForManager()
		manager, err := multidb.NewDatabaseManager(base, nil)
		require.NoError(t, err)
		require.NoError(t, manager.CreateDatabase("crash_a"))
		require.NoError(t, manager.CreateDatabase("crash_b"))
		for _, id := range []storage.NodeID{"crash_a:old", "crash_a:other", "crash_b:peer"} {
			_, err := base.CreateNode(&storage.Node{ID: id, Labels: []string{"BeforeCrash"}})
			require.NoError(t, err)
		}
		require.NoError(t, base.CreateEdge(&storage.Edge{ID: "crash_a:old_edge", StartNode: "crash_a:old", EndNode: "crash_a:other", Type: "OLD"}))
		// This public notification runs after the autonomous record's forced
		// flush and lifetime retirement, before physical/catalog completion.
		storage.RegisterNamespaceRetirement(base, func(string) error { return nil }, func(name string) {
			if name == "crash_a" {
				fmt.Println("crash after durable namespace journal, before physical completion")
				os.Exit(73)
			}
		})
		require.NoError(t, manager.DropDatabase("crash_a"))
		t.Fatal("DROP did not reach the crash boundary")
	}
	for _, mode := range []string{"sync", "strict", "buffered"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			child := exec.Command(os.Args[0], "-test.run=^TestOpenRecoversNamespaceLifecycleWithoutWriteBehind$")
			child.Env = append(os.Environ(), "NORNIC_TEST_NAMESPACE_CRASH_DIR="+dir, "NORNIC_TEST_NAMESPACE_CRASH_MODE="+mode)
			output, err := child.CombinedOutput()
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, "%s", output)
			require.Equal(t, 73, exited.ExitCode(), "%s", output)
			require.Contains(t, string(output), "crash after durable namespace journal")
			var unfinished uint64
			require.NoError(t, storage.VisitWALEntriesAfterFromDir(filepath.Join(dir, "wal"), 0, func(entry storage.WALEntry) error {
				if entry.Operation == storage.OpNamespaceLifecycle && entry.Database == "crash_a" {
					unfinished = entry.Sequence
				}
				return nil
			}))
			require.NotZero(t, unfinished, "actual public DROP must have durably journaled")
			db, err := Open(dir, configuration(mode))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			base := db.GetBaseStorageForManager()
			_, err = base.GetNode("crash_a:old")
			require.ErrorIs(t, err, storage.ErrNotFound, "Open must finish physical cleanup even when write-behind is disabled")
			_, err = base.GetEdge("crash_a:old_edge")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = base.GetNode("crash_b:peer")
			require.NoError(t, err)
			catalog, err := base.GetNode("system:databases:metadata")
			require.NoError(t, err)
			require.NotContains(t, catalog.Properties["data"], "crash_a", "Open must finish the committed catalog delta")
			manager, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			require.False(t, manager.Exists("crash_a"))
			require.NoError(t, manager.CreateDatabase("crash_a"))
			view, err := manager.GetStorage("crash_a")
			require.NoError(t, err)
			nodes, err := view.NodeCount()
			require.NoError(t, err)
			require.Zero(t, nodes, "recreation starts with no pre-crash entities")
			edges, err := view.EdgeCount()
			require.NoError(t, err)
			require.Zero(t, edges)
			_, err = db.Cypher(context.Background(), "RETURN 1 AS ready", nil)
			require.NoError(t, err, "public startup must finish recovery before serving traffic")
		})
	}
}
