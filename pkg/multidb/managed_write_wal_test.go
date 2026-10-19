package multidb

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedWritesPreserveJournalAndPreCommitFailure(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base := storage.NewMemoryEngine()
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
	require.NoError(t, err)
	manager, err := NewDatabaseManager(storage.NewWALEngine(base, wal), nil)
	require.NoError(t, err)
	defer manager.Close()
	const db = "managed_wal"
	require.NoError(t, manager.CreateDatabase(db))
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	require.NoError(t, engine.BulkCreateNodes([]*storage.Node{{ID: "a"}, {ID: "b"}}))
	_, err = engine.CreateNode(&storage.Node{ID: "n"})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": "small"}}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
	require.NoError(t, engine.UpdateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: map[string]any{"payload": "small"}}))
	require.NoError(t, engine.DeleteEdge("e"))
	require.NoError(t, engine.DeleteNode("n"))
	read := func() []storage.WALEntry {
		t.Helper()
		require.NoError(t, wal.Sync())
		entries, err := storage.ReadWALEntriesFromDir(dir)
		require.NoError(t, err)
		var own []storage.WALEntry
		for _, entry := range entries {
			if entry.Database == db {
				own = append(own, entry)
			}
		}
		return own
	}
	entries := read()
	var operations []storage.OperationType
	for _, entry := range entries {
		switch entry.Operation {
		case storage.OpBulkNodes:
			var data storage.WALBulkNodesData
			require.NoError(t, json.Unmarshal(entry.Data, &data))
			require.Len(t, data.Nodes, 2)
			require.Equal(t, storage.NodeID("a"), data.Nodes[0].ID)
			operations = append(operations, entry.Operation)
		case storage.OpCreateNode, storage.OpUpdateNode, storage.OpCreateEdge, storage.OpUpdateEdge, storage.OpDeleteNode, storage.OpDeleteEdge:
			operations = append(operations, entry.Operation)
		}
	}
	require.Equal(t, []storage.OperationType{storage.OpBulkNodes, storage.OpCreateNode, storage.OpUpdateNode, storage.OpCreateEdge, storage.OpUpdateEdge, storage.OpDeleteEdge, storage.OpDeleteNode}, operations)
	before, _, _ := manager.GetStorageSize(db)
	require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxBytes: before}}))
	beforeEntries := len(read())
	require.ErrorIs(t, engine.UpdateNode(&storage.Node{ID: "a", Properties: map[string]any{"payload": strings.Repeat("x", 4096)}}), ErrStorageLimitExceeded)
	require.Len(t, read(), beforeEntries, "quota rejection appends no entity journal record")
	require.NoError(t, manager.SetDatabaseLimits(db, nil))
	require.NoError(t, wal.Close())
	err = engine.UpdateNode(&storage.Node{ID: "a"})
	require.ErrorIs(t, err, storage.ErrWALClosed)
	require.True(t, nornicerrors.IsCommitRolledBack(err), "journal failure precedes physical commit")
	after, _, _ := manager.GetStorageSize(db)
	require.Equal(t, before, after)
}
