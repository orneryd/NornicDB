package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropSnapshotPreservesRetirementBeforeMetadataReplay(t *testing.T) {
	primary, standby, bootstrap := storage.NewMemoryEngine(), storage.NewMemoryEngine(), storage.NewMemoryEngine()
	defer primary.Close()
	defer standby.Close()
	defer bootstrap.Close()
	a, err := NewStorageAdapterWithWAL(primary, t.TempDir())
	require.NoError(t, err)
	defer a.Close()
	f, err := NewStorageAdapterWithWAL(standby, t.TempDir())
	require.NoError(t, err)
	defer f.Close()
	r := NewStandaloneReplicator(DefaultConfig(), a)
	require.NoError(t, r.Start(context.Background()))
	m, err := multidb.NewDatabaseManager(NewReplicatedEngine(primary, r, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, m.CreateDatabase("snapshot_retired"))
	require.NoError(t, a.FlushWAL())
	initial, err := a.GetWALEntries(0, 100)
	require.NoError(t, err)
	applier := NewWALApplier(f)
	_, err = applier.ApplyBatch(initial)
	require.NoError(t, err)
	readOnly, err := multidb.NewDatabaseManager(&namespaceCatalogStandby{standby}, nil)
	require.NoError(t, err)
	require.NoError(t, m.DropDatabase("snapshot_retired"))
	require.NoError(t, a.FlushWAL())
	entries, err := a.GetWALEntries(initial[len(initial)-1].Position, 100)
	require.NoError(t, err)
	var drop *WALEntry
	for _, entry := range entries {
		if entry.Command.Type == CmdDeleteByPrefix {
			drop = entry
			break
		}
	}
	require.NotNil(t, drop)
	_, err = applier.ApplyBatch([]*WALEntry{drop})
	require.NoError(t, err)
	require.False(t, readOnly.Exists("snapshot_retired"))
	// A replica must not publish an independent full-catalog write from DROP
	// completion: it could overtake the primary's recreated catalog lifetime.
	node, err := standby.GetNode("system:databases:metadata")
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal([]byte(node.Properties["data"].(string)), &raw))
	require.Contains(t, raw, "snapshot_retired", "the primary's metadata-removal entry has not replayed yet")
	var snapshot bytes.Buffer
	require.NoError(t, f.WriteSnapshot(&snapshot))
	receiver, err := NewStorageAdapterWithWAL(bootstrap, t.TempDir())
	require.NoError(t, err)
	defer receiver.Close()
	require.NoError(t, receiver.RestoreSnapshot(&snapshot))
	fresh, err := multidb.NewDatabaseManager(&namespaceCatalogStandby{bootstrap}, nil)
	require.NoError(t, err)
	require.False(t, fresh.Exists("snapshot_retired"), "snapshot bootstrap must preserve committed DROP even before metadata removal replay")
}
