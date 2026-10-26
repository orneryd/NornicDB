package storage_test

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). Metadata retained by a public
// transaction boundary is later encoded into the committed change stream.
func TestResidualMetadataIngressOwnership(t *testing.T) {
	for _, origin := range []string{"input", "returned"} {
		t.Run(origin, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			b.SetChangeCapture("test", storage.ChangeCaptureFull)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			metadata := map[string]any{
				"nested": map[string]any{"values": []any{"before"}},
				"typed":  []any{map[string]string{"state": "before"}},
			}
			require.NoError(t, tx.SetMetadata(metadata))
			mutated := metadata
			if origin == "returned" {
				mutated = tx.GetMetadata()
			}
			mutated["nested"].(map[string]any)["values"].([]any)[0] = "changed"
			switch value := mutated["typed"].([]any)[0].(type) {
			case map[string]string:
				value["state"] = "changed"
			case map[string]any:
				value["state"] = "changed"
			default:
				t.Fatalf("unexpected metadata map %T", value)
			}
			_, err = tx.CreateNode(&storage.Node{ID: "test:metadata-owned"})
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			events, err := b.ChangeEvents("test", 0, 0, 0)
			require.NoError(t, err)
			require.Len(t, events, 1)
			require.Equal(t, "before", events[0].TxMetadata["nested"].(map[string]any)["values"].([]any)[0])
			require.Equal(t, "map[state:before]", fmt.Sprint(events[0].TxMetadata["typed"].([]any)[0]))
		})
	}
}

// Author: #1548 implementation lane (Codex). A byte-serialization boundary
// never retains the original node, including the interval before batch commit.
func TestResidualBatchSerializationOwnsInput(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, &storage.WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	batch := wal.NewBatch()
	node := &storage.Node{ID: "test:serialized", Properties: map[string]any{"nested": map[string]any{"value": "before"}}}
	require.NoError(t, batch.AppendNode(storage.OpCreateNode, node))
	node.Properties["nested"].(map[string]any)["value"] = "changed"
	require.NoError(t, batch.Commit())
	require.NoError(t, wal.Close())
	entries, err := storage.ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	b, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	found := false
	for _, entry := range entries {
		if entry.Operation == storage.OpCreateNode {
			entry.Database = "test"
			require.NoError(t, storage.ReplayWALEntry(b, entry))
			found = true
		}
	}
	require.True(t, found)
	stored, err := b.GetStoredNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, "before", stored.Properties["nested"].(map[string]any)["value"])
}
