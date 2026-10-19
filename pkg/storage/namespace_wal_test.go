package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNamespaceLifecycleNativeWALRecovery(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "buffered"}[buffered], func(t *testing.T) {
			t.Cleanup(config.WithWALEnabled())
			dir := t.TempDir()
			base, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			wal, err := NewWAL(dir+"/wal", DefaultWALConfig())
			require.NoError(t, err)
			engine := NewWALEngine(base, wal)
			writeNode := func(id NodeID) {
				if !buffered {
					_, err := engine.CreateNode(&Node{ID: id})
					require.NoError(t, err)
					return
				}
				tx, err := base.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace("native_a"))
				_, err = tx.CreateNode(&Node{ID: id})
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
			}
			record := &Node{ID: "system:databases:metadata", Labels: []string{"_Metadata"}, Properties: map[string]any{"type": "databases"}}
			mutation := NamespaceCatalogMutation{Namespace: "native_a", Record: record, Create: true, Value: json.RawMessage(`{"id":"old","name":"native_a"}`)}
			require.NoError(t, PublishNamespaceCatalogContext(context.Background(), engine, mutation))
			writeNode("native_a:old")
			op := NewNamespaceDrop(nil)
			op.SetCatalogMutation(NamespaceCatalogMutation{Namespace: "native_a", Record: record, Expected: mutation.Value})
			if buffered {
				_, _, err = base.DeleteByPrefixContext(WithNamespaceDrop(context.Background(), op), "native_a:")
			} else {
				_, _, err = engine.DeleteByPrefixContext(WithNamespaceDrop(context.Background(), op), "native_a:")
			}
			require.NoError(t, err)
			mutation.Value = json.RawMessage(`{"id":"new","name":"native_a"}`)
			require.NoError(t, PublishNamespaceCatalogContext(context.Background(), engine, mutation))
			writeNode("native_a:fresh")
			require.NoError(t, engine.Close())
			base, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer base.Close()
			wal, err = NewWAL(dir+"/wal", DefaultWALConfig())
			require.NoError(t, err)
			defer wal.Close()
			_, err = RecoverBufferedCommits(base, wal)
			require.NoError(t, err)
			_, err = base.GetNode("native_a:fresh")
			require.NoError(t, err, "completed old DROP must never replay over new acknowledged writes")
			_, err = base.GetNode("native_a:old")
			require.ErrorIs(t, err, ErrNotFound)
			catalog, err := base.GetNode(record.ID)
			require.NoError(t, err)
			require.Contains(t, catalog.Properties["data"], `"new"`)
			// A fresh recovery destination applies the same ordered autonomous entries.
			require.NoError(t, VisitWALEntriesAfterFromDir(wal.Config().Dir, 0, func(entry WALEntry) error {
				t.Logf("native WAL sequence=%d database=%s operation=%s", entry.Sequence, entry.Database, entry.Operation)
				if entry.Operation == OpNamespaceLifecycle {
					t.Logf("namespace payload=%s", entry.Data)
				}
				return nil
			}))
			restored := NewMemoryEngine()
			defer restored.Close()
			result, _, err := RecoverIntoEngine(restored.BadgerEngine, wal.Config().Dir, "")
			require.NoError(t, err)
			require.Zero(t, result.Failed)
			_, err = restored.GetNode("native_a:fresh")
			require.NoError(t, err)
			_, err = restored.GetNode("native_a:old")
			require.ErrorIs(t, err, ErrNotFound)
			count, err := NewNamespacedEngine(restored, "native_a").NodeCount()
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
		})
	}
}
