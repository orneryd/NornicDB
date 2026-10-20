package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestXHighFailedDropSurvivesCompaction(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact=%v", compact), func(t *testing.T) {
			dir := t.TempDir()
			base, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir + "/badger", LowMemory: true})
			require.NoError(t, err)
			wal, err := NewWAL(dir+"/wal", &WALConfig{SyncMode: "none", SnapshotInterval: time.Hour})
			require.NoError(t, err)
			engine := NewWALEngine(base, wal)
			defer func() {
				if engine != nil {
					require.NoError(t, engine.Close())
				}
			}()
			ctx := context.Background()
			record := &Node{ID: "system:databases:metadata", Properties: map[string]any{"type": "databases"}}
			oldValue := json.RawMessage(`{"id":"old","name":"compact_a"}`)
			require.NoError(t, PublishNamespaceCatalogContext(ctx, engine, NamespaceCatalogMutation{Namespace: "compact_a", Record: record, Create: true, Value: oldValue}))
			for _, id := range []NodeID{"compact_a:first", "compact_a:remaining", "compact_b:peer"} {
				_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Old"}})
				require.NoError(t, err)
			}
			op := NewNamespaceDrop(nil)
			op.nativeWAL = wal
			op.SetCatalogMutation(NamespaceCatalogMutation{Namespace: "compact_a", Record: record, Expected: oldValue})
			require.NoError(t, base.PrepareNamespaceDrop(ctx, "compact_a:", op))
			release, err := base.beginSchemaWriteContext(ctx)
			require.NoError(t, err)
			err = op.commitLocked(ctx)
			release()
			require.NoError(t, err)
			require.True(t, op.Committed())
			require.NotZero(t, op.nativeSequence)
			sequence := op.nativeSequence
			// Same admitted partial-cleanup seam as the committed intrinsic-failure
			// regression: one durable body is deleted, another remains for replay.
			require.NoError(t, base.db.DropPrefix(append([]byte{prefixNode}, []byte("compact_a:first")...)))
			require.Error(t, op.physicalComplete(ctx, errors.New("transient physical cleanup I/O failure")))
			_, err = engine.CreateNode(&Node{ID: "compact_a:blocked"})
			require.ErrorIs(t, err, ErrConflict)
			if compact {
				require.NoError(t, engine.EnableAutoCompaction(dir+"/snapshots"))
				// Invoke the exact automatic owner deterministically. Refusing to
				// compact unfinished work is allowed; losing its intent is not.
				t.Logf("compaction result: %v", engine.createSnapshotAndCompact())
				engine.DisableAutoCompaction()
			}
			require.NoError(t, engine.Close())
			engine = nil
			base, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir + "/badger", LowMemory: true})
			require.NoError(t, err)
			wal, err = NewWAL(dir+"/wal", &WALConfig{SyncMode: "none"})
			require.NoError(t, err)
			engine = NewWALEngine(base, wal)
			_, err = RecoverBufferedCommits(base, wal)
			require.NoError(t, err)
			done, err := base.namespaceCompleted("compact_a", sequence)
			require.NoError(t, err)
			if !done {
				t.Error("reopen lost the unfinished lifecycle record instead of completing cleanup")
			}
			require.NoError(t, PublishNamespaceCatalogContext(ctx, engine, NamespaceCatalogMutation{Namespace: "compact_a", Record: record, Create: true, Value: json.RawMessage(`{"id":"new","name":"compact_a"}`)}))
			fresh := NewNamespacedEngine(engine, "compact_a")
			_, err = fresh.CreateNode(&Node{ID: "fresh"})
			require.NoError(t, err)
			_, err = fresh.GetNode("remaining")
			require.ErrorIs(t, err, ErrNotFound, "recreated namespace must not expose an old lifetime's remaining body")
			_, err = base.GetNode("compact_b:peer")
			require.NoError(t, err)
		})
	}
}
