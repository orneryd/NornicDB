package storage

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNamespaceRecoveryObligationPinsAllRetirementPaths(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, segmented := range []bool{false, true} {
		t.Run(map[bool]string{false: "rewrite", true: "segments"}[segmented], func(t *testing.T) {
			root := t.TempDir()
			base, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(root, "data"), LowMemory: true})
			require.NoError(t, err)
			cfg := &WALConfig{SyncMode: "none"}
			if segmented {
				cfg.RetentionMaxAge = time.Nanosecond
			}
			wal, err := NewWAL(filepath.Join(root, "wal"), cfg)
			require.NoError(t, err)
			engine := NewWALEngine(base, wal)
			defer func() {
				if engine != nil {
					require.NoError(t, engine.Close())
				}
			}()
			for _, id := range []NodeID{"obligation_a:first", "obligation_a:remaining", "obligation_b:peer"} {
				_, err = engine.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			op := NewNamespaceDrop(nil)
			op.nativeWAL = wal
			require.NoError(t, base.PrepareNamespaceDrop(context.Background(), "obligation_a:", op))
			release, err := base.beginSchemaWriteContext(context.Background())
			require.NoError(t, err)
			err = op.commitLocked(context.Background())
			release()
			require.NoError(t, err)
			require.NotZero(t, op.nativeSequence)
			require.NoError(t, base.db.DropPrefix(append([]byte{prefixNode}, []byte("obligation_a:first")...)))
			require.Error(t, op.physicalComplete(context.Background(), errors.New("cleanup interrupted")))
			for attempt := 0; attempt < 2; attempt++ {
				require.ErrorIs(t, wal.TruncateAfterSnapshot(wal.sequence.Load()), ErrConflict)
				_, err = wal.CreateSnapshot(base)
				require.ErrorIs(t, err, ErrConflict)
				var snapshot bytes.Buffer
				require.ErrorIs(t, WriteSnapshot(context.Background(), base, &snapshot, SnapshotOptions{Sequence: wal.sequence.Load()}), ErrConflict)
				require.Empty(t, snapshot.Bytes(), "do not publish a recovery-complete snapshot header")
				require.ErrorIs(t, base.Backup(filepath.Join(root, "unsafe.backup")), ErrConflict)
			}
			require.NoError(t, engine.Close())
			engine = nil
			base, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: filepath.Join(root, "data"), LowMemory: true})
			require.NoError(t, err)
			wal, err = NewWAL(filepath.Join(root, "wal"), cfg)
			require.NoError(t, err)
			// The raw WAL refuses even before binding a physical owner.
			require.ErrorIs(t, wal.TruncateAfterSnapshot(wal.sequence.Load()), ErrConflict)
			engine = NewWALEngine(base, wal)
			require.ErrorIs(t, wal.TruncateAfterSnapshot(wal.sequence.Load()), ErrConflict)
			_, err = RecoverBufferedCommits(base, wal)
			require.NoError(t, err)
			_, err = base.GetNode("obligation_a:remaining")
			require.ErrorIs(t, err, ErrNotFound)
			_, err = base.GetNode("obligation_b:peer")
			require.NoError(t, err)
			require.NoError(t, wal.TruncateAfterSnapshot(wal.sequence.Load()), "intrinsic recovery completion discharges the obligation")
		})
	}
}
