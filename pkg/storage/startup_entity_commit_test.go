package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAcceptedWALNativeStartupLineage(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "buffered"}[buffered], func(t *testing.T) {
			dir := t.TempDir()
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !buffered, manualDrain: true})
			w := b.bufferedCommitWAL
			owner := NewWALEngine(b, w)
			mode := CommitModeDirect
			if buffered {
				mode = CommitModeImplicit
			}
			tx, err := BeginEntityTransaction(b, mode)
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			_, err = tx.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(9)}})
			require.NoError(t, err)
			p, err := tx.PrepareEntityCommit(context.Background(), "startup-lineage")
			require.NoError(t, err)
			r, err := owner.SubmitEntityCommit(context.Background(), p, nil)
			require.NoError(t, err)
			require.NoError(t, r.Wait(context.Background()))
			seq := w.Sequence()
			if buffered {
				require.True(t, tx.AcknowledgedBuffered())
				require.Positive(t, b.writeBehind.PendingOps())
			}
			crashClose(t, b, w)
			b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !buffered, manualDrain: true})
			w = b.bufferedCommitWAL
			n, err := RecoverBufferedCommitsAtOpen(b, w)
			require.NoError(t, err)
			if buffered {
				require.Positive(t, n)
			}
			node, err := b.GetStoredNode("test:a")
			require.NoError(t, err)
			require.Equal(t, int64(9), node.Properties["v"])
			count, err := b.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
			require.Equal(t, seq, w.Sequence())
			if buffered {
				require.Zero(t, b.writeBehind.PendingOps())
			}
			n, err = RecoverBufferedCommitsAtOpen(b, w)
			require.NoError(t, err)
			require.Zero(t, n)
			_, _, err = NewWALEngine(b, w).DeleteByPrefix("test:")
			require.NoError(t, err)
			require.NoError(t, b.Close())
			require.NoError(t, w.Close())
			b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !buffered, manualDrain: true})
			w = b.bufferedCommitWAL
			_, err = RecoverBufferedCommitsAtOpen(b, w)
			require.NoError(t, err)
			_, err = b.GetStoredNode("test:a")
			require.ErrorIs(t, err, ErrNotFound)
			count, err = b.NodeCount()
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}

func TestAcceptedBufferedReceiptVisibleBeforeFlush(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	tx, err := BeginEntityTransaction(b, CommitModeImplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:a"})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), "live-receipt")
	require.NoError(t, err)
	r, err := b.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	require.NoError(t, r.Wait(context.Background()))
	require.True(t, tx.AcknowledgedBuffered())
	next, err := b.BeginTransaction()
	require.NoError(t, err)
	defer next.Rollback()
	require.NoError(t, next.SetNamespace("test"))
	exists, err := next.PrepareLogicalCommitReceipt("live-receipt")
	require.NoError(t, err)
	require.True(t, exists, "acknowledged receipt must prevent duplicate semantic application before physical flush")
}

// The durable record below is the precise crash boundary after accepted WAL
// append and before native submission. Recovery must not depend on an entity
// body or buffered physical record already existing.
func TestAcceptedWALUnappliedRecordStartup(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "buffered"}[buffered], func(t *testing.T) {
			dir := t.TempDir()
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !buffered, manualDrain: true})
			w := b.bufferedCommitWAL
			payload, err := EncodeEntityTransaction("test", "unapplied-startup", []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:new", Properties: map[string]any{"v": int64(42)}}}}, CommitModeImplicit)
			require.NoError(t, err)
			require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
			seq := w.Sequence()
			_, err = b.GetStoredNode("test:new")
			require.ErrorIs(t, err, ErrNotFound)
			crashClose(t, b, w)
			b = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !buffered, manualDrain: true})
			w = b.bufferedCommitWAL
			n, err := RecoverBufferedCommitsAtOpen(b, w)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			node, err := b.GetStoredNode("test:new")
			require.NoError(t, err)
			require.Equal(t, int64(42), node.Properties["v"])
			require.Equal(t, seq, w.Sequence())
			if buffered {
				require.Zero(t, b.writeBehind.PendingOps())
			}
			n, err = RecoverBufferedCommitsAtOpen(b, w)
			require.NoError(t, err)
			require.Zero(t, n)
		})
	}
}
