package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A synchronous commit holds its write-order stripes until its Badger commit
// is done and its notifications are reserved, not through an explicit
// COMMIT's fsync: the write is visible to every reader from its Badger commit
// on, so a write-behind commit of the same entity acknowledged from then on
// is later than it. Such a commit is acknowledged without waiting for the
// fsync and stays ordered after the synchronous write in reads, after the
// flush, and after a crash and replay of the write-behind log.
func TestBufferedCommitAfterSynchronousCommitPointIsOrderedAfterIt(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	_, err := engine.CreateNode(&Node{ID: "test:n", Properties: map[string]any{"v": "initial"}})
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind())

	update := func(implicit bool, v string) error {
		tx, err := engine.BeginTransaction()
		if err != nil {
			return err
		}
		if implicit {
			if err := tx.SetImplicit(true); err != nil {
				return err
			}
		}
		if err := tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:n", Properties: map[string]any{"v": v}}, Policy: ClientNodeEdits}); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	acknowledged := make(chan error, 1)
	var once sync.Once
	restore := setCommitTailHook(func() {
		once.Do(func() {
			// The explicit commit is in Badger; its fsync has not run.
			done := make(chan error, 1)
			go func() { done <- update(true, "buffered") }()
			select {
			case err := <-done:
				acknowledged <- err
			case <-time.After(30 * time.Second):
				acknowledged <- errors.New("a write-behind commit of the same entity waited for the explicit commit's fsync")
			}
		})
	})
	require.NoError(t, update(false, "explicit"))
	restore()
	require.NoError(t, <-acknowledged)

	// Read past the node cache: its tail publication is a separate,
	// inherited issue (bug queue: stale node cache after a commit tail).
	engine.invalidateCachesAfterRestore()
	stored, err := engine.GetStoredNode("test:n")
	require.NoError(t, err)
	require.Equal(t, "buffered", stored.Properties["v"], "the later acknowledged commit is the newest body")

	// Crash before the flush: replay lands the buffered commit after the
	// explicit commit already in Badger.
	crashClose(t, engine, wal)
	recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal2 := recovered.bufferedCommitWAL
	t.Cleanup(func() { _ = wal2.Close(); _ = recovered.Close() })
	_, err = RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	recovered.invalidateCachesAfterRestore()
	stored, err = recovered.GetStoredNode("test:n")
	require.NoError(t, err)
	require.Equal(t, "buffered", stored.Properties["v"], "replay keeps the acknowledged order")
}
