package storage

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// newWriteBehindWALTestEngine builds a write-behind BadgerEngine with a WAL
// wired in (as NewWALEngine does) so buffered commits log OpBufferedCommit
// payloads before their ACK.
func newWriteBehindWALTestEngine(t *testing.T, dir string, interval time.Duration) (*BadgerEngine, *WAL) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             dir,
		WriteBehind:         true,
		WriteBehindInterval: interval,
		// Fixtures seeded as pre-V4 stores reopen as an upgrade would.
		AllowStorageUpgrade: true,
	})
	require.NoError(t, err)
	walCfg := DefaultWALConfig()
	walCfg.Dir = dir + "/wal"
	walCfg.BatchSyncInterval = time.Hour // do not fsync in the background during tests
	wal, err := NewWAL(walCfg.Dir, walCfg)
	require.NoError(t, err)
	engine.SetBufferedCommitWAL(wal)
	return engine, wal
}

// crashClose tears the engine down the way kill -9 does: stop the flusher
// without draining and discard the in-memory buffer's unflushed writes. It
// mirrors BadgerEngine.Close's teardown minus the write-behind drain. WAL is
// closed normally because its records already reached the kernel page cache
// from the forced append flush.
func crashClose(t *testing.T, engine *BadgerEngine, wal *WAL) {
	t.Helper()
	engine.stopEdgeBetweenIndexBackfill()
	engine.stopLabelIndexBackfill()
	engine.stopDeleteCleanup()
	engine.stopCountDeltaFoldWorker()
	if engine.writeBehind != nil {
		engine.writeBehind.Close() // stops the background flusher, no drain
	}
	engine.mu.RLock()
	lc := engine.lifecycleController
	engine.mu.RUnlock()
	if lc != nil {
		lc.StopLifecycle()
	}
	engine.writeBarrier.Lock()
	engine.mu.Lock()
	engine.closed = true
	db := engine.db
	engine.mu.Unlock()
	require.NoError(t, db.Close())
	engine.writeBarrier.Unlock()
	require.NoError(t, wal.Close())
}

func TestWriteBehind_BufferedCommitSurvivesCrashAndReplays(t *testing.T) {
	dir := t.TempDir()
	engine, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
	// No cleanup for engine/wal: crashClose below tears them down.

	// Buffered, acknowledged create: nothing reaches Badger, the payload
	// reaches the WAL.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(42)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())

	// The WAL must carry the buffered commit payload before the ACK.
	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	found := false
	for _, e := range entries {
		if e.Operation == OpBufferedCommit {
			found = true
		}
	}
	require.True(t, found, "expected an OpBufferedCommit entry in the WAL")

	// Simulate kill -9: the buffer is lost, Badger has nothing.
	crashClose(t, engine, wal)

	// Reopen a fresh engine + WAL on the same directories.
	recovered, wal2 := newWriteBehindWALTestEngine(t, dir, time.Hour)
	t.Cleanup(func() { _ = wal2.Close(); _ = recovered.Close() })

	// Before recovery the node is gone from Badger.
	_, err = recovered.GetNode("test:n1")
	require.Error(t, err)

	// Replay the acknowledged-but-lost commit.
	n, err := RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got, err := recovered.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(42), got.Properties["v"])

	// Derived counts were rebuilt from the committed entity.
	count, err := recovered.NodeCountByLabelInNamespace("test", "L")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestWriteBehind_RecoverSkipsFlushedCommits(t *testing.T) {
	dir := t.TempDir()
	engine, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
	t.Cleanup(func() { _ = wal.Close(); _ = engine.Close() })

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(7)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Flush lands the data AND the flush marker in Badger.
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())

	// Graceful close (flushes again — a no-op — and closes).
	require.NoError(t, wal.Close())
	require.NoError(t, engine.Close())

	recovered, wal2 := newWriteBehindWALTestEngine(t, dir, time.Hour)
	t.Cleanup(func() { _ = wal2.Close(); _ = recovered.Close() })

	// The marker says the commit is already applied: nothing to replay, and
	// no double-apply.
	n, err := RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	got, err := recovered.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(7), got.Properties["v"])

	count, err := recovered.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestReplayWALEntry_AppliesBufferedCommit(t *testing.T) {
	dir := t.TempDir()

	// Produce a buffered commit payload on a throwaway engine, then grab its
	// serialized form from the WAL.
	engine, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(99)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	entries, err := ReadWALEntriesFromDir(wal.Config().Dir)
	require.NoError(t, err)
	var bufferedEntry WALEntry
	for _, e := range entries {
		if e.Operation == OpBufferedCommit {
			bufferedEntry = e
			break
		}
	}
	require.NotEmpty(t, bufferedEntry.Data)
	crashClose(t, engine, wal)

	// A fresh Badger engine, no WAL wiring needed: ReplayWALEntry must apply
	// the payload through the buffered-commit application path.
	fresh, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })

	require.NoError(t, ReplayWALEntry(fresh, bufferedEntry))

	got, err := fresh.GetNode("test:n1")
	require.NoError(t, err)
	// Legacy integer lexemes retain exact signed-integer values through the
	// same recovery reader used by ordinary WAL entries and startup replay.
	require.Equal(t, int64(99), got.Properties["v"])
}
