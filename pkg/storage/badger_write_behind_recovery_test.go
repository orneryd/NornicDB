package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBufferedRecoveryRetiresBatchBeforeReadingLaterInvalidRecord(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
	bufferedCreate(t, engine, &Node{ID: "test:bounded", Labels: []string{"L"}, Properties: map[string]any{"payload": strings.Repeat("x", 5<<20)}})
	require.NoError(t, wal.Append(OpBufferedCommit, "invalid buffered commit"))
	crashClose(t, engine, wal)

	engine = newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})

	wal = engine.bufferedCommitWAL
	replayed, err := RecoverBufferedCommits(engine, wal)
	require.ErrorContains(t, err, "unmarshal buffered commit")
	require.Equal(t, 1, replayed, "a completed bounded batch must not remain retained until WAL EOF")
	node, err := engine.GetNode("test:bounded")
	require.NoError(t, err)
	require.Len(t, node.Properties["payload"].(string), 5<<20)
	count, err := engine.NodeCountByLabelInNamespace("test", "L")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	replayed, err = RecoverBufferedCommits(engine, wal)
	require.ErrorContains(t, err, "unmarshal buffered commit")
	require.Zero(t, replayed, "retry must skip the durable batch")
}

// crashClose tears the engine down the way kill -9 does: stop the flusher
// without draining and discard the in-memory buffer's unflushed writes. It
// mirrors BadgerEngine.Close's teardown minus the write-behind drain. WAL is
// closed normally because its records already reached the kernel page cache
// from the forced append flush.
func crashClose(t testing.TB, engine *BadgerEngine, wal *WAL) {
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
	// A crash discards the buffer; later fixture cleanup must not drain it.
	engine.writeBehind = nil
	db := engine.db
	engine.mu.Unlock()
	require.NoError(t, db.Close())
	engine.writeBarrier.Unlock()
	require.NoError(t, wal.Close())
}

func TestWriteBehind_BufferedCommitSurvivesCrashAndReplays(t *testing.T) {
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
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
	recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal2 := recovered.bufferedCommitWAL

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
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL

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

	recovered := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})

	wal2 := recovered.bufferedCommitWAL

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
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true})
	wal := engine.bufferedCommitWAL
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
	fresh, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Close() })

	require.NoError(t, ReplayWALEntry(fresh, bufferedEntry))

	got, err := fresh.GetNode("test:n1")
	require.NoError(t, err)
	// Legacy integer lexemes retain exact signed-integer values through the
	// same recovery reader used by ordinary WAL entries and startup replay.
	require.Equal(t, int64(99), got.Properties["v"])
}
