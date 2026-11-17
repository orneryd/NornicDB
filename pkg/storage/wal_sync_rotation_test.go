package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A segment roll-over fsyncs the active handle, then closes and replaces it.
// Sync drains the bufio writer under w.mu and then fsyncs the *captured*
// handle after releasing w.mu, so a roll-over can retire that handle between
// its two steps. The roll-over fsyncs the handle before closing it, so every
// record Sync flushed is already durable when the handle is retired: being
// retired is not a sync failure, and Sync must not report the handle's
// os.ErrClosed as its own failure.
//
// This drives the exported Append/Sync/Close paths, with MaxEntries making
// every append seal a segment, and requires that no Sync call reports an error
// and that every acknowledged record survives a reopen.
//
// Which Sync call observes a retirement depends on the scheduler and on the
// retirement owner reaching syncMu first, so this test exercises the race
// through contention and repetition rather than forcing one interleaving.
func TestWALSyncReportsNoFailureWhileSegmentsRotate(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())

	dir := filepath.Join(t.TempDir(), "wal")
	cfg := DefaultWALConfig()
	cfg.Dir = dir
	// batch: Append itself never fsyncs, Sync does. The hour-long interval
	// keeps the background loop out of the way so this test observes only its
	// own Sync calls, whose errors it can attribute.
	cfg.SyncMode = "batch"
	cfg.BatchSyncInterval = time.Hour
	// Every append seals a segment, so roll-overs overlap the Sync calls.
	cfg.MaxEntries = 1
	// The record reaches the kernel only when Sync or the roll-over flushes.
	cfg.DeferAppendFlush = true

	wal, err := NewWAL("", cfg)
	require.NoError(t, err)

	const (
		records = 64
		syncers = 4
		// Bounds the work each syncer adds while the appends run, so a slow
		// filesystem cannot turn the contention into an unbounded fsync loop.
		maxSyncsPerWorker = 1000
	)
	var (
		errsMu   sync.Mutex
		syncErrs []error
		stop     = make(chan struct{})
		workers  sync.WaitGroup
	)
	for i := 0; i < syncers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < maxSyncsPerWorker; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := wal.Sync(); err != nil {
					errsMu.Lock()
					syncErrs = append(syncErrs, err)
					errsMu.Unlock()
					return
				}
			}
		}()
	}

	for i := 1; i <= records; i++ {
		require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": i}))
	}
	close(stop)
	workers.Wait()

	errsMu.Lock()
	reported := append([]error(nil), syncErrs...)
	errsMu.Unlock()
	require.Empty(t, reported, "Sync reported a failure for an active handle that a segment roll-over had already fsynced and retired")
	require.NoError(t, wal.Close())

	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Len(t, manifest.Segments, records, "every append must have sealed a segment")

	reopened, err := NewWAL("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, uint64(records), reopened.Sequence())

	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Len(t, entries, records)
	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.Sequence)
	}
	// The journal keeps accepting records after the roll-overs and the reopen.
	require.NoError(t, reopened.Append(OpCheckpoint, map[string]any{"entry": records + 1}))
}

// A retired active handle is not a sync failure, but a journal that was closed
// is: Sync keeps reporting ErrWALClosed for its own closed state and never the
// handle's os.ErrClosed. This keeps a correction from passing the roll-over
// case by returning nil, or a closed-file error, for every retirement.
func TestWALSyncOnClosedJournalReportsWALClosed(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())

	dir := filepath.Join(t.TempDir(), "wal")
	cfg := DefaultWALConfig()
	cfg.Dir = dir
	cfg.SyncMode = "batch"
	cfg.BatchSyncInterval = time.Hour

	wal, err := NewWAL("", cfg)
	require.NoError(t, err)
	require.NoError(t, wal.Append(OpCheckpoint, map[string]any{"entry": 1}))
	require.NoError(t, wal.Close())

	require.ErrorIs(t, wal.Sync(), ErrWALClosed)
	require.NotErrorIs(t, wal.Sync(), os.ErrClosed)
}

func TestWALSyncPreservesCurrentHandleFailure(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	cfg := DefaultWALConfig()
	cfg.SyncMode = "batch"
	cfg.BatchSyncInterval = 0
	wal, err := NewWAL(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	// A broken current handle is not a successfully retired segment.
	require.NoError(t, wal.file.Close())
	require.ErrorIs(t, wal.Sync(), os.ErrClosed)
}
