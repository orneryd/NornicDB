package storage

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// restoreFixture writes a backup of nodes whose properties go through the
// property-key dictionary, and returns its path.
func restoreFixture(t *testing.T, nodes int) string {
	t.Helper()
	source, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer source.Close()
	for i := 0; i < nodes; i++ {
		_, err := source.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("restore:n%d", i)),
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"name": fmt.Sprintf("doc %d", i), "rank": i, fmt.Sprintf("key%d", i%7): true},
		})
		require.NoError(t, err)
	}
	path := filepath.Join(t.TempDir(), "nornicdb.backup")
	require.NoError(t, source.Backup(path))
	return path
}

// Readers that run while a restore replaces the store neither crash nor see
// a half-restored store: each read sees the store before or after the
// restore, or is told the store is restoring (#1020).
func TestBadgerEngine_RestoreWithConcurrentReaders(t *testing.T) {
	const nodes = 200
	backup := restoreFixture(t, nodes)
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer engine.Close()
	require.NoError(t, engine.Restore(backup))

	stop := make(chan struct{})
	var reads, restoring atomic.Int64
	var failures sync.Map
	var wg sync.WaitGroup
	reader := func(read func() error) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			err := read()
			switch {
			case err == nil:
				reads.Add(1)
			case errors.Is(err, ErrStorageRestoring):
				restoring.Add(1)
			default:
				failures.Store(err.Error(), true)
			}
		}
	}
	for worker := 0; worker < 4; worker++ {
		worker := worker
		wg.Add(3)
		go reader(func() error {
			node, err := engine.GetNode(NodeID(fmt.Sprintf("restore:n%d", worker*37%nodes)))
			if err != nil {
				return err
			}
			if node.Properties["name"] != fmt.Sprintf("doc %d", worker*37%nodes) {
				return fmt.Errorf("node read with the wrong properties: %v", node.Properties)
			}
			return nil
		})
		go reader(func() error {
			all, err := engine.AllNodes()
			if err != nil {
				return err
			}
			if len(all) != nodes {
				return fmt.Errorf("read %d nodes of %d", len(all), nodes)
			}
			return nil
		})
		go reader(func() error {
			engine.RefreshPendingEmbeddingsIndex()
			return nil
		})
	}
	for i := 0; i < 5; i++ {
		require.NoError(t, engine.Restore(backup))
	}
	close(stop)
	wg.Wait()

	failures.Range(func(key, _ any) bool {
		t.Errorf("read failed during restore: %v", key)
		return true
	})
	require.Positive(t, reads.Load())
	node, err := engine.GetNode("restore:n5")
	require.NoError(t, err)
	require.Equal(t, "doc 5", node.Properties["name"])
}

// Restore waits for an open transaction; reads that start meanwhile are
// turned away with ErrStorageRestoring, and the restore runs once the
// transaction ends.
func TestBadgerEngine_RestoreWaitsForOpenTransaction(t *testing.T) {
	backup := restoreFixture(t, 3)
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer engine.Close()
	_, err = engine.CreateNode(&Node{ID: "restore:before", Labels: []string{"Doc"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	restored := make(chan error, 1)
	go func() { restored <- engine.Restore(backup) }()
	require.Eventually(t, func() bool {
		_, err := engine.GetNode("restore:uncached")
		return errors.Is(err, ErrStorageRestoring)
	}, 5*time.Second, time.Millisecond, "store reads are turned away while the restore waits")
	_, err = engine.BeginTransaction()
	require.ErrorIs(t, err, ErrStorageRestoring)
	require.ErrorContains(t, err, "the database is being restored")
	select {
	case err := <-restored:
		t.Fatalf("restore ran with a transaction open: %v", err)
	default:
	}

	require.NoError(t, tx.Rollback())
	require.NoError(t, <-restored)
	_, err = engine.GetNode("restore:before")
	require.ErrorIs(t, err, ErrNotFound)
	node, err := engine.GetNode("restore:n1")
	require.NoError(t, err)
	require.Equal(t, "doc 1", node.Properties["name"])
}

// A transaction still open after the wait leaves the store as it was.
func TestBadgerEngine_RestoreBusyChangesNothing(t *testing.T) {
	backup := restoreFixture(t, 3)
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer engine.Close()
	_, err = engine.CreateNode(&Node{ID: "restore:kept", Labels: []string{"Doc"}})
	require.NoError(t, err)
	previous := restoreReadDrainTimeout
	restoreReadDrainTimeout = 50 * time.Millisecond
	t.Cleanup(func() { restoreReadDrainTimeout = previous })

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	err = engine.Restore(backup)
	require.ErrorContains(t, err, "restore not started: reads or transactions were still open after 50ms")
	require.NoError(t, tx.Rollback())

	_, err = engine.GetNode("restore:kept")
	require.NoError(t, err, "reads resume and the store is unchanged")
	_, err = engine.GetNode("restore:n1")
	require.ErrorIs(t, err, ErrNotFound)
}

// Restore on a closed engine reports it and touches nothing.
func TestBadgerEngine_RestoreClosedEngine(t *testing.T) {
	backup := restoreFixture(t, 1)
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, engine.Close())
	require.ErrorIs(t, engine.Restore(backup), ErrStorageClosed)
}

// While reads are held, every entry point that opens a Badger read is turned
// away, and a transaction's snapshot refresh keeps its current snapshot.
func TestManagedBadgerReadsHeld(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer engine.Close()
	_, err = engine.CreateNode(&Node{ID: "held:n", Labels: []string{"Doc"}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	snapshot := tx.snapshotTx

	db := engine.db
	db.oracle.mu.Lock()
	db.oracle.readsHeld = true
	db.oracle.mu.Unlock()
	noop := func(*badger.Txn) error { return nil }
	require.ErrorIs(t, db.View(noop), ErrStorageRestoring)
	require.ErrorIs(t, db.Update(noop), ErrStorageRestoring)
	_, err = db.Backup(&bytes.Buffer{}, 0)
	require.ErrorIs(t, err, ErrStorageRestoring)
	_, _, err = db.beginTxn(false)
	require.ErrorIs(t, err, ErrStorageRestoring)
	require.ErrorIs(t, engine.withUpdate(noop), ErrStorageRestoring)
	tx.mu.Lock()
	err = tx.refreshSnapshotLocked()
	tx.mu.Unlock()
	require.ErrorIs(t, err, ErrStorageRestoring)
	require.Same(t, snapshot, tx.snapshotTx, "the transaction keeps its snapshot")
	require.NoError(t, db.viewHeld(noop), "the loaders read past the hold")
	db.oracle.releaseReads()

	require.NoError(t, db.View(noop))
	_, err = tx.GetNode("held:n")
	require.NoError(t, err)
}

// viewHeld on a closed Badger handle reports it.
func TestManagedBadgerViewHeldClosed(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	db := engine.db
	require.NoError(t, engine.Close())
	require.ErrorIs(t, db.viewHeld(func(*badger.Txn) error { return nil }), badger.ErrDBClosed)
}
