package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// countDeleteMarkers counts the Badger delete markers stored under prefix,
// across all versions.
func countDeleteMarkers(t *testing.T, eng *BadgerEngine, prefix byte) int {
	t.Helper()
	markers := 0
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		opts := badgerPrefixIteratorOptions([]byte{prefix})
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if it.Item().IsDeletedOrExpired() {
				markers++
			}
		}
		return nil
	}))
	return markers
}

// After enough deletes, once deletes stop, the clean-up leaves no delete
// marker in the node records or the label index, and a writer committing
// throughout never fails.
func TestDeleteCleanupDropsDeleteMarkers(t *testing.T) {
	eng, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer eng.Close()
	c := eng.deleteCleanup
	require.NotNil(t, c)
	c.threshold, c.quiet = 200, 500*time.Millisecond

	for i := 0; i < 50; i++ {
		_, err := eng.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:keep-%d", i)), Labels: []string{"Keep"}})
		require.NoError(t, err)
	}
	junk := make([]NodeID, 0, 300)
	for i := 0; i < 300; i++ {
		id := NodeID(fmt.Sprintf("test:junk-%d", i))
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"Junk"}})
		require.NoError(t, err)
		junk = append(junk, id)
	}
	for i := 0; i+1 < len(junk); i += 2 {
		require.NoError(t, eng.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:e-%d", i)), StartNode: junk[i], EndNode: junk[i+1], Type: "R"}))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	writeErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := eng.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:w-%d", i)), Labels: []string{"W"}}); err != nil {
				writeErr <- err
				return
			}
		}
	}()

	require.NoError(t, eng.BulkDeleteNodes(junk))
	require.Positive(t, countDeleteMarkers(t, eng, prefixNode))
	require.Eventually(t, func() bool { return c.deletes.Load() < c.threshold }, 10*time.Second, 10*time.Millisecond)
	close(stop)
	wg.Wait()
	select {
	case err := <-writeErr:
		t.Fatalf("a commit failed during the clean-up: %v", err)
	default:
	}

	require.Zero(t, countDeleteMarkers(t, eng, prefixNode))
	require.Zero(t, countDeleteMarkers(t, eng, prefixLabelIndex))
	keep, err := eng.GetNodesByLabel("Keep")
	require.NoError(t, err)
	require.Len(t, keep, 50)
}

// DropPrefix waits for a commit in progress instead of failing it, and a
// commit waits for DropPrefix.
func TestManagedDropPrefixHoldsCommitGate(t *testing.T) {
	eng, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer eng.Close()
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(deleteCleanupMarkerKey, []byte{1})
	}))

	eng.db.commitGate.RLock()
	dropped := make(chan error, 1)
	go func() { dropped <- eng.db.DropPrefix(deleteCleanupMarkerKey) }()
	select {
	case err := <-dropped:
		t.Fatalf("DropPrefix ran during a commit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	eng.db.commitGate.RUnlock()
	require.NoError(t, <-dropped)
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(deleteCleanupMarkerKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

// Closing the engine stops a clean-up that is waiting for deletes to stop;
// a clean-up that fails keeps its count; an in-memory engine runs none.
func TestDeleteCleanupLifecycle(t *testing.T) {
	eng, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	c := eng.deleteCleanup
	c.threshold, c.quiet = 1, time.Hour
	_, err = eng.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, eng.DeleteNode("test:a"))
	closed := make(chan error, 1)
	go func() { closed <- eng.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for the quiet period")
	}
	eng.stopDeleteCleanup()

	c.deletes.Store(5)
	eng.runDeleteCleanup(5)
	require.Equal(t, int64(5), c.deletes.Load())

	mem, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	require.Nil(t, mem.deleteCleanup)
	_, err = mem.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, mem.DeleteNode("test:a"))
	require.NoError(t, mem.Close())
}
