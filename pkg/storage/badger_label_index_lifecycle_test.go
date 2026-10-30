package storage

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestLabelIndexReadyProofInvalidatedByRestore(t *testing.T) {
	source, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, source.BulkCreateNodes([]*Node{
		{ID: "test:a", Labels: []string{"Target"}},
		{ID: "test:b", Labels: []string{"Target"}},
		{ID: "test:c", Labels: []string{"Target"}},
	}))
	wipeLabelIndexAndMarker(t, source, prefixLabelIndex)
	require.NoError(t, source.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(edgeBetweenIndexReadyKey)
	}))
	backup := filepath.Join(t.TempDir(), "legacy.backup")
	require.NoError(t, source.Backup(backup))
	require.NoError(t, source.Close())

	dest, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	paused, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var pauseOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	restoreHook := setCommitTailHook(func() {
		dest.db.oracle.mu.Lock()
		held := dest.db.oracle.readsHeld
		dest.db.oracle.mu.Unlock()
		if held {
			return
		}
		err := dest.withView(func(txn *badger.Txn) error {
			_, readErr := txn.Get(labelIndexReadyKey)
			return readErr
		})
		if err == badger.ErrKeyNotFound {
			pauseOnce.Do(func() {
				close(paused)
				<-resume
			})
		}
	})
	var restoreErr error
	go func() {
		restoreErr = dest.Restore(backup)
		close(finished)
	}()
	defer func() {
		release()
		restoreHook()
		<-finished
		_ = dest.Close()
	}()

	select {
	case <-paused:
	case <-time.After(time.Minute):
		t.Fatal("Restore did not reach the post-read-hold window")
	}
	nodes, err := dest.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	require.Len(t, nodes, 3, "restored markerless stores must use the complete body scan")
	release()
	select {
	case <-finished:
		require.NoError(t, restoreErr)
	case <-time.After(time.Minute):
		t.Fatal("Restore did not finish")
	}
}

func TestLabelIndexReadyReaderKeepsFallbackStateSeparate(t *testing.T) {
	require.LessOrEqual(t, unsafe.Sizeof(nodeIndexReader{}), uintptr(176),
		"ready label reads must not allocate the fallback reader's state")
}

func TestLabelIndexReadinessCacheHandlesUnknownAndStaleProofs(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: "test:cached", Labels: []string{"Target"}}}))
	require.NoError(t, engine.startIndexBackfill(startupLabelIndex, initializeIndexBackfill))

	// An unknown proof is filled from the marker in the same snapshot.
	engine.invalidateLabelIndexReadiness()
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		ready, err := engine.labelIndexReadyInTxn(txn)
		require.NoError(t, err)
		require.True(t, ready)
		return nil
	}))
	require.Equal(t, engine.labelIndex.epoch.Load(), engine.labelIndex.readinessEpoch.Load())

	generation := engine.labelIndex.epoch.Load()
	require.NotZero(t, generation)
	engine.labelIndex.readinessEpoch.Store(generation - 1)
	_, _, _, _, known := engine.cachedLabelIndexReadiness()
	require.False(t, known, "proofs from an older generation are unknown")

	engine.labelIndex.readinessEpoch.Store(generation)
	engine.labelIndex.state.Store(labelIndexReadinessReady)
	engine.labelIndex.readyAt.Store(0)
	_, _, _, _, known = engine.cachedLabelIndexReadiness()
	require.False(t, known, "a ready state without its marker version is unknown")

	engine.labelIndex.state.Store(labelIndexReadinessUnknown + 99)
	_, _, _, _, known = engine.cachedLabelIndexReadiness()
	require.False(t, known, "unrecognized states cannot authorize indexed reads")

	engine.cacheLabelIndexReadiness(generation-1, labelIndexReadinessReady, 1, 0)
	require.Equal(t, generation, engine.labelIndex.readinessEpoch.Load(),
		"a stale asynchronous proof must not replace the current generation")
}

func TestLabelFallbackReaderStopsAfterStoredError(t *testing.T) {
	wantErr := errors.New("stored scan error")
	reader := &labelFallbackReader{err: wantErr}
	require.False(t, reader.Next())
	require.ErrorIs(t, reader.Err(), wantErr)
}

func TestLabelIndexReadyCountPathsReuseReadinessProof(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:a", Labels: []string{"Rare"}},
	}))
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		reader := engine.newNodeIndexReader(context.Background(), txn, "", labelIndexPrefix("Rare"), "")
		defer reader.Close()
		require.Nil(t, reader.IndexKey(), "a reader has no item before the first candidate")
		require.True(t, reader.Next())
		require.NotEmpty(t, reader.IndexKey(), "ready candidates retain their physical index key")
		return nil
	}))

	for _, test := range []struct {
		name string
		max  float64
		read func() (int64, error)
	}{
		{
			name: "namespace",
			max:  30,
			read: func() (int64, error) { return engine.NodeCountByLabelInNamespace("test", "Rare") },
		},
		{
			name: "global",
			max:  29,
			read: func() (int64, error) { return engine.NodeCountByLabel("Rare") },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				count, err := test.read()
				require.NoError(t, err)
				require.EqualValues(t, 1, count)
			}
			allocs := testing.AllocsPerRun(100, func() {
				count, err := test.read()
				if err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("count=%d, want 1", count)
				}
			})
			t.Logf("READY_COUNT_ALLOCS %s %.0f", test.name, allocs)
			require.LessOrEqual(t, allocs, test.max,
				"ready count reads must reuse the label-index readiness proof")
		})
	}
}

func TestLabelIndexRebuildDuringReadUsesFallbackAndPreservesOldSnapshot(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:a", Labels: []string{"Target"}},
		{ID: "test:b", Labels: []string{"Target"}},
		{ID: "test:c", Labels: []string{"Target"}},
	}))

	oldSnapshot, oldReadTs, err := engine.db.beginTxn(false)
	require.NoError(t, err)
	defer func() {
		oldSnapshot.Discard()
		engine.db.endRead(oldReadTs)
	}()

	cleared, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	engine.labelIndexAfterClearHook = func() {
		close(cleared)
		<-resume
	}
	var rebuilt int
	var rebuildErr error
	go func() {
		rebuilt, rebuildErr = engine.rebuildLabelIndex(context.Background())
		close(finished)
	}()
	t.Cleanup(func() {
		release()
		<-finished
		engine.labelIndexAfterClearHook = nil
	})

	select {
	case <-cleared:
	case <-time.After(time.Minute):
		t.Fatal("label rebuild did not pause after clearing the index")
	}
	ready, err := engine.labelIndexReady()
	require.NoError(t, err)
	require.False(t, ready, "new snapshots must observe the rebuilding generation")
	current, err := engine.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	require.Len(t, current, 3, "reads during the cleared-index window must use bodies")
	namespaceCount, err := engine.NodeCountByLabelInNamespace("test", "Target")
	require.NoError(t, err)
	require.EqualValues(t, 3, namespaceCount, "namespace counts use the body fallback during rebuild")
	globalCount, err := engine.NodeCountByLabel("Target")
	require.NoError(t, err)
	require.EqualValues(t, 3, globalCount, "global counts use the body fallback during rebuild")

	release()
	select {
	case <-finished:
	case <-time.After(time.Minute):
		t.Fatal("label rebuild did not finish")
	}
	require.NoError(t, rebuildErr)
	require.Equal(t, 3, rebuilt)

	var oldIDs []NodeID
	reader := engine.newLabelIndexReader(context.Background(), oldSnapshot, "test:", "Target", "", visibleRead, true)
	defer reader.Close()
	for reader.Next() {
		oldIDs = append(oldIDs, reader.NodeID())
	}
	require.NoError(t, reader.Err())
	require.Equal(t, []NodeID{"test:a", "test:b", "test:c"}, oldIDs,
		"a read snapshot opened before the rebuild retains its complete index")
}

func TestLabelIndexReadyProofAfterNamespaceDropAndRecreate(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:old", Labels: []string{"Target"}},
	}))
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "other:kept", Labels: []string{"Target"}},
	}))
	ready, err := engine.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)
	oldGeneration := engine.labelIndex.epoch.Load()

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("test:")
	require.NoError(t, err)
	require.EqualValues(t, 1, nodesDeleted)
	require.Zero(t, edgesDeleted)
	require.Greater(t, engine.labelIndex.epoch.Load(), oldGeneration,
		"namespace reset must re-key the ready proof")

	_, err = engine.CreateNode(&Node{ID: "test:old", Labels: []string{"Target"}})
	require.NoError(t, err)
	got, err := engine.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, NodeID("test:old"), got[0].ID)
	count, err := engine.NodeCountByLabel("Target")
	require.NoError(t, err)
	require.EqualValues(t, 2, count, "the recreated namespace and untouched namespace both remain indexed")
}
