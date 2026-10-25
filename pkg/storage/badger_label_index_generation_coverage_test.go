package storage

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestLabelIndexMarkerDecodeStates(t *testing.T) {
	readyMarker := encodeLabelIndexMarker(labelIndexMarkerReady, 17)
	buildingMarker := encodeLabelIndexMarker(labelIndexMarkerBuilding, 23)
	badVersion := append([]byte(nil), readyMarker...)
	badVersion[0]++
	badState := append([]byte(nil), readyMarker...)
	badState[1] = 99

	cases := []struct {
		name      string
		marker    []byte
		wantReady bool
		wantGen   uint64
		wantValid bool
	}{
		{name: "legacy ready", marker: []byte{labelIndexSchemeVersion}, wantReady: true, wantValid: true},
		{name: "ready generation", marker: readyMarker, wantReady: true, wantGen: 17, wantValid: true},
		{name: "building generation", marker: buildingMarker, wantGen: 23, wantValid: true},
		{name: "empty", marker: nil},
		{name: "short", marker: []byte{labelIndexSchemeVersion, labelIndexMarkerReady}},
		{name: "version", marker: badVersion},
		{name: "state", marker: badState},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ready, generation, valid := decodeLabelIndexMarker(tc.marker)
			require.Equal(t, tc.wantReady, ready)
			require.Equal(t, tc.wantGen, generation)
			require.Equal(t, tc.wantValid, valid)
		})
	}
}

func TestLabelIndexReadinessMaintenanceFailurePaths(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	r4EnsureLabelIndexReady(t, b)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, b.clearLabelIndexPrefixVersioned(cancelled), context.Canceled)
	require.NoError(t, b.db.oracle.waitForReadersBefore(nil, 0))
	require.NoError(t, b.db.oracle.waitForReadersBefore(nil, 1))

	failed := errors.New("commit oracle failed")
	b.db.oracle.mu.Lock()
	b.db.oracle.failed = failed
	b.db.oracle.mu.Unlock()
	require.ErrorIs(t, b.db.oracle.waitForReadersBefore(context.Background(), 1), failed)
	b.db.oracle.mu.Lock()
	b.db.oracle.failed = nil
	b.db.oracle.mu.Unlock()

	require.NoError(t, b.Close())
	_, err = b.writeLabelIndexBuildingMarker(80)
	require.Error(t, err)
	require.Error(t, b.completeLabelIndexGeneration(81))
	_, err = b.rebuildLabelIndex(context.Background())
	require.Error(t, err)
}

func TestLabelIndexSnapshotDictionaryLookupAndFallbackMerge(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer b.Close()
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Target"}})
		require.NoError(t, err)
	}
	r4EnsureLabelIndexReady(t, b)

	num, ok := b.idDict.lookupNodeNumID("test:a")
	require.True(t, ok)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		ready, admittedGeneration, err := b.labelIndexAdmissionInTxn(txn)
		require.NoError(t, err)
		require.True(t, ready)
		require.Equal(t, b.labelIndex.markerGeneration.Load(), admittedGeneration)

		readyReader := b.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", visibleRead, false)
		indexReader, ok := readyReader.(*nodeIndexReader)
		require.True(t, ok)
		require.Same(t, txn, indexReader.txn, "the reader's MVCC snapshot binds the admitted generation")
		indexReader.Close()

		key, err := b.labelIndexKeyStringLookupInTxn(txn, "Target", "test:a")
		require.NoError(t, err)
		require.NotNil(t, key)
		key, err = b.labelIndexKeyStringLookupInTxn(txn, "Target", "test:absent")
		require.NoError(t, err)
		require.Nil(t, key)

		id, found := b.labelIndexNodeID(num)
		require.True(t, found)
		require.Equal(t, NodeID("test:a"), id)
		_, found = b.labelIndexNodeID(^uint64(0))
		require.False(t, found)
		return nil
	}))

	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeIDForwardKey("test:a"), []byte{1})
	}))
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := b.labelIndexKeyStringLookupInTxn(txn, "Target", "test:a")
		require.ErrorIs(t, err, ErrInvalidData)
		return nil
	}))
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeIDForwardKey("test:a"), encodeNumID(num))
	}))

	generation := b.labelIndex.markerGeneration.Load() + 1
	_, err = b.writeLabelIndexBuildingMarker(generation)
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Delete(nodeIDForwardKey("test:a")); err != nil {
			return err
		}
		return txn.Delete(nodeKey("test:b"))
	}))
	var got []NodeID
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		reader := b.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", visibleRead, true)
		defer reader.Close()
		fallback, ok := reader.(*labelFallbackReader)
		require.True(t, ok)
		require.NotNil(t, fallback)
		for reader.Next() {
			got = append(got, reader.NodeID())
		}
		return reader.Err()
	}))
	require.ElementsMatch(t, []NodeID{"test:a", "test:b", "test:c"}, got)
}

func TestLabelIndexCachedReadinessAndHasLabelFallback(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer b.Close()
	_, err = b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:other", Labels: []string{"Other"}})
	require.NoError(t, err)
	r4EnsureLabelIndexReady(t, b)

	collect := func() []NodeID {
		t.Helper()
		var ids []NodeID
		require.NoError(t, b.ForEachNodeIDByLabel("Target", func(id NodeID) bool {
			ids = append(ids, id)
			return true
		}))
		return ids
	}
	require.Equal(t, []NodeID{"test:a"}, collect())
	require.Equal(t, []NodeID{"test:a"}, collect()) // validated cached first ID

	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "Target", "test:absent")
	require.Equal(t, []NodeID{"test:a"}, collect()) // no dictionary entry
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "Target", "test:other")
	require.Equal(t, []NodeID{"test:a"}, collect()) // dictionary entry, no label key

	generation := b.labelIndex.markerGeneration.Load() + 1
	_, err = b.writeLabelIndexBuildingMarker(generation)
	require.NoError(t, err)
	got, err := b.HasLabelBatch([]NodeID{"test:a", "test:other", "test:absent"}, "Target")
	require.NoError(t, err)
	require.Equal(t, map[NodeID]bool{"test:a": true}, got)
	count, err := b.NodeCountByLabel("Target")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	count, err = b.NodeCountByLabelInNamespace("test", "Target")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.NoError(t, b.completeLabelIndexGeneration(generation))

	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)
	got, err = b.HasLabelBatch([]NodeID{"test:a", "test:other"}, "Target")
	require.NoError(t, err)
	require.Equal(t, map[NodeID]bool{"test:a": true}, got)
}

func TestLabelIndexInvalidPersistedMarkerUsesFallback(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer b.Close()
	_, err = b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(labelIndexReadyKey, []byte{labelIndexSchemeVersion, 99})
	}))
	b.invalidateLabelIndexReadiness()
	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	require.False(t, ready)
	got, err := b.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestLabelIndexDeletePrefixReadyAndBuildingPaths(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer b.Close()
	for _, node := range []*Node{
		{ID: "test:drop:a", Labels: []string{"Target"}},
		{ID: "test:keep:b", Labels: []string{"Target"}},
	} {
		_, err := b.CreateNode(node)
		require.NoError(t, err)
	}
	r4EnsureLabelIndexReady(t, b)

	nodesDeleted, edgesDeleted, err := b.DeleteByPrefix("test:drop:")
	require.NoError(t, err)
	require.EqualValues(t, 1, nodesDeleted)
	require.Zero(t, edgesDeleted)
	ready, err := b.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)
	count, err := b.NodeCountByLabel("Target")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	generation := b.labelIndex.markerGeneration.Load() + 1
	_, err = b.writeLabelIndexBuildingMarker(generation)
	require.NoError(t, err)
	nodesDeleted, edgesDeleted, err = b.DeleteByPrefix("test:keep:")
	require.NoError(t, err)
	require.EqualValues(t, 1, nodesDeleted)
	require.Zero(t, edgesDeleted)
	ready, err = b.labelIndexReady()
	require.NoError(t, err)
	require.True(t, ready)
}

func TestLabelIndexReadinessFailureConsumers(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer b.Close()
	_, err = b.CreateNode(&Node{ID: "test:a", Labels: []string{"Target"}})
	require.NoError(t, err)
	r4EnsureLabelIndexReady(t, b)

	readTs := b.db.oracle.published.Load()
	txn := b.db.DB.NewTransactionAt(readTs, false)
	txn.Discard()
	b.invalidateLabelIndexReadiness()
	_, err = b.labelIndexReadyInTxn(txn)
	require.Error(t, err)
	_, err = b.labelNumIDsForDictionaryCheck(txn)
	require.Error(t, err)
	_, err = b.labelIndexKeyStringLookupInTxn(txn, "Target", "test:a")
	require.Error(t, err)

	generation := b.labelIndex.markerGeneration.Load() + 1
	_, err = b.writeLabelIndexBuildingMarker(generation)
	require.NoError(t, err)
	_, found, err := b.lostNodeNumID("test:a", nil)
	require.NoError(t, err)
	require.False(t, found)

	b.db.oracle.mu.Lock()
	b.db.oracle.readsHeld = true
	b.db.oracle.mu.Unlock()
	b.invalidateLabelIndexReadiness()
	_, err = b.NodeCountByLabel("Target")
	require.Error(t, err)
	_, err = b.NodeCountByLabelInNamespace("test", "Target")
	require.Error(t, err)
	_, err = b.writeLabelIndexBuildingMarker(generation + 1)
	require.Error(t, err)
	require.Error(t, b.completeLabelIndexGeneration(generation+1))
	_, err = b.rebuildLabelIndex(context.Background())
	require.Error(t, err)
	b.db.oracle.mu.Lock()
	b.db.oracle.readsHeld = false
	b.db.oracle.mu.Unlock()
}
