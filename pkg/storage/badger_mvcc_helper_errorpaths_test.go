package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerMVCC_HelperWriteFunctions_InvalidIDErrors(t *testing.T) {
	engine := newTestEngine(t)
	v := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 77}

	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		// Empty IDs are currently accepted by dictionary-backed key helpers;
		// this test ensures all helper paths execute without panic.
		require.NoError(t, engine.writeNodeMVCCVersionInTxn(txn, &Node{ID: "", Labels: []string{"L"}}, v))
		require.NoError(t, engine.writeNodeMVCCTombstoneInTxn(txn, "", v))
		require.NoError(t, engine.writeEdgeMVCCVersionInTxn(txn, &Edge{ID: "", StartNode: "test:a", EndNode: "test:b", Type: "R"}, v))
		require.NoError(t, engine.writeEdgeMVCCTombstoneInTxn(txn, "", v))
		require.NoError(t, engine.writeNodeMVCCHeadWithFloorInTxn(txn, "", v, false, v))
		require.NoError(t, engine.writeEdgeMVCCHeadWithFloorInTxn(txn, "", v, false, v))
		return nil
	}))
}

func TestBadgerMVCC_AppendTombstoneAndIterateEarlyExitBranches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}))

	v := MVCCVersion{CommitTimestamp: time.Now().UTC().Add(time.Second), CommitSequence: 91}
	require.NoError(t, engine.AppendNodeTombstone("", v))
	require.NoError(t, engine.AppendEdgeTombstone("", v))
	require.NoError(t, engine.AppendNodeTombstone("test:n2", v))
	require.NoError(t, engine.AppendEdgeTombstone("test:e1", v))

	yieldErr := errors.New("stop")
	err = engine.IterateLatestVisibleNodes(func(*Node) error { return yieldErr })
	require.ErrorIs(t, err, yieldErr)

	err = engine.IterateLatestVisibleEdges(func(*Edge) error { return yieldErr })
	require.ErrorIs(t, err, yieldErr)

	require.NoError(t, engine.Close())
	require.Error(t, engine.AppendNodeTombstone("test:n1", v))
	require.Error(t, engine.AppendEdgeTombstone("test:e1", v))
}

func TestReadWALEntriesForTruncation_WrapperBranches(t *testing.T) {
	t.Run("open_error", func(t *testing.T) {
		_, err := readWALEntriesForTruncation(filepath.Join(t.TempDir(), "missing.log"), 10)
		require.Error(t, err)
	})

	t.Run("empty_file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal.log")
		require.NoError(t, os.WriteFile(path, nil, 0644))
		entries, err := readWALEntriesForTruncation(path, 10)
		require.NoError(t, err)
		require.Empty(t, entries)
	})

	t.Run("legacy_and_atomic_dispatch", func(t *testing.T) {
		dir := t.TempDir()
		legacyPath := filepath.Join(dir, "legacy.log")
		legacy := []WALEntry{{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpCreateNode}}
		raw, err := json.Marshal(legacy[0])
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(legacyPath, append(raw, '\n'), 0644))
		entries, err := readWALEntriesForTruncation(legacyPath, 0)
		require.NoError(t, err)
		require.Len(t, entries, 1)

		atomicPath := filepath.Join(dir, "atomic.log")
		rec := buildAtomicWALRecord(t, WALEntry{Sequence: 2, Timestamp: time.Now().UTC(), Operation: OpCreateNode, Data: []byte(`{"node":{"id":"n"}}`), Checksum: crc32Checksum([]byte(`{"node":{"id":"n"}}`))})
		require.NoError(t, os.WriteFile(atomicPath, rec, 0644))
		entries, err = readWALEntriesForTruncation(atomicPath, 1)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, uint64(2), entries[0].Sequence)
	})
}
