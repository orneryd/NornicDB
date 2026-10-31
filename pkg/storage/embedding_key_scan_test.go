package storage

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingRangeReaderRetainedKeysAndLongCursor(t *testing.T) {
	engine := newTestMemoryEngine(t).BadgerEngine
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	id := NodeID("test:" + strings.Repeat("long-owner", 40))
	prefix := embeddingPrefix(id)
	first, second := embeddingKey(id, 0), embeddingKey(id, 1)
	child := append(append([]byte(nil), first...), 0, 0, 1)
	short := [][]byte{first[:len(first)-4], first[:len(first)-3], first[:len(first)-2], first[:len(first)-1]}
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		for _, key := range append(append([][]byte(nil), short...), first, child, second) {
			require.NoError(t, txn.Set(key, []byte("value")))
		}
		return nil
	}))
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		var obsolete [][]byte
		require.NoError(t, visitEmbeddingPrefixKeys(txn, prefix, nil, func(key []byte) error {
			obsolete = append(obsolete, key)
			return nil
		}, embeddingRangeRetention{contains: func(key []byte) bool { return bytes.Equal(key, first) }}))
		require.Equal(t, append(append([][]byte(nil), short...), child, second), obsolete, "keeping a base key must not hide shorter keys or obsolete shard children")
		var visited []string
		require.NoError(t, visitEmbeddingPrefixKeys(txn, prefix, nil, func(key []byte) error {
			visited = append(visited, string(key))
			key[0] ^= 0xff // The owned callback key cannot change the seek cursor.
			return nil
		}))
		want := []string{string(short[0]), string(short[1]), string(short[2]), string(short[3]), string(first), string(child), string(second)}
		require.Equal(t, want, visited)
		return nil
	}))
}

func TestEmbeddingRangeReaderSnapshotsAndStagedWrites(t *testing.T) {
	engine := newTestMemoryEngine(t).BadgerEngine
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	id := NodeID("test:range-reader")
	prefix := embeddingPrefix(id)
	first, second, expired := embeddingKey(id, 0), embeddingKey(id, 1), embeddingKey(id, 2)
	child := append(append([]byte(nil), first...), 0, 0, 0)
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		require.NoError(t, txn.Set(first, []byte("original")))
		return txn.Set(second, []byte("removed later"))
	}))
	db := engine.db
	old, readTs, err := db.beginTxn(false)
	require.NoError(t, err)
	t.Cleanup(func() {
		old.Discard()
		db.endRead(readTs)
	})
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		require.NoError(t, txn.Set(first, []byte("replacement")))
		require.NoError(t, txn.Delete(second))
		require.NoError(t, txn.Set(child, []byte("nested part")))
		return txn.SetEntry(badger.NewEntry(expired, []byte("expired")).WithTTL(-time.Hour))
	}))
	collect := func(txn *badger.Txn, after []byte) [][]byte {
		t.Helper()
		var keys [][]byte
		require.NoError(t, visitEmbeddingPrefixKeys(txn, prefix, after, func(key []byte) error {
			keys = append(keys, key)
			return nil
		}))
		return keys
	}
	require.Equal(t, [][]byte{first, second}, collect(old, nil), "old snapshot retains its live versions")
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		require.Equal(t, [][]byte{first, child}, collect(txn, nil), "deleted and expired heads cannot expose older versions")
		require.Equal(t, [][]byte{child}, collect(txn, append(append([]byte(nil), first...), 0)), "resume keeps nested keys")
		stop := errors.New("stop reading")
		require.ErrorIs(t, visitEmbeddingPrefixKeys(txn, prefix, nil, func([]byte) error { return stop }), stop)
		require.NoError(t, visitEmbeddingPrefixKeys(txn, embeddingPrefix("test:missing"), nil, func([]byte) error {
			t.Fatal("empty range returned a key")
			return nil
		}))
		return nil
	}))
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		require.NoError(t, txn.Delete(first))
		require.NoError(t, txn.Set(second, []byte("staged replacement")))
		require.Equal(t, [][]byte{child, second}, collect(txn, nil), "staged writes and tombstones share the read view")
		return nil
	}))
}
