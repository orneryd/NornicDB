package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A parked failure must have a durable, failure-only lookup key. Listing via
// the embedding prefix cannot satisfy P08: it visits every chunk and metadata
// record even when no failures are parked.
func TestEmbeddingFailureIndexTracksSidecarWrites(t *testing.T) {
	b := newSidecarTestBadger(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "failure-index", map[string]any{"title": "v1"}, now)
	key := append([]byte{0x27}, []byte(node.ID)...)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, node.ID, nil,
		map[string]any{"embedding_failed": true, "embedding_error": "boom"}, now)))
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	}))

	require.NoError(t, b.UpdateNodeEmbeddingSidecar(context.Background(), embeddingWriteback(t, b, node.ID, nil, nil, now)))
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestEmbeddingFailureIndexDeleteAndRebuild(t *testing.T) {
	dir := t.TempDir()
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	defer b.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "failure-rebuild", map[string]any{"title": "v1"}, now)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, node.ID, nil,
		map[string]any{"embedding_failed": true, "embedding_error": "boom"}, now)))
	assertOneFailure := func(engine *BadgerEngine) {
		t.Helper()
		count, err := engine.StreamParkedEmbeddingFailures(t.Context(), func(id NodeID, meta map[string]any) error {
			require.Equal(t, node.ID, id)
			require.Equal(t, "boom", meta["embedding_error"])
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, count)
	}
	assertOneFailure(b)
	// Simulate a store created before the index. The fallback remains correct
	// without the marker, and opening the store rebuilds the durable index.
	require.NoError(t, b.withUpdate(t.Context(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		if err := txn.Delete(embeddingFailureKey(node.ID)); err != nil {
			return err
		}
		return txn.Delete(embeddingFailureIndexReadyKey)
	}))
	assertOneFailure(b)
	require.NoError(t, b.Close())
	b, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	defer b.Close()
	assertOneFailure(b)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		ready, err := embeddingFailureIndexReady(txn)
		require.NoError(t, err)
		require.True(t, ready)
		_, err = txn.Get(embeddingFailureKey(node.ID))
		return err
	}))
	require.NoError(t, b.DeleteNode(t.Context(), node.ID))
	count, err := b.StreamParkedEmbeddingFailures(t.Context(), func(NodeID, map[string]any) error { return nil })
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(embeddingFailureKey(node.ID))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestEmbeddingFailureIndexRestoreOlderBackup(t *testing.T) {
	source, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer source.Close()
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, source, "failure-restore", map[string]any{"title": "v1"}, now)
	require.NoError(t, source.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, source, node.ID, nil,
		map[string]any{"embedding_failed": true, "embedding_error": "restore error"}, now)))
	require.NoError(t, source.withUpdate(t.Context(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		if err := txn.Delete(embeddingFailureKey(node.ID)); err != nil {
			return err
		}
		return txn.Delete(embeddingFailureIndexReadyKey)
	}))
	backup := filepath.Join(t.TempDir(), "legacy.backup")
	require.NoError(t, source.Backup(backup))
	target, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer target.Close()
	require.NoError(t, target.Restore(backup))
	count, err := target.StreamParkedEmbeddingFailures(t.Context(), func(id NodeID, meta map[string]any) error {
		require.Equal(t, node.ID, id)
		require.Equal(t, "restore error", meta["embedding_error"])
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, target.withView(func(txn *badger.Txn) error {
		ready, err := embeddingFailureIndexReady(txn)
		require.NoError(t, err)
		require.True(t, ready)
		_, err = txn.Get(embeddingFailureKey(node.ID))
		return err
	}))
}

// The same parked population with a larger healthy embedding corpus isolates
// the storage listing cost from fixture construction and HTTP JSON encoding.
func BenchmarkParkedFailureStorageListing(b *testing.B) {
	for _, nodes := range []int{500, 2000} {
		b.Run(fmt.Sprintf("nodes_%d", nodes), func(b *testing.B) {
			engine := newTestMemoryEngine(b).BadgerEngine
			for start := 0; start < nodes; start += 25 {
				end := min(start+25, nodes)
				err := engine.withUpdate(b.Context(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
					for i := start; i < end; i++ {
						id := NodeID(fmt.Sprintf("bench:n%05d", i))
						for chunk := 0; chunk < 17; chunk++ {
							if err := txn.Set(embeddingKey(id, chunk), []byte{1}); err != nil {
								return err
							}
						}
						meta := map[string]any{"embedding_failed": i < 20}
						data, err := encodeValue(meta)
						if err != nil {
							return err
						}
						if err := txn.Set(embeddingMetaKey(id), data); err != nil {
							return err
						}
						if i < 20 {
							if err := txn.Set(append([]byte{0x27}, []byte(id)...), []byte{}); err != nil {
								return err
							}
						}
					}
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			// The new index can be read only after a complete backfill. The
			// baseline reader ignores this marker, so fixture data is identical.
			if err := engine.withUpdate(b.Context(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
				return txn.Set([]byte{prefixMVCCMeta, 0x13}, []byte{1})
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count, err := engine.StreamParkedEmbeddingFailures(b.Context(), func(NodeID, map[string]any) error { return nil })
				if err != nil || count != 20 {
					b.Fatalf("failures=%d err=%v", count, err)
				}
			}
		})
	}
}
