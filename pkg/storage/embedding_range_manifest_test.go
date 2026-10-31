package storage

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingRangeManifestReplacementSnapshotsAndMetadata(t *testing.T) {
	engine := newTestMemoryEngine(t).BadgerEngine
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	body := &Node{ID: "test:manifest-owner", Labels: []string{"Doc"}, Properties: map[string]any{"text": "same source"}}
	_, err := engine.CreateNode(body)
	require.NoError(t, err)
	large := make([]float32, maxEmbeddingValueBytes/4+100)
	for i := range large {
		large[i] = float32(i)
	}
	initial := [][]float32{large, {3, 4}}
	write := func(vectors [][]float32, meta map[string]any) {
		t.Helper()
		payload := CopyNode(body)
		payload.ChunkEmbeddings, payload.EmbedMeta = vectors, meta
		require.NoError(t, engine.UpdateNodeEmbeddingSidecar(payload))
		require.NotContains(t, payload.EmbedMeta, embeddingRangeManifestKey, "the physical index cannot enter publication")
	}
	write(initial, map[string]any{"chunk_count": 2, "embedding_model": "first"})
	old, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, old.Rollback()) })
	write([][]float32{{5, 6}}, map[string]any{"chunk_count": 1, "embedding_model": "second"})
	current, err := engine.GetNode(body.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{5, 6}}, current.ChunkEmbeddings)
	require.NotContains(t, current.EmbedMeta, embeddingRangeManifestKey)
	meta, err := engine.GetNodeEmbeddingMeta(body.ID)
	require.NoError(t, err)
	require.NotContains(t, meta, embeddingRangeManifestKey)
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		for _, key := range [][]byte{embeddingKey(body.ID, 1), embeddingChunkPartKey(body.ID, 0, 0), embeddingChunkPartKey(body.ID, 0, 1)} {
			_, err := txn.Get(key)
			require.ErrorIs(t, err, badger.ErrKeyNotFound, "obsolete chunk/parts must be deleted by the manifest owner")
		}
		return nil
	}))
	held, err := old.GetNode(body.ID)
	require.NoError(t, err)
	require.Equal(t, initial, held.ChunkEmbeddings, "a new manifest cannot change a pinned public snapshot")
	write(nil, map[string]any{"embedding_failed": true, "embedding_model": "failed"})
	count, err := engine.StreamParkedEmbeddingFailures(context.Background(), func(id NodeID, meta map[string]any) error {
		require.Equal(t, body.ID, id)
		require.NotContains(t, meta, embeddingRangeManifestKey)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	write(nil, nil)
	current, err = engine.GetNode(body.ID)
	require.NoError(t, err)
	require.Empty(t, current.ChunkEmbeddings)
	require.Nil(t, current.EmbedMeta, "clearing a sidecar must remove its internal index too")
	require.NoError(t, engine.DeleteNode(body.ID))
	held, err = old.GetNode(body.ID)
	require.NoError(t, err)
	require.Equal(t, initial, held.ChunkEmbeddings)
}

func TestEmbeddingRangeManifestInvalidRecordRepairsThroughPublicReplacement(t *testing.T) {
	tooMany := binary.AppendUvarint([]byte{1}, maxEmbeddingManifestKeys)
	tooManyParts := binary.AppendUvarint([]byte{1, 1}, maxEmbeddingManifestKeys)
	for name, marker := range map[string]any{
		"absent":          nil,
		"wrong_type":      true,
		"wrong_version":   []byte{2, 0},
		"truncated_count": []byte{1, 0x80},
		"truncated_parts": []byte{1, 1},
		"trailing_bytes":  []byte{1, 0, 0},
		"too_many_chunks": tooMany,
		"too_many_parts":  tooManyParts,
	} {
		t.Run(name, func(t *testing.T) {
			engine := newTestMemoryEngine(t).BadgerEngine
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			body := &Node{ID: "test:manifest-repair", Properties: map[string]any{"text": "unchanged"}}
			_, err := engine.CreateNode(body)
			require.NoError(t, err)
			payload := CopyNode(body)
			payload.ChunkEmbeddings = [][]float32{{1, 2}}
			payload.EmbedMeta = map[string]any{"chunk_count": 1}
			require.NoError(t, engine.UpdateNodeEmbeddingSidecar(payload))
			require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
				meta := map[string]any{"chunk_count": 1}
				if marker != nil {
					meta[embeddingRangeManifestKey] = marker
				}
				data, err := encodeValue(meta)
				require.NoError(t, err)
				// Corruption of a bound record must still exercise syntax checks,
				// independently of the old-writer provenance fallback.
				require.NoError(t, txn.SetEntry(badger.NewEntry(embeddingMetaKey(body.ID), data).WithMeta(embeddingRangeManifestBound)))
				data, err = encodeEmbedding([]float32{7})
				require.NoError(t, err)
				return txn.Set(embeddingKey(body.ID, 3), data)
			}))
			payload.ChunkEmbeddings = [][]float32{{8, 9}}
			require.NoError(t, engine.UpdateNodeEmbeddingSidecar(payload))
			require.NoError(t, engine.withView(func(txn *badger.Txn) error {
				_, err := txn.Get(embeddingKey(body.ID, 3))
				require.ErrorIs(t, err, badger.ErrKeyNotFound, "invalid/missing index must fall back to full owned-range cleanup")
				return nil
			}))
			saved, err := engine.GetNode(body.ID)
			require.NoError(t, err)
			require.Equal(t, payload.ChunkEmbeddings, saved.ChunkEmbeddings)
			require.NotContains(t, saved.EmbedMeta, embeddingRangeManifestKey)
		})
	}
}

func TestEmbeddingRangeManifestLargeMetadataKeepsStorageLimit(t *testing.T) {
	engine := newTestMemoryEngine(t).BadgerEngine
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	body := &Node{ID: "test:manifest-large-meta", Properties: map[string]any{"text": "unchanged"}}
	_, err := engine.CreateNode(body)
	require.NoError(t, err)
	payload := CopyNode(body)
	payload.ChunkEmbeddings = [][]float32{{1, 2}}
	payload.EmbedMeta = map[string]any{"chunk_count": 1, "detail": strings.Repeat("x", maxEmbeddingValueBytes)}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(payload))
	saved, err := engine.GetNodeEmbeddingMeta(body.ID)
	require.NoError(t, err)
	require.Equal(t, payload.EmbedMeta["detail"], saved["detail"])
	require.NotContains(t, saved, embeddingRangeManifestKey)
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(embeddingMetaKey(body.ID))
		require.NoError(t, err)
		require.Zero(t, item.UserMeta(), "metadata without a retained manifest must remain unbound")
		return item.Value(func(data []byte) error {
			var physical map[string]any
			require.NoError(t, decodeValue(data, &physical))
			require.NotContains(t, physical, embeddingRangeManifestKey, "a larger record must keep the original format instead of growing past its storage budget")
			return nil
		})
	}))
}
