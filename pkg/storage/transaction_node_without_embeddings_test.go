package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// lazyEmbeddingCase is a committed node whose chunk vectors live in the
// embedding key space: written by the body encoder when the node is too large
// (legacy), or by the embed worker's sidecar.
type lazyEmbeddingCase struct {
	name   string
	chunks int
	create func(t *testing.T, b *BadgerEngine, id NodeID) [][]float32
}

func lazyEmbeddingCases() []lazyEmbeddingCase {
	return []lazyEmbeddingCase{
		{name: "body-overflow", chunks: 4, create: func(t *testing.T, b *BadgerEngine, id NodeID) [][]float32 {
			chunks := makeLargeChunkEmbeddings()
			_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"title": "t"}, ChunkEmbeddings: chunks})
			require.NoError(t, err)
			return chunks
		}},
		{name: "worker-sidecar", chunks: 1, create: func(t *testing.T, b *BadgerEngine, id NodeID) [][]float32 {
			_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"title": "t"}})
			require.NoError(t, err)
			vector := make([]float32, 2048)
			for i := range vector {
				vector[i] = float32(i%7) + 0.5
			}
			chunks := [][]float32{vector}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, b, id, chunks, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})))
			return chunks
		}},
	}
}

// corruptFirstEmbeddingChunk overwrites the node's first stored chunk with a
// value no reader can decode, so any read of it fails.
func corruptFirstEmbeddingChunk(t *testing.T, b *BadgerEngine, id NodeID) {
	t.Helper()
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(embeddingKey(id, 0), []byte{0xc1})
	}))
}

// TestTransactionGetNodeWithoutEmbeddingsReadsNoChunk: the transaction's
// embedding-free read, which query execution uses, settles the node's
// embedding metadata (the sidecar's included) but never reads a stored chunk
// vector, on a body-cache miss and hit alike; GetNode still reads them all.
func TestTransactionGetNodeWithoutEmbeddingsReadsNoChunk(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			id := NodeID("test:lazy")
			chunks := tc.create(t, b, id)

			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			full, err := tx.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, chunks, full.ChunkEmbeddings)
			require.NoError(t, tx.Rollback())

			corruptFirstEmbeddingChunk(t, b, id)
			tx, err = b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			for read := 0; read < 2; read++ {
				node, err := tx.GetNodeWithoutEmbeddings(id)
				require.NoError(t, err)
				require.Empty(t, node.ChunkEmbeddings)
				require.False(t, node.EmbeddingsStoredSeparately)
				require.Equal(t, tc.chunks, chunkCountFromMeta(node.EmbedMeta))
				require.True(t, node.HasChunkEmbeddings())
				require.Equal(t, "t", node.Properties["title"])
				require.Equal(t, []string{"Doc"}, node.Labels)
			}
			_, err = tx.GetNode(id)
			require.ErrorContains(t, err, "embedding chunk 0")
		})
	}
}

// TestTransactionGetNodeWithoutEmbeddingsKeepsFullReads: an embedding-free
// read leaves no embedding-free node behind for later full reads (body cache,
// node cache), and an update written from it keeps the stored vectors while
// the embedding source is unchanged and, for the worker's managed embeddings,
// drops them when it changes (#963), as an update from a full read does.
func TestTransactionGetNodeWithoutEmbeddingsKeepsFullReads(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"other"}, IncludeLabels: true})
			id := NodeID("test:lazy")
			chunks := tc.create(t, b, id)

			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			node, err := tx.GetNodeWithoutEmbeddings(id)
			require.NoError(t, err)
			require.Empty(t, node.ChunkEmbeddings)
			full, err := tx.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, chunks, full.ChunkEmbeddings)

			node.Properties["other"] = int64(1)
			require.NoError(t, tx.UpdateNode(node))
			require.NoError(t, tx.Commit())

			stored, err := b.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, int64(1), stored.Properties["other"])
			require.Equal(t, chunks, stored.ChunkEmbeddings)
			require.Equal(t, tc.chunks, chunkCountFromMeta(stored.EmbedMeta))

			tx, err = b.BeginTransaction()
			require.NoError(t, err)
			reread, err := tx.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, chunks, reread.ChunkEmbeddings)
			require.NoError(t, tx.Rollback())

			if tc.name != "worker-sidecar" {
				return // managed embeddings only
			}
			tx, err = b.BeginTransaction()
			require.NoError(t, err)
			node, err = tx.GetNodeWithoutEmbeddings(id)
			require.NoError(t, err)
			node.Properties["title"] = "new text"
			require.NoError(t, tx.UpdateNode(node))
			require.NoError(t, tx.Commit())
			stored, err = b.GetNode(id)
			require.NoError(t, err)
			require.Empty(t, stored.ChunkEmbeddings)
			require.False(t, stored.HasChunkEmbeddings())
		})
	}
}

// TestTransactionGetNodeWithoutEmbeddingsVisibility: the embedding-free read
// sees what GetNode sees in the transaction (its pending writes, its
// deletes), sharing in-memory vectors instead of copying them.
func TestTransactionGetNodeWithoutEmbeddingsVisibility(t *testing.T) {
	b := newSidecarTestBadger(t)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	pending := &Node{ID: "test:pending", Labels: []string{"Doc"}, Properties: map[string]any{"title": "p"}, ChunkEmbeddings: [][]float32{{1, 2}}}
	_, err = tx.CreateNode(pending)
	require.NoError(t, err)
	got, err := tx.GetNodeWithoutEmbeddings(pending.ID)
	require.NoError(t, err)
	require.Equal(t, "p", got.Properties["title"])
	require.True(t, got.HasChunkEmbeddings())
	got.Properties["title"] = "changed"
	again, err := tx.GetNode(pending.ID)
	require.NoError(t, err)
	require.Equal(t, "p", again.Properties["title"])

	require.NoError(t, tx.DeleteNode(pending.ID))
	_, err = tx.GetNodeWithoutEmbeddings(pending.ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = tx.GetNodeWithoutEmbeddings("test:missing")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestNodeHasChunkEmbeddings(t *testing.T) {
	require.False(t, (*Node)(nil).HasChunkEmbeddings())
	require.False(t, (&Node{}).HasChunkEmbeddings())
	require.False(t, (&Node{ChunkEmbeddings: [][]float32{{}}}).HasChunkEmbeddings())
	require.True(t, (&Node{ChunkEmbeddings: [][]float32{{1}}}).HasChunkEmbeddings())
	require.True(t, (&Node{EmbedMeta: map[string]any{"chunk_count": int64(2)}}).HasChunkEmbeddings())
	require.False(t, (&Node{EmbedMeta: map[string]any{"chunk_count": 0}}).HasChunkEmbeddings())
}

// TestTransactionGetNodeWithoutEmbeddingsKeepsEmbeddingKeyLifecycle: a node
// read without vectors and then written back or deleted in the same
// transaction leaves the stored embedding keys exactly as a full read does:
// the write-back keeps what the full read keeps, and the delete removes them.
func TestTransactionGetNodeWithoutEmbeddingsKeepsEmbeddingKeyLifecycle(t *testing.T) {
	type outcome struct {
		keysAfterCreate int
		keysAfterUpdate int
		chunksAfter     [][]float32
		keysAfterDelete int
	}
	run := func(t *testing.T, tc lazyEmbeddingCase, vectorFree bool) outcome {
		b := newSidecarTestBadger(t)
		id := NodeID("test:lazy-lifecycle")
		tc.create(t, b, id)
		read := func(tx *BadgerTransaction) (*Node, error) {
			if vectorFree {
				return tx.GetNodeWithoutEmbeddings(id)
			}
			return tx.GetNode(id)
		}
		var out outcome
		out.keysAfterCreate = countEmbeddingKeys(t, b, id)

		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		node, err := read(tx)
		require.NoError(t, err)
		node.Properties["seen"] = true
		require.NoError(t, tx.UpdateNode(node))
		require.NoError(t, tx.Commit())
		out.keysAfterUpdate = countEmbeddingKeys(t, b, id)
		b.invalidateCachesAfterRestore()
		stored, err := b.GetNode(id)
		require.NoError(t, err)
		out.chunksAfter = stored.ChunkEmbeddings

		tx, err = b.BeginTransaction()
		require.NoError(t, err)
		_, err = read(tx)
		require.NoError(t, err)
		require.NoError(t, tx.DeleteNode(id))
		require.NoError(t, tx.Commit())
		out.keysAfterDelete = countEmbeddingKeys(t, b, id)
		return out
	}
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			full := run(t, tc, false)
			lazy := run(t, tc, true)
			require.Positive(t, full.keysAfterCreate)
			require.Equal(t, full, lazy)
			require.Zero(t, lazy.keysAfterDelete, "delete must remove the stored embedding keys")
		})
	}
}
