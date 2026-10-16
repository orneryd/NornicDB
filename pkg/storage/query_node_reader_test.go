package storage

import (
	"sync/atomic"
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

// TestTransactionGetQueryNodeReadsNoChunk: the transaction's
// embedding-free read, which query execution uses, settles the node's
// embedding metadata (the sidecar's included) but never reads a stored chunk
// vector, on a body-cache miss and hit alike; GetNode still reads them all.
func TestTransactionGetQueryNodeReadsNoChunk(t *testing.T) {
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
				node, err := tx.GetQueryNode(id)
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

// TestTransactionGetQueryNodeKeepsFullReads: an embedding-free
// read leaves no embedding-free node behind for later full reads (body cache,
// node cache), and an update written from it keeps the stored vectors while
// the embedding source is unchanged and, for the worker's managed embeddings,
// drops them when it changes (#963), as an update from a full read does.
func TestTransactionGetQueryNodeKeepsFullReads(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"other"}, IncludeLabels: true})
			id := NodeID("test:lazy")
			chunks := tc.create(t, b, id)

			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			node, err := tx.GetQueryNode(id)
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
			node, err = tx.GetQueryNode(id)
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

// TestTransactionGetQueryNodeVisibility: the embedding-free read
// sees what GetNode sees in the transaction (its pending writes, its
// deletes), sharing in-memory vectors instead of copying them.
func TestTransactionGetQueryNodeVisibility(t *testing.T) {
	b := newSidecarTestBadger(t)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	pending := &Node{ID: "test:pending", Labels: []string{"Doc"}, Properties: map[string]any{"title": "p"}, ChunkEmbeddings: [][]float32{{1, 2}}}
	_, err = tx.CreateNode(pending)
	require.NoError(t, err)
	got, err := tx.GetQueryNode(pending.ID)
	require.NoError(t, err)
	require.Equal(t, "p", got.Properties["title"])
	require.True(t, got.HasChunkEmbeddings())
	got.Properties["title"] = "changed"
	again, err := tx.GetNode(pending.ID)
	require.NoError(t, err)
	require.Equal(t, "p", again.Properties["title"])

	require.NoError(t, tx.DeleteNode(pending.ID))
	_, err = tx.GetQueryNode(pending.ID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = tx.GetQueryNode("test:missing")
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

// TestTransactionGetQueryNodeKeepsEmbeddingKeyLifecycle: a node
// read without vectors and then written back or deleted in the same
// transaction leaves the stored embedding keys exactly as a full read does:
// the write-back keeps what the full read keeps, and the delete removes them.
func TestTransactionGetQueryNodeKeepsEmbeddingKeyLifecycle(t *testing.T) {
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
				return tx.GetQueryNode(id)
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

// TestEngineGetQueryNodeReadsNoChunk: the engine stack's query-node read
// (BadgerEngine, through the WAL and namespaced wrappers) settles the node's
// embedding metadata as GetNode does, the worker sidecar's included, without
// reading a stored chunk vector; GetNode still reads them.
func TestEngineGetQueryNodeReadsNoChunk(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			id := NodeID("test:lazy")
			chunks := tc.create(t, b, id)
			full, err := b.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, chunks, full.ChunkEmbeddings)

			walLog, err := NewWAL(t.TempDir(), &WALConfig{SyncMode: "none"})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, walLog.Close()) })
			stack := NewNamespacedEngine(NewWALEngine(b, walLog), "test")

			b.invalidateCachesAfterRestore()
			corruptFirstEmbeddingChunk(t, b, id)
			_, err = b.GetNode(id)
			require.Error(t, err, "the full read reads the corrupted chunk")
			for name, read := range map[string]func() (*Node, error){
				"badger": func() (*Node, error) { return b.GetQueryNode(id) },
				"stack":  func() (*Node, error) { return stack.GetQueryNode("lazy") },
			} {
				node, err := read()
				require.NoError(t, err, name)
				require.Empty(t, node.ChunkEmbeddings, name)
				require.False(t, node.EmbeddingsStoredSeparately, name)
				require.Equal(t, tc.chunks, chunkCountFromMeta(node.EmbedMeta), name)
				require.True(t, node.HasChunkEmbeddings(), name)
				require.Equal(t, "t", node.Properties["title"], name)
			}
			node, err := stack.GetQueryNode("lazy")
			require.NoError(t, err)
			require.Equal(t, NodeID("lazy"), node.ID)
			_, err = b.GetQueryNode("test:missing")
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}

// TestEngineGetQueryNodeMatchesGetNode: apart from the vectors, the query-node
// read returns what GetNode returns: from the node cache (in-memory vectors
// shared, not copied), and from the write-behind overlay before a flush,
// buffered deletes included.
func TestEngineGetQueryNodeMatchesGetNode(t *testing.T) {
	stripVectors := func(n *Node) *Node {
		c := CopyNode(n)
		c.ChunkEmbeddings = nil
		c.NamedEmbeddings = nil
		return c
	}
	t.Run("node-cache", func(t *testing.T) {
		b := newSidecarTestBadger(t)
		id := NodeID("test:lazy")
		chunks := lazyEmbeddingCases()[0].create(t, b, id)
		full, err := b.GetNode(id) // fills the node cache
		require.NoError(t, err)
		node, err := b.GetQueryNode(id)
		require.NoError(t, err)
		require.Equal(t, chunks, node.ChunkEmbeddings, "a cached node's vectors are shared")
		require.Equal(t, stripVectors(full), stripVectors(node))
	})
	t.Run("write-behind", func(t *testing.T) {
		b := newWriteBehindTestEngine(t, time.Hour)
		id := NodeID("test:buffered")
		bufferedCreate(t, b, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"title": "t"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
		full, err := b.GetNode(id)
		require.NoError(t, err)
		node, err := b.GetQueryNode(id)
		require.NoError(t, err)
		require.Equal(t, stripVectors(full), stripVectors(node))
		require.True(t, node.HasChunkEmbeddings())

		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.DeleteNode(id))
		require.NoError(t, tx.Commit())
		_, err = b.GetNode(id)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = b.GetQueryNode(id)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// TestEngineGetQueryNodeCache: the engine keeps the vector-free node it read
// for later query-node reads, apart from the node cache: a full read still
// reads the stored vectors, and a body update, an embedding sidecar write and
// a delete are seen by the next query-node read.
func TestEngineGetQueryNodeCache(t *testing.T) {
	for _, tc := range lazyEmbeddingCases() {
		t.Run(tc.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			id := NodeID("test:lazy")
			chunks := tc.create(t, b, id)
			b.invalidateCachesAfterRestore()

			for read := 0; read < 2; read++ {
				hits := atomic.LoadInt64(&b.cacheHits)
				node, err := b.GetQueryNode(id)
				require.NoError(t, err)
				require.Equal(t, int64(read), atomic.LoadInt64(&b.cacheHits)-hits, "the second read is a cache hit")
				require.Empty(t, node.ChunkEmbeddings)
				require.True(t, node.HasChunkEmbeddings())
				require.Equal(t, "t", node.Properties["title"])
				node.Properties["title"] = "caller's copy"
			}
			full, err := b.GetNode(id)
			require.NoError(t, err)
			require.Equal(t, chunks, full.ChunkEmbeddings, "a full read never takes the vector-free entry")
			require.Equal(t, "t", full.Properties["title"])

			b.invalidateCachesAfterRestore()
			_, err = b.GetQueryNode(id)
			require.NoError(t, err)
			updated := CopyNode(full)
			updated.Properties["other"] = int64(2)
			require.NoError(t, b.UpdateNode(updated))
			node, err := b.GetQueryNode(id)
			require.NoError(t, err)
			require.Equal(t, int64(2), node.Properties["other"])

			// The writeback reads the node in full; build it first so the
			// query-node entry is in place when the sidecar is written.
			writeback := embeddingWriteback(t, b, id, [][]float32{{1, 2}, {3, 4}, {5, 6}}, map[string]any{"has_embedding": true, "chunk_count": 3}, time.Time{})
			b.invalidateCachesAfterRestore()
			_, err = b.GetQueryNode(id)
			require.NoError(t, err)
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(writeback))
			node, err = b.GetQueryNode(id)
			require.NoError(t, err)
			require.Equal(t, 3, chunkCountFromMeta(node.EmbedMeta), "a sidecar write is seen")

			require.NoError(t, b.DeleteNode(id))
			_, err = b.GetQueryNode(id)
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
}
