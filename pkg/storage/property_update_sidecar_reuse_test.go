package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

const propertyUpdateVectorDims = 256

// propertyUpdateNamedVector is the name of the fixture node's user-provided
// vector. Unlike the managed chunk vectors it never leaves the node body, so it
// travels with every body write, journal records included.
const propertyUpdateNamedVector = "title"

func newPropertyUpdateVectorEngine(t testing.TB, records WALRecordPolicy) (*BadgerEngine, *WALEngine) {
	t.Helper()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{
		dataDir:     t.TempDir(),
		wal:         true,
		synchronous: true,
		manualDrain: true,
		records:     records,
	})
	// Only "text" feeds the managed embedding, so a "flag" change keeps the
	// vectors the stored node carries.
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	return b, NewWALEngine(b, b.bufferedCommitWAL)
}

// propertyUpdateNamedVectorValues returns the fixture's deterministic named
// vector, computed on demand so a check never holds a second copy of it.
func propertyUpdateNamedVectorValues() []float32 {
	vector := make([]float32, propertyUpdateVectorDims)
	for j := range vector {
		vector[j] = float32(2) + float32(j)/7
	}
	return vector
}

// seedPropertyUpdateVectorNode stores id with text "alpha", the fixture named
// vector and then chunks deterministic 256-dimensional managed vectors through
// the public sidecar route.
func seedPropertyUpdateVectorNode(t testing.TB, b *BadgerEngine, w *WALEngine, id NodeID, chunks int) {
	t.Helper()
	ctx := context.Background()
	_, err := w.CreateNode(ctx, &Node{
		ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"},
		NamedEmbeddings: map[string][]float32{propertyUpdateNamedVector: propertyUpdateNamedVectorValues()},
	})
	require.NoError(t, err)
	stored, err := w.GetNode(ctx, id)
	require.NoError(t, err)
	require.Len(t, stored.NamedEmbeddings[propertyUpdateNamedVector], propertyUpdateVectorDims, "the seeded named vector must be stored")
	embedded := CopyNode(stored)
	embedded.ChunkEmbeddings = largeRecordVectors(chunks, 1)
	embedded.EmbedMeta = map[string]any{
		"has_embedding":        true,
		"chunk_count":          chunks,
		"embedding_dimensions": propertyUpdateVectorDims,
	}
	require.NoError(t, w.UpdateNodeEmbeddingSidecar(ctx, embedded))

	got, err := b.GetNode(ctx, id)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, chunks, "the sidecar write must store the seeded vectors")
	require.Equal(t, true, got.EmbedMeta["has_embedding"])
}

// requirePropertyUpdateVectors checks the full deterministic fixture payload
// against the stored vectors without allocating a second copy of it.
func requirePropertyUpdateVectors(t testing.TB, node *Node, chunks int) {
	t.Helper()
	require.Len(t, node.ChunkEmbeddings, chunks)
	for i, vector := range node.ChunkEmbeddings {
		require.Len(t, vector, propertyUpdateVectorDims)
		for j, got := range vector {
			want := float32(1) + float32(i*propertyUpdateVectorDims+j)/7
			if got != want {
				t.Fatalf("vector[%d][%d] = %v, want %v", i, j, got, want)
			}
		}
	}
	named := node.NamedEmbeddings[propertyUpdateNamedVector]
	require.Len(t, named, propertyUpdateVectorDims, "named vector of %s", node.ID)
	for j, got := range named {
		want := float32(2) + float32(j)/7
		if got != want {
			t.Fatalf("named vector[%d] = %v, want %v", j, got, want)
		}
	}
	require.Equal(t, true, node.EmbedMeta["has_embedding"])
	require.EqualValues(t, chunks, node.EmbedMeta["chunk_count"])
}

func TestPropertyUpdateKeepsUnchangedSidecarRecords(t *testing.T) {
	b, w := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
	const id NodeID = "test:keep-sidecars"
	const chunks = 1024
	seedPropertyUpdateVectorNode(t, b, w, id, chunks)
	versions := func() [2]uint64 {
		var result [2]uint64
		require.NoError(t, b.db.View(func(txn *badger.Txn) error {
			for i, key := range [][]byte{embeddingKey(id, 0), embeddingMetaKey(id)} {
				item, err := txn.Get(key)
				if err == badger.ErrKeyNotFound {
					continue
				}
				if err != nil {
					return err
				}
				result[i] = item.Version()
			}
			return nil
		}))
		return result
	}
	before := versions()
	require.Positive(t, before[0])
	require.Positive(t, before[1])
	node, err := w.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = true
	require.NoError(t, w.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	require.Equal(t, before, versions(), "an unchanged sidecar must neither be rewritten nor lose its metadata record")
	stored, err := w.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, true, stored.Properties["flag"])
	requirePropertyUpdateVectors(t, stored, chunks)

	// Metadata changes and replacement vectors still use the physical writer.
	stored.EmbedMeta["provider"] = "replacement"
	require.NoError(t, w.UpdateNode(t.Context(), NodeUpdateRequest{After: stored}))
	stored, err = w.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "replacement", stored.EmbedMeta["provider"])
	requirePropertyUpdateVectors(t, stored, chunks)
	stored.ChunkEmbeddings[0][0] = 42
	require.NoError(t, w.UpdateNode(t.Context(), NodeUpdateRequest{After: stored}))
	stored, err = w.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, float32(42), stored.ChunkEmbeddings[0][0])
	require.Equal(t, "replacement", stored.EmbedMeta["provider"])

	// An actual source change invalidates carried vectors rather than reusing
	// the sidecar that belongs to the previous text.
	stored.Properties["text"] = "beta"
	require.NoError(t, w.UpdateNode(t.Context(), NodeUpdateRequest{After: stored}))
	stored, err = w.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Empty(t, stored.ChunkEmbeddings)
	require.NotEqual(t, true, stored.EmbedMeta["has_embedding"])
}

func TestCarriedVectorComparisonPreservesFloatBits(t *testing.T) {
	positiveZero := float32(0)
	negativeZero := math.Float32frombits(1 << 31)
	nan := math.Float32frombits(0x7fc00001)
	require.False(t, sameChunkEmbeddings([][]float32{{positiveZero}}, [][]float32{{negativeZero}}))
	require.True(t, sameChunkEmbeddings([][]float32{{nan}}, [][]float32{{nan}}))
	require.False(t, sameChunkEmbeddings([][]float32{{nan}}, [][]float32{{math.Float32frombits(0x7fc00002)}}))
}
