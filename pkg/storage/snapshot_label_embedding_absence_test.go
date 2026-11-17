package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// The label consumers of the embedding-absence proof: a whole-label scan reads
// its population from its own view of the pinned snapshot, and every committed
// point read, write target and cache-admission probe of the same transaction
// answers the same range question from the transaction's own record of that
// snapshot. These tests establish what those consumers must keep, through the
// public native transaction only.

// A whole-label scan hydrates its rows from the scan's own view of the
// snapshot, and the transaction's cached point reads and writes of the scanned
// targets answer the same snapshot's range question from the transaction's own
// record: a population with no embedding record at all costs one probe, not
// one per scanned row.
func TestSnapshotLabelScanEmbeddingAbsenceServesCachedPointReadsAndDeletes(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+8)

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())

	visited := 0
	require.NoError(t, tx.StreamNodesByLabelProjected(t.Context(), "Held", nil, func(node *Node) error {
		require.Empty(t, node.ChunkEmbeddings, "a snapshot with no embedding record hydrates no vectors")
		visited++
		return nil
	}))
	require.Equal(t, len(ids), visited)
	require.Contains(t, tx.snapshotLabelNodeByID, ids[0], "the completed whole-label stream is the transaction's point-read cache")
	require.False(t, tx.embeddingAbsence.proved, "the scan takes its own view's proof, not the transaction's record")

	// Every cached point read asks whether the scanned image hides a worker
	// sidecar. The snapshot's answered range question replaces those lookups.
	for _, id := range ids {
		node, err := tx.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, []string{"Held"}, node.Labels)
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.True(t, tx.embeddingAbsence.proved, "the transaction's own record answered its cached point reads")
	require.True(t, tx.embeddingAbsence.absent)

	// A delete of a scanned target takes that same cached image as its old
	// version, and the deleted target stays deleted for this transaction.
	require.NoError(t, tx.DeleteGraphNode(t.Context(), ids[0], CascadeEdges))
	_, err := tx.GetNode(t.Context(), ids[0])
	require.ErrorIs(t, err, ErrNotFound, "a deleted scanned target is not visible to the transaction")
	remaining, err := tx.GetNode(t.Context(), ids[1])
	require.NoError(t, err)
	require.Empty(t, remaining.ChunkEmbeddings)
	require.Equal(t, ids[1], remaining.ID)
}

// A snapshot that does hold an embedding record never claims absence, so a
// whole-label scan still hydrates the stored vectors of the rows that have
// them, and the cached image of such a row is re-read by the point route
// instead of being served without them.
func TestSnapshotLabelScanKeepsStoredVectorsUnderPositivePrefix(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+8)
	embedded := ids[len(ids)-1]
	chunks := [][]float32{{0.125, 0.5}, {0.75, 0.25}}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, embedded, chunks, map[string]any{"has_embedding": true, "chunk_count": len(chunks)}, time.Time{})))

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())

	var scanned *Node
	require.NoError(t, tx.StreamNodesByLabelProjected(t.Context(), "Held", nil, func(node *Node) error {
		if node.ID == embedded {
			scanned = node
		}
		return nil
	}))
	require.NotNil(t, scanned, "the whole-label scan visits the embedded row")
	require.Equal(t, chunks, scanned.ChunkEmbeddings, "a scanned row still hydrates its separately stored vectors")
	// The whole-label stream keeps the row's image for later writes; the point
	// route must therefore still see the sidecar and re-read it.
	require.Contains(t, tx.snapshotLabelNodeByID, embedded)

	for _, id := range ids {
		node, err := tx.GetNode(t.Context(), id)
		require.NoError(t, err)
		if id == embedded {
			require.Equal(t, chunks, node.ChunkEmbeddings, "the cached scanned image never replaces the row's stored vectors")
			require.Equal(t, len(chunks), chunkCountFromMeta(node.EmbedMeta))
			continue
		}
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.True(t, tx.embeddingAbsence.proved, "the snapshot still answers its range question once")
	require.False(t, tx.embeddingAbsence.absent, "a snapshot holding an embedding record never claims absence")
}

// The same rule on the query row shape: a query label stream settles each
// row's embedding metadata without its vectors, and the point read of a row
// with stored vectors still hydrates them.
func TestSnapshotLabelQueryScanKeepsStoredVectorsUnderPositivePrefix(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+8)
	embedded := ids[0]
	chunks := [][]float32{{0.25, 0.75}}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, embedded, chunks, map[string]any{"has_embedding": true, "chunk_count": len(chunks)}, time.Time{})))

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())

	visited := 0
	require.NoError(t, tx.StreamNodesWithOptions(t.Context(), StreamNodesOptions{Label: "Held", QueryNodes: true}, func(node *Node) error {
		require.Empty(t, node.ChunkEmbeddings, "a query row carries embedding metadata, never vectors")
		visited++
		return nil
	}))
	require.Equal(t, len(ids), visited)

	for _, id := range ids {
		node, err := tx.GetNode(t.Context(), id)
		require.NoError(t, err)
		if id != embedded {
			require.Empty(t, node.ChunkEmbeddings)
			continue
		}
		require.Equal(t, chunks, node.ChunkEmbeddings, "a query image never replaces the row's stored vectors")
		hasEmbedding, _ := node.EmbedMeta["has_embedding"].(bool)
		require.True(t, hasEmbedding)
	}
	require.True(t, tx.embeddingAbsence.proved)
	require.False(t, tx.embeddingAbsence.absent, "a snapshot holding an embedding record never claims absence")
}

// The range probe is a read of the snapshot, so a canceled one is a read
// failure: it keeps no proof and admits nothing to the caches, and the next
// read of that snapshot probes again.
func TestSnapshotEmbeddingAbsenceCanceledProbeAdmitsNothing(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads)

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())
	images := pinnedAbsenceReadQueryNodes(t, tx, ids[:snapshotEmbeddingAbsenceProbeReads-1])
	require.False(t, tx.embeddingAbsence.proved, "a bounded read takes no proof")
	require.Equal(t, uint64(snapshotEmbeddingAbsenceProbeReads-1), tx.embeddingAbsence.reads)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, tx.withSnapshotViewLocked(func(snapshot *badger.Txn) error {
		return tx.cacheSnapshotPrefixNodeByIDLocked(canceled, snapshot, images[0])
	}), context.Canceled, "a canceled probe is a read failure, never an empty range")
	require.False(t, tx.embeddingAbsence.proved, "a canceled probe keeps no proof")
	_, admitted := tx.snapshotPrefixNodeByID[images[0].ID]
	require.False(t, admitted, "a canceled probe admits nothing")

	// The next read of that snapshot probes again and admits the body it read.
	node, err := tx.GetQueryNode(t.Context(), images[0].ID)
	require.NoError(t, err)
	require.Empty(t, node.ChunkEmbeddings)
	require.True(t, tx.embeddingAbsence.proved)
	require.True(t, tx.embeddingAbsence.absent)
	_, admitted = tx.snapshotPrefixNodeByID[images[0].ID]
	require.True(t, admitted, "the next read of the snapshot admits the body it read")
}

// The engine's own label-scan kernel takes the same per-view proof through the
// public scoped reader: a range that is not empty keeps every row's stored
// vectors, and a range that is empty never suppresses vectors a body carries
// itself.
func TestEngineLabelScanEmbeddingAbsenceKeepsStoredAndBodyVectors(t *testing.T) {
	t.Run("positive prefix", func(t *testing.T) {
		b := newTestEngine(t)
		ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+8)
		embedded := ids[len(ids)-1]
		chunks := [][]float32{{0.125, 0.5}}
		require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, embedded, chunks, map[string]any{"has_embedding": true, "chunk_count": len(chunks)}, time.Time{})))

		full := map[NodeID]*Node{}
		require.NoError(t, b.StreamNodesByLabelProjectedInScope(t.Context(), "test", "Held", nil, func(node *Node) error {
			full[node.ID] = node
			return nil
		}))
		require.Len(t, full, len(ids))
		require.Equal(t, chunks, full[embedded].ChunkEmbeddings, "a scanned row still hydrates its separately stored vectors")
		require.Empty(t, full[ids[0]].ChunkEmbeddings)

		query := map[NodeID]*Node{}
		require.NoError(t, b.StreamNodesWithOptions(t.Context(), StreamNodesOptions{Prefix: "test", Label: "Held", QueryNodes: true}, func(node *Node) error {
			query[node.ID] = node
			return nil
		}))
		require.Len(t, query, len(ids))
		require.Empty(t, query[embedded].ChunkEmbeddings, "a query row carries embedding metadata, never vectors")
		hasEmbedding, _ := query[embedded].EmbedMeta["has_embedding"].(bool)
		require.True(t, hasEmbedding)
	})

	t.Run("empty range", func(t *testing.T) {
		b := newTestEngine(t)
		ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+8)
		// Vectors the body carries are outside the embedding key range, so an
		// empty range says nothing about them.
		inlineID := NodeID("test:pinned-absence-inline")
		inlineChunks := [][]float32{{0.375, 0.875}}
		inlineNamed := map[string][]float32{"title": {0.25}}
		_, err := b.CreateNode(t.Context(), &Node{
			ID:              inlineID,
			Labels:          []string{"Held"},
			ChunkEmbeddings: inlineChunks,
			NamedEmbeddings: inlineNamed,
		})
		require.NoError(t, err)

		full := map[NodeID]*Node{}
		require.NoError(t, b.StreamNodesByLabelProjectedInScope(t.Context(), "test", "Held", nil, func(node *Node) error {
			full[node.ID] = node
			return nil
		}))
		require.Len(t, full, len(ids)+1)
		require.Equal(t, inlineChunks, full[inlineID].ChunkEmbeddings, "the empty-range proof never suppresses vectors a body carries")
		require.Equal(t, inlineNamed, full[inlineID].NamedEmbeddings)
		for _, id := range ids {
			require.Empty(t, full[id].ChunkEmbeddings)
		}
	})
}

// BenchmarkSnapshotLabelScanThenEmbeddingReads measures the shape the label
// cache and the absence proof serve together: one whole-label scan followed by
// a point read of every scanned target in the same transaction. Each pass runs
// in its own transaction, because a repeated read of the same population
// inside one transaction is served by that transaction's own snapshot caches.
// Sizes cross the delayed-probe threshold so the one probe can be compared with
// the per-row embedding lookups it replaces; the proof is one fixed-size
// record whatever the population size, so no per-row state grows with it.
func BenchmarkSnapshotLabelScanThenEmbeddingReads(b *testing.B) {
	for _, size := range []int{1, 63, 64, 65, 256, 2000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = engine.Close() })
			ids := make([]NodeID, size)
			for i := range ids {
				ids[i] = NodeID(fmt.Sprintf("test:label-absence-%d", i))
				if _, err := engine.CreateNode(b.Context(), &Node{
					ID:         ids[i],
					Labels:     []string{"Held"},
					Properties: map[string]any{"n": int64(i)},
				}); err != nil {
					b.Fatal(err)
				}
			}
			readPass := func() {
				tx, err := engine.BeginTransaction(b.Context(), OrdinaryBegin)
				if err != nil {
					b.Fatal(err)
				}
				if err := tx.SetNamespace("test"); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				if err := tx.StreamNodesByLabelProjected(b.Context(), "Held", nil, func(*Node) error { return nil }); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				for _, id := range ids {
					if _, err := tx.GetNode(b.Context(), id); err != nil {
						_ = tx.Rollback()
						b.Fatal(err)
					}
				}
				if err := tx.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
			readPass()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				readPass()
			}
			b.StopTimer()
		})
	}
}

func TestSnapshotEmbeddingAbsenceCancellationPrecedesMemoReuse(t *testing.T) {
	b := newTestEngine(t)
	tx := nor160BeginPinned(t, b, "test")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, proved := range []bool{false, true} {
		proof := snapshotEmbeddingAbsence{readTs: tx.snapshotTx.ReadTs(), proved: proved, absent: true}
		before := proof
		absent, err := proof.recordsAbsent(ctx, tx.snapshotTx)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, absent)
		require.Equal(t, before, proof, "cancellation neither advances nor consumes the proof")
	}
}
