package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Experiment: every pinned point read hydrates embedding state through
// loadNodeEmbeddingState, which asks for the node's sidecar record — a
// missing-key lookup per node on a snapshot that holds no embedding record at
// all. The scan path already proves that absence once per snapshot
// (embeddingRecordsAbsentAt). These tests establish the snapshot behavior the
// pinned point-read route must keep when it reuses that one proof for its own
// snapshot, through the public native transaction only.

// pinnedAbsenceSeededNodes commits count nodes without separately stored
// embedding state under namespace test.
func pinnedAbsenceSeededNodes(t *testing.T, b *BadgerEngine, count int) []NodeID {
	t.Helper()
	ids := make([]NodeID, 0, count)
	for i := 0; i < count; i++ {
		id := NodeID(fmt.Sprintf("test:pinned-absence-%d", i))
		_, err := b.CreateNode(t.Context(), &Node{
			ID:         id,
			Labels:     []string{"Held"},
			Properties: map[string]any{"n": int64(i)},
		})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	return ids
}

// pinnedAbsenceReadQueryNodes reads every id through the transaction's query
// row read, which settles the node's embedding metadata without its vectors.
func pinnedAbsenceReadQueryNodes(t *testing.T, tx *BadgerTransaction, ids []NodeID) []*Node {
	t.Helper()
	nodes := make([]*Node, 0, len(ids))
	for _, id := range ids {
		node, err := tx.GetQueryNode(t.Context(), id)
		require.NoError(t, err)
		require.NotNil(t, node)
		nodes = append(nodes, node)
	}
	return nodes
}

// A snapshot holding no embedding record answers that range question once, at
// the read count the proof is delayed to, and every later read of the snapshot
// reuses it. Body-held vectors are never suppressed by it.
func TestPinnedSnapshotEmbeddingAbsenceProvesEmptyRangeOnce(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+8)
	// Named vectors and small chunk vectors live in the node body, not in the
	// embedding key range, so the range proof has nothing to do with them.
	inline := NodeID("test:pinned-absence-inline")
	named := map[string][]float32{"title": {0.5, 0.25}}
	chunks := [][]float32{{0.125, 0.375}}
	_, err := b.CreateNode(t.Context(), &Node{
		ID:              inline,
		Labels:          []string{"Held"},
		Properties:      map[string]any{"n": int64(1000)},
		NamedEmbeddings: named,
		ChunkEmbeddings: chunks,
	})
	require.NoError(t, err)

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())

	// A bounded read of the snapshot keeps the per-node lookup: the proof costs
	// one iterator over the embedding range and is not worth taking for a
	// single node.
	for _, node := range pinnedAbsenceReadQueryNodes(t, tx, ids[:snapshotEmbeddingAbsenceProbeReads-1]) {
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.False(t, tx.embeddingAbsence.proved, "a bounded read must not take the range proof")
	require.Equal(t, uint64(snapshotEmbeddingAbsenceProbeReads-1), tx.embeddingAbsence.reads)

	// The snapshot's 64th node read answers the range question once.
	node, err := tx.GetQueryNode(t.Context(), ids[snapshotEmbeddingAbsenceProbeReads-1])
	require.NoError(t, err)
	require.Empty(t, node.ChunkEmbeddings)
	proof := tx.embeddingAbsence
	require.True(t, proof.proved)
	require.True(t, proof.absent, "a snapshot with no embedding record proves its range empty")
	require.Equal(t, tx.snapshotTx.ReadTs(), proof.readTs, "the proof belongs to the snapshot the reads run on")

	// Every later read of this snapshot reuses that one record, so the route
	// retains nothing per node.
	for _, later := range pinnedAbsenceReadQueryNodes(t, tx, ids[snapshotEmbeddingAbsenceProbeReads:]) {
		require.Empty(t, later.ChunkEmbeddings)
		require.Empty(t, later.EmbedMeta)
	}
	full, err := tx.GetNode(t.Context(), inline)
	require.NoError(t, err)
	require.Equal(t, chunks, full.ChunkEmbeddings, "vectors the body carries are never suppressed by the range proof")
	require.Equal(t, named, full.NamedEmbeddings)
	require.Equal(t, proof, tx.embeddingAbsence, "the snapshot keeps one proof record, not one per node")
}

// A snapshot that does hold an embedding record never answers absent, so every
// node keeps its own hydration and stored vectors still arrive exactly.
func TestPinnedSnapshotEmbeddingAbsenceKeepsLookupWhenSnapshotHoldsEmbeddings(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+1)
	withVectors := ids[0]
	chunks := [][]float32{{0.125, 0.25}, {0.5, 0.75}}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, withVectors, chunks, map[string]any{"has_embedding": true, "chunk_count": len(chunks)}, time.Time{})))

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())
	for _, node := range pinnedAbsenceReadQueryNodes(t, tx, ids) {
		require.Empty(t, node.ChunkEmbeddings, "a query row carries embedding metadata, never vectors")
	}
	require.True(t, tx.embeddingAbsence.proved, "the snapshot still answers its range question once")
	require.False(t, tx.embeddingAbsence.absent, "a snapshot holding an embedding record never claims absence")

	// The per-node lookup stays, so the separately stored chunks hydrate.
	full, err := tx.GetNode(t.Context(), withVectors)
	require.NoError(t, err)
	require.Equal(t, chunks, full.ChunkEmbeddings)
	require.Equal(t, len(chunks), chunkCountFromMeta(full.EmbedMeta))
	hasEmbedding, _ := full.EmbedMeta["has_embedding"].(bool)
	require.True(t, hasEmbedding)
}

// A peer's embedding write is not in an older pinned snapshot, however many of
// that snapshot's nodes are read; a transaction pinned afterwards sees it.
func TestPinnedSnapshotEmbeddingAbsenceKeepsPeerEmbeddingsOutOfPinnedSnapshot(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads)
	peer := ids[len(ids)-1]
	chunks := [][]float32{{0.5, 0.75}}

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())
	// The peer stores embeddings after this transaction's cut.
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, peer, chunks, map[string]any{"has_embedding": true, "chunk_count": len(chunks)}, time.Time{})))

	for _, node := range pinnedAbsenceReadQueryNodes(t, tx, ids[:len(ids)-1]) {
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.False(t, tx.embeddingAbsence.proved)
	old, err := tx.GetNode(t.Context(), peer)
	require.NoError(t, err)
	require.Empty(t, old.ChunkEmbeddings, "a peer's post-cut embedding state stays out of the pinned snapshot")
	require.True(t, tx.embeddingAbsence.absent, "the snapshot that proved its range empty is this transaction's own")

	// A transaction pinned after the peer's write reads the stored vectors.
	fresh := nor160BeginPinned(t, b, "test")
	node, err := fresh.GetNode(t.Context(), peer)
	require.NoError(t, err)
	require.Equal(t, chunks, node.ChunkEmbeddings)
}

// The transaction's own staged vectors precede the pinned body, so the range
// proof cannot suppress them.
func TestPinnedSnapshotEmbeddingAbsenceKeepsOwnStagedVectors(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads+1)
	target := ids[snapshotEmbeddingAbsenceProbeReads]

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())
	for _, node := range pinnedAbsenceReadQueryNodes(t, tx, ids[:snapshotEmbeddingAbsenceProbeReads]) {
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.True(t, tx.embeddingAbsence.absent)

	before, err := tx.GetQueryNode(t.Context(), target)
	require.NoError(t, err)
	staged := [][]float32{{0.25, 0.5}}
	named := map[string][]float32{"title": {0.75}}
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: &Node{
		ID:              target,
		Labels:          []string{"Held"},
		Properties:      map[string]any{"n": int64(7)},
		ChunkEmbeddings: staged,
		NamedEmbeddings: named,
	}}))
	own, err := tx.GetNode(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, staged, own.ChunkEmbeddings, "the transaction's staged vectors are read-your-writes")
	require.Equal(t, named, own.NamedEmbeddings)
	require.Equal(t, int64(7), own.Properties["n"])
}

// A canceled read retires with the caller's error and keeps no proof, so the
// next read of the same snapshot still answers with it.
func TestPinnedSnapshotEmbeddingAbsenceHonorsCancellation(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads)
	last := ids[len(ids)-1]

	tx := nor160BeginPinned(t, b, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked())
	for _, node := range pinnedAbsenceReadQueryNodes(t, tx, ids[:len(ids)-1]) {
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.False(t, tx.embeddingAbsence.proved)
	require.Equal(t, uint64(snapshotEmbeddingAbsenceProbeReads-1), tx.embeddingAbsence.reads)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tx.mu.Lock()
	_, err := tx.readPinnedSnapshotNodeLocked(canceled, last, false, visibleRead)
	tx.mu.Unlock()
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, tx.embeddingAbsence.proved, "a canceled read never keeps a proof")

	node, err := tx.GetQueryNode(t.Context(), last)
	require.NoError(t, err)
	require.Empty(t, node.ChunkEmbeddings)
	require.True(t, tx.embeddingAbsence.proved, "the next read of the snapshot takes it")
	require.True(t, tx.embeddingAbsence.absent)
}

// An advanced snapshot proves its own range: the previous cut's absence is
// never inherited, so a record that landed in between becomes visible.
func TestPinnedSnapshotEmbeddingAbsenceRecomputesAfterSnapshotContinuation(t *testing.T) {
	b := newTestEngine(t)
	ids := pinnedAbsenceSeededNodes(t, b, snapshotEmbeddingAbsenceProbeReads)
	target := ids[0]
	chunks := [][]float32{{0.5, 0.75}}

	tx := nor160BeginPinned(t, b, "test")
	// The implicit continuation is the transaction's supported re-pin: it keeps
	// this transaction's own writes and advances its pinned snapshot.
	require.NoError(t, tx.SetImplicit(true))
	require.True(t, tx.readsPinnedSnapshotBodyLocked())
	for _, node := range pinnedAbsenceReadQueryNodes(t, tx, ids) {
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.True(t, tx.embeddingAbsence.proved)
	require.True(t, tx.embeddingAbsence.absent)
	previous := tx.embeddingAbsence.readTs

	// A peer stores embeddings, then the transaction continues onto a snapshot
	// taken past that commit.
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, b, target, chunks, map[string]any{"has_embedding": true, "chunk_count": len(chunks)}, time.Time{})))
	require.NoError(t, tx.ResumeAfterIndependentCommit(t.Context()))
	require.True(t, tx.readsPinnedSnapshotBodyLocked(), "the continuation re-pins this namespace")
	refreshed := tx.snapshotTx.ReadTs()
	require.Greater(t, refreshed, previous, "the continuation advances the physical snapshot")

	// The refreshed snapshot reads its own cut, under its own proof.
	full, err := tx.GetNode(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, chunks, full.ChunkEmbeddings, "the refreshed snapshot's embedding record hydrates")
	require.Equal(t, refreshed, tx.embeddingAbsence.readTs)
	require.False(t, tx.embeddingAbsence.proved, "the refreshed snapshot has proved nothing yet")

	for _, node := range pinnedAbsenceReadQueryNodes(t, tx, ids) {
		require.Empty(t, node.ChunkEmbeddings)
	}
	require.True(t, tx.embeddingAbsence.proved)
	require.False(t, tx.embeddingAbsence.absent, "a snapshot holding that record never claims absence")
}

// BenchmarkPinnedSnapshotEmbeddingAbsence measures one pinned pass over a
// population, which is where the per-node missing-sidecar lookup is paid. Each
// pass runs in its own transaction, because a repeated read of the same node
// inside one transaction is served by that transaction's existing bounded
// snapshot cache. Sizes cross the delayed-probe threshold so the bounded probe
// can be compared with the per-node lookups it replaces; the proof itself is
// one fixed-size record whatever the population size, so no per-node state
// grows with it.
func BenchmarkPinnedSnapshotEmbeddingAbsence(b *testing.B) {
	for _, size := range []int{1, 8, 63, 64, 65, 256, 2000} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = engine.Close() })
			ids := make([]NodeID, size)
			for i := range ids {
				ids[i] = NodeID(fmt.Sprintf("test:pinned-absence-%d", i))
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
				for _, id := range ids {
					if _, err := tx.GetQueryNode(b.Context(), id); err != nil {
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
