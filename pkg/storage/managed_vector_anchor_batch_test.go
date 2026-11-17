package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// B30: the managed vector anchor is journaled through the journal's existing
// batch owner (managedVectorAnchorBatch over BatchWriter), so one transaction
// that omits many images pays one sync per bounded batch instead of one sync
// per anchor. These are the deterministic controls for that change:
//
//   - WALStats.TotalSyncs is the journal's own sync counter, so the number of
//     syncs a call needs is measured, not inferred.
//   - every batched anchor keeps the exact database of its own node, which is
//     what replay resolves the record through.
//   - replay restores the exact coordinates the anchor recorded, bit for bit.
//   - a reference and a reusable proof exist only for an anchor that is
//     durable: a queued, cancelled or failed batch yields neither.

// managedAnchorBatchJournal opens the journal in the durable mode the stalled
// job used: an ordinary append there fsyncs every record, so TotalSyncs counts
// the records that were written one by one.
func managedAnchorBatchJournal(t *testing.T, dir string) *WAL {
	t.Helper()
	cfg := DefaultWALConfig()
	cfg.Dir = dir
	cfg.SyncMode = "immediate"
	cfg.BatchSyncInterval = time.Hour
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	return wal
}

// managedAnchorBatchImage returns the acknowledged managed state of id: a
// database-qualified ID with the chunk vectors a worker stored.
func managedAnchorBatchImage(id NodeID, chunks ...[]float32) *Node {
	return &Node{
		ID:              id,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "alpha"},
		ChunkEmbeddings: chunks,
		EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": len(chunks)},
	}
}

// managedAnchorBatchBody returns the committed body of id, with no vectors: the
// body a store restored from a snapshot holds before the journal after it.
func managedAnchorBatchBody(id NodeID, at time.Time) *Node {
	return &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}, CreatedAt: at, UpdatedAt: at}
}

// managedAnchorBatchProof returns the proof values the reference path records
// for an image.
func managedAnchorBatchProof(t *testing.T, image *Node) ([sha256.Size]byte, [sha256.Size]byte) {
	t.Helper()
	digest, err := managedVectorFingerprint(t.Context(), image.ChunkEmbeddings)
	require.NoError(t, err)
	return sha256.Sum256([]byte(image.ID)), digest
}

// managedAnchorProofCached reports whether the journal holds a reusable proof
// for the image at its current managed vector epoch.
func managedAnchorProofCached(wal *WAL, identity, digest [sha256.Size]byte) bool {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.managedVectorAnchors.contains(identity, digest, wal.managedVectorGeneration.Load())
}

// managedAnchorRecordCount returns how many anchor records dir holds.
func managedAnchorRecordCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	count := 0
	for _, entry := range entries {
		if entry.Operation == opManagedVectorAnchor {
			count++
		}
	}
	return count
}

// managedAnchorReplayedStore replays dir's records into a fresh store that
// holds bodies, the way recovery does after a snapshot.
func managedAnchorReplayedStore(t *testing.T, dir string, bodies ...*Node) *MemoryEngine {
	t.Helper()
	recovered := newTestMemoryEngine(t)
	for _, body := range bodies {
		_, err := recovered.CreateNode(t.Context(), body)
		require.NoError(t, err)
	}
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, entry := range entries {
		require.NoError(t, ReplayWALEntry(t.Context(), recovered, entry))
	}
	return recovered
}

// requireExactManagedVectors compares coordinates by float bits, so signed zero
// and a NaN payload are the values that were recorded.
func requireExactManagedVectors(t *testing.T, want, got [][]float32, what string) {
	t.Helper()
	require.Len(t, got, len(want), what)
	for chunk := range want {
		require.Len(t, got[chunk], len(want[chunk]), "%s chunk %d", what, chunk)
		for offset := range want[chunk] {
			require.Equal(t, math.Float32bits(want[chunk][offset]), math.Float32bits(got[chunk][offset]), "%s chunk %d offset %d", what, chunk, offset)
		}
	}
}

// Anchors of one call are written in bounded batches: 128 anchors of 16 KiB of
// coordinates each need a handful of journal syncs, not one per anchor, while
// every anchor is still durable and recorded exactly once. One anchor whose own
// record is larger than the bound alone is journaled too
// (TestManagedVectorAnchorAboveFrameLimitSpansFrames drives a 17.4 MB anchor
// through the same owner).
func TestManagedVectorAnchorBatchBoundsJournalSyncs(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	const anchors = 128
	operations := make([]Operation, 0, anchors)
	for index := 0; index < anchors; index++ {
		before := managedAnchorBatchImage(NodeID(fmt.Sprintf("test:batched-anchor-%d", index)), largeRecordVectors(16, float32(index))...)
		after := CopyNode(before)
		after.Properties["flag"] = int64(index)
		operations = append(operations, Operation{Type: OpUpdateNode, Node: after, OldNode: before, nodeAnchorEligible: true})
	}

	computed := wal.Stats().TotalSyncs
	references, err := wal.managedReferences(t.Context(), operations, func() error { return nil })
	require.NoError(t, err)
	require.Len(t, references.entries, anchors, "every omitted image carries a reference")

	syncs := wal.Stats().TotalSyncs - computed
	require.Positive(t, syncs, "the anchors are durable before their references are used")
	require.LessOrEqual(t, syncs, int64(anchors/16), "%d anchors of 16 KiB must not cost one journal sync each", anchors)
	require.Equal(t, anchors, managedAnchorRecordCount(t, wal.Config().Dir), "each image is anchored exactly once")
}

// Two databases that hold the same unqualified node ID are anchored in one
// batch, and replay restores each node's own coordinates: a batched record
// keeps its own database instead of inheriting one from the batch.
func TestManagedVectorAnchorBatchKeepsEachDatabase(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	native := newTestMemoryEngine(t)
	wrapped := NewWALEngine(native, wal)
	t.Cleanup(func() { _ = wrapped.Close() })

	at := time.Now().UTC().Truncate(time.Millisecond)
	fixtures := []struct {
		database string
		vectors  [][]float32
	}{
		{"test", [][]float32{{0.5, -1.5}, {2.5, 3.5}}},
		{"other", [][]float32{{9.5}}},
	}
	images := make([]*Node, 0, len(fixtures))
	for _, fixture := range fixtures {
		id := NodeID(fixture.database + ":shared")
		_, err := native.CreateNode(t.Context(), managedAnchorBatchBody(id, at))
		require.NoError(t, err)
		meta := map[string]any{"has_embedding": true, "chunk_count": len(fixture.vectors)}
		require.NoError(t, wrapped.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, native, id, fixture.vectors, meta, time.Time{})))
		stored, err := native.GetNode(t.Context(), id)
		require.NoError(t, err)
		images = append(images, CopyNode(stored))
	}

	anchors := wal.newManagedVectorAnchorBatch()
	for _, image := range images {
		identity, digest := managedAnchorBatchProof(t, image)
		anchored, err := anchors.anchored(t.Context(), image, identity, digest)
		require.NoError(t, err)
		require.True(t, anchored)
	}
	require.NoError(t, anchors.flush(t.Context()))

	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	journaled := make(map[string][][]float32, len(images))
	for _, entry := range entries {
		if entry.Operation != opManagedVectorAnchor {
			continue
		}
		var payload walManagedVectorAnchorPayload
		require.NoError(t, decodeRecoveryPayload(entry.Data, &payload))
		require.Equal(t, NodeID("shared"), payload.SourceNode.ID, "the record stores the source database-unqualified")
		journaled[entry.Database] = payload.SourceNode.ChunkEmbeddings
	}
	require.Len(t, journaled, len(fixtures), "each anchor is journaled under its own database")
	for _, fixture := range fixtures {
		requireExactManagedVectors(t, fixture.vectors, journaled[fixture.database], fixture.database+" anchor")
	}

	bodies := make([]*Node, 0, len(fixtures))
	for _, fixture := range fixtures {
		bodies = append(bodies, managedAnchorBatchBody(NodeID(fixture.database+":shared"), at))
	}
	recovered := managedAnchorReplayedStore(t, wal.Config().Dir, bodies...)
	for _, fixture := range fixtures {
		node, err := recovered.GetNode(t.Context(), NodeID(fixture.database+":shared"))
		require.NoError(t, err)
		requireExactManagedVectors(t, fixture.vectors, node.ChunkEmbeddings, fixture.database+" replay")
	}
}

// Replay restores the coordinates an anchor recorded exactly, including the
// values float equality cannot carry: signed zero and distinct NaN payloads.
func TestManagedVectorAnchorBatchReplaysExactCoordinates(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	native := newTestMemoryEngine(t)
	wrapped := NewWALEngine(native, wal)
	t.Cleanup(func() { _ = wrapped.Close() })

	at := time.Now().UTC().Truncate(time.Millisecond)
	vectors := [][]float32{
		{math.Float32frombits(1 << 31), math.Float32frombits(0x7fc00001)},
		{math.Float32frombits(0x7fc00002), 1.5},
	}
	const id NodeID = "test:exact-coordinates"
	_, err := native.CreateNode(t.Context(), managedAnchorBatchBody(id, at))
	require.NoError(t, err)
	meta := map[string]any{"has_embedding": true, "chunk_count": len(vectors)}
	require.NoError(t, wrapped.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, native, id, vectors, meta, time.Time{})))
	acknowledged, err := native.GetNode(t.Context(), id)
	require.NoError(t, err)
	image := CopyNode(acknowledged)

	identity, digest := managedAnchorBatchProof(t, image)
	anchors := wal.newManagedVectorAnchorBatch()
	anchored, err := anchors.anchored(t.Context(), image, identity, digest)
	require.NoError(t, err)
	require.True(t, anchored)
	require.NoError(t, anchors.flush(t.Context()))

	recovered := managedAnchorReplayedStore(t, wal.Config().Dir, managedAnchorBatchBody(id, at))
	node, err := recovered.GetNode(t.Context(), id)
	require.NoError(t, err)
	requireExactManagedVectors(t, image.ChunkEmbeddings, node.ChunkEmbeddings, "replayed coordinates")
}

// A queued anchor is not a durable one: no record and no proof exist until the
// batch commits, and the proof is reusable only at the epoch the journal
// reached after the commit.
func TestManagedVectorAnchorBatchKeepsProofsBehindDurability(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	image := managedAnchorBatchImage("test:durable-proof", []float32{1, 2}, []float32{3, 4})
	identity, digest := managedAnchorBatchProof(t, image)

	anchors := wal.newManagedVectorAnchorBatch()
	anchored, err := anchors.anchored(t.Context(), image, identity, digest)
	require.NoError(t, err)
	require.True(t, anchored)
	require.False(t, managedAnchorProofCached(wal, identity, digest), "a queued anchor proves nothing")
	require.Zero(t, managedAnchorRecordCount(t, wal.Config().Dir), "the batch is not committed yet")

	require.NoError(t, anchors.flush(t.Context()))
	require.True(t, managedAnchorProofCached(wal, identity, digest), "the durable anchor's proof is reusable")
	require.Equal(t, 1, managedAnchorRecordCount(t, wal.Config().Dir))

	// The durable proof is reused: the same image needs no anchor and no sync.
	syncs := wal.Stats().TotalSyncs
	reuse := wal.newManagedVectorAnchorBatch()
	reused, err := reuse.anchored(t.Context(), image, identity, digest)
	require.NoError(t, err)
	require.True(t, reused)
	require.Equal(t, syncs, wal.Stats().TotalSyncs, "a reused proof writes nothing")

	// An embedding audit moves the epoch, so the earlier anchor no longer
	// answers for the node's managed vectors.
	require.NoError(t, wal.AppendWithDatabase(OpUpdateEmbedding, WALNodeData{Node: &Node{ID: "durable-proof"}}, "test"))
	require.False(t, managedAnchorProofCached(wal, identity, digest), "a proof is current only at its own epoch")
}

// A cancelled batch leaves no record and no proof, and a call cancelled before
// its first anchor returns no reference: a reference never stands on an anchor
// the journal does not hold.
func TestManagedVectorAnchorBatchCancellationLeavesNothingProven(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	image := managedAnchorBatchImage("test:cancelled-anchor", []float32{1, 2})
	identity, digest := managedAnchorBatchProof(t, image)

	ctx, cancel := context.WithCancel(t.Context())
	anchors := wal.newManagedVectorAnchorBatch()
	anchored, err := anchors.anchored(ctx, image, identity, digest)
	require.NoError(t, err)
	require.True(t, anchored)
	cancel()
	require.ErrorIs(t, anchors.flush(ctx), context.Canceled)
	require.Zero(t, managedAnchorRecordCount(t, wal.Config().Dir), "a cancelled batch commits nothing")
	require.False(t, managedAnchorProofCached(wal, identity, digest), "a cancelled batch proves nothing")

	after := CopyNode(image)
	after.Properties["flag"] = true
	operations := []Operation{{Type: OpUpdateNode, Node: after, OldNode: image, nodeAnchorEligible: true}}
	cancelled, cancelCall := context.WithCancel(t.Context())
	references, err := wal.managedReferences(cancelled, operations, func() error {
		cancelCall()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, references.entries, "a cancelled call references nothing")
	require.Zero(t, managedAnchorRecordCount(t, wal.Config().Dir), "a cancelled call journals nothing")
	require.False(t, managedAnchorProofCached(wal, identity, digest), "a cancelled call proves nothing")
}

// A batch whose commit fails leaves no reusable proof, so the next call anchors
// the image again instead of trusting a record the journal never finished.
func TestManagedVectorAnchorBatchCommitFailureLeavesNoProof(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	image := managedAnchorBatchImage("test:failed-batch", []float32{1, 2})
	identity, digest := managedAnchorBatchProof(t, image)

	anchors := wal.newManagedVectorAnchorBatch()
	anchored, err := anchors.anchored(t.Context(), image, identity, digest)
	require.NoError(t, err)
	require.True(t, anchored)

	require.NoError(t, wal.Close())
	require.Error(t, anchors.flush(t.Context()), "a closed journal cannot commit the batch")
	require.False(t, managedAnchorProofCached(wal, identity, digest), "a failed batch proves nothing")
	require.Equal(t, 0, anchors.batch.Len(), "the failed batch releases its pending records")
}

func TestManagedVectorAnchorBatchDoesNotCertifyNewerGeneration(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	image := managedAnchorBatchImage("test:epoch-image", []float32{1, 2})
	identity, digest := managedAnchorBatchProof(t, image)
	anchors := wal.newManagedVectorAnchorBatch()
	queued, err := anchors.anchored(t.Context(), image, identity, digest)
	require.NoError(t, err)
	require.True(t, queued)
	// An unrelated embedding audit advances the actual journal epoch during
	// the same durable batch, without any timing hook or scheduler assumption.
	require.NoError(t, anchors.batch.append(OpUpdateEmbedding, WALNodeData{Node: &Node{ID: "another"}}, "test"))
	require.NoError(t, anchors.flush(t.Context()))
	require.False(t, managedAnchorProofCached(wal, identity, digest), "an earlier image must not inherit a later epoch")
	require.Equal(t, 1, managedAnchorRecordCount(t, wal.Config().Dir))
}

func TestManagedVectorReferencesWithoutVectorsAllocateNothing(t *testing.T) {
	wal := managedAnchorBatchJournal(t, t.TempDir())
	operations := []Operation{{Type: OpUpdateNode, Node: &Node{ID: "test:plain"}, OldNode: &Node{ID: "test:plain"}}}
	ctx := t.Context()
	admission := func() error { t.Fatal("a non-vector update needs no anchor admission"); return nil }
	allocations := testing.AllocsPerRun(100, func() {
		references, err := wal.managedReferences(ctx, operations, admission)
		if err != nil || len(references.entries) != 0 {
			t.Fatal("ordinary writes must not manufacture vector references", err)
		}
	})
	require.Zero(t, allocations, "ordinary writes must not allocate vector batching state")
}
