package storage

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/stretchr/testify/require"
)

func TestManagedVectorFingerprintPreservesShapeAndBits(t *testing.T) {
	images := [][][]float32{
		nil, {}, {nil}, {{}}, {{1, 2}}, {{1}, {2}}, {{0}},
		{{math.Float32frombits(0x80000000)}},
		{{math.Float32frombits(0x7fc00001)}},
		{{math.Float32frombits(0x7fc00002)}},
	}
	seen := make(map[[32]byte]int, len(images))
	for index, image := range images {
		fingerprint, err := managedVectorFingerprint(t.Context(), image)
		require.NoError(t, err)
		previous, duplicate := seen[fingerprint]
		require.False(t, duplicate, "images %d and %d must stay distinct", previous, index)
		seen[fingerprint] = index
		copyHash, err := managedVectorFingerprint(t.Context(), image)
		require.NoError(t, err)
		require.Equal(t, fingerprint, copyHash)
	}
}

func TestManagedVectorFingerprintStopsDuringOneLargeVector(t *testing.T) {
	ctx := newIndexPollCancelContext(t, 3)
	_, err := managedVectorFingerprint(ctx, [][]float32{make([]float32, 1<<16)})
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("the fingerprint must observe actual cancellation")
	}
}

func TestManagedAnchorProofCacheRequiresIdentityDigestAndGeneration(t *testing.T) {
	var cache walManagedAnchorCache
	identity := [32]byte{1}
	digest := [32]byte{2}
	require.False(t, cache.contains(identity, digest, 0))
	cache.remember(identity, digest, 0)
	require.True(t, cache.contains(identity, digest, 0))
	require.False(t, cache.contains([32]byte{3}, digest, 0))
	require.False(t, cache.contains(identity, [32]byte{4}, 0))
	require.False(t, cache.contains(identity, digest, 1))
	cache.remember(identity, [32]byte{4}, 1)
	require.False(t, cache.contains(identity, digest, 0))
	require.True(t, cache.contains(identity, [32]byte{4}, 1))
}

func TestManagedAnchorProofCacheEvictionOnlyRemovesReuse(t *testing.T) {
	var cache walManagedAnchorCache
	for index := 0; index <= walManagedAnchorCacheSlots; index++ {
		cache.remember([32]byte{byte(index)}, [32]byte{2}, 3)
	}
	require.False(t, cache.contains([32]byte{0}, [32]byte{2}, 3))
	for index := 1; index <= walManagedAnchorCacheSlots; index++ {
		require.True(t, cache.contains([32]byte{byte(index)}, [32]byte{2}, 3))
	}
}

func TestManagedVectorReferenceReplayRequiresExactPredecessor(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "mismatched", true: "missing"}[missing], func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			const id NodeID = "test:reference-source"
			if !missing {
				_, err := engine.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: [][]float32{{9, 9}}})
				require.NoError(t, err)
			}
			digest, err := managedVectorFingerprint(t.Context(), [][]float32{{1, 2}})
			require.NoError(t, err)
			payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "exact-reference", Mode: CommitModeDirect,
				Operations:       []Operation{{Type: OpUpdateNode, Node: &Node{ID: id, Properties: map[string]any{"text": "alpha", "flag": true}}}},
				VectorReferences: ManagedVectorReferences{entries: []managedVectorReference{{Change: 0, Digest: digest}}}})
			require.NoError(t, err)
			require.Error(t, ApplyAcceptedEntityTransaction(engine, payload))
			node, err := engine.GetNode(t.Context(), id)
			if missing {
				require.ErrorIs(t, err, ErrNotFound)
				return
			}
			require.NoError(t, err)
			require.NotContains(t, node.Properties, "flag")
			require.Equal(t, [][]float32{{9, 9}}, node.ChunkEmbeddings, "failed replay must not regenerate or substitute vectors")
		})
	}
}

func TestManagedVectorReferenceDuplicateReplayDoesNotNeedOldImage(t *testing.T) {
	engine := newTestMemoryEngine(t)
	const id NodeID = "test:reference-duplicate"
	_, err := engine.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: [][]float32{{1, 2}}})
	require.NoError(t, err)
	digest, err := managedVectorFingerprint(t.Context(), [][]float32{{1, 2}})
	require.NoError(t, err)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "duplicate-reference", Mode: CommitModeDirect,
		Operations:       []Operation{{Type: OpUpdateNode, Node: &Node{ID: id, Properties: map[string]any{"text": "alpha", "flag": true}}}},
		VectorReferences: ManagedVectorReferences{entries: []managedVectorReference{{Change: 0, Digest: digest}}}})
	require.NoError(t, err)
	require.NoError(t, ApplyAcceptedEntityTransaction(engine, payload))
	node, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 2}}, node.ChunkEmbeddings)
	node.ChunkEmbeddings = [][]float32{{3, 4}}
	require.NoError(t, engine.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	require.NoError(t, ApplyAcceptedEntityTransaction(engine, payload), "receipt is observed before resolving retired predecessor images")
	node, err = engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{3, 4}}, node.ChunkEmbeddings)
}

func TestManagedVectorAnchorReusedAcrossPropertyUpdates(t *testing.T) {
	engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsStandard)
	const id NodeID = "test:anchor-reuse"
	seedPropertyUpdateVectorNode(t, engine, wrapped, id, 64)
	wal := wrapped.GetWAL()
	before := wal.Sequence()
	for index := 0; index < 3; index++ {
		node, err := wrapped.GetNode(t.Context(), id)
		require.NoError(t, err)
		node.Properties["flag"] = int64(index)
		cut := wal.Sequence()
		require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
		record := propertyUpdateJournalUpdate(t, wal, cut)
		require.Equal(t, 1, record.References)
		require.LessOrEqual(t, record.Bytes, propertyUpdateJournalByteBound)
	}
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	anchors := 0
	for _, entry := range entries {
		if entry.Sequence > before && entry.Operation == opManagedVectorAnchor {
			anchors++
		}
	}
	require.Equal(t, 1, anchors, "steady updates reuse one complete anchor rather than rewriting vectors")
	destination := openLargeCommitTestEngine(t, t.TempDir())
	for _, entry := range entries {
		require.NoError(t, ReplayWALEntry(t.Context(), destination, entry))
	}
	node, err := destination.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.EqualValues(t, 2, node.Properties["flag"])
	requirePropertyUpdateVectors(t, node, 64)
}

func TestManagedVectorReferenceWireRejectsInvalidClaims(t *testing.T) {
	digest, err := managedVectorFingerprint(t.Context(), [][]float32{{1, 2}})
	require.NoError(t, err)
	request := EntityTransactionRequest{Namespace: "test", LogicalID: "wire-reference", Mode: CommitModeDirect,
		Operations:       []Operation{{Type: OpUpdateNode, Node: &Node{ID: "test:one", ChunkEmbeddings: [][]float32{{1, 2}}}}},
		VectorReferences: ManagedVectorReferences{entries: []managedVectorReference{{Change: 0, Digest: digest}}}}
	payload, err := EncodeEntityTransaction(request)
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		edit func(*semanticTransactionWire[semanticTransactionTimestamp])
	}{
		{"out_of_range", func(w *semanticTransactionWire[semanticTransactionTimestamp]) { w.ManagedReferences[0].Change = 1 }},
		{"duplicate", func(w *semanticTransactionWire[semanticTransactionTimestamp]) {
			w.ManagedReferences = append(w.ManagedReferences, w.ManagedReferences[0])
		}},
		{"create", func(w *semanticTransactionWire[semanticTransactionTimestamp]) { w.Changes[0].Type = EntityCreateNode }},
		{"coordinates_not_omitted", func(w *semanticTransactionWire[semanticTransactionTimestamp]) {
			w.Changes[0].Node.ChunkEmbeddings = [][]float32{{1, 2}}
		}},
		{"wrong_version", func(w *semanticTransactionWire[semanticTransactionTimestamp]) { w.Version = EntityTransactionVersion }},
		{"missing_references", func(w *semanticTransactionWire[semanticTransactionTimestamp]) { w.ManagedReferences = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire semanticTransactionWire[semanticTransactionTimestamp]
			require.NoError(t, msgpack.Unmarshal(payload, &wire))
			tc.edit(&wire)
			bad, err := msgpack.Marshal(wire)
			require.NoError(t, err)
			_, _, err = DecodeEntityTransaction(bad)
			require.ErrorIs(t, err, ErrInvalidData)
		})
	}
}

func TestManagedVectorReferencesDoNotAnchorStagedReplacement(t *testing.T) {
	engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
	const id NodeID = "test:staged-replacement"
	seedPropertyUpdateVectorNode(t, engine, wrapped, id, 16)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = int64(1)
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node}))
	node, err = tx.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = int64(2)
	node.ChunkEmbeddings[0][0] = 42
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node}))
	prepared, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	defer prepared.Abort()
	receipt, err := wrapped.SubmitEntityCommit(t.Context(), prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(t.Context()))
	wal := wrapped.GetWAL()
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	destination := openLargeCommitTestEngine(t, t.TempDir())
	for _, entry := range entries {
		require.NoError(t, ReplayWALEntry(t.Context(), destination, entry))
	}
	recovered, err := destination.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.EqualValues(t, 2, recovered.Properties["flag"])
	require.Equal(t, float32(42), recovered.ChunkEmbeddings[0][0], "uncommitted replacement remains in its semantic image, never an earlier anchor")
}

func TestManagedVectorReferenceRecoveryAfterSnapshotRetiresAnchor(t *testing.T) {
	engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
	const id NodeID = "test:retired-anchor"
	seedPropertyUpdateVectorNode(t, engine, wrapped, id, 16)
	node, err := wrapped.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = int64(1)
	require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	wal := wrapped.GetWAL()
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.bin")
	snapshot, err := os.Create(snapshotPath)
	require.NoError(t, err)
	require.NoError(t, WriteSnapshot(t.Context(), wrapped, snapshot, SnapshotOptions{Sequence: wal.Sequence()}))
	require.NoError(t, snapshot.Close())
	require.NoError(t, wal.ManageSnapshot(t.Context(), wal.Sequence(), WALSnapshotTruncate))
	node, err = wrapped.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = int64(2)
	require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	anchors := 0
	for _, entry := range entries {
		if entry.Operation == opManagedVectorAnchor {
			anchors++
		}
	}
	require.Equal(t, 1, anchors, "retirement requires a new anchor rather than retaining a stale cache proof")
	destination := openLargeCommitTestEngine(t, t.TempDir())
	result, _, err := RecoverIntoEngine(t.Context(), destination, wal.Config().Dir, snapshotPath)
	require.NoError(t, err)
	require.Zero(t, result.Failed, "%v", result.Errors)
	recovered, err := destination.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.EqualValues(t, 2, recovered.Properties["flag"])
	requirePropertyUpdateVectors(t, recovered, 16)
}

func TestManagedVectorReferenceRecoveryAfterWorkerReplacesImage(t *testing.T) {
	engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
	const id NodeID = "test:worker-replacement-anchor"
	seedPropertyUpdateVectorNode(t, engine, wrapped, id, 16)
	node, err := wrapped.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = int64(1)
	require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	node, err = wrapped.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.ChunkEmbeddings[0][0] = 42
	require.NoError(t, wrapped.UpdateNodeEmbeddingSidecar(t.Context(), node))
	node, err = wrapped.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = int64(2)
	require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	wal := wrapped.GetWAL()
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	anchors := 0
	destination := openLargeCommitTestEngine(t, t.TempDir())
	for _, entry := range entries {
		if entry.Operation == opManagedVectorAnchor {
			anchors++
		}
		require.NoError(t, ReplayWALEntry(t.Context(), destination, entry))
	}
	require.Equal(t, 2, anchors, "the worker audit invalidates previous proof reuse")
	recovered, err := destination.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.EqualValues(t, 2, recovered.Properties["flag"])
	require.Equal(t, float32(42), recovered.ChunkEmbeddings[0][0])
	require.Equal(t, propertyUpdateNamedVectorValues(), recovered.NamedEmbeddings[propertyUpdateNamedVector])
}
