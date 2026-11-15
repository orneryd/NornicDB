package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// B30/B37: what the journal writes for a property-only node update that keeps
// the node's managed chunk vectors and its user-provided named vector.
//
// The behaviour belongs to the existing owners these tests call:
//   - WALEngine.UpdateNode is the direct public Go write path.
//   - BadgerTransaction.UpdateGraphNode stages the image the record carries,
//     and valueEncoder.entityTransaction is the one semantic encoding owner
//     that writes OpUpdateNode images (NamedEmbeddings and ChunkEmbeddings
//     unconditionally).
//   - ReplayWALEntry is the one apply owner; replaying the file's records into
//     an empty native store is journal-only recovery (no snapshot).
//
// A property-only "flag" edit on this fixture journals every unchanged vector:
// the measured records are in the log of
// TestPropertyOnlyUpdateJournalBytesScaleWithUnchangedVectors, and the desired
// bound below is not met at baseline 2950aa88f. That failing assertion is the
// reproduction; the other tests here record the causal relation and the
// recovery control that any bounded record must keep true.

// propertyUpdateJournalByteBound is the largest journal record a property-only
// update may write however many unchanged vectors the node holds. It is a
// desired bound, not a current one: the baseline record carries the vectors.
const propertyUpdateJournalByteBound = 8 << 10

// propertyUpdateJournalRecord is one decoded OpEntityCommit record written by
// one update: its physical and semantic sizes and the image it carries.
type propertyUpdateJournalRecord struct {
	Bytes      int
	Payload    int
	References int
	Images     map[NodeID]*Node
}

// propertyUpdateJournalUpdate returns the single OpEntityCommit record the
// update after afterSeq wrote.
func propertyUpdateJournalUpdate(t *testing.T, wal *WAL, afterSeq uint64) propertyUpdateJournalRecord {
	t.Helper()
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	var record *WALEntry
	for i, entry := range entries {
		if entry.Sequence <= afterSeq || entry.Operation != OpEntityCommit {
			continue
		}
		require.Nil(t, record, "one property update must write one journal record")
		record = &entries[i]
	}
	require.NotNil(t, record, "the update must be journalled")
	var data walEntityCommitData
	require.NoError(t, decodeRecoveryPayload(record.Data, &data))
	envelope, operations, err := DecodeEntityTransaction(data.Payload)
	require.NoError(t, err)
	images := make(map[NodeID]*Node, len(operations))
	for _, operation := range operations {
		require.Equal(t, OpUpdateNode, operation.Type)
		require.NotNil(t, operation.Node)
		images[operation.NodeID] = operation.Node
	}
	return propertyUpdateJournalRecord{Bytes: len(record.Data), Payload: len(data.Payload), References: len(envelope.VectorReferences.entries), Images: images}
}

// propertyUpdateVectorBytes is the raw float payload of count unchanged
// fixture vectors. The journal encodes a []float32 as a MessagePack array, so
// the record is larger than this floor.
func propertyUpdateVectorBytes(count int) int {
	return count * propertyUpdateVectorDims * 4
}

// TestPropertyOnlyUpdateJournalsUnchangedVectorBytes is the B30/B37
// reproduction on the direct path: under both record policies, a "flag"
// property update that keeps every managed chunk vector and the named vector
// must journal a bounded record while storing the same vectors.
func TestPropertyOnlyUpdateJournalsUnchangedVectorBytes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy WALRecordPolicy
	}{
		{name: "standard", policy: WALRecordsStandard},
		{name: "large", policy: WALRecordsLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, wrapped := newPropertyUpdateVectorEngine(t, tc.policy)
			const id NodeID = "test:journal-property-only"
			const chunks = 256
			seedPropertyUpdateVectorNode(t, engine, wrapped, id, chunks)
			wal := wrapped.GetWAL()

			node, err := wrapped.GetNode(t.Context(), id)
			require.NoError(t, err)
			requirePropertyUpdateVectors(t, node, chunks)
			node.Properties["flag"] = true
			after := wal.Sequence()
			require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))

			record := propertyUpdateJournalUpdate(t, wal, after)
			image := record.Images[id]
			require.NotNil(t, image, "the record carries the updated node")
			require.Equal(t, true, image.Properties["flag"])
			t.Logf("%s: unchanged chunk bytes=%d named vector bytes=%d record bytes=%d payload bytes=%d",
				tc.name, propertyUpdateVectorBytes(chunks), propertyUpdateVectorBytes(1), record.Bytes, record.Payload)

			// The stored result is correct today and must stay correct.
			stored, err := wrapped.GetNode(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, true, stored.Properties["flag"])
			require.Equal(t, "alpha", stored.Properties["text"])
			requirePropertyUpdateVectors(t, stored, chunks)

			// Desired outcome: unchanged vectors do not reach the journal, so
			// the record is bounded independently of how many the node holds.
			require.LessOrEqual(t, record.Bytes, propertyUpdateJournalByteBound,
				"a property-only update journalled %d bytes, %d of them unchanged chunk vectors and %d the named vector",
				record.Bytes, propertyUpdateVectorBytes(chunks), propertyUpdateVectorBytes(1))
		})
	}
}

// TestPropertyOnlyUpdateJournalBytesScaleWithUnchangedVectors shows the cause
// is the vector count and not the edit: identical "flag" edits journal records
// that differ by at least the vectors added between them, while every stored
// result stays exact.
func TestPropertyOnlyUpdateJournalBytesScaleWithUnchangedVectors(t *testing.T) {
	type sample struct {
		chunks int
		bytes  int
	}
	samples := make([]sample, 0, 3)
	for _, chunks := range []int{16, 64, 256} {
		engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
		id := NodeID(fmt.Sprintf("test:journal-scale-%d", chunks))
		seedPropertyUpdateVectorNode(t, engine, wrapped, id, chunks)
		wal := wrapped.GetWAL()

		node, err := wrapped.GetNode(t.Context(), id)
		require.NoError(t, err)
		node.Properties["flag"] = true
		after := wal.Sequence()
		require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
		record := propertyUpdateJournalUpdate(t, wal, after)
		require.Contains(t, record.Images, id)

		// The record is at least the unchanged vectors it carries.
		require.LessOrEqual(t, record.Bytes, propertyUpdateJournalByteBound)
		samples = append(samples, sample{chunks: chunks, bytes: record.Bytes})
		t.Logf("%d unchanged chunk vectors (raw %d bytes) + 1 named vector: record bytes=%d",
			chunks, propertyUpdateVectorBytes(chunks), record.Bytes)

		stored, err := wrapped.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, true, stored.Properties["flag"])
		requirePropertyUpdateVectors(t, stored, chunks)
	}
	for i := 1; i < len(samples); i++ {
		grew := samples[i].bytes - samples[i-1].bytes
		added := propertyUpdateVectorBytes(samples[i].chunks - samples[i-1].chunks)
		t.Logf("%d -> %d unchanged vectors: record grew %d bytes for %d bytes of added vectors",
			samples[i-1].chunks, samples[i].chunks, grew, added)
		require.LessOrEqual(t, grew, 64,
			"reference metadata, not unchanged vectors, bounds record growth")
	}
}

// TestPropertyOnlyUpdateJournalOnlyRecoveryKeepsExactVectors is the required
// control: replaying the journal file alone into an empty native store (no
// snapshot) restores the exact chunk and named vectors and the edited
// property. Any bounded unchanged-vector record must keep this true rather than
// earn its bytes by dropping what replay needs.
func TestPropertyOnlyUpdateJournalOnlyRecoveryKeepsExactVectors(t *testing.T) {
	engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
	const id NodeID = "test:journal-recovery"
	const chunks = 64
	seedPropertyUpdateVectorNode(t, engine, wrapped, id, chunks)
	wal := wrapped.GetWAL()

	node, err := wrapped.GetNode(t.Context(), id)
	require.NoError(t, err)
	node.Properties["flag"] = true
	node.Properties["n"] = int64(7)
	require.NoError(t, wrapped.UpdateNode(t.Context(), NodeUpdateRequest{After: node}))
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(t.Context(), wal.Config().Dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	// Journal only: no snapshot path is supplied, and the destination starts
	// empty.
	destination := openLargeCommitTestEngine(t, t.TempDir())
	for _, entry := range entries {
		require.NoError(t, ReplayWALEntry(t.Context(), destination, entry), "replaying sequence %d (%s)", entry.Sequence, entry.Operation)
	}
	recovered, err := destination.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, true, recovered.Properties["flag"])
	require.EqualValues(t, 7, recovered.Properties["n"])
	require.Equal(t, "alpha", recovered.Properties["text"])
	requirePropertyUpdateVectors(t, recovered, chunks)
}

// TestPropertyUpdateJournalDistinguishesVectorImages records what the journal
// writes for the cases the causal inventory must separate: unchanged vectors,
// replacement vectors, source-text invalidation, and the embedding sidecar's
// own bounded audit record.
func TestPropertyUpdateJournalDistinguishesVectorImages(t *testing.T) {
	engine, wrapped := newPropertyUpdateVectorEngine(t, WALRecordsLarge)
	const id NodeID = "test:journal-images"
	const chunks = 16
	seedPropertyUpdateVectorNode(t, engine, wrapped, id, chunks)
	wal := wrapped.GetWAL()
	ctx := context.Background()

	// The embedding sidecar writeback journals an OpUpdateEmbedding audit; it
	// is the existing precedent for a bounded record and carries no vectors.
	require.NoError(t, wal.Sync())
	entries, err := ReadWALEntriesFromDir(ctx, wal.Config().Dir)
	require.NoError(t, err)
	var audits int
	for _, entry := range entries {
		if entry.Operation != OpUpdateEmbedding {
			continue
		}
		audits++
		require.Less(t, len(entry.Data), 1024, "the sidecar audit must not scale with vectors")
	}
	require.Equal(t, 1, audits)

	// Unchanged vectors: the node's full image (the reproduction's case).
	node, err := wrapped.GetNode(ctx, id)
	require.NoError(t, err)
	node.Properties["flag"] = true
	after := wal.Sequence()
	require.NoError(t, wrapped.UpdateNode(ctx, NodeUpdateRequest{After: node}))
	unchanged := propertyUpdateJournalUpdate(t, wal, after)

	// Replacement vectors are a new image and legitimately in the record.
	node, err = wrapped.GetNode(ctx, id)
	require.NoError(t, err)
	node.ChunkEmbeddings[0][0] = 42
	after = wal.Sequence()
	require.NoError(t, wrapped.UpdateNode(ctx, NodeUpdateRequest{After: node}))
	replaced := propertyUpdateJournalUpdate(t, wal, after)

	// A source-text change invalidates the carried vectors, so the update's
	// image already has none.
	node, err = wrapped.GetNode(ctx, id)
	require.NoError(t, err)
	node.Properties["text"] = "beta"
	after = wal.Sequence()
	require.NoError(t, wrapped.UpdateNode(ctx, NodeUpdateRequest{After: node}))
	invalidated := propertyUpdateJournalUpdate(t, wal, after)

	t.Logf("unchanged vectors: %d bytes (raw chunk %d, raw named %d); replacement: %d bytes; invalidation: %d bytes",
		unchanged.Bytes, propertyUpdateVectorBytes(chunks), propertyUpdateVectorBytes(1), replaced.Bytes, invalidated.Bytes)

	require.Nil(t, unchanged.Images[id].ChunkEmbeddings, "the exact anchor supplies unchanged managed vectors")
	require.Equal(t, 1, unchanged.References)
	require.Zero(t, replaced.References, "replacement vectors remain complete")
	require.Len(t, unchanged.Images[id].NamedEmbeddings[propertyUpdateNamedVector], propertyUpdateVectorDims)
	require.GreaterOrEqual(t, replaced.Bytes, propertyUpdateVectorBytes(chunks), "a replacement image carries the new vectors")
	require.Empty(t, invalidated.Images[id].ChunkEmbeddings, "an invalidated source journals no vectors")
	require.Less(t, invalidated.Bytes, propertyUpdateVectorBytes(chunks), "invalidation is already bounded")

	stored, err := wrapped.GetNode(ctx, id)
	require.NoError(t, err)
	require.Empty(t, stored.ChunkEmbeddings)
	require.NotEqual(t, true, stored.EmbedMeta["has_embedding"])
	require.Len(t, stored.NamedEmbeddings[propertyUpdateNamedVector], propertyUpdateVectorDims,
		"a user-provided named vector is the caller's, not managed embedding state")
}
