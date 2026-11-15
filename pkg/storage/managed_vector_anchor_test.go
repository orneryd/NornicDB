package storage

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// B30: the managed vector anchor record (`managed_vector_anchor`), the one
// journal record that carries a node's acknowledged managed embedding state so
// a store restored from a snapshot plus the journal after it can restore the
// vectors an embedding worker acknowledged. These tests drive the existing
// owners the component uses:
//
//   - WAL.appendManagedVectorAnchor is the only writer of the record; its
//     sequence, checksum, framing and roll-over are the journal's existing
//     owners (encodeNextRecordLocked, appendRecordLocked, maybeRotateLocked).
//   - walRecordMaySpan is the one approval that lets that record span frames
//     under WALRecordsStandard; every other operation keeps the 16 MiB limit.
//   - ReplayWALEntry/replayManagedVectorAnchor is the one apply owner, and it
//     restores managed chunk vectors and metadata through the existing
//     BadgerEngine.UpdateNodeEmbeddingSidecar source-validation owner.
//
// The store is a native MemoryEngine (a BadgerEngine with in-memory options)
// with the node body seeded directly, as a snapshot would: the journal holds
// only the embedding audit and the anchor, and replay is applied to a store
// that already holds the committed bodies.

const managedAnchorID NodeID = "test:anchor-node"

func managedAnchorBody(at time.Time) *Node {
	return &Node{
		ID:              managedAnchorID,
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"text": "alpha", "flag": false, "a": int64(1)},
		CreatedAt:       at,
		UpdatedAt:       at,
		NamedEmbeddings: map[string][]float32{"title": {0.25, -0.5, 1.5}},
	}
}

func managedAnchorState(chunks int, seed float32) ([][]float32, map[string]any) {
	return largeRecordVectors(chunks, seed), map[string]any{
		"has_embedding":        true,
		"chunk_count":          chunks,
		"embedding_dimensions": largeRecordDims,
		"embedding_model":      "synthetic",
	}
}

// managedAnchorFixture opens a standard-policy journal over a native
// in-memory store, seeds the committed body, publishes the managed vectors
// through the WAL engine (which journals the OpUpdateEmbedding audit) and
// returns the journal and the acknowledged node image an anchor is built from.
func managedAnchorFixture(t *testing.T, cfg *WALConfig, chunks int, seed float32) (*WAL, *Node) {
	t.Helper()
	native := newTestMemoryEngine(t)
	dir := t.TempDir()
	if cfg == nil {
		cfg = DefaultWALConfig()
	}
	cfg.Dir = dir
	wal, err := NewWAL(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	wrapped := NewWALEngine(native, wal)
	t.Cleanup(func() { _ = wrapped.Close() })

	at := time.Now().UTC().Truncate(time.Millisecond)
	_, err = native.CreateNode(t.Context(), managedAnchorBody(at))
	require.NoError(t, err)
	vectors, meta := managedAnchorState(chunks, seed)
	require.NoError(t, wrapped.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, native, managedAnchorID, vectors, meta, time.Time{})))
	acknowledged, err := native.GetNode(t.Context(), managedAnchorID)
	require.NoError(t, err)
	require.Equal(t, vectors, acknowledged.ChunkEmbeddings)
	return wal, CopyNode(acknowledged)
}

// managedAnchorReplay applies every journal entry of dir to a fresh in-memory
// store that holds seed's committed bodies, the way recovery does after a
// snapshot.
func managedAnchorReplay(t *testing.T, dir string, seed func(*MemoryEngine)) (*MemoryEngine, error) {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	recovered := newTestMemoryEngine(t)
	if seed != nil {
		seed(recovered)
	}
	for _, entry := range entries {
		if err := ReplayWALEntry(t.Context(), recovered, entry); err != nil {
			return recovered, err
		}
	}
	return recovered, nil
}

func managedAnchorEntry(t *testing.T, dir string) (WALEntry, walManagedVectorAnchorPayload) {
	t.Helper()
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	var found *WALEntry
	for i, entry := range entries {
		if entry.Operation == opManagedVectorAnchor {
			require.Nil(t, found, "one anchor append writes one anchor record")
			found = &entries[i]
		}
	}
	require.NotNil(t, found, "the journal holds the anchor")
	var payload walManagedVectorAnchorPayload
	require.NoError(t, decodeRecoveryPayload(found.Data, &payload))
	return *found, payload
}

// An anchor larger than one 16 MiB frame is written in parts and reads back
// whole under both record policies: the anchor's own approved spanning
// capability is not the journal's record policy. The record carries the
// acknowledged managed state - chunk vectors, metadata and the source body -
// with the caller's named vectors left out.
func TestManagedVectorAnchorAboveFrameLimitSpansFrames(t *testing.T) {
	const chunks = 17000 // x 256 dims x 4 bytes = 17.4 MB
	for _, policy := range []struct {
		name string
		cfg  WALRecordPolicy
	}{
		{"standard", WALRecordsStandard},
		{"large", WALRecordsLarge},
	} {
		t.Run(policy.name, func(t *testing.T) {
			cfg := DefaultWALConfig()
			cfg.RecordPolicy = policy.cfg
			wal, acknowledged := managedAnchorFixture(t, cfg, chunks, 1)

			seq, err := wal.appendManagedVectorAnchor(t.Context(), acknowledged)
			require.NoError(t, err, "an anchor larger than one frame must be journaled")
			require.NoError(t, wal.Sync())

			entry, payload := managedAnchorEntry(t, wal.Config().Dir)
			require.Equal(t, seq, entry.Sequence)
			require.Equal(t, "test", entry.Database)
			require.Greater(t, len(entry.Data), int(walMaxEntrySize), "the anchor is larger than one frame")
			require.Equal(t, walFormatVersionSpanned, entry.framingVersion, "the record was stored in parts")
			require.Equal(t, managedVectorAnchorVersion, payload.Version)
			require.NotNil(t, payload.SourceNode)
			require.Equal(t, NodeID("anchor-node"), payload.SourceNode.ID, "the source is stored database-unqualified")
			require.Nil(t, payload.SourceNode.NamedEmbeddings, "named vectors are not part of the anchor")
			require.Equal(t, acknowledged.ChunkEmbeddings, payload.SourceNode.ChunkEmbeddings)
			requireSameStoredMap(t, acknowledged.EmbedMeta, payload.SourceNode.EmbedMeta, "embedding metadata")
			require.Equal(t, acknowledged.Properties, payload.SourceNode.Properties)
			require.Equal(t, acknowledged.Labels, payload.SourceNode.Labels)
			require.True(t, acknowledged.CreatedAt.Equal(payload.SourceNode.CreatedAt))
			require.True(t, acknowledged.UpdatedAt.Equal(payload.SourceNode.UpdatedAt))

			// The physical record is part frames carrying the one sequence.
			parts := 0
			for _, frame := range walTestFrames(t, walActivePath(wal.Config().Dir)) {
				if frame.version != walFormatVersionSpanned {
					continue
				}
				require.Equal(t, OpRecordPart, frame.op)
				require.Equal(t, seq, frame.seq)
				parts++
			}
			require.GreaterOrEqual(t, parts, 2, "a record over one frame is written in parts")
		})
	}
}

// Ordinary records keep the standard policy's 16 MiB one-frame limit: a large
// OpCreateNode or OpUpdateNode record is refused before anything is written,
// and the sequence and entry count do not move.
func TestOrdinaryLargeRecordStillRefusedUnderStandardPolicy(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	require.Equal(t, WALRecordsStandard, wal.recordPolicy)

	big := strings.Repeat("x", int(walMaxEntrySize)+1)
	for _, op := range []OperationType{OpCreateNode, OpUpdateNode} {
		node := &Node{ID: "test:ordinary-large", Labels: []string{"Doc"}, Properties: map[string]any{"text": big}}
		err := wal.AppendWithDatabase(op, WALNodeData{Node: node}, "test")
		require.Error(t, err, "%s of more than one frame must be refused", op)
		require.Contains(t, err.Error(), "exceeds maximum WAL frame size")
		require.Zero(t, wal.Sequence(), "%s refused before the record was numbered", op)
		require.Zero(t, wal.Stats().EntryCount)
	}
	info, err := os.Stat(walActivePath(dir))
	if err == nil {
		require.Zero(t, info.Size(), "a refused record writes nothing")
	}
}

// A journal that ends inside the last part of a spanned anchor delivers no
// anchor: the readers drop the incomplete record, the integrity check names it,
// and recovery replaying the entries publishes no managed vectors.
func TestManagedVectorAnchorWithoutLastPartIsNotPublished(t *testing.T) {
	restoreChunk := walSpannedChunkBytes
	walSpannedChunkBytes = 64 << 10
	defer func() { walSpannedChunkBytes = restoreChunk }()

	wal, acknowledged := managedAnchorFixture(t, nil, 17000, 2)
	walSpannedChunkBytes = 64 << 10
	seq, err := wal.appendManagedVectorAnchor(t.Context(), acknowledged)
	require.NoError(t, err)
	require.NoError(t, wal.Close())
	dir := wal.Config().Dir

	frames := walTestFrames(t, walActivePath(dir))
	var partFrames []walTestFrame
	for _, frame := range frames {
		if frame.version == walFormatVersionSpanned {
			require.Equal(t, seq, frame.seq)
			partFrames = append(partFrames, frame)
		}
	}
	require.GreaterOrEqual(t, len(partFrames), 3, "the anchor spans several parts")
	// Cut the file inside the last part.
	require.NoError(t, os.Truncate(walActivePath(dir), partFrames[len(partFrames)-2].end+int64(1<<10)))

	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotEqual(t, opManagedVectorAnchor, entry.Operation, "an incomplete anchor is not delivered")
	}
	report, err := CheckWALIntegrity(walActivePath(dir))
	require.NoError(t, err)
	require.Contains(t, strings.Join(report.Errors, "; "), "incomplete spanned record")

	recovered, err := managedAnchorReplay(t, dir, func(engine *MemoryEngine) {
		at := acknowledged.UpdatedAt
		_, createErr := engine.CreateNode(t.Context(), managedAnchorBody(at))
		require.NoError(t, createErr)
	})
	require.NoError(t, err)
	node, err := recovered.GetNode(t.Context(), managedAnchorID)
	require.NoError(t, err)
	require.Empty(t, node.ChunkEmbeddings, "no acknowledged state means no restored vectors")
	require.Empty(t, node.EmbedMeta)

	// The tail repair on open cuts the unfinished record away; a later anchor
	// is delivered as usual.
	reopened, err := NewWAL(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	entries, err = ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotEqual(t, opManagedVectorAnchor, entry.Operation)
	}
	_, err = reopened.appendManagedVectorAnchor(t.Context(), acknowledged)
	require.NoError(t, err)
	_, payload := managedAnchorEntry(t, dir)
	require.Equal(t, acknowledged.ChunkEmbeddings, payload.SourceNode.ChunkEmbeddings)
}

// Replay restores exactly the anchor's managed chunk vectors and metadata onto
// the committed node, and leaves the node's body, labels, timestamps and named
// vectors as the store has them.
func TestManagedVectorAnchorReplayRestoresOnlyManagedState(t *testing.T) {
	wal, acknowledged := managedAnchorFixture(t, nil, 24, 3)
	_, err := wal.appendManagedVectorAnchor(t.Context(), acknowledged)
	require.NoError(t, err)
	require.NoError(t, wal.Sync())

	recovered, err := managedAnchorReplay(t, wal.Config().Dir, func(engine *MemoryEngine) {
		_, createErr := engine.CreateNode(t.Context(), managedAnchorBody(acknowledged.UpdatedAt))
		require.NoError(t, createErr)
	})
	require.NoError(t, err)

	node, err := recovered.GetNode(t.Context(), managedAnchorID)
	require.NoError(t, err)
	require.Equal(t, acknowledged.ChunkEmbeddings, node.ChunkEmbeddings, "the acknowledged vectors are restored")
	require.Equal(t, acknowledged.EmbedMeta["has_embedding"], node.EmbedMeta["has_embedding"])
	require.EqualValues(t, len(acknowledged.ChunkEmbeddings), node.EmbedMeta["chunk_count"])
	require.EqualValues(t, largeRecordDims, node.EmbedMeta["embedding_dimensions"])
	require.Equal(t, "synthetic", node.EmbedMeta["embedding_model"])
	require.Equal(t, acknowledged.NamedEmbeddings, node.NamedEmbeddings, "named vectors are not restored and not removed")
	require.Equal(t, acknowledged.Properties, node.Properties)
	require.Equal(t, acknowledged.Labels, node.Labels)
	require.True(t, acknowledged.CreatedAt.Equal(node.CreatedAt))
	require.True(t, acknowledged.UpdatedAt.Equal(node.UpdatedAt))
}

// An anchor whose recorded source is not the committed node fails replay and
// changes nothing: no node is created for a missing one and no vectors are
// restored onto a body that moved on.
func TestManagedVectorAnchorReplayRejectsOtherSource(t *testing.T) {
	wal, acknowledged := managedAnchorFixture(t, nil, 8, 4)
	_, err := wal.appendManagedVectorAnchor(t.Context(), acknowledged)
	require.NoError(t, err)
	require.NoError(t, wal.Sync())
	dir := wal.Config().Dir

	t.Run("missing node", func(t *testing.T) {
		recovered, err := managedAnchorReplay(t, dir, nil)
		require.ErrorIs(t, err, ErrManagedVectorAnchorSource)
		require.ErrorIs(t, err, ErrNotFound)
		_, readErr := recovered.GetNode(t.Context(), managedAnchorID)
		require.ErrorIs(t, readErr, ErrNotFound, "no node is fabricated")
	})

	for _, other := range []struct {
		name string
		body func(*Node)
	}{
		{"other properties", func(node *Node) { node.Properties["text"] = "beta" }},
		{"other labels", func(node *Node) { node.Labels = []string{"Other"} }},
		{"other timestamps", func(node *Node) { node.UpdatedAt = node.UpdatedAt.Add(time.Second) }},
	} {
		t.Run(other.name, func(t *testing.T) {
			recovered, err := managedAnchorReplay(t, dir, func(engine *MemoryEngine) {
				body := managedAnchorBody(acknowledged.UpdatedAt)
				other.body(body)
				_, createErr := engine.CreateNode(t.Context(), body)
				require.NoError(t, createErr)
			})
			require.ErrorIs(t, err, ErrManagedVectorAnchorSource)
			node, readErr := recovered.GetNode(t.Context(), managedAnchorID)
			require.NoError(t, readErr)
			require.Empty(t, node.ChunkEmbeddings, "nothing is restored onto another body")
			require.Empty(t, node.EmbedMeta)
			require.Equal(t, acknowledged.NamedEmbeddings, node.NamedEmbeddings)
		})
	}
}

// The acknowledged anchor is the node's current managed state for the ordinary
// write path: after the embedding audit and the anchor, a property update that
// does not feed the embedding keeps the restored vectors.
func TestManagedVectorAnchorSurvivesLaterPropertyUpdate(t *testing.T) {
	wal, acknowledged := managedAnchorFixture(t, nil, 12, 5)
	_, err := wal.appendManagedVectorAnchor(t.Context(), acknowledged)
	require.NoError(t, err)
	require.NoError(t, wal.Sync())

	recovered, err := managedAnchorReplay(t, wal.Config().Dir, func(engine *MemoryEngine) {
		_, createErr := engine.CreateNode(t.Context(), managedAnchorBody(acknowledged.UpdatedAt))
		require.NoError(t, createErr)
	})
	require.NoError(t, err)
	recovered.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})

	updated, err := recovered.GetNode(t.Context(), managedAnchorID)
	require.NoError(t, err)
	updated = CopyNode(updated)
	updated.Properties["flag"] = true
	require.NoError(t, recovered.UpdateNode(t.Context(), NodeUpdateRequest{After: updated}))

	node, err := recovered.GetNode(t.Context(), managedAnchorID)
	require.NoError(t, err)
	require.Equal(t, true, node.Properties["flag"])
	require.Equal(t, acknowledged.ChunkEmbeddings, node.ChunkEmbeddings, "the property update keeps the acknowledged vectors")
}

// The managed vector provenance epoch moves forward at the owners whose work
// can stale what the journal proved about managed vectors: the embedding
// audit's acceptance, an embedding reset and journal
// retirement.
func TestManagedVectorGenerationAdvancesOnManagedVectorOwners(t *testing.T) {
	wal, acknowledged := managedAnchorFixture(t, nil, 4, 6)
	require.EqualValues(t, 1, wal.managedVectorGeneration.Load(), "publishing the fixture's vectors journals one audit")

	_, err := wal.appendManagedVectorAnchor(t.Context(), acknowledged)
	require.NoError(t, err)
	require.EqualValues(t, 1, wal.managedVectorGeneration.Load(), "adding an anchor does not invalidate other anchor proofs")
	require.NoError(t, wal.AppendWithDatabase(OpUpdateEmbedding, WALNodeData{Node: &Node{ID: "anchor-node"}}, "test"))
	require.EqualValues(t, 2, wal.managedVectorGeneration.Load(), "generic audit append invalidates proofs too")

	native := newTestMemoryEngine(t)
	_, err = native.CreateNode(t.Context(), managedAnchorBody(acknowledged.UpdatedAt))
	require.NoError(t, err)
	wrapped := NewWALEngine(native, wal)
	require.NoError(t, wrapped.UpdateNodeEmbeddingSidecar(t.Context(), embeddingWriteback(t, wrapped.GetInnerEngine(), managedAnchorID, acknowledged.ChunkEmbeddings, acknowledged.EmbedMeta, time.Time{})))
	require.EqualValues(t, 3, wal.managedVectorGeneration.Load(), "an accepted embedding audit advances the epoch")

	_, err = wrapped.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	require.EqualValues(t, 4, wal.managedVectorGeneration.Load(), "an embedding reset advances the epoch")

	require.NoError(t, wal.ManageSnapshot(context.Background(), wal.Sequence(), WALSnapshotTruncate))
	require.EqualValues(t, 5, wal.managedVectorGeneration.Load(), "journal retirement advances the epoch")
}

// The anchor is private journal state: its payload keeps registered temporal
// property types and its source is refused for an ID the embedding sidecar
// owner could not restore.
func TestManagedVectorAnchorPayloadKeepsStoredTypes(t *testing.T) {
	wal, acknowledged := managedAnchorFixture(t, nil, 2, 7)
	stamped := time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.UTC)
	source := CopyNode(acknowledged)
	source.EmbedMeta = map[string]any{"has_embedding": true, "embedded_at": stamped}

	seq, err := wal.appendManagedVectorAnchor(t.Context(), source)
	require.NoError(t, err)
	require.NoError(t, wal.Sync())
	entry, payload := managedAnchorEntry(t, wal.Config().Dir)
	require.Equal(t, seq, entry.Sequence)
	embeddedAt, ok := payload.SourceNode.EmbedMeta["embedded_at"].(time.Time)
	require.True(t, ok, "a registered temporal type survives the record: %T", payload.SourceNode.EmbedMeta["embedded_at"])
	require.True(t, stamped.Equal(embeddedAt))

	unqualified := CopyNode(acknowledged)
	unqualified.ID = "unqualified"
	_, err = wal.appendManagedVectorAnchor(t.Context(), unqualified)
	require.ErrorIs(t, err, ErrInvalidID, "the sidecar owner restores only database-qualified IDs")
}
