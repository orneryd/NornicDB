package storage

// With write-behind enabled,
// validateMutationPreimagesLocked compares the caller's observed
// predecessor with the current stored body: properties and labels only. It
// passed a nil property projection to readProjectedNodeItemInTxn, which is the
// full-embedding reader, so the comparison decoded this node's separately
// stored chunk vectors while the commit held its locks. These tests drive the
// public physical-node write-behind update/admission path and prove the
// commit-time comparison neither reads nor rewrites embedding chunk payloads,
// while full reads still hydrate it and stale images still conflict.
//
// Fixtures reuse the canonical write-behind owner (newWriteBehindTestEngine),
// the sidecar owner (UpdateNodeEmbeddingSidecar), the embedding-free body read
// (GetNodeWithoutEmbeddings), the cache invalidation owner (cacheDeleteNode)
// and the package's raw Badger read/write helpers.

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// preimageSidecarSentinel is not a msgpack vector and not a sharded-chunk
// marker, so any reader of the chunk fails instead of silently skipping it.
var preimageSidecarSentinel = []byte("invalid-preimage-embedding-chunk")

// preimageFixture opens one engine holding one node whose chunk vectors are
// stored in the embedding sidecar, and returns the engine, the node ID and the
// chunks a full read must return.
func preimageFixture(t *testing.T, writeBehind bool) (*BadgerEngine, NodeID, [][]float32) {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: !writeBehind, manualDrain: writeBehind})
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	node := &Node{
		ID:         "test:preimage",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"text": "same", "n": int64(0)},
	}
	_, err := engine.CreateNode(context.Background(), node)
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind(context.Background()), "the predecessor body must be stored before the sidecar")

	chunks := makeLargeChunkEmbeddings()
	embedded := embeddingWriteback(t, engine, node.ID, chunks, map[string]any{"embedding_model": "original", "chunk_count": len(chunks)}, time.Time{})
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(context.Background(), embedded))
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	return engine, node.ID, chunks
}

// preimageBodyCached reports whether the commit-time current-body cache holds
// this node's versioned body.
func preimageBodyCached(engine *BadgerEngine, id NodeID) bool {
	engine.nodeBodyCacheMu.RLock()
	defer engine.nodeBodyCacheMu.RUnlock()
	entry, ok := engine.nodeBodyCache[id]
	return ok && entry != nil && entry.node != nil
}

// preimagePhysicalBody is the complete physical predecessor body a caller
// observes before staging an update: properties, labels and no vectors.
func preimagePhysicalBody(t *testing.T, engine *BadgerEngine, id NodeID) *Node {
	t.Helper()
	body, err := engine.GetNodeWithoutEmbeddings(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, body)
	require.Empty(t, body.ChunkEmbeddings, "the stored body must not carry the chunk vectors")
	require.Equal(t, "same", body.Properties["text"])
	require.Equal(t, int64(0), body.Properties["n"])
	require.ElementsMatch(t, []string{"Doc"}, body.Labels)
	return body
}

// preimageAssertStoredImage requires the full public read to hydrate exactly
// the sidecar chunks and the expected body properties.
func preimageAssertStoredImage(t *testing.T, engine *BadgerEngine, id NodeID, chunks [][]float32, n int64) {
	t.Helper()
	stored, err := engine.GetStoredNode(context.Background(), id)
	require.NoError(t, err)
	require.True(t, slices.EqualFunc(chunks, stored.ChunkEmbeddings, slices.Equal[[]float32]), "a full read must hydrate every stored embedding value")
	require.Equal(t, "original", stored.EmbedMeta["embedding_model"])
	require.Equal(t, "same", stored.Properties["text"])
	require.Equal(t, n, stored.Properties["n"])
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	engine.cacheDeleteNode(id, nodeCacheAllForms)
	visible, err := engine.GetNode(context.Background(), id)
	require.NoError(t, err)
	require.True(t, slices.EqualFunc(chunks, visible.ChunkEmbeddings, slices.Equal[[]float32]), "visible vectors must match every stored value")
	require.Equal(t, n, visible.Properties["n"])
}

// TestWriteBehindPreimageValidationSkipsSeparateEmbeddingChunks is the
// fail-first regression. A malformed chunk vector proves whether the
// commit-time physical read decoded the sidecar: before the fix validation read
// the full embedding state and failed on it, and the identical update must
// succeed when validation reads only the body. The warm case keeps the
// version-matched body in the body cache, so the same comparison answers from
// the cached body instead of a stored decode.
func TestWriteBehindPreimageValidationSkipsSeparateEmbeddingChunks(t *testing.T) {
	_, decodeErr := decodeEmbedding(preimageSidecarSentinel)
	require.Error(t, decodeErr, "the sentinel must never decode")
	_, sharded := parseEmbeddingShardMarker(preimageSidecarSentinel)
	require.False(t, sharded, "the sentinel must not look like a sharded chunk")

	for _, warm := range []bool{false, true} {
		name := "cold-body-cache"
		if warm {
			name = "warm-body-cache"
		}
		t.Run(name, func(t *testing.T) {
			engine, id, chunks := preimageFixture(t, true)
			physical := preimagePhysicalBody(t, engine, id)

			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.SetNamespace("test"))
			after := CopyNode(physical)
			after.Properties["n"] = int64(1)
			require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: physical, After: after, Policy: ClientNodeEdits}))

			original := rawVersionRecord(t, engine, embeddingKey(id, 0))
			require.NotEqual(t, preimageSidecarSentinel, original)
			writeRawValue(t, engine, embeddingKey(id, 0), preimageSidecarSentinel)

			// Fault writes publish unknown cache scope. Establish both the full-read
			// failure and the desired body-cache state after that invalidation.
			engine.cacheDeleteNode(id, nodeCacheAllForms)
			_, err = engine.GetStoredNode(context.Background(), id)
			require.ErrorContains(t, err, "failed to decode embedding chunk")
			engine.cacheDeleteNode(id, nodeCacheAllForms)
			if warm {
				preimagePhysicalBody(t, engine, id)
			}
			require.Equal(t, warm, preimageBodyCached(engine, id), "commit-time body-cache state")
			require.Zero(t, nor136Pending(engine), "validation must read the physical predecessor, not a buffered node")

			commitErr := tx.Commit()
			require.NoError(t, commitErr, "commit-time preimage validation must not decode embedding chunks")
			nor136AssertAcked(t, engine, nor136Config{writeBehind: true}, tx, 0)
			require.True(t, bytes.Equal(preimageSidecarSentinel, rawVersionRecord(t, engine, embeddingKey(id, 0))),
				"commit-time preimage validation must not rewrite the chunk vectors")

			writeRawValue(t, engine, embeddingKey(id, 0), original)
			preimageAssertStoredImage(t, engine, id, chunks, int64(1))
		})
	}
}

// TestWriteBehindPreimageValidationStillRejectsStaleImages keeps the conflict
// meaning of the same body-only comparison: an image acknowledging a
// predecessor a peer has already replaced is still rejected, by properties and
// by labels, and publishes nothing.
func TestWriteBehindPreimageValidationStillRejectsStaleImages(t *testing.T) {
	for _, stale := range []string{"properties", "labels"} {
		t.Run(stale, func(t *testing.T) {
			engine, id, chunks := preimageFixture(t, true)
			physical := preimagePhysicalBody(t, engine, id)

			staleTx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer staleTx.Rollback()
			require.NoError(t, staleTx.SetImplicit(true))
			require.NoError(t, staleTx.SetNamespace("test"))
			after := CopyNode(physical)
			after.Properties["n"] = int64(2)
			require.NoError(t, staleTx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: physical, After: after, Policy: ClientNodeEdits}))

			// A peer replaces the predecessor after the stale frame observed it.
			peer, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			require.NoError(t, peer.SetImplicit(true))
			require.NoError(t, peer.SetNamespace("test"))
			peerAfter := CopyNode(physical)
			switch stale {
			case "properties":
				peerAfter.Properties["n"] = int64(1)
			case "labels":
				peerAfter.Labels = []string{"Doc", "Acked"}
			}
			require.NoError(t, peer.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: peerAfter, Policy: ClientNodeEdits}))
			require.NoError(t, peer.Commit())
			require.NoError(t, engine.FlushWriteBehind(context.Background()), "the peer body must be the physical predecessor")

			commitErr := staleTx.Commit()
			require.ErrorIs(t, commitErr, ErrConflict, "the stale %s image must still conflict", stale)
			var failure *CommitFailureError
			require.False(t, errors.As(commitErr, &failure), "a stale image is a retryable conflict, not a commit failure: %v", commitErr)

			wantN := int64(1)
			if stale == "labels" {
				wantN = int64(0)
			}
			preimageAssertStoredImage(t, engine, id, chunks, wantN)
			stored, err := engine.GetStoredNode(context.Background(), id)
			require.NoError(t, err)
			if stale == "labels" {
				require.ElementsMatch(t, []string{"Doc", "Acked"}, stored.Labels, "the peer relabel must stand")
			} else {
				require.ElementsMatch(t, []string{"Doc"}, stored.Labels)
			}
		})
	}
}

// TestConstraintOnlyPreimageValidationStillRejectsStaleKey covers the unchanged
// constraint-projection read: with write-behind disabled the same owner reads
// only the constrained property and still rejects a replaced key.
func TestConstraintOnlyPreimageValidationStillRejectsStaleKey(t *testing.T) {
	engine, id, chunks := preimageFixture(t, false)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "doc_n", Type: ConstraintUnique, Label: "Doc", Properties: []string{"n"}}))

	physical := preimagePhysicalBody(t, engine, id)
	staleTx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer staleTx.Rollback()
	require.NoError(t, staleTx.SetImplicit(true))
	require.NoError(t, staleTx.SetNamespace("test"))
	after := CopyNode(physical)
	after.Properties["n"] = int64(2)
	require.NoError(t, staleTx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: physical, After: after, Policy: ClientNodeEdits}))

	peer, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, peer.SetImplicit(true))
	require.NoError(t, peer.SetNamespace("test"))
	peerAfter := CopyNode(physical)
	peerAfter.Properties["n"] = int64(1)
	require.NoError(t, peer.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: peerAfter, Policy: ClientNodeEdits}))
	require.NoError(t, peer.Commit())

	commitErr := staleTx.Commit()
	require.ErrorIs(t, commitErr, ErrConflict, "the replaced constrained key must still conflict")
	preimageAssertStoredImage(t, engine, id, chunks, int64(1))
}
