package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// newSidecarTestBadger returns the Badger core of an in-memory engine.
func newSidecarTestBadger(t *testing.T) *BadgerEngine {
	t.Helper()
	return NewMemoryEngine().BadgerEngine
}

func sidecarTestNode(t *testing.T, b *BadgerEngine, id string, props map[string]any, at time.Time) *Node {
	t.Helper()
	node := &Node{
		ID:         NodeID("test:" + id),
		Labels:     []string{"Doc"},
		Properties: props,
		CreatedAt:  at,
		UpdatedAt:  at,
	}
	_, err := b.CreateNode(node)
	require.NoError(t, err)
	return node
}

// TestEmbeddingSidecar_WritebackDoesNotTouchNodeRecord pins AC1/AC3: the
// sidecar write creates no new MVCC node version and leaves the body
// (properties, labels, UpdatedAt) untouched, while reads return the persisted
// embedding and metadata.
func TestEmbeddingSidecar_WritebackDoesNotTouchNodeRecord(t *testing.T) {
	b := newSidecarTestBadger(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "sidecar-writeback", map[string]any{"title": "hello"}, now)

	headBefore, err := loadMVCCHead[nodeMVCCHeadKeyLookup](b, string(node.ID))
	require.NoError(t, err)

	payload := &Node{
		ID:              node.ID,
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 1},
		UpdatedAt:       now,
	}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(payload))

	// Read-back merges the sidecar.
	got, err := b.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.1, 0.2, 0.3}}, got.ChunkEmbeddings)
	require.Equal(t, true, got.EmbedMeta["has_embedding"])
	_, leaked := got.EmbedMeta[embeddingContentUpdatedAtKey]
	require.False(t, leaked, "internal content stamp must not leak into reads")
	// Body untouched.
	require.Equal(t, map[string]any{"title": "hello"}, got.Properties)
	require.Equal(t, []string{"Doc"}, got.Labels)
	require.Equal(t, now, got.UpdatedAt.UTC())

	headAfter, err := loadMVCCHead[nodeMVCCHeadKeyLookup](b, string(node.ID))
	require.NoError(t, err)
	require.Equal(t, headBefore, headAfter, "sidecar write must not create a new MVCC node version")
}

// TestEmbeddingSidecar_BusinessWriteIsAuthoritative pins AC2/AC3: any node
// body write drops the sidecar record, so an incoming change invalidates the
// embedding and readers never see stale derived data.
func TestEmbeddingSidecar_BusinessWriteIsAuthoritative(t *testing.T) {
	b := newSidecarTestBadger(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "sidecar-authoritative", map[string]any{"title": "v1"}, now)

	require.NoError(t, b.UpdateNodeEmbeddingSidecar(&Node{
		ID:              node.ID,
		ChunkEmbeddings: [][]float32{{0.1, 0.2}},
		EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 1},
		UpdatedAt:       now,
	}))
	got, err := b.GetNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 1)

	// A business write invalidates embedding state and drops the sidecar.
	updated := *got
	updated.Properties = map[string]any{"title": "v2"}
	updated.ChunkEmbeddings = nil
	updated.EmbedMeta = nil
	require.NoError(t, b.UpdateNode(&updated))

	got, err = b.GetNode(node.ID)
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings)
	require.Empty(t, got.EmbedMeta)
	require.Equal(t, map[string]any{"title": "v2"}, got.Properties)

	// The sidecar key space is gone: no stale metadata or chunk keys remain.
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(embeddingMetaKey(node.ID))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		_, err = txn.Get(embeddingKey(node.ID, 0))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

// TestEmbeddingSidecar_StaleSidecarIgnored pins the content-stamp guard: a
// sidecar written for an older body version is not served, leaving the node
// embedding-free until the worker re-embeds the new content.
func TestEmbeddingSidecar_StaleSidecarIgnored(t *testing.T) {
	b := newSidecarTestBadger(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "sidecar-stale", map[string]any{"title": "v1"}, now)

	// Stamp the sidecar with an UpdatedAt from BEFORE the body's own time.
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(&Node{
		ID:              node.ID,
		ChunkEmbeddings: [][]float32{{0.1, 0.2}},
		EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 1},
		UpdatedAt:       now.Add(-time.Hour),
	}))

	got, err := b.GetNode(node.ID)
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings, "stale sidecar must not be served")
	require.Empty(t, got.EmbedMeta, "stale sidecar metadata must not be served")
}

// TestEmbeddingSidecar_DeleteThenRecreateIgnoresStaleMeta: deletion does not
// eagerly clean the embedding key space (no storage-write-path changes), but a
// recreated node with the same ID can never inherit the old embedding — the
// content stamp no longer matches the new body's UpdatedAt, so the stale
// metadata is ignored.
func TestEmbeddingSidecar_DeleteThenRecreateIgnoresStaleMeta(t *testing.T) {
	b := newSidecarTestBadger(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "sidecar-delete", map[string]any{"title": "v1"}, now)

	require.NoError(t, b.UpdateNodeEmbeddingSidecar(&Node{
		ID:              node.ID,
		ChunkEmbeddings: [][]float32{{0.1, 0.2}, {0.3, 0.4}},
		EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 2},
		UpdatedAt:       now,
	}))
	require.NoError(t, b.DeleteNode(node.ID))

	// Recreate a node with the same ID but a new UpdatedAt.
	later := now.Add(time.Hour)
	recreated := &Node{
		ID:         node.ID,
		Labels:     []string{"Doc"},
		Properties: map[string]any{"title": "v2"},
		CreatedAt:  later,
		UpdatedAt:  later,
	}
	_, err := b.CreateNode(recreated)
	require.NoError(t, err)

	got, err := b.GetNode(node.ID)
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings, "stale metadata from the deleted node must not be served")
	require.Empty(t, got.EmbedMeta, "stale metadata from the deleted node must not be served")
	require.Equal(t, map[string]any{"title": "v2"}, got.Properties)
}

// TestEmbeddingSidecar_FailureMarkersAndStreaming pins the parked-failure
// scan over the metadata key space.
func TestEmbeddingSidecar_FailureMarkersAndStreaming(t *testing.T) {
	b := newSidecarTestBadger(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, b, "sidecar-failed", map[string]any{"title": "v1"}, now)

	require.NoError(t, b.UpdateNodeEmbeddingSidecar(&Node{
		ID:        node.ID,
		EmbedMeta: map[string]any{"embedding_failed": true, "embedding_error": "boom", "embedding_failed_at": "2026-10-04T00:00:00Z"},
		UpdatedAt: now,
	}))

	var visited []EmbeddingFailureLike
	count, err := b.StreamParkedEmbeddingFailures(context.Background(), func(nodeID NodeID, meta map[string]any) error {
		visited = append(visited, EmbeddingFailureLike{NodeID: nodeID, Error: meta["embedding_error"].(string)})
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Len(t, visited, 1)
	require.Equal(t, node.ID, visited[0].NodeID)
	require.Equal(t, "boom", visited[0].Error)

	// Clearing embedding state removes the marker.
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(&Node{ID: node.ID, UpdatedAt: now}))
	count, err = b.StreamParkedEmbeddingFailures(context.Background(), func(nodeID NodeID, meta map[string]any) error { return nil })
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

// TestEmbeddingSidecar_NotFoundDoesNotCreate pins that a sidecar write for a
// deleted node returns ErrNotFound and writes nothing.
func TestEmbeddingSidecar_NotFoundDoesNotCreate(t *testing.T) {
	b := newSidecarTestBadger(t)
	err := b.UpdateNodeEmbeddingSidecar(&Node{
		ID:              NodeID("test:sidecar-missing"),
		ChunkEmbeddings: [][]float32{{0.1}},
		EmbedMeta:       map[string]any{"has_embedding": true},
		UpdatedAt:       time.Now(),
	})
	require.ErrorIs(t, err, ErrNotFound)
	_, err = b.GetNode(NodeID("test:sidecar-missing"))
	require.ErrorIs(t, err, ErrNotFound)
}

type EmbeddingFailureLike struct {
	NodeID NodeID
	Error  string
}

// BenchmarkUpdateNodeEmbeddingSidecar measures the worker's new embedding-only
// writeback in the production shape: a persistent engine and a pool of nodes,
// each writeback targeting the next node (bounded per-key churn).
func BenchmarkUpdateNodeEmbeddingSidecar(b *testing.B) {
	engine := NewMemoryEngine().BadgerEngine
	defer engine.Close()
	now := time.Now()
	const pool = 2000
	nodes := make([]*Node, pool)
	for i := 0; i < pool; i++ {
		node := &Node{
			ID:         NodeID(fmt.Sprintf("test:bench-sidecar-%d", i)),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"content": "hello"},
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if _, err := engine.CreateNode(node); err != nil {
			b.Fatal(err)
		}
		nodes[i] = node
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload := &Node{
			ID:              nodes[i%pool].ID,
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
			EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 1},
			UpdatedAt:       nodes[i%pool].UpdatedAt,
		}
		if err := engine.UpdateNodeEmbeddingSidecar(payload); err != nil {
			b.Fatal(err)
		}
	}
}
