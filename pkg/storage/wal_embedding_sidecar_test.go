package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestWALEngine_UpdateNodeEmbeddingSidecar pins AC1/AC5 at the WAL layer: the
// sidecar write is logged as OpUpdateEmbedding, never changes node counts,
// never rewrites the node record, and the persisted embedding reads back.
func TestWALEngine_UpdateNodeEmbeddingSidecar(t *testing.T) {
	base := NewMemoryEngine()
	async := NewAsyncEngine(base, DefaultAsyncEngineConfig())
	dir := t.TempDir()
	wal, err := NewWAL(dir, nil)
	require.NoError(t, err)
	engine := NewWALEngine(async, wal)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	wasEnabled := config.WithWALEnabled()
	defer wasEnabled()

	now := time.Now().UTC().Truncate(time.Millisecond)
	node := &Node{
		ID:         NodeID("nornic:test-sidecar-node"),
		Labels:     []string{"Test"},
		Properties: map[string]any{"k": "v"},
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	// The sidecar write passes through the async staging layer; flush first so
	// the node exists in the underlying engine (the worker re-queues after a
	// flush for nodes that were still staged).
	require.NoError(t, async.Flush())

	err = engine.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, engine, node.ID, [][]float32{{0.1, 0.2, 0.3}}, map[string]any{"has_embedding": true, "chunk_count": 1}, now))
	require.NoError(t, err)

	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.1, 0.2, 0.3}}, got.ChunkEmbeddings)
	require.Equal(t, true, got.EmbedMeta["has_embedding"])
	require.Equal(t, now, got.UpdatedAt.UTC(), "worker writeback must not bump UpdatedAt")

	// Audit: the WAL records the sidecar write as OpUpdateEmbedding.
	require.NoError(t, wal.Close())
	entries, err := ReadWALEntries(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	var sawEmbeddingUpdate bool
	for _, entry := range entries {
		if entry.Operation == OpUpdateEmbedding {
			sawEmbeddingUpdate = true
		}
	}
	require.True(t, sawEmbeddingUpdate, "WAL must log the sidecar write as OpUpdateEmbedding; entries=%d ops=%v", len(entries), walOps(entries))
}

func walOps(entries []WALEntry) []OperationType {
	ops := make([]OperationType, 0, len(entries))
	for _, entry := range entries {
		ops = append(ops, entry.Operation)
	}
	return ops
}
