package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUpdateNode_InvalidatesManagedEmbeddings(t *testing.T) {
	ctx := context.Background()

	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()

	nodeID := "invalidate-1"
	_, err = db.storage.CreateNode(&storage.Node{
		ID:     storage.NodeID(nodeID),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"id":   nodeID,
			"name": "Alice",
		},
		EmbedMeta: map[string]any{
			"has_embedding":        true,
			"embedding_model":      "test-model",
			"embedding_dimensions": 1024,
			"embedded_at":          "2024-01-01T00:00:00Z",
			"chunk_count":          1,
		},
		ChunkEmbeddings: [][]float32{make([]float32, 1024)},
	})
	require.NoError(t, err)

	before, err := db.storage.GetNode(storage.NodeID(nodeID))
	require.NoError(t, err)
	require.NotEmpty(t, before.ChunkEmbeddings, "sanity: node should start with embeddings")

	// Mutate an embeddable property; this should clear managed embeddings + metadata.
	_, err = db.UpdateNode(ctx, nodeID, map[string]interface{}{"name": "Bob"})
	require.NoError(t, err)

	after, err := db.storage.GetNode(storage.NodeID(nodeID))
	require.NoError(t, err)
	require.Empty(t, after.ChunkEmbeddings, "managed embeddings should be cleared on mutation")
	require.Empty(t, after.EmbedMeta, "managed embedding metadata should be cleared on mutation")
}

func TestExecuteCypher_SetInvalidatesManagedEmbeddings(t *testing.T) {
	ctx := context.Background()

	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()

	nodeID := "invalidate-cypher-1"
	_, err = db.storage.CreateNode(&storage.Node{
		ID:     storage.NodeID(nodeID),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"id":   nodeID,
			"name": "Alice",
		},
		ChunkEmbeddings: [][]float32{make([]float32, 1024)},
	})
	require.NoError(t, err)

	// Cypher SET should invalidate managed embeddings when changing non-metadata properties.
	_, err = db.ExecuteCypher(ctx, "MATCH (n {id: 'invalidate-cypher-1'}) SET n.name = 'Bob' RETURN n", nil)
	require.NoError(t, err)

	after, err := db.storage.GetNode(storage.NodeID(nodeID))
	require.NoError(t, err)
	require.Empty(t, after.ChunkEmbeddings, "managed embeddings should be cleared on Cypher SET mutation")
}

func TestUpdateNode_MetadataOnlyUpdate_PreservesManagedEmbeddings(t *testing.T) {
	ctx := context.Background()

	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()

	nodeID := "invalidate-meta-only-1"
	origEmb := []float32{0.1, 0.2, 0.3}
	_, err = db.storage.CreateNode(&storage.Node{
		ID:     storage.NodeID(nodeID),
		Labels: []string{"Test"},
		Properties: map[string]any{
			"id":   nodeID,
			"name": "Alice",
		},
		EmbedMeta: map[string]any{
			"has_embedding":        true,
			"embedding_model":      "test-model",
			"embedding_dimensions": 3,
			"embedded_at":          "2024-01-01T00:00:00Z",
			"chunk_count":          1,
		},
		ChunkEmbeddings: [][]float32{origEmb},
	})
	require.NoError(t, err)

	_, err = db.UpdateNode(ctx, nodeID, map[string]any{
		"updatedAt": "2026-03-10T00:00:00Z",
	})
	require.NoError(t, err)

	after, err := db.storage.GetNode(storage.NodeID(nodeID))
	require.NoError(t, err)
	require.Len(t, after.ChunkEmbeddings, 1, "metadata-only update should not invalidate embeddings")
	require.Equal(t, origEmb, after.ChunkEmbeddings[0], "embedding content should be preserved")
	require.NotNil(t, after.EmbedMeta, "metadata-only update should keep embedding metadata")
	require.Equal(t, true, after.EmbedMeta["has_embedding"])
}

func TestEmbeddingInvalidationHelpers(t *testing.T) {
	t.Run("metadata key allowlist", func(t *testing.T) {
		for _, key := range []string{
			"embedding",
			"has_embedding",
			"embedding_skipped",
			"embedding_model",
			"embedding_dimensions",
			"embedded_at",
			"has_chunks",
			"chunk_count",
			"createdAt",
			"updatedAt",
			"id",
		} {
			require.True(t, embeddingutil.IsMetadataPropertyKey(key), "expected metadata key: %s", key)
		}
		require.False(t, embeddingutil.IsMetadataPropertyKey("name"))
	})

	t.Run("invalidate helper nil and clear branches", func(t *testing.T) {
		embeddingutil.InvalidateManagedEmbeddings(nil)

		node := &storage.Node{
			ID:              "n-1",
			ChunkEmbeddings: [][]float32{{0.1, 0.2}},
			EmbedMeta: map[string]any{
				"has_embedding": true,
			},
		}
		embeddingutil.InvalidateManagedEmbeddings(node)
		require.Nil(t, node.ChunkEmbeddings)
		require.Nil(t, node.EmbedMeta)
	})
}

// TestUpdateKeepsManagedEmbeddingsForUnembeddedProperties: with only `text`
// embedded, an update of another property keeps the node's embeddings, and
// one of `text` invalidates them, through DB.UpdateNode and Cypher SET
// (#963).
func TestUpdateKeepsManagedEmbeddingsForUnembeddedProperties(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.EmbeddingWorker.PropertiesInclude = []string{"text"}
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer db.Close()

	for _, id := range []string{"keep-admin", "keep-cypher"} {
		_, err = db.storage.CreateNode(&storage.Node{
			ID:              storage.NodeID(id),
			Labels:          []string{"Src"},
			Properties:      map[string]any{"id": id, "text": "boiler invoice", "tags": []any{"a"}},
			EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 1},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		})
		require.NoError(t, err)
	}

	_, err = db.UpdateNode(ctx, "keep-admin", map[string]any{"tags": []any{"a", "b"}})
	require.NoError(t, err)
	_, err = db.ExecuteCypher(ctx, "MATCH (n:Src {id: 'keep-cypher'}) SET n.tags = ['a', 'b'] RETURN n", nil)
	require.NoError(t, err)
	for _, id := range []string{"keep-admin", "keep-cypher"} {
		got, err := db.storage.GetNode(storage.NodeID(id))
		require.NoError(t, err)
		require.Equal(t, [][]float32{{0.1, 0.2, 0.3}}, got.ChunkEmbeddings, id)
	}

	_, err = db.UpdateNode(ctx, "keep-admin", map[string]any{"text": "boiler receipt"})
	require.NoError(t, err)
	_, err = db.ExecuteCypher(ctx, "MATCH (n:Src {id: 'keep-cypher'}) SET n.text = 'boiler receipt' RETURN n", nil)
	require.NoError(t, err)
	for _, id := range []string{"keep-admin", "keep-cypher"} {
		got, err := db.storage.GetNode(storage.NodeID(id))
		require.NoError(t, err)
		require.Empty(t, got.ChunkEmbeddings, id)
	}
}

// TestEnqueueEndsRecentlyProcessedWait: a write to a node ends the worker's
// recently-processed wait for it, with or without its database prefix, so a
// changed embedding source is embedded at once (#963).
func TestEnqueueEndsRecentlyProcessedWait(t *testing.T) {
	worker := &EmbedWorker{
		trigger: make(chan struct{}, 1),
		recentlyProcessed: map[string]time.Time{
			"nornic:a": time.Now(),
			"b":        time.Now(),
			"nornic:c": time.Now(),
		},
		loggedSkip: map[string]bool{"nornic:a": true},
	}
	worker.Enqueue("a")
	worker.Enqueue("b")
	require.False(t, worker.wasRecentlyProcessed("nornic:a"))
	require.False(t, worker.wasRecentlyProcessed("b"))
	require.True(t, worker.wasRecentlyProcessed("nornic:c"))
}
