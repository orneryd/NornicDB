package nornicdb

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryExpansionPassageResolver(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	node := &storage.Node{
		ID:         "document",
		Labels:     []string{"Document"},
		Properties: map[string]any{"content": "alpha beta gamma delta epsilon"},
	}
	nodeID, err := engine.CreateNode(node)
	require.NoError(t, err)

	emb := &chunkingTestEmbedder{dims: 4}
	config := &EmbedWorkerConfig{ChunkSize: 2, ChunkOverlap: 0, IncludeLabels: false}
	resolver := &queryExpansionPassageResolver{
		db:     &DB{embedQueue: &EmbedQueue{embedder: emb}, embedWorkerConfig: config},
		engine: engine,
	}
	text := embeddingutil.BuildText(node.Properties, node.Labels, embeddingutil.EmbedTextOptionsFromFields(nil, nil, false))
	chunks, err := emb.ChunkText(text, config.ChunkSize, config.ChunkOverlap)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(chunks), 2)

	resolved, err := resolver.ResolvePassages(context.Background(), []search.ExpansionSource{
		{VectorID: string(nodeID), NodeID: string(nodeID), SemanticRank: 1, SemanticScore: 0.9},
		{VectorID: string(nodeID) + "-chunk-1", NodeID: string(nodeID), SemanticRank: 2, SemanticScore: 0.8},
		{VectorID: string(nodeID) + "-named-title", NodeID: string(nodeID), SemanticRank: 3, SemanticScore: 0.7},
		{VectorID: string(nodeID) + "-chunk-bad", NodeID: string(nodeID), SemanticRank: 4, SemanticScore: 0.6},
	})
	require.NoError(t, err)
	require.Len(t, resolved, 2)
	assert.Equal(t, text, resolved[0].Text)
	assert.Equal(t, chunks[1], resolved[1].Text)
}

func TestTruncateExpansionPassageUTF8(t *testing.T) {
	prefix := strings.Repeat("a", 127)
	truncated := truncateExpansionPassage(prefix+"\U0001F642 trailing", 128)
	assert.Equal(t, prefix, truncated)
}
