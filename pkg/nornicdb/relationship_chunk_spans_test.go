package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship's chunks are placed in its string properties as a node's
// are, with or without the leading type line; none without one vector per
// chunk.
func TestRelationshipChunkSpans(t *testing.T) {
	edge := &storage.Edge{ID: "r", Type: "LINK", Properties: map[string]any{"search_text": "the stallion ledger", "weight": 2}}
	config := &EmbedWorkerConfig{PropertiesInclude: []string{"search_text"}}
	text := relationshipEmbeddingText(edge, config)
	require.Equal(t, []embeddingutil.ChunkSpan{{Chunk: 0, Property: "search_text", Start: 0, End: 19}},
		relationshipChunkSpans(edge, config, []string{text}, 1))
	require.Equal(t, []embeddingutil.ChunkSpan{{Chunk: 0, Property: "search_text", Start: 0, End: 12}, {Chunk: 1, Property: "search_text", Start: 13, End: 19}},
		relationshipChunkSpans(edge, config, []string{"the stallion", "ledger"}, 2))

	labelled := &EmbedWorkerConfig{PropertiesInclude: []string{"search_text"}, IncludeLabels: true}
	text = relationshipEmbeddingText(edge, labelled)
	require.Equal(t, "type: LINK\nsearch_text: the stallion ledger", text)
	require.Equal(t, []embeddingutil.ChunkSpan{{Chunk: 0, Property: "search_text", Start: 0, End: 19}},
		relationshipChunkSpans(edge, labelled, []string{text}, 1))

	require.Nil(t, relationshipChunkSpans(edge, config, []string{"a", "b"}, 1))
	require.Nil(t, relationshipChunkSpans(edge, config, nil, 0))
}
