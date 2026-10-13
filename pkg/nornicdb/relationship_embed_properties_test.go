package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A relationship's embedding text reads the relationship property lists
// where set, each otherwise the node list.
func TestRelationshipEmbeddingTextReadsRelationshipLists(t *testing.T) {
	edge := &storage.Edge{ID: "r", Type: "LINK", Properties: map[string]any{"search_text": "why linked", "text": "user note", "secret": "x"}}
	nodeLists := &EmbedWorkerConfig{PropertiesInclude: []string{"text", "search_text"}}
	require.Contains(t, relationshipEmbeddingText(edge, nodeLists), "text: user note")
	require.Contains(t, relationshipEmbeddingText(edge, nodeLists), "search_text: why linked")

	own := &EmbedWorkerConfig{PropertiesInclude: []string{"text", "search_text"}, RelationshipPropertiesInclude: []string{"search_text"}}
	require.Equal(t, "search_text: why linked", relationshipEmbeddingText(edge, own))
	require.Empty(t, relationshipEmbeddingText(&storage.Edge{ID: "p", Type: "LINK", Properties: map[string]any{"text": "user note"}}, own))

	excluded := &EmbedWorkerConfig{PropertiesExclude: []string{"text"}, RelationshipPropertiesExclude: []string{"secret", "text"}}
	require.NotContains(t, relationshipEmbeddingText(edge, excluded), "secret")
	require.Contains(t, relationshipEmbeddingText(edge, excluded), "why linked")
}
