package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Relationship search's property lists come from their own environment
// variables and YAML keys, apart from the node lists.
func TestRelationshipSearchPropertySettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nornicdb.yaml")
	require.NoError(t, os.WriteFile(path, []byte("search:\n  relationship_bm25_properties: [why]\nembedding_worker:\n  relationship_properties_include: [why]\n  relationship_properties_exclude: [hidden]\n"), 0o600))
	fromFile, err := LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, []string{"why"}, fromFile.Memory.SearchRelationshipBM25Properties)
	require.Equal(t, []string{"why"}, fromFile.EmbeddingWorker.RelationshipPropertiesInclude)
	require.Equal(t, []string{"hidden"}, fromFile.EmbeddingWorker.RelationshipPropertiesExclude)

	t.Setenv("NORNICDB_SEARCH_RELATIONSHIP_BM25_PROPERTIES", " search_text ")
	t.Setenv("NORNICDB_EMBEDDING_RELATIONSHIP_PROPERTIES_INCLUDE", "search_text")
	t.Setenv("NORNICDB_EMBEDDING_RELATIONSHIP_PROPERTIES_EXCLUDE", "secret")
	cfg := LoadFromEnv()
	require.Equal(t, []string{"search_text"}, cfg.Memory.SearchRelationshipBM25Properties)
	require.Equal(t, []string{"search_text"}, cfg.EmbeddingWorker.RelationshipPropertiesInclude)
	require.Equal(t, []string{"secret"}, cfg.EmbeddingWorker.RelationshipPropertiesExclude)
}
