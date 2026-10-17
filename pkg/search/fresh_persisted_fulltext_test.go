package search

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestFreshPersistedDatabaseServesFulltextAfterFirstWrite: a new persisted
// database's first build over empty storage succeeds, and its first write is
// found by fulltext search without a restart, with the default (scalar) graph
// compression and with float coordinates.
func TestFreshPersistedDatabaseServesFulltextAfterFirstWrite(t *testing.T) {
	for _, mode := range []string{"", "none"} {
		name := mode
		if name == "" {
			name = "default-scalar"
		}
		t.Run(name, func(t *testing.T) {
			if mode != "" {
				t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", mode)
			}
			engine := newNamespacedEngine(t)
			dir := t.TempDir()
			svc := NewServiceWithDimensions(engine, 3)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
			svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
			svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
			svc.SetPersistenceEnabled(true)
			require.NoError(t, svc.BuildIndexes(context.Background()), "first build of an empty persisted database")

			node := &storage.Node{ID: "doc", Properties: map[string]any{"text": "first persisted write"}, NamedEmbeddings: map[string][]float32{"default": {1, 0, 0}}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node))

			resp, err := svc.Search(context.Background(), "persisted", nil, DefaultSearchOptions())
			require.NoError(t, err)
			require.NotEmpty(t, resp.Results)
			require.Equal(t, "doc", resp.Results[0].ID)
		})
	}
}
