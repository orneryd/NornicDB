package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"archive/zip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Run on v1.4.3 production sources against a graph and search indexes written
// after the release fix upgraded a 1.4.3 store and added rollback-control.
func TestVectorEntryRollbackToRelease143(t *testing.T) {
	dir := t.TempDir()
	archive, err := zip.OpenReader("testdata/vector-rollback.zip")
	require.NoError(t, err)
	for _, entry := range archive.File {
		path := filepath.Join(dir, filepath.FromSlash(entry.Name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		in, err := entry.Open()
		require.NoError(t, err)
		data, err := io.ReadAll(in)
		require.NoError(t, err)
		require.NoError(t, in.Close())
		require.NoError(t, os.WriteFile(path, data, 0o600))
	}
	require.NoError(t, archive.Close())
	// The documented rollback operation discards only derived search caches.
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "search")))
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
	require.NoError(t, err)
	defer base.Close()
	engine := storage.NewNamespacedEngine(base, "nornic")
	for id, chunks := range map[string]int{"x": 3, "x-chunk-1": 1, "rollback-control": 1} {
		node, err := engine.GetNode(storage.NodeID(id))
		require.NoError(t, err)
		require.Len(t, node.ChunkEmbeddings, chunks, "all authoritative embeddings remain readable after downgrade")
	}
	svc := NewServiceWithDimensions(engine, 4)
	defer svc.Close()
	svc.SetFulltextIndexPath(filepath.Join(dir, "search", "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "search", "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "search", "hnsw"))
	svc.SetPersistenceEnabled(true)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	hits, err := svc.VectorQueryNodes(context.Background(), []float32{0, 0, 1, 0}, VectorQuerySpec{Limit: 10})
	require.NoError(t, err)
	var found bool
	for _, hit := range hits {
		if hit.ID == "rollback-control" {
			found = true
		}
	}
	require.True(t, found, "a write made after upgrade must survive rollback and rebuild")
	// v1.4.3 still has the original collision bug; this probe proves rollback
	// readability and rebuild, not collision correctness in the old binary.
}
