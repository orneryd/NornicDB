package search

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The fixture was written by v1.4.3 itself, not by stamping a current file's
// version. Both authoritative graph data and derived vector/HNSW caches are
// reopened, so overwritten index entries must be recovered from graph storage.
func TestVectorEntryUpgradeFromRelease143Store(t *testing.T) {
	dir := t.TempDir()
	archive, err := zip.OpenReader("testdata/vector-v143/store.zip")
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
	open := func() (*storage.BadgerEngine, storage.Engine, *Service) {
		base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(dir, "graph")))
		if base != nil {
			fixtureowner.Own(t, base)
		}
		require.NoError(t, err)
		engine := storage.NewNamespacedEngine(base, "nornic")
		svc := NewServiceWithDimensions(engine, vectorOwnerDims)
		svc.SetFulltextIndexPath(filepath.Join(dir, "search", "bm25"))
		svc.SetVectorIndexPath(filepath.Join(dir, "search", "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(dir, "search", "hnsw"))
		svc.SetPersistenceEnabled(true)
		require.NoError(t, svc.BuildIndexes(context.Background()))
		return base, engine, svc
	}
	base, engine, svc := open()
	require.Equal(t, 4, svc.EmbeddingCount(), "recover the vector overwritten in the 1.4.3 search cache")
	assertVectorOwners(t, svc, []vectorOwnerQuery{{axis: 1, owner: "x"}, {axis: 3, owner: "x-chunk-1"}})
	// Exercise writes made after the upgrade, then reopen the graph and indexes.
	control := vectorOwnerChunkNode("rollback-control", 2)
	_, err = engine.CreateNode(control)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(control))
	svc.PersistIndexesToDisk()
	require.NoError(t, svc.Close())
	require.NoError(t, base.Close())
	if os.Getenv("NORNICDB_VECTOR_ROLLBACK_FIXTURE") == "1" {
		writeVectorRollbackFixture(t, dir)
	}
	base, engine, svc = open()
	t.Cleanup(func() { _ = svc.Close(); _ = base.Close() })
	require.Equal(t, 5, svc.EmbeddingCount())
	assertVectorOwners(t, svc, []vectorOwnerQuery{{axis: 1, owner: "x"}, {axis: 3, owner: "x-chunk-1"}})
	require.NoError(t, engine.DeleteNode("x"))
	require.NoError(t, svc.RemoveNode("x"))
	assertVectorOwners(t, svc, []vectorOwnerQuery{{axis: 3, owner: "x-chunk-1"}})
}

// Export only when running the explicit cross-version rollback probe. The
// archive is consumed by unmodified v1.4.3 production code on TrueNAS.
func writeVectorRollbackFixture(t *testing.T, dir string) {
	t.Helper()
	var buf bytes.Buffer
	archive := zip.NewWriter(&buf)
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		out, err := archive.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, err = out.Write(data)
		return err
	}))
	require.NoError(t, archive.Close())
	t.Log("VECTOR144_FIXTURE=" + base64.StdEncoding.EncodeToString(buf.Bytes()))
}
