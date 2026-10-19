package search

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This writer runs on the unmodified v1.4.3 production sources on TrueNAS.
// Its output is a checked-in upgrade fixture, including authoritative Badger
// data and the colliding derived search artifacts.
func TestWriteRelease143VectorFixture(t *testing.T) {
	dir := t.TempDir()
	base, err := storage.NewBadgerEngine(filepath.Join(dir, "graph"))
	require.NoError(t, err)
	engine := storage.NewNamespacedEngine(base, "nornic")
	for _, node := range []*storage.Node{
		{ID: "x", Labels: []string{"Doc"}, Properties: map[string]any{"text": "owner x"}, ChunkEmbeddings: [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0}}},
		{ID: "x-chunk-1", Labels: []string{"Doc"}, Properties: map[string]any{"text": "other owner"}, ChunkEmbeddings: [][]float32{{0, 0, 0, 1}}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	svc := NewServiceWithDimensions(engine, 4)
	svc.SetFulltextIndexPath(filepath.Join(dir, "search", "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "search", "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "search", "hnsw"))
	svc.SetPersistenceEnabled(true)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	svc.PersistIndexesToDisk()
	require.NoError(t, svc.Close())
	require.NoError(t, base.Close())
	var buf bytes.Buffer
	archive := zip.NewWriter(&buf)
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() { return err }
		rel, err := filepath.Rel(dir, path)
		if err != nil { return err }
		out, err := archive.Create(filepath.ToSlash(rel))
		if err != nil { return err }
		in, err := os.Open(path)
		if err != nil { return err }
		defer in.Close()
		_, err = io.Copy(out, in)
		return err
	}))
	require.NoError(t, archive.Close())
	fmt.Println("VECTOR143_FIXTURE=" + base64.StdEncoding.EncodeToString(buf.Bytes()))
}
