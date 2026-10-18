package search

// Saved search indexes stay readable across versions: a store one version
// writes is restored by another from its saved artifacts, without a rebuild,
// and serves the same answers. The same file runs on every version compared.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingStoreEngine counts full node iterations: restoring saved
// artifacts needs none.
type countingStoreEngine struct {
	*storage.BadgerEngine
	iterations atomic.Int32
}

func (e *countingStoreEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.iterations.Add(1)
	return e.BadgerEngine.IterateNodes(fn)
}

func (e *countingStoreEngine) AllNodes() ([]*storage.Node, error) {
	e.iterations.Add(1)
	return e.BadgerEngine.AllNodes()
}

// OpenReadSnapshot counts a node read through the population's snapshot too.
func (e *countingStoreEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := e.BadgerEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &hookedReadSnapshot{ReadSnapshotView: inner, beforeNodes: func() error { e.iterations.Add(1); return nil }}, nil
}

func compatStoreService(engine storage.Engine, dir string) *Service {
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	svc.FixVectorDimensions()
	svc.SetFulltextIndexPath(filepath.Join(dir, "search", "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "search", "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "search", "hnsw"))
	svc.SetPersistenceEnabled(true)
	return svc
}

var compatStoreNodes = []*storage.Node{
	{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha written first"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
	{ID: "nornic:b", Labels: []string{"Doc"}, Properties: map[string]any{"text": "bravo named vector"}, NamedEmbeddings: map[string][]float32{"v": {0, 1, 0}}},
	{ID: "nornic:c", Labels: []string{"Doc"}, Properties: map[string]any{"text": "charlie property vector", "p": []float32{0, 0, 1}}},
}

// compatStoreLiveNode is written after the build and saved by persistence.
var compatStoreLiveNode = &storage.Node{ID: "nornic:d", Labels: []string{"Doc"}, Properties: map[string]any{"text": "delta live write"}, ChunkEmbeddings: [][]float32{{0.6, 0.8, 0}}}

func writeCompatStore(t *testing.T, dir string) {
	t.Helper()
	engine, err := storage.NewBadgerEngine(filepath.Join(dir, "graph"))
	require.NoError(t, err)
	for _, n := range compatStoreNodes {
		_, err := engine.CreateNode(storage.CopyNode(n))
		require.NoError(t, err)
	}
	svc := compatStoreService(engine, dir)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	live := storage.CopyNode(compatStoreLiveNode)
	_, err = engine.CreateNode(live)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(live))
	svc.PersistIndexesToDisk()
	require.NoError(t, svc.Close())
	require.NoError(t, engine.Close())
}

func requireCompatStoreServes(t *testing.T, dir string) {
	t.Helper()
	badger, err := storage.NewBadgerEngine(filepath.Join(dir, "graph"))
	require.NoError(t, err)
	engine := &countingStoreEngine{BadgerEngine: badger}
	svc := compatStoreService(engine, dir)
	defer func() { require.NoError(t, svc.Close()); require.NoError(t, badger.Close()) }()
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Zero(t, engine.iterations.Load(), "the saved indexes were not restored: the build iterated storage")
	require.True(t, svc.IsReady())
	require.Equal(t, 4, svc.EmbeddingCount(), "a, b-named-v, c-prop-p and d")
	for query, want := range map[string]string{"alpha": "nornic:a", "bravo": "nornic:b", "charlie": "nornic:c", "delta": "nornic:d"} {
		resp, err := svc.Search(context.Background(), query, nil, &SearchOptions{Limit: 5})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results, query)
		require.Equal(t, want, resp.Results[0].ID, query)
	}
	for _, probe := range []struct {
		vec  []float32
		want string
	}{{[]float32{1, 0, 0}, "nornic:a"}, {[]float32{0, 1, 0}, "nornic:b"}, {[]float32{0, 0, 1}, "nornic:c"}, {[]float32{0.6, 0.8, 0}, "nornic:d"}} {
		resp, err := svc.Search(context.Background(), "", probe.vec, &SearchOptions{Limit: 1})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results, probe.want)
		require.Equal(t, probe.want, resp.Results[0].ID)
	}
}

// TestStoreCompatRoundTrip: a store this version writes is restored by this
// version.
func TestStoreCompatRoundTrip(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	dir := t.TempDir()
	writeCompatStore(t, dir)
	requireCompatStoreServes(t, dir)
}

// TestStoreCompatWriteFixture writes a store and prints it, for another
// version to read (NORNICDB_STORE_COMPAT_WRITE=1).
func TestStoreCompatWriteFixture(t *testing.T) {
	if os.Getenv("NORNICDB_STORE_COMPAT_WRITE") != "1" {
		t.Skip("set NORNICDB_STORE_COMPAT_WRITE=1 to print a store fixture")
	}
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	dir := t.TempDir()
	writeCompatStore(t, dir)
	var archive bytes.Buffer
	zw := gzip.NewWriter(&archive)
	tw := tar.NewWriter(zw)
	require.NoError(t, filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644, Size: int64(len(data)), ModTime: info.ModTime()}); err != nil {
			return err
		}
		_, err = tw.Write(data)
		return err
	}))
	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())
	fmt.Printf("STORE-COMPAT-BEGIN\n%s\nSTORE-COMPAT-END\n", base64.StdEncoding.EncodeToString(archive.Bytes()))
}

// TestStoreCompatReadsFixtures: every store fixture in
// testdata/store_compat, written by another version, is restored and served.
func TestStoreCompatReadsFixtures(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	fixtures, err := filepath.Glob(filepath.Join("testdata", "store_compat", "*.tgz.b64"))
	require.NoError(t, err)
	if len(fixtures) == 0 {
		t.Skip("no store fixtures")
	}
	for _, fixture := range fixtures {
		t.Run(strings.TrimSuffix(filepath.Base(fixture), ".tgz.b64"), func(t *testing.T) {
			encoded, err := os.ReadFile(fixture)
			require.NoError(t, err)
			raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
			require.NoError(t, err)
			zr, err := gzip.NewReader(bytes.NewReader(raw))
			require.NoError(t, err)
			tr := tar.NewReader(zr)
			dir := t.TempDir()
			for {
				header, err := tr.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				path := filepath.Join(dir, filepath.FromSlash(header.Name))
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
				data, err := io.ReadAll(tr)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, data, 0o644))
				require.NoError(t, os.Chtimes(path, header.ModTime, header.ModTime))
			}
			requireCompatStoreServes(t, dir)
		})
	}
}
