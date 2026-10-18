package search

// Legacy search layouts remain usable across versions. Pre-vkey2 indexes are
// rebuilt from their graph records once; current indexes restore without a scan.
// Generate the old layouts in temporary directories instead of shipping archives.

import (
	"context"
	"path/filepath"
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

// OpenReadSnapshot counts a node read through the build's snapshot too.
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

func requireCompatStoreServes(t *testing.T, dir string, wantIterations int32) {
	t.Helper()
	badger, err := storage.NewBadgerEngine(filepath.Join(dir, "graph"))
	require.NoError(t, err)
	engine := &countingStoreEngine{BadgerEngine: badger}
	svc := compatStoreService(engine, dir)
	defer func() { require.NoError(t, svc.Close()); require.NoError(t, badger.Close()) }()
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, wantIterations, engine.iterations.Load(), "legacy indexes rebuild once; current indexes restore without scanning storage")
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
	svc.PersistIndexesToDisk()
}

// TestStoreCompatRoundTrip: a store this version writes is restored by this
// version.
func TestStoreCompatRoundTrip(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	dir := t.TempDir()
	writeCompatStore(t, dir)
	requireCompatStoreServes(t, dir, 0)
}

func TestStoreCompatReadsFixtures(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, layout := range []struct {
		name   string
		scalar bool
	}{{"v1.4.3", false}, {"release-v1.4.4-24d08776c", true}} {
		t.Run(layout.name, func(t *testing.T) {
			dir := t.TempDir()
			writeLegacyCompatStore(t, dir, layout.scalar)
			// The pre-vkey2 keys cannot be reused. The accepted upgrade contract
			// rebuilds once, preserves all four text/vector answers, and saves a
			// current store that the next open restores without scanning storage.
			requireCompatStoreServes(t, dir, 1)
			requireCompatStoreServes(t, dir, 0)
		})
	}
}
