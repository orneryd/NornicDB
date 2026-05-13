package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func buildSearchProfileFixture(tb testing.TB, nodeCount int, dims int) (*Service, []float32) {
	tb.Helper()
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "nornic")
	svc := NewServiceWithDimensions(engine, dims)

	// Build deterministic synthetic data with mixed lexical content and vectors.
	for i := 0; i < nodeCount; i++ {
		emb := make([]float32, dims)
		emb[i%dims] = 1
		emb[(i+7)%dims] = 0.25
		emb[(i+13)%dims] = 0.15
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("n-%d", i)),
			Labels: []string{"Document"},
			Properties: map[string]any{
				"title":   fmt.Sprintf("Prescription document %d", i),
				"content": "where are my prescriptions and refill history",
			},
			ChunkEmbeddings: [][]float32{emb},
		}
		_, err := engine.CreateNode(node)
		require.NoError(tb, err)
		require.NoError(tb, svc.IndexNode(node))
	}

	queryEmb := make([]float32, dims)
	queryEmb[0] = 1
	queryEmb[7%dims] = 0.25
	queryEmb[13%dims] = 0.15
	return svc, queryEmb
}

func TestSearchProfile_Fixture_RRFHybrid(t *testing.T) {
	t.Parallel()
	svc, queryEmb := buildSearchProfileFixture(t, 2000, 64)

	opts := DefaultSearchOptions()
	opts.Limit = 20
	resp, err := svc.Search(context.Background(), "where are my prescriptions?", queryEmb, opts)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "rrf_hybrid", resp.SearchMethod)
	require.GreaterOrEqual(t, len(resp.Results), 1)
}

func BenchmarkSearchProfile_RRFHybrid(b *testing.B) {
	svc, queryEmb := buildSearchProfileFixture(b, 9000, 64)
	svc.resultCache = nil // profile steady-state search path, not cached responses
	opts := DefaultSearchOptions()
	opts.Limit = 20
	query := "where are my prescriptions?"

	// Warm pipeline/HNSW once so benchmark reflects steady-state latency.
	_, err := svc.Search(context.Background(), query, queryEmb, opts)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := svc.Search(context.Background(), query, queryEmb, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func profileDiskFixtureConfig(tb testing.TB) (string, string, int) {
	tb.Helper()
	dataDir := os.Getenv("NORNICDB_PROFILE_DATA_DIR")
	if dataDir == "" {
		dataDir = "~/src/NornicDB/data/test"
	}
	dbName := os.Getenv("NORNICDB_PROFILE_DB")
	if dbName == "" {
		dbName = "translations"
	}
	dims := 1024
	if raw := os.Getenv("NORNICDB_PROFILE_DIMS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		require.NoError(tb, err)
		require.Greater(tb, parsed, 0)
		dims = parsed
	}
	return dataDir, dbName, dims
}

func loadSearchProfileDiskFixture(tb testing.TB) (*Service, []float32) {
	tb.Helper()

	dataDir, dbName, dims := profileDiskFixtureConfig(tb)
	if _, err := os.Stat(dataDir); err != nil {
		tb.Skipf("disk fixture data dir unavailable: %s (%v)", dataDir, err)
	}
	searchDir := filepath.Join(dataDir, "search", dbName)
	fulltextPath := filepath.Join(searchDir, "bm25")
	vectorPath := filepath.Join(searchDir, "vectors")
	hnswPath := filepath.Join(searchDir, "hnsw")
	if _, err := os.Stat(vectorPath + ".vec"); err != nil {
		tb.Skipf("disk fixture vector file unavailable: %s.vec (%v)", vectorPath, err)
	}

	baseEngine, err := storage.NewBadgerEngine(dataDir)
	if err != nil {
		tb.Skipf("disk fixture storage unavailable (possibly already open/locked): %v", err)
	}
	tb.Cleanup(func() {
		_ = baseEngine.Close()
	})

	engine := storage.NewNamespacedEngine(baseEngine, dbName)
	svc := NewServiceWithDimensions(engine, dims)
	svc.SetFulltextIndexPath(fulltextPath)
	svc.SetVectorIndexPath(vectorPath)
	svc.SetHNSWIndexPath(hnswPath)

	_ = svc.fulltextIndex.Load(fulltextPath)

	vfs, err := NewVectorFileStore(vectorPath, dims)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = vfs.Close()
	})
	require.NoError(tb, vfs.Load())
	svc.mu.Lock()
	svc.vectorFileStore = vfs
	svc.mu.Unlock()

	if _, err := os.Stat(hnswPath); err == nil {
		loaded, err := LoadHNSWIndex(hnswPath, svc.getVectorLookup())
		require.NoError(tb, err)
		if loaded != nil {
			svc.hnswMu.Lock()
			svc.hnswIndex = loaded
			svc.hnswMu.Unlock()
		}
	}

	var firstID string
	vfs.mu.RLock()
	for id := range vfs.idToOff {
		firstID = id
		break
	}
	vfs.mu.RUnlock()
	require.NotEmpty(tb, firstID, "disk fixture vector store is empty")
	queryEmb, ok := vfs.GetVector(firstID)
	require.True(tb, ok)
	require.NotEmpty(tb, queryEmb)

	return svc, queryEmb
}

func TestSearchProfile_DiskFixture_LoadAndSearch(t *testing.T) {
	t.Parallel()
	if os.Getenv("NORNICDB_PROFILE_USE_DISK_FIXTURE") == "" {
		t.Skip("set NORNICDB_PROFILE_USE_DISK_FIXTURE=1 to run with local data/test fixture")
	}

	svc, queryEmb := loadSearchProfileDiskFixture(t)
	opts := DefaultSearchOptions()
	opts.Limit = 20

	resp, err := svc.Search(context.Background(), "where are my prescriptions?", queryEmb, opts)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.SearchMethod)
	require.GreaterOrEqual(t, len(resp.Results), 1)
}

func BenchmarkSearchProfile_RRFHybrid_FromDisk(b *testing.B) {
	svc, queryEmb := loadSearchProfileDiskFixture(b)
	svc.resultCache = nil

	opts := DefaultSearchOptions()
	opts.Limit = 20
	query := "where are my prescriptions?"

	_, err := svc.Search(context.Background(), query, queryEmb, opts)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := svc.Search(context.Background(), query, queryEmb, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}
