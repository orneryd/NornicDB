package search

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func bm25DiskPath(tb testing.TB) string {
	tb.Helper()
	if raw := os.Getenv("NORNICDB_PROFILE_BM25_V2_PATH"); raw != "" {
		return raw
	}
	base := "~/src/NornicDB/data/test"
	if raw := os.Getenv("NORNICDB_PROFILE_DATA_DIR"); raw != "" {
		base = raw
	}
	db := "translations"
	if raw := os.Getenv("NORNICDB_PROFILE_DB"); raw != "" {
		db = raw
	}
	return filepath.Join(base, "search", db, "bm25.v2")
}

func loadBM25FromDisk(tb *testing.B) *FulltextIndex {
	tb.Helper()
	path := bm25DiskPath(tb)
	if _, err := os.Stat(path); err != nil {
		tb.Skipf("bm25 disk fixture unavailable: %s (%v)", path, err)
	}
	idx := NewFulltextIndex(nil)
	if err := idx.Load(path); err != nil {
		tb.Fatalf("failed to load bm25 from %s: %v", path, err)
	}
	if idx.Count() == 0 {
		tb.Fatalf("bm25 index is empty: %s", path)
	}
	return idx
}

func TestBM25DiskSnapshotSearchDeterministic(t *testing.T) {
	queries := []string{
		"where are my prescriptions",
		"prescription refill status",
		"drug interactions warnings",
		"medication dosage history",
	}
	index := NewFulltextIndex(nil)
	for _, query := range queries {
		index.Index(query, query, nil, nil, false)
	}
	path := filepath.Join(t.TempDir(), "bm25.v2")
	require.NoError(t, index.Save(path))
	loaded := NewFulltextIndex(nil)
	require.NoError(t, loaded.Load(path))
	for _, query := range queries {
		results := loaded.Search(query, 20)
		require.NotEmpty(t, results, "query %q", query)
		require.Equal(t, index.Search(query, 20), results, "query %q", query)
		require.Equal(t, results, loaded.Search(query, 20), "query %q", query)
	}
}

func BenchmarkBM25_FromDisk(b *testing.B) {
	idx := loadBM25FromDisk(b)
	queries := []string{
		"where are my prescriptions",
		"prescription refill status",
		"drug interactions warnings",
		"medication dosage history",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Search(queries[i%len(queries)], 40)
	}
}

func BenchmarkBM25_LoadOnly_FromDisk(b *testing.B) {
	path := bm25DiskPath(b)
	if _, err := os.Stat(path); err != nil {
		b.Skipf("bm25 disk fixture unavailable: %s (%v)", path, err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx := NewFulltextIndex(nil)
		if err := idx.Load(path); err != nil {
			b.Fatalf("failed to load bm25 from %s: %v", path, err)
		}
		if idx.Count() == 0 {
			b.Fatalf("bm25 index is empty: %s", path)
		}
	}
}
