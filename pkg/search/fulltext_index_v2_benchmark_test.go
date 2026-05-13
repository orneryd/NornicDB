package search

import (
	"os"
	"path/filepath"
	"testing"
)

func bm25LegacyDiskPath(tb testing.TB) string {
	tb.Helper()
	if raw := os.Getenv("NORNICDB_PROFILE_BM25_PATH"); raw != "" {
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
	return filepath.Join(base, "search", db, "bm25")
}

func bm25V2DiskPath(tb testing.TB) string {
	tb.Helper()
	if raw := os.Getenv("NORNICDB_PROFILE_BM25_V2_PATH"); raw != "" {
		return raw
	}
	return bm25LegacyDiskPath(tb) + ".v2"
}

func loadLegacyBM25FromDisk(tb testing.TB) *FulltextIndex {
	tb.Helper()
	path := bm25LegacyDiskPath(tb)
	if _, err := os.Stat(path); err != nil {
		tb.Skipf("bm25 disk file unavailable: %s (%v)", path, err)
	}
	idx := NewFulltextIndex()
	if err := idx.Load(path); err != nil {
		tb.Skipf("failed to load legacy bm25 from %s: %v", path, err)
	}
	if idx.Count() == 0 {
		tb.Skipf("legacy bm25 index is empty: %s", path)
	}
	return idx
}

func loadV2BM25FromDisk(tb testing.TB) *FulltextIndexV2 {
	tb.Helper()
	path := bm25V2DiskPath(tb)
	if _, err := os.Stat(path); err != nil {
		tb.Skipf("bm25 v2 disk file unavailable: %s (%v)", path, err)
	}
	idx := NewFulltextIndexV2()
	if err := idx.Load(path); err != nil {
		tb.Skipf("failed to load v2 bm25 from %s: %v", path, err)
	}
	if idx.Count() == 0 {
		tb.Skipf("v2 bm25 index is empty: %s", path)
	}
	return idx
}

func TestBM25V2_DiskTopKOverlap(t *testing.T) {
	if os.Getenv("NORNICDB_PROFILE_USE_DISK_FIXTURE") == "" {
		t.Skip("set NORNICDB_PROFILE_USE_DISK_FIXTURE=1 to run disk parity tests")
	}
	legacy := loadLegacyBM25FromDisk(t)
	v2 := loadV2BM25FromDisk(t)

	queries := []string{
		"where are my prescriptions",
		"prescription refill status",
		"drug interactions warnings",
		"medication dosage history",
	}
	for _, q := range queries {
		lr := legacy.Search(q, 20)
		vr := v2.Search(q, 20)
		if len(lr) == 0 || len(vr) == 0 {
			t.Fatalf("empty results for query %q (legacy=%d v2=%d)", q, len(lr), len(vr))
		}
		overlap := 0
		set := make(map[string]struct{}, len(lr))
		for _, r := range lr {
			set[r.ID] = struct{}{}
		}
		for _, r := range vr {
			if _, ok := set[r.ID]; ok {
				overlap++
			}
		}
		if overlap < 10 {
			t.Fatalf("insufficient top-20 overlap for query %q: overlap=%d", q, overlap)
		}
	}
}

func BenchmarkBM25Legacy_FromDisk(b *testing.B) {
	idx := loadLegacyBM25FromDisk(b)
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

func BenchmarkBM25V2_FromDisk(b *testing.B) {
	idx := loadV2BM25FromDisk(b)
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

func BenchmarkBM25Legacy_LoadOnly_FromDisk(b *testing.B) {
	path := bm25LegacyDiskPath(b)
	if _, err := os.Stat(path); err != nil {
		b.Skipf("bm25 disk file unavailable: %s (%v)", path, err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx := NewFulltextIndex()
		if err := idx.Load(path); err != nil {
			b.Fatalf("failed to load legacy bm25 from %s: %v", path, err)
		}
		if idx.Count() == 0 {
			b.Fatalf("legacy bm25 index is empty: %s", path)
		}
	}
}

func BenchmarkBM25V2_LoadOnly_FromDisk(b *testing.B) {
	path := bm25V2DiskPath(b)
	if _, err := os.Stat(path); err != nil {
		b.Skipf("bm25 v2 disk file unavailable: %s (%v)", path, err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx := NewFulltextIndexV2()
		if err := idx.Load(path); err != nil {
			b.Fatalf("failed to load v2 bm25 from %s: %v", path, err)
		}
		if idx.Count() == 0 {
			b.Fatalf("v2 bm25 index is empty: %s", path)
		}
	}
}

func BenchmarkBM25Legacy_SearchOnly_FromDisk(b *testing.B) {
	BenchmarkBM25Legacy_FromDisk(b)
}

func BenchmarkBM25V2_SearchOnly_FromDisk(b *testing.B) {
	BenchmarkBM25V2_FromDisk(b)
}
