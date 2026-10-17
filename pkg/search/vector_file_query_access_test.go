package search

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorFileQueryReadsFollowMutationsAndCompaction(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")
	v, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 3)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v.Close()) })
	require.NoError(t, v.Add("a", []float32{1, 0, 0}))
	require.NoError(t, v.Add("b", []float32{0, 1, 0}))
	owned, ok := v.GetVector("a")
	require.True(t, ok)
	owned[0] = 99
	again, ok := v.GetVector("a")
	require.True(t, ok)
	require.Equal(t, float32(1), again[0])
	require.NoError(t, v.Add("a", []float32{0, 1, 0}))
	require.True(t, v.Remove("b"))
	require.NoError(t, v.Add("c", []float32{0, 0, 1}))
	check := func() {
		got, ok := v.GetVector("a")
		require.True(t, ok)
		require.Equal(t, []float32{0, 1, 0}, got)
		_, ok = v.GetVector("b")
		require.False(t, ok)
		score, ok := v.scoreVectorDot([]float32{0, 1, 0}, "a")
		require.True(t, ok)
		require.Equal(t, float64(1), score)
		_, ok = v.scoreVectorDot([]float32{0, 1, 0}, "b")
		require.False(t, ok)
		scored, err := v.scoreCandidatesDot(context.Background(), []float32{0, 1, 0}, []Candidate{{ID: "c"}, {ID: "b"}, {ID: "a"}})
		require.NoError(t, err)
		require.Equal(t, []ScoredCandidate{{ID: "a", Score: 1}, {ID: "c", Score: 0}}, scored)
	}
	check()
	compacted, err := v.CompactIfNeeded()
	require.NoError(t, err)
	require.True(t, compacted)
	check()
	require.NoError(t, v.Save())
	require.NoError(t, v.Add("uncommitted", []float32{1, 0, 0}))
	_, ok = v.GetVector("uncommitted")
	require.True(t, ok)
	require.NoError(t, v.Load())
	_, ok = v.GetVector("uncommitted")
	require.False(t, ok)
	check()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = v.scoreCandidatesDot(ctx, []float32{0, 1, 0}, []Candidate{{ID: "a"}})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, v.Close())
	_, ok = v.GetVector("a")
	require.False(t, ok)
	scored, err := v.scoreCandidatesDot(context.Background(), []float32{1, 0, 0}, []Candidate{{ID: "a"}})
	require.NoError(t, err)
	require.Empty(t, scored)
}

func TestVectorFileQueryReadsConcurrentWithRewrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")
	v, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
	require.NoError(t, err)
	defer v.Close()
	require.NoError(t, v.Add("live", []float32{1, 0}))
	var readers sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				vec, ok := v.GetVector("live")
				if !ok || len(vec) != 2 || (vec[0] != 0 && vec[0] != 1) {
					t.Error("invalid owned vector")
					return
				}
				got, err := v.scoreCandidatesDot(context.Background(), []float32{1, 0}, []Candidate{{ID: "live"}})
				if err != nil || len(got) != 1 || (got[0].Score != 0 && got[0].Score != 1) {
					t.Error("invalid score during rewrite")
					return
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		vec := []float32{1, 0}
		if i%2 == 1 {
			vec = []float32{0, 1}
		}
		if err := v.Add("live", vec); err != nil {
			t.Error(err)
			break
		}
		if _, err := v.CompactIfNeeded(); err != nil {
			t.Error(err)
			break
		}
	}
	close(done)
	readers.Wait()
}

func BenchmarkVectorFileQueryCandidateScoring(b *testing.B) {
	v, err := NewVectorFileStore(filepath.Join(b.TempDir(), "vectors"), 1024)
	if err != nil {
		b.Fatal(err)
	}
	defer v.Close()
	for i := 0; i < 4096; i++ {
		if err := v.Add(fmt.Sprint(i), benchmarkStorageVector(1024, int64(i))); err != nil {
			b.Fatal(err)
		}
	}
	query := benchmarkStorageVector(1024, 7)
	candidates := make([]Candidate, 15)
	for i := range candidates {
		candidates[i] = Candidate{ID: fmt.Sprint(i*257 + 3)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := v.scoreCandidatesDot(context.Background(), query, candidates)
		if err != nil || len(got) != len(candidates) {
			b.Fatalf("invalid candidates: %d, %v", len(got), err)
		}
	}
}

func TestVectorFileQueryMappingFailuresReleaseResources(t *testing.T) {
	if !vectorFileMappingSupported {
		t.Skip("query mappings are unavailable on this platform")
	}
	t.Run("unavailable file leaves locks usable", func(t *testing.T) {
		v, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
		require.NoError(t, err)
		require.NoError(t, v.Add("a", []float32{1, 0}))
		require.NoError(t, v.file.Close())
		_, ok := v.GetVector("a")
		require.False(t, ok)
		_, ok = v.scoreVectorDot([]float32{1, 0}, "a")
		require.False(t, ok)
		require.True(t, v.mu.TryLock())
		v.mu.Unlock()
		_, err = v.scoreCandidatesDot(context.Background(), []float32{1, 0}, []Candidate{{ID: "a"}})
		require.Error(t, err)
		require.Error(t, v.Close())
	})
	t.Run("mapping close error still closes file", func(t *testing.T) {
		v, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
		require.NoError(t, err)
		require.NoError(t, v.Add("a", []float32{1, 0}))
		_, ok := v.GetVector("a")
		require.True(t, ok)
		original := v.queryReadMap.unmap
		failure := fmt.Errorf("injected mapping close failure")
		v.queryReadMap.unmap = func() error { require.NoError(t, original()); return failure }
		require.ErrorIs(t, v.Close(), failure)
		require.Nil(t, v.file)
		require.Nil(t, v.queryReadMap)
		require.NoError(t, v.Close())
	})
}

func TestVectorFileBorrowedDotDoesNotAllocateOrEscape(t *testing.T) {
	v, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 4)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v.Close()) })
	require.NoError(t, v.Add("one", []float32{1, 0, 0, 0}))
	query := []float32{1, 0, 0, 0}
	score, ok := v.scoreVectorDot(query, "one")
	require.True(t, ok)
	require.Equal(t, float64(1), score)
	if vectorFileMappingSupported && nativeLittleEndian {
		allocations := testing.AllocsPerRun(100, func() {
			score, ok := v.scoreVectorDot(query, "one")
			if !ok || score != 1 {
				t.Fatal("borrowed score unavailable")
			}
		})
		require.Zero(t, allocations)
	}
	require.NoError(t, v.Close())
	_, err = v.beginReadLease()
	require.ErrorIs(t, err, errVecFileClosed)
	_, ok = v.scoreVectorDot(query, "one")
	require.False(t, ok)
}

func TestVectorFileQueryReadsUnavailableRows(t *testing.T) {
	for _, size := range []int64{0, vecHeaderSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			v, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, v.Close()) })
			require.NoError(t, v.Add("one", []float32{1, 0}))
			require.NoError(t, os.Truncate(v.vecPath, size))
			_, ok := v.GetVector("one")
			require.False(t, ok)
			_, ok = v.scoreVectorDot([]float32{1, 0}, "one")
			require.False(t, ok)
			if size == vecHeaderSize {
				got, err := v.scoreCandidatesDot(context.Background(), []float32{1, 0}, []Candidate{{ID: "one"}})
				require.NoError(t, err)
				require.Empty(t, got)
			}
		})
	}
}

// A readable file the platform will not map (for example ENOMEM from a
// whole-file mapping) keeps serving copied reads. The mapping is retried only
// after growth, and a failed refresh after growth does not hide existing rows.
func TestVectorFileQueryReadsFallBackWhenMappingFails(t *testing.T) {
	ctx := context.Background()
	mapped, err := NewVectorFileStore(filepath.Join(t.TempDir(), "mapped"), 3)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mapped.Close()) })
	unmapped, err := NewVectorFileStore(filepath.Join(t.TempDir(), "unmapped"), 3)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, unmapped.Close()) })
	platformMap := unmapped.mapFile
	mapFails, mapCalls := true, 0
	unmapped.mapFile = func(file *os.File, size int) ([]byte, func() error, error) {
		mapCalls++
		if mapFails {
			return nil, nil, errors.New("cannot allocate memory")
		}
		return platformMap(file, size)
	}

	var ids []string
	add := func(id string, vec []float32) {
		require.NoError(t, mapped.Add(id, vec))
		require.NoError(t, unmapped.Add(id, vec))
		ids = append(ids, id)
	}
	query := []float32{0.6, 0, 0.8}
	check := func(wantMapped bool, wantCalls int) {
		t.Helper()
		candidates := []Candidate{{ID: "missing"}}
		for _, id := range ids {
			want, ok := mapped.GetVector(id)
			require.True(t, ok)
			got, ok := unmapped.GetVector(id)
			require.True(t, ok, "readable row %q reported absent", id)
			require.Equal(t, want, got)
			wantScore, ok := mapped.scoreVectorDot(query, id)
			require.True(t, ok)
			gotScore, ok := unmapped.scoreVectorDot(query, id)
			require.True(t, ok, "readable row %q not scored", id)
			require.Equal(t, wantScore, gotScore)
			candidates = append(candidates, Candidate{ID: id})
		}
		_, ok := unmapped.GetVector("missing")
		require.False(t, ok)
		want, err := mapped.scoreCandidatesDot(ctx, query, candidates)
		require.NoError(t, err)
		got, err := unmapped.scoreCandidatesDot(ctx, query, candidates)
		require.NoError(t, err)
		require.Len(t, got, len(ids))
		require.Equal(t, want, got)
		if !vectorFileMappingSupported {
			require.Zero(t, mapCalls)
			return
		}
		require.Equal(t, wantCalls, mapCalls)
		unmapped.mu.RLock()
		require.Equal(t, wantMapped, unmapped.queryReadMap != nil)
		unmapped.mu.RUnlock()
	}

	add("a", []float32{1, 0, 0})
	add("b", []float32{0, 1, 0})
	check(false, 1)
	check(false, 1) // copied reads are kept for this size; no remap per query
	add("c", []float32{0.6, 0.8, 0})
	check(false, 2)
	mapFails = false
	add("d", []float32{0, 0, 1})
	check(true, 3)
	mapFails = true
	add("e", []float32{0.8, 0, 0.6}) // the refresh releases the old mapping first
	check(false, 4)
}

// The public vector search returns the same results when the platform will not
// map the service's readable vector file.
func TestVectorSearchUsesCopiedReadsWhenMappingFails(t *testing.T) {
	newService := func(failMapping bool) (*Service, *atomic.Int32) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
		t.Cleanup(func() { require.NoError(t, svc.Close()) })
		svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
		svc.SetPersistenceEnabled(true)
		var mapCalls atomic.Int32
		for i := 0; i < 40; i++ {
			require.NoError(t, svc.IndexNode(&storage.Node{
				ID:              storage.NodeID(fmt.Sprintf("node-%02d", i)),
				ChunkEmbeddings: [][]float32{{float32(i%5) + 1, float32(i%7) + 0.5, float32(i % 3)}},
			}))
			if i == 0 && failMapping {
				vfs := svc.vectorFileStore
				vfs.mu.Lock()
				vfs.mapFile = func(*os.File, int) ([]byte, func() error, error) {
					mapCalls.Add(1)
					return nil, nil, errors.New("cannot allocate memory")
				}
				vfs.mu.Unlock()
			}
		}
		return svc, &mapCalls
	}
	mapped, _ := newService(false)
	unmapped, mapCalls := newService(true)
	opts := DefaultSearchOptions()
	opts.Limit = 10
	query := []float32{0.2, 0.9, 0.4}
	want, err := mapped.VectorSearchCandidates(context.Background(), query, opts)
	require.NoError(t, err)
	require.Len(t, want, 10)
	got, err := unmapped.VectorSearchCandidates(context.Background(), query, opts)
	require.NoError(t, err)
	require.Equal(t, want, got)
	if vectorFileMappingSupported {
		require.Positive(t, mapCalls.Load())
	}
}

// HNSW construction without a lease looks vectors up inside IterateChunked,
// which holds the read lock. After growth the query mapping cannot be
// refreshed there; rows it does not cover are read with ReadAt instead of
// waiting for the write lock.
func TestVectorFileQueryReadsInsideIterationAfterGrowth(t *testing.T) {
	v, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, v.Close()) })
	require.NoError(t, v.Add("early", []float32{1, 0}))
	_, ok := v.GetVector("early")
	require.True(t, ok)
	require.NoError(t, v.Add("late", []float32{0, 1}))
	// Before the fix this deadlocked: the nested lookup waited for the write lock.
	require.NoError(t, v.IterateChunked(10, func([]string, [][]float32) error {
		for _, id := range []string{"early", "late"} {
			got, ok := v.GetVector(id)
			require.True(t, ok, id)
			score, ok := v.scoreVectorDot(got, id)
			require.True(t, ok, id)
			require.InDelta(t, 1, score, 1e-6, id)
		}
		scored, err := v.scoreCandidatesDot(context.Background(), []float32{0, 1}, []Candidate{{ID: "early"}, {ID: "late"}})
		require.NoError(t, err)
		require.Equal(t, []ScoredCandidate{{ID: "late", Score: 1}, {ID: "early", Score: 0}}, scored)
		if vectorFileMappingSupported {
			require.NotEqual(t, v.nextOrdinal, v.queryReadSlots, "the mapping still covers only the early row")
		}
		return nil
	}))
	got, ok := v.GetVector("late")
	require.True(t, ok)
	require.Equal(t, []float32{0, 1}, got)
	if vectorFileMappingSupported {
		v.mu.RLock()
		defer v.mu.RUnlock()
		require.Equal(t, v.nextOrdinal, v.queryReadSlots, "an unnested query refreshes the mapping")
	}
}
