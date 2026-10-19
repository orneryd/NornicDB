package search

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Tests of a ready backend wait explicitly; first-use request tests above keep
// construction paused and exercise the exact fallback instead.
func requireHNSWReady(t testing.TB, svc *Service) {
	t.Helper()
	t.Cleanup(func() { _ = svc.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := svc.getOrCreateHNSWIndex(ctx, svc.VectorIndexDimensions())
	require.NoError(t, err)
	_, err = svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	require.Equal(t, strategyModeHNSW, svc.currentPipelineStrategy())
}

func TestHNSWBackgroundBuildAnswersExactly(t *testing.T) {
	for _, fileBacked := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "file"}[fileBacked], func(t *testing.T) { testHNSWBackgroundExact(t, fileBacked) })
	}
}

func testHNSWBackgroundExact(t *testing.T, fileBacked bool) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { _ = svc.Close(); _ = engine.Close() })
	if fileBacked {
		vfs, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 3)
		require.NoError(t, err)
		svc.vectorFileStore = vfs
	}
	for _, item := range []struct {
		id string
		v  []float32
	}{
		{"best", []float32{1, 0, 0}}, {"next", []float32{1, 1, 0}}, {"other", []float32{0, 1, 0}},
		{"excluded", []float32{1, 0, 0}},
	} {
		node := &storage.Node{ID: storage.NodeID(item.id), ChunkEmbeddings: [][]float32{item.v}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	hnswBeforeInstallHook = func(context.Context) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
	}
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { finish(); hnswBeforeInstallHook = nil })
	type answer struct {
		response *SearchResponse
		err      error
	}
	answers := make(chan answer, 8)
	minimum := -1.0
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	eligible := func(node *storage.Node) bool { return node.ID != "excluded" }
	for i := 0; i < cap(answers); i++ {
		go func(i int) {
			query := ""
			if i%2 == 1 {
				query = "zzzz"
			}
			resp, err := svc.Search(ctx, query, []float32{1, 0, 0}, &SearchOptions{Limit: 2, MinSimilarity: &minimum, Where: eligible})
			answers <- answer{resp, err}
		}(i)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("build did not start")
	}
	for i := 0; i < cap(answers); i++ {
		select {
		case got := <-answers:
			require.NoError(t, got.err)
			pipeline, err := svc.getOrCreateVectorPipeline(ctx)
			require.NoError(t, err)
			if fileBacked {
				require.IsType(t, &FileStoreBruteForceCandidateGen{}, pipeline.candidateGen)
			} else {
				require.IsType(t, &BruteForceCandidateGen{}, pipeline.candidateGen)
			}
			require.Len(t, got.response.Results, 2)
			require.Equal(t, "best", got.response.Results[0].ID)
			require.Equal(t, "next", got.response.Results[1].ID)
		case <-time.After(time.Second):
			finish()
			t.Fatal("request waited for construction instead of exact search")
		}
	}
	<-ctx.Done()
	require.Equal(t, int32(1), calls.Load())
	finish()
	idx, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, 4, idx.Size())
	resp, err := svc.vectorSearchOnly(context.Background(), []float32{1, 0, 0}, &SearchOptions{Limit: 2, MinSimilarity: &minimum, Where: eligible})
	require.NoError(t, err)
	require.Equal(t, "vector_hnsw", resp.SearchMethod)
	require.Equal(t, "best", resp.Results[0].ID)
	require.Equal(t, "next", resp.Results[1].ID)
}

func TestHNSWBackgroundBuildCancelledByOwner(t *testing.T) {
	for _, drop := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "drop"}[drop], func(t *testing.T) {
			svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
			t.Cleanup(func() { _ = svc.Close() })
			require.NoError(t, svc.IndexNode(&storage.Node{ID: "before", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
			started := make(chan struct{})
			hnswBeforeInstallHook = func(ctx context.Context) { close(started); <-ctx.Done() }
			t.Cleanup(func() { hnswBeforeInstallHook = nil })
			_, job, err := svc.startHNSWBuild(3)
			require.NoError(t, err)
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("build did not start")
			}
			done := make(chan struct{})
			go func() {
				if drop {
					svc.ClearVectorIndex()
				} else {
					_ = svc.Close()
				}
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("owner did not join construction")
			}
			<-job.done
			require.ErrorIs(t, job.err, context.Canceled)
			require.Nil(t, svc.hnswIndex)
			require.False(t, svc.hnswRebuildInFlight.Load())
			hnswBeforeInstallHook = nil
			if drop {
				require.NoError(t, svc.IndexNode(&storage.Node{ID: "after", ChunkEmbeddings: [][]float32{{0, 1, 0}}}))
				idx, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
				require.NoError(t, err)
				require.Equal(t, 1, idx.Size())
			} else {
				_, _, err := svc.startHNSWBuild(3)
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

type cancelSearchCandidateGenerator struct {
	cancel context.CancelFunc
	calls  int
}

func (g *cancelSearchCandidateGenerator) SearchCandidates(ctx context.Context, _ []float32, _ int, _ float64) ([]Candidate, error) {
	g.calls++
	g.cancel()
	return nil, ctx.Err()
}

func TestSearchCancelledContextDoesNotRetryFallback(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	t.Cleanup(func() { _ = svc.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gen := &cancelSearchCandidateGenerator{cancel: cancel}
	svc.vectorPipeline = NewVectorSearchPipeline(gen, NewCPUExactScorer(svc.vectorIndex))
	_, err := svc.Search(ctx, "missing", []float32{1, 0, 0}, &SearchOptions{Limit: 2})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, gen.calls)
	pipeline, err := svc.getOrCreateVectorPipeline(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, pipeline, "even a cached pipeline must not be retried on a cancelled request")
}

type countingHNSWAccelerator struct {
	*CPUHNSWBuildAccelerator
	prepares int
}

func (a *countingHNSWAccelerator) Prepare(dim, count int) error {
	a.prepares++
	return a.CPUHNSWBuildAccelerator.Prepare(dim, count)
}

func TestHNSWCancelledContextDoesNotStartGPUOrCPUFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	accel := &countingHNSWAccelerator{CPUHNSWBuildAccelerator: NewCPUHNSWBuildAccelerator()}
	iterations := 0
	iter := func(_ int, fn func([]hnswBuildPair) error) error { iterations++; return fn(nil) }
	_, _, err := buildHNSWWithOptionalGPU(ctx, 3, DefaultHNSWConfig(), nil, 1, iter, accel)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, accel.prepares)
	require.Zero(t, iterations)
}

type cancellingHNSWAccelerator struct {
	*CPUHNSWBuildAccelerator
	cancel context.CancelFunc
}

func (a *cancellingHNSWAccelerator) Prepare(int, int) error {
	a.cancel()
	return errors.New("accelerator preparation interrupted")
}

func TestHNSWCancelledPreparationDoesNotStartCPUFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accel := &cancellingHNSWAccelerator{CPUHNSWBuildAccelerator: NewCPUHNSWBuildAccelerator(), cancel: cancel}
	iterations := 0
	iter := func(_ int, fn func([]hnswBuildPair) error) error { iterations++; return fn(nil) }
	cfg := DefaultHNSWConfig()
	cfg.UseGPUBuild = true
	_, _, err := buildHNSWWithOptionalGPU(ctx, 3, cfg, nil, 1, iter, accel)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, iterations, "cancelled preparation started CPU construction")
}

func TestHNSWBuildDefersSnapshotWhileVectorsAreReplaced(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "old", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
	svc.hnswVectorsChanging.Store(true)
	pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.IsType(t, &BruteForceCandidateGen{}, pipeline.candidateGen)
	require.Nil(t, svc.hnswBuild)
	require.Nil(t, svc.vectorPipeline, "a paused fallback must not prevent construction after reset")
	require.NoError(t, svc.RemoveNode("old"))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "new", ChunkEmbeddings: [][]float32{{0, 1, 0}}}))
	svc.hnswVectorsChanging.Store(false)
	requireHNSWReady(t, svc)
	results, err := svc.hnswIndex.Search(context.Background(), []float32{0, 1, 0}, 5, -1)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "new", results[0].ID)
}

func TestHNSWPublishedGraphUsesCurrentVectorStore(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "item", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
	idx, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	vfs, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 3)
	require.NoError(t, err)
	require.NoError(t, vfs.Add("item", []float32{0, 1, 0}))
	svc.mu.Lock()
	svc.vectorFileStore = vfs
	svc.mu.Unlock()
	svc.vectorIndex.Remove("item")
	results, err := idx.Search(context.Background(), []float32{0, 1, 0}, 1, 0.9)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "item", results[0].ID)
}

func TestHNSWFailedJobsReleaseContexts(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "item", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
	// A dimension mismatch fails inside the worker, after source validation.
	_, err := svc.getOrCreateHNSWIndex(context.Background(), 2)
	require.Error(t, err)
	failed := svc.hnswBuild
	require.NotNil(t, failed)
	_, err = svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	require.ErrorIs(t, failed.ctx.Err(), context.Canceled)
}
