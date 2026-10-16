package search

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorDimensionReplacementRetiresConcurrentFactory(t *testing.T) {
	for _, maximum := range []string{"0", "10"} {
		t.Run(maximum, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", maximum)
			svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
			t.Cleanup(func() { _ = svc.Close() })
			// Complete initial NODE admission before installing the graph whose
			// retirement must be caused by the later dimension replacement.
			require.NoError(t, svc.BuildIndexes(context.Background()))
			_, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
			require.NoError(t, err)
			// Gate the real dimension helper after it replaces the memory source,
			// but before retiring the old ANN graph and finishing its cache barrier.
			// No production hook is needed: cluster retirement is an existing lock.
			svc.clusterHNSWMu.Lock()
			var once sync.Once
			release := func() { once.Do(svc.clusterHNSWMu.Unlock) }
			defer release()
			node := &storage.Node{ID: "replacement", ChunkEmbeddings: [][]float32{{0, 0, 0, 0, 1}}}
			_, err = svc.engine.CreateNode(node)
			require.NoError(t, err)
			done := make(chan struct{})
			var indexErr error
			defer func() {
				release()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("dimension worker did not join during cleanup")
				}
			}()
			id := make(chan string, 1)
			go func() {
				id <- serviceTestGoroutineID()
				indexErr = svc.IndexNode(node)
				close(done)
			}()
			writerID := <-id
			require.Eventually(t, func() bool {
				return serviceStackContains(writerID, "(*Service).replaceEmptyVectorDimensionsLocked", "sync.(*RWMutex).Lock")
			}, 5*time.Second, time.Millisecond, "dimension helper did not reach backend retirement")
			require.Equal(t, 5, svc.VectorIndexDimensions())
			_, err = svc.getOrCreateVectorPipeline(context.Background())
			require.NoError(t, err)
			release()
			select {
			case <-done:
				require.NoError(t, indexErr)
			case <-time.After(5 * time.Second):
				t.Fatal("dimension replacement did not finish")
			}
			cached := cachedVectorPipeline(svc)
			assert.Nil(t, cached, "dimension replacement retained a pipeline admitted before backend retirement")
			// The stale HNSW pipeline rejects this valid new-dimension query
			// before adaptive exact completion; no candidate cap is necessary.
			candidates, err := svc.VectorSearchCandidates(context.Background(), []float32{0, 0, 0, 0, 1}, nil)
			require.NoError(t, err)
			require.Len(t, candidates, 1)
			require.Equal(t, "replacement", candidates[0].ID)
		})
	}
}

func TestVectorDimensionReplacementJoinsBackgroundBuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	previous := hnswBeforeInstallHook
	hnswBeforeInstallHook = func(ctx context.Context) { entered <- ctx; <-release }
	buildDone, indexDone := make(chan struct{}), make(chan struct{})
	var buildErr, indexErr error
	indexStarted := false
	defer func() {
		unblock()
		// Close joins construction even if the caller failed before observing
		// the installation gate. Restore the shared test hook only after join.
		_ = svc.Close()
		for _, done := range []<-chan struct{}{buildDone, indexDone} {
			if done == indexDone && !indexStarted {
				continue
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("dimension construction worker did not join during cleanup")
			}
		}
		hnswBeforeInstallHook = previous
	}()
	go func() {
		_, buildErr = svc.getOrCreateHNSWIndex(context.Background(), 3)
		close(buildDone)
	}()
	var buildCtx context.Context
	select {
	case buildCtx = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old-dimension build did not reach installation")
	}
	node := &storage.Node{ID: "replacement", ChunkEmbeddings: [][]float32{{0, 0, 0, 0, 1}}}
	_, err := svc.engine.CreateNode(node)
	require.NoError(t, err)
	indexStarted = true
	go func() { indexErr = svc.IndexNode(node); close(indexDone) }()
	select {
	case <-buildCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("dimension replacement did not cancel construction")
	}
	select {
	case <-indexDone:
		t.Fatalf("dimension replacement returned before construction joined: %v", indexErr)
	default:
	}
	unblock()
	select {
	case <-buildDone:
		require.ErrorIs(t, buildErr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("retired construction did not finish")
	}
	select {
	case <-indexDone:
		require.NoError(t, indexErr)
	case <-time.After(5 * time.Second):
		t.Fatal("dimension replacement did not finish after construction")
	}
	// Remove the gate before the new graph is requested. Both workers are
	// joined above, so no goroutine can still be reading the test hook.
	hnswBeforeInstallHook = previous
	graph, err := svc.getOrCreateHNSWIndex(context.Background(), 5)
	require.NoError(t, err)
	require.Equal(t, 5, graph.GetDimensions())
	candidates, err := svc.VectorSearchCandidates(context.Background(), []float32{0, 0, 0, 0, 1}, nil)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "replacement", candidates[0].ID)
}
