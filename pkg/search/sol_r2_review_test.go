package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// persistedGraphService stores four nodes and builds a persisted service whose
// native graph is saved under dir.
func persistedGraphService(t *testing.T) (*Service, string) {
	t.Helper()
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	for i := range 4 {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i)), Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, float32(i), 0}}})
		require.NoError(t, err)
	}
	dir := t.TempDir()
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
	svc.SetPersistenceEnabled(true)
	return svc, dir
}

// sol r2 finding 1: a graph save that copied the graph before the source was
// retired must not publish that graph afterwards, where a restart would
// restore it over the new source.
func TestRetiredGraphSaveIsNotPublished(t *testing.T) {
	svc, dir := persistedGraphService(t)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	hnswPath := filepath.Join(dir, "hnsw")
	_, err := os.Stat(hnswPath)
	require.NoError(t, err, "precondition: the build saved its graph")

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	holdPublish := func(s *Service, path string) {
		if s == svc && path == hnswPath {
			once.Do(func() { close(entered); <-release })
		}
	}
	artifactPublishHook.Store(&holdPublish)
	t.Cleanup(func() { artifactPublishHook.Store(nil) })

	saved := make(chan struct{})
	go func() { svc.persistHNSWBackground(hnswPath); close(saved) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the graph save did not reach publication")
	}
	svc.ClearVectorIndex()
	_, err = os.Stat(hnswPath)
	require.True(t, os.IsNotExist(err), "Clear removes the saved graph")
	close(release)
	select {
	case <-saved:
	case <-time.After(10 * time.Second):
		t.Fatal("the graph save did not finish")
	}
	_, err = os.Stat(hnswPath)
	require.True(t, os.IsNotExist(err), "a graph saved from the retired source was published after Clear")
}

// sol r2 finding 2: a build cancelled during its final persistence returns
// the cancellation and does not publish readiness.
func TestBuildCancelledDuringFinalPersistence(t *testing.T) {
	svc, _ := persistedGraphService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancelAtFinalPersist := func(s *Service) {
		if s == svc {
			cancel()
		}
	}
	finalPersistHook.Store(&cancelAtFinalPersist)
	t.Cleanup(func() { finalPersistHook.Store(nil) })
	require.ErrorIs(t, svc.BuildIndexes(ctx), context.Canceled)
	finalPersistHook.Store(nil)
	require.False(t, svc.IsReady(), "readiness was published by a cancelled build")
	require.False(t, svc.BuildInProgress())
	_, err := svc.Search(context.Background(), "", []float32{1, 0, 0}, DefaultSearchOptions())
	require.ErrorIs(t, err, ErrSearchIndexBuilding)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.True(t, svc.IsReady())
}

// sol r2 finding 3: with a saved vector file that failed staging, replaying a
// recorded vector-bearing write must not load that file under indexMu, and
// writes during the population still return at once.
func TestFailedStagingReplayDoesNotLoadUnderWriteLock(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			dir := t.TempDir()
			seed := configureNodeAdmissionService(t, engine, dir, backend, nil)
			require.NoError(t, seed.BuildIndexes(context.Background()))
			seed.PersistIndexesToDisk()
			require.NoError(t, seed.Close())
			require.NoError(t, os.WriteFile(filepath.Join(dir, "vectors.vec"), []byte("not a vector file"), 0o600))

			svc := configureNodeAdmissionService(t, engine, dir, backend, nil)
			var lockedLoads atomic.Int32
			count := func(s *Service) {
				if s == svc {
					lockedLoads.Add(1)
				}
			}
			vectorStoreLockedLoadHook.Store(&count)
			entered, release := make(chan struct{}), make(chan struct{})
			holdStage := func(s *Service) {
				if s == svc {
					close(entered)
					<-release
				}
			}
			populationStageHook.Store(&holdStage)
			t.Cleanup(func() {
				vectorStoreLockedLoadHook.Store(nil)
				populationStageHook.Store(nil)
			})

			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err = engine.CreateNode(b)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(b)) // recorded; starts the population
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the population did not reach staging")
			}
			populationStageHook.Store(nil)
			c := admissionNode("nornic:c", "cterm", []float32{0, 0, 1}, "named")
			_, err = engine.CreateNode(c)
			require.NoError(t, err)
			wrote := make(chan error, 1)
			go func() { wrote <- svc.IndexNode(c) }()
			select {
			case err := <-wrote:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("a live write waited during the population")
			}
			close(release)
			require.Eventually(t, func() bool { return svc.pendingNodeAdmission() == nil }, 10*time.Second, 10*time.Millisecond)

			require.Zero(t, lockedLoads.Load(), "replay loaded the failed saved vector file under indexMu")
			require.Contains(t, vectorHitNodeIDs(t, svc, []float32{0, 1, 0}), "nornic:b")
			require.Contains(t, vectorHitNodeIDs(t, svc, []float32{0, 0, 1}), "nornic:c")
		})
	}
}

// sol r2 test gap: a search that starts before a source step and reads while
// the step has withdrawn the recorded writes must still return them, and the
// cache must not serve an answer computed inside the step.
func TestSearchStartedBeforeSourceStepSeesRecordedWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	svc, engine := persistedRestartService(t, "v2")
	stageEntered, stageRelease := make(chan struct{}), make(chan struct{})
	holdStage := func(s *Service) {
		if s == svc {
			close(stageEntered)
			<-stageRelease
		}
	}
	populationStageHook.Store(&holdStage)
	searchEntered, searchRelease := make(chan struct{}), make(chan struct{})
	var searchOnce sync.Once
	holdSearch := func(s *Service) {
		if s == svc {
			searchOnce.Do(func() { close(searchEntered); <-searchRelease })
		}
	}
	searchReadHook.Store(&holdSearch)
	releaseSearchInStep := func(s *Service) {
		if s == svc {
			close(searchRelease)
			time.Sleep(100 * time.Millisecond) // the search reads inside the step
		}
	}
	populationRestoreHook.Store(&releaseSearchInStep)
	t.Cleanup(func() {
		populationStageHook.Store(nil)
		searchReadHook.Store(nil)
		populationRestoreHook.Store(nil)
	})

	b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
	_, err := engine.CreateNode(b)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(b)) // recorded; the population waits at staging
	select {
	case <-stageEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the population did not reach staging")
	}
	populationStageHook.Store(nil)
	opts := DefaultSearchOptions()
	opts.Limit = 10
	result := make(chan []string, 1)
	go func() {
		resp, err := svc.Search(context.Background(), "", []float32{0, 1, 0}, opts) // cache-enabled vector query
		var ids []string
		if err == nil {
			for _, r := range resp.Results {
				ids = append(ids, r.ID)
			}
		}
		result <- ids
	}()
	select {
	case <-searchEntered: // the search began before the step
	case <-time.After(10 * time.Second):
		t.Fatal("the search did not start")
	}
	close(stageRelease)
	var during []string
	select {
	case during = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("the search did not finish")
	}
	require.Contains(t, during, "nornic:b", "a search spanning the step returned an answer read inside it")
	require.Eventually(t, func() bool { return svc.pendingNodeAdmission() == nil }, 10*time.Second, 10*time.Millisecond)
	again, err := svc.Search(context.Background(), "", []float32{0, 1, 0}, opts)
	require.NoError(t, err)
	var ids []string
	for _, r := range again.Results {
		ids = append(ids, r.ID)
	}
	require.Contains(t, ids, "nornic:b", "the cache served an answer computed inside the step")
}
