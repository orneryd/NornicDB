package search

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// traversalGatedEngine holds the next node traversal once armed, the point a
// NODE population reaches after it has replaced the node sources.
type traversalGatedEngine struct {
	*storage.MemoryEngine
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
}

func (e *traversalGatedEngine) arm() (entered chan struct{}, release func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entered, e.release = make(chan struct{}), make(chan struct{})
	ch := e.release
	var once sync.Once
	return e.entered, func() { once.Do(func() { close(ch) }) }
}

func (e *traversalGatedEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.mu.Lock()
	entered, release := e.entered, e.release
	e.entered, e.release = nil, nil
	e.mu.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}
	nodes, err := e.MemoryEngine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if !fn(node) {
			break
		}
	}
	return nil
}

// A write to a persisted service whose initial population was never admitted
// must not run that population inline (F-I86 B1): with no build owning the
// base lock, the old boundary ran the whole traversal on the writer.
func TestLiveWriteNeverRunsInitialPopulationInline(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := &traversalGatedEngine{MemoryEngine: storage.NewMemoryEngine()}
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
			_, err := engine.CreateNode(a)
			require.NoError(t, err)
			svc := configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
			entered, release := engine.arm()
			defer release()

			b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
			_, err = engine.CreateNode(b)
			require.NoError(t, err)
			writeDone := make(chan error, 1)
			go func() { writeDone <- svc.IndexNode(b) }()
			select {
			case err := <-writeDone:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("a live write ran or waited for the initial NODE population")
			}

			// The population runs as the owner's background worker.
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the write did not start the initial population in the background")
			}
			require.False(t, svc.IsReady())
			deleteDone := make(chan error, 1)
			go func() { deleteDone <- svc.RemoveNode(a.ID) }()
			select {
			case err := <-deleteDone:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("a live delete waited for the held population")
			}
			require.NoError(t, engine.DeleteNode(a.ID))
			release()

			// The population folds both writes in: b is found, a is gone.
			require.Eventually(t, func() bool {
				resp, err := svc.Search(context.Background(), "bterm", nil, &SearchOptions{Limit: 10})
				return err == nil && len(resp.Results) == 1 && resp.Results[0].ID == "nornic:b"
			}, 10*time.Second, 20*time.Millisecond)
			resp, err := svc.Search(context.Background(), "aterm", nil, &SearchOptions{Limit: 10})
			require.NoError(t, err)
			require.Empty(t, resp.Results)
		})
	}
}

// Close cancels and joins an admission worker started by a write, so the
// held population cannot outlive the service.
func TestCloseJoinsWriteStartedPopulation(t *testing.T) {
	engine := &traversalGatedEngine{MemoryEngine: storage.NewMemoryEngine()}
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	dir := t.TempDir()
	svc.FixVectorDimensions()
	svc.SetFulltextIndexPath(dir + "/bm25")
	svc.SetVectorIndexPath(dir + "/vectors")
	svc.SetHNSWIndexPath(dir + "/hnsw")
	svc.SetPersistenceEnabled(true)
	entered, release := engine.arm()
	defer release()

	b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
	require.NoError(t, svc.IndexNode(b))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the write did not start the initial population")
	}
	closed := make(chan error, 1)
	go func() { closed <- svc.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while the population worker was still running")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join the population worker")
	}
	require.Equal(t, lifecycleClosed, svc.lifecycleState())
	_, _, ok := svc.runWorker(context.Background(), workerAdmission)
	require.False(t, ok, "a closed service admitted a worker")
}
