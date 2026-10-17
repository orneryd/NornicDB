package server

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type readinessChangingEmbedder struct {
	countingEmbedder
	once        sync.Once
	beforeEmbed func()
}

func (e *readinessChangingEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if text == "needle" {
		e.once.Do(e.beforeEmbed)
	}
	return e.countingEmbedder.Embed(ctx, text)
}

// iterationGatedEngine holds the next base population inside its storage
// traversal once armed. The population clears readiness before it iterates,
// so a held rebuild is a deterministic "sources replaced, not ready" point.
type iterationGatedEngine struct {
	storage.Engine
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
}

func (e *iterationGatedEngine) arm() (entered, release chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entered, e.release = make(chan struct{}), make(chan struct{})
	return e.entered, e.release
}

func (e *iterationGatedEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.mu.Lock()
	entered, release := e.entered, e.release
	e.entered, e.release = nil, nil
	e.mu.Unlock()
	if entered != nil {
		close(entered)
		<-release
	}
	if iterator, ok := e.Engine.(interface {
		IterateNodes(func(*storage.Node) bool) error
	}); ok {
		return iterator.IterateNodes(fn)
	}
	nodes, err := e.Engine.AllNodes()
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

func TestHandleSearch_ReadinessChangesAfterInitialCheck(t *testing.T) {
	server, _ := setupTestServer(t)
	server.auth = nil
	const database = "readiness_race"
	require.NoError(t, server.dbManager.CreateDatabase(database))
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	vector := make([]float32, 1024)
	vector[0] = 1
	_, err = engine.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}, ChunkEmbeddings: [][]float32{vector}})
	require.NoError(t, err)
	gated := &iterationGatedEngine{Engine: engine}
	// The database may already own a service built over its plain engine;
	// replace it so the next service traverses through the gate.
	server.db.ResetSearchService(database)
	service, err := server.db.GetOrCreateSearchService(database, gated)
	require.NoError(t, err)
	// Only database creation starts a build, so build the replacement here
	// (BuildIndexes serialises with any build the database started).
	require.NoError(t, service.BuildIndexes(context.Background()))
	require.True(t, service.IsReady())
	rebuildResult := make(chan error, 1)
	server.db.SetEmbedder(&readinessChangingEmbedder{
		countingEmbedder: countingEmbedder{dims: 1024},
		beforeEmbed: func() {
			// A rebuild cancelled after it replaced the node sources leaves this
			// service unavailable. (One cancelled before it starts changes
			// nothing, and the complete indexes keep answering.) Doing this in
			// query embedding deterministically crosses the handler's first gate.
			entered, release := gated.arm()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- service.BuildIndexes(ctx) }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Error("rebuild did not reach its node traversal")
			}
			if service.IsReady() {
				t.Error("a rebuild replacing the node sources must clear readiness")
			}
			cancel()
			close(release)
			rebuildResult <- <-done
		},
	})
	body := map[string]interface{}{"database": database, "query": "needle", "limit": 1}
	response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "")
	select {
	case err := <-rebuildResult:
		require.ErrorIs(t, err, context.Canceled)
	default:
		t.Fatal("query embedding did not exercise the readiness transition")
	}
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
	var unavailable map[string]interface{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &unavailable))
	require.Equal(t, "search_not_ready", unavailable["request_status"])
	require.Equal(t, database, unavailable["database"])
	require.Equal(t, true, unavailable["retryable"])
	status, ok := unavailable["search_status"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, false, status["ready"])
	require.NoError(t, service.BuildIndexes(context.Background()))
	response = makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
}

// A rebuild cancelled before it starts replaces nothing, so search keeps
// answering from the complete indexes.
func TestHandleSearch_CancelledRebuildBeforeStartKeepsServing(t *testing.T) {
	server, _ := setupTestServer(t)
	server.auth = nil
	const database = "readiness_cancel_early"
	require.NoError(t, server.dbManager.CreateDatabase(database))
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	vector := make([]float32, 1024)
	vector[0] = 1
	_, err = engine.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}, ChunkEmbeddings: [][]float32{vector}})
	require.NoError(t, err)
	service, err := server.db.GetOrCreateSearchService(database, engine)
	require.NoError(t, err)
	require.Eventually(t, service.IsReady, 10*time.Second, 10*time.Millisecond)
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, service.BuildIndexes(cancelled), context.Canceled)
	require.True(t, service.IsReady())
	body := map[string]interface{}{"database": database, "query": "needle", "limit": 1}
	response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "seed", "the complete indexes still return the seed")
}

// A write to a persisted search service that has never been built starts its
// population in the background (F-I86 decision B). HTTP search gates on the
// database's readiness, which only a completed build establishes, so while
// that population is held the handler answers the documented retryable 503;
// once the database's build returns, the write is found with no further wait.
func TestHandleSearch_WriteDuringHeldPopulationIsFoundOnceReady(t *testing.T) {
	server, _ := setupTestServer(t)
	server.auth = nil
	const database = "write_during_population"
	require.NoError(t, server.dbManager.CreateDatabase(database))
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	gated := &iterationGatedEngine{Engine: engine}
	server.db.ResetSearchService(database)
	service, err := server.db.GetOrCreateSearchService(database, gated)
	require.NoError(t, err)
	dir := t.TempDir()
	service.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	service.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	service.SetPersistenceEnabled(true)
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})

	entered, release := gated.arm()
	released := false
	releaseOnce := func() {
		if !released {
			released = true
			close(release)
		}
	}
	defer releaseOnce()
	node := &storage.Node{ID: "written", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "zephyrine"}}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, service.IndexNode(node))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the write did not start the population")
	}

	body := map[string]interface{}{"database": database, "query": "zephyrine", "limit": 1}
	response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "")
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
	var unavailable map[string]interface{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &unavailable))
	require.Equal(t, "search_not_ready", unavailable["request_status"])
	require.Equal(t, true, unavailable["retryable"])

	releaseOnce()
	require.NoError(t, service.BuildIndexes(context.Background()))
	response = makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "written")
}
