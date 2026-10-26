package server

import (
	"context"
	"encoding/json"
	"net/http"
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

// readinessSnapshotEngine retains a real captured storage view and cancels
// only when a rebuild iterates it, after readiness has been invalidated. The
// capability probe opens/closes a view without consuming this one-shot hook.
type readinessSnapshotEngine struct {
	storage.Engine
	mu            sync.Mutex
	beforeIterate func()
}

func (e *readinessSnapshotEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	view, err := storage.OpenReadSnapshot(ctx, e.Engine)
	if err != nil {
		return nil, err
	}
	return &readinessSnapshotView{ReadSnapshotView: view, owner: e}, nil
}

type readinessSnapshotView struct {
	storage.ReadSnapshotView
	owner *readinessSnapshotEngine
}

func (v *readinessSnapshotView) IterateNodes(visit func(*storage.Node) bool) error {
	v.owner.mu.Lock()
	hook := v.owner.beforeIterate
	v.owner.beforeIterate = nil
	v.owner.mu.Unlock()
	if hook != nil {
		hook()
	}
	return v.ReadSnapshotView.IterateNodes(visit)
}

func TestHandleSearch_ReadinessChangesAfterInitialCheck(t *testing.T) {
	server, _ := setupTestServer(t)
	server.auth = nil
	const database = "readiness_race"
	require.NoError(t, server.dbManager.CreateDatabase(database))
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	// Own the service's decorated reader before producer events from the seed
	// can create and cache a service with the undecorated manager view.
	buildEngine := &readinessSnapshotEngine{Engine: engine}
	service, err := server.db.GetOrCreateSearchService(database, buildEngine)
	require.NoError(t, err)
	vector := make([]float32, 1024)
	vector[0] = 1
	_, err = engine.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}, ChunkEmbeddings: [][]float32{vector}})
	require.NoError(t, err)
	// Wait for the DB owner's initial build; starting a second BuildIndexes
	// here would itself race the asynchronous initial build.
	require.Eventually(t, service.IsReady, 10*time.Second, 10*time.Millisecond)
	service.SetPersistenceEnabled(false)
	rebuildResult := make(chan error, 1)
	enteredIteration := make(chan struct{})
	server.db.SetEmbedder(&readinessChangingEmbedder{
		countingEmbedder: countingEmbedder{dims: 1024},
		beforeEmbed: func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			buildEngine.mu.Lock()
			buildEngine.beforeIterate = func() { cancel(); close(enteredIteration) }
			buildEngine.mu.Unlock()
			defer func() {
				buildEngine.mu.Lock()
				buildEngine.beforeIterate = nil
				buildEngine.mu.Unlock()
			}()
			// Cancel during the captured baseline read, not before snapshot
			// admission, which may correctly leave the existing index ready.
			rebuildResult <- service.BuildIndexes(ctx)
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
	select {
	case <-enteredIteration:
	default:
		t.Fatal("the canceled rebuild did not enter its captured baseline")
	}
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
