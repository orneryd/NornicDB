package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

// A local Badger rollback releases its registered MVCC snapshot synchronously.
// Hold that registry-cleanup lock: this exercises the real storage rollback,
// not a substituted Fabric participant callback or a sleeping query.
type idleSnapshotLock struct {
	pressureWarningLifecycleController
	mu       sync.Mutex
	entered  chan struct{}
	finished chan struct{}
}

func (c *idleSnapshotLock) AcquireSnapshotReader(info storage.SnapshotReaderInfo) (func(), error) {
	if info.Namespace != "idleblocked" {
		return func() {}, nil
	}
	return func() {
		close(c.entered)
		c.mu.Lock()
		c.mu.Unlock()
		close(c.finished)
	}, nil
}

func TestFabricHTTPIdleLocalStorageCleanupBound(t *testing.T) {
	server, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	require.NoError(t, server.dbManager.CreateDatabase("idleblocked"))
	leaf, err := server.dbManager.GetStorage("idleblocked")
	require.NoError(t, err)
	_, err = leaf.CreateNode(&storage.Node{ID: "seed", Labels: []string{"IdleProbe"}})
	require.NoError(t, err)
	require.NoError(t, server.dbManager.CreateCompositeDatabase("idlecomp", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "idleblocked", Type: "local", AccessMode: "read_write"}}))
	setter, ok := unwrapInnerStorage(leaf).(interface {
		SetLifecycleController(storage.MVCCLifecycleController)
	})
	require.True(t, ok)
	controller := &idleSnapshotLock{entered: make(chan struct{}), finished: make(chan struct{})}
	setter.SetLifecycleController(controller)
	defer setter.SetLifecycleController(nil)
	controller.mu.Lock()
	var release sync.Once
	defer release.Do(controller.mu.Unlock)
	server.txSessions = txsession.NewManager(500*time.Millisecond, server.newExecutorForDatabase)
	server.txSessions.SetLifetimeContext(server.requestsCtx)
	router := server.buildRouter()
	request := func(path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		r.Header.Set("Authorization", token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	begin := request("/db/idlecomp/tx", `{"statements":[{"statement":"USE idlecomp.l MATCH (n:IdleProbe) RETURN count(n) AS c"}]}`)
	require.Equal(t, http.StatusCreated, begin.Code, begin.Body.String())
	var opened TransactionResponse
	require.NoError(t, json.Unmarshal(begin.Body.Bytes(), &opened))
	require.Empty(t, opened.Errors)
	require.NotEmpty(t, opened.Commit)
	select {
	case <-controller.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("idle rollback did not reach the local snapshot-registry cleanup")
	}
	started := time.Now()
	// Measure unrelated HTTP BEGIN and COMMIT while local cleanup is blocked.
	other := make(chan *httptest.ResponseRecorder, 1)
	go func() { other <- request("/db/nornic/tx", `{"statements":[]}`) }()
	select {
	case response := <-other:
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		var tx TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &tx))
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- request(tx.Commit, `{"statements":[]}`) }()
		select {
		case response := <-done:
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			t.Logf("unrelated HTTP BEGIN/COMMIT completed in %s while local rollback lock held", time.Since(started))
		case <-time.After(2 * time.Second):
			t.Error("unrelated COMMIT stalled behind local idle cleanup")
		}
	case <-time.After(2 * time.Second):
		t.Error("unrelated BEGIN stalled behind local idle cleanup")
	}
	retired := make(chan *httptest.ResponseRecorder, 1)
	go func() { retired <- request(strings.TrimSuffix(opened.Commit, "/commit"), `{"statements":[]}`) }()
	select {
	case response := <-retired:
		require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
		t.Logf("idle session retired in %s with local rollback still blocked", time.Since(started))
	case <-time.After(32 * time.Second):
		t.Error("idle cleanup exceeded its advertised 30-second terminal bound")
	}
	release.Do(controller.mu.Unlock)
	select {
	case <-controller.finished:
	case <-time.After(5 * time.Second):
		t.Error("local rollback did not finish after storage lock was released")
	}
}
