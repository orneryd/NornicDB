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

// fabricLocalSnapshotFixture captures the shared native construction of a
// Fabric composite database and its local leaf, authentication, and an idle
// snapshot lifecycle controller. The
// constructor runs every original setup step (test server, token, database,
// leaf, seed, composite, lifecycle setter resolution, controller allocation)
// but deliberately does not install the controller or take the lock; the
// caller installs the controller and takes the lock so deferred lifetime
// ownership stays visible at the call site.
type fabricLocalSnapshotFixture struct {
	server *Server
	leaf   storage.Engine
	token  string
	setter interface {
		SetLifecycleController(storage.MVCCLifecycleController)
	}
	controller *idleSnapshotLock
	release    sync.Once
}

// newFabricLocalSnapshotFixture performs the original shared native
// construction. It does not install the controller and does not lock, so the
// failing-require ownership expectations at each test site remain unchanged.
func newFabricLocalSnapshotFixture(
	t *testing.T,
	label string,
	composite string,
) *fabricLocalSnapshotFixture {
	t.Helper()
	server, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "idleblocked"}))
	leaf, err := server.dbManager.GetStorage("idleblocked")
	require.NoError(t, err)
	_, err = leaf.CreateNode(&storage.Node{ID: "seed", Labels: []string{label}})
	require.NoError(t, err)
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), composite, []multidb.ConstituentRef{{Alias: "l", DatabaseName: "idleblocked", Type: "local", AccessMode: "read_write"}}))
	setter, ok := unwrapInnerStorage(leaf).(interface {
		SetLifecycleController(storage.MVCCLifecycleController)
	})
	require.True(t, ok)
	controller := &idleSnapshotLock{entered: make(chan struct{}), finished: make(chan struct{})}
	return &fabricLocalSnapshotFixture{
		server:     server,
		leaf:       leaf,
		token:      token,
		setter:     setter,
		controller: controller,
	}
}

// requestFor runs the original session-manager lifetime/context setup and
// router construction (once per fixture), then returns the original HTTP
// request closure bound to the fixture server and token.
func (f *fabricLocalSnapshotFixture) requestFor(ttl time.Duration) func(path, body string) *httptest.ResponseRecorder {
	f.server.txSessions = txsession.NewManager(ttl, f.server.newExecutorForDatabase)
	f.server.txSessions.SetLifetimeContext(f.server.requestsCtx)
	router := f.server.buildRouter()
	return func(path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		r.Header.Set("Authorization", f.token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
}

func TestFabricHTTPIdleLocalStorageCleanupBound(t *testing.T) {
	f := newFabricLocalSnapshotFixture(t, "IdleProbe", "idlecomp")
	controller := f.controller
	setter := f.setter
	setter.SetLifecycleController(controller)
	defer setter.SetLifecycleController(nil)
	controller.mu.Lock()
	release := &f.release
	defer release.Do(controller.mu.Unlock)
	request := f.requestFor(500 * time.Millisecond)
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

func TestFabricHTTPCommitWaitsForLocalStorageOutcome(t *testing.T) {
	f := newFabricLocalSnapshotFixture(t, "Seed", "commitcomp")
	leaf := f.leaf
	controller := f.controller
	setter := f.setter
	setter.SetLifecycleController(controller)
	defer setter.SetLifecycleController(nil)
	controller.mu.Lock()
	release := &f.release
	defer release.Do(controller.mu.Unlock)
	request := f.requestFor(time.Minute)
	begin := request("/db/commitcomp/tx", `{"statements":[{"statement":"USE commitcomp.l CREATE (:CommitOutcome {id:'late'}) RETURN 7 AS k"}]}`)
	require.Equal(t, http.StatusCreated, begin.Code, begin.Body.String())
	var opened TransactionResponse
	require.NoError(t, json.Unmarshal(begin.Body.Bytes(), &opened))
	require.Empty(t, opened.Errors)
	committed := make(chan *httptest.ResponseRecorder, 1)
	go func() { committed <- request(opened.Commit, `{"statements":[]}`) }()
	select {
	case <-controller.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("COMMIT did not reach local storage snapshot release")
	}
	started := time.Now()
	var response *httptest.ResponseRecorder
	select {
	case response = <-committed:
		t.Errorf("COMMIT responded before its storage outcome at %s: %s", time.Since(started), response.Body.String())
	case <-time.After(32 * time.Second):
		t.Log("COMMIT still waits for the real outcome beyond the idle cleanup bound")
	}
	release.Do(controller.mu.Unlock)
	if response == nil {
		select {
		case response = <-committed:
		case <-time.After(5 * time.Second):
			t.Fatal("COMMIT did not return after storage unblocked")
		}
	}
	var result TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	if len(result.Errors) != 0 {
		t.Errorf("COMMIT returned error despite its eventual committed write: %v", result.Errors)
	}
	require.Eventually(t, func() bool { nodes, err := leaf.GetNodesByLabel("CommitOutcome"); return err == nil && len(nodes) == 1 }, 5*time.Second, 10*time.Millisecond)
	t.Logf("local write committed; HTTP outcome returned after %s", time.Since(started))
}
