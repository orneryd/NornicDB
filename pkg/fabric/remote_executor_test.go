package fabric

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCacheKeyIncludesAuthContext(t *testing.T) {
	loc := &LocationRemote{URI: "http://remote.example", DBName: "tenant", AuthMode: "oidc_forwarding"}

	k1 := cacheKey(loc, "Bearer token-a")
	k2 := cacheKey(loc, "Bearer token-b")
	if k1 == k2 {
		t.Fatalf("expected distinct cache keys for different forwarded auth tokens")
	}

	userPassLoc := &LocationRemote{URI: "http://remote.example", DBName: "tenant", AuthMode: "user_password", User: "alice", Password: "secret"}
	k3 := cacheKey(userPassLoc, "Bearer ignored")
	k4 := cacheKey(userPassLoc, "Bearer also-ignored")
	if k3 != k4 {
		t.Fatalf("expected user/password auth cache key to ignore forwarded token")
	}
}

func TestRemoteFragmentExecutorCacheIsConcurrentAndAuthIsolated(t *testing.T) {
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: "http://remote.example", DBName: "tenant", AuthMode: "oidc_forwarding"}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if _, err := re.getOrCreateEngine(loc, "Bearer same-token"); err != nil {
				t.Errorf("getOrCreateEngine failed: %v", err)
			}
		}()
	}
	wg.Wait()

	re.mu.RLock()
	if got := len(re.engineCache); got != 1 {
		re.mu.RUnlock()
		t.Fatalf("expected single cached engine for same auth context, got %d", got)
	}
	re.mu.RUnlock()

	if _, err := re.getOrCreateEngine(loc, "Bearer other-token"); err != nil {
		t.Fatalf("getOrCreateEngine failed for second auth context: %v", err)
	}

	re.mu.RLock()
	if got := len(re.engineCache); got != 2 {
		re.mu.RUnlock()
		t.Fatalf("expected auth-isolated cache entries, got %d", got)
	}
	re.mu.RUnlock()
}

func TestRemoteFragmentExecutor_ExplicitTxHandleLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name, transactionID, pathID string
		commit                      bool
	}{
		{"commit", "tx-remote-commit", "1", true},
		{"rollback", "tx-remote-rollback", "2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var openCount, execCount, commitCount, rollbackCount atomic.Int32
			path := "/db/tenant/tx/" + tc.pathID
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				response := map[string]any{"results": []any{}, "errors": []any{}}
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/db/tenant/tx":
					openCount.Add(1)
					response["commit"] = "http://" + r.Host + path + "/commit"
				case r.Method == http.MethodPost && r.URL.Path == path:
					execCount.Add(1)
					response["results"] = []any{map[string]any{"columns": []string{"n"}, "data": []any{map[string]any{"row": []any{1}}}}}
				case r.Method == http.MethodPost && r.URL.Path == path+"/commit":
					commitCount.Add(1)
				case r.Method == http.MethodDelete && r.URL.Path == path:
					rollbackCount.Add(1)
				default:
					http.NotFound(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer srv.Close()
			re := NewRemoteFragmentExecutor()
			defer func() { _ = re.Close() }()
			loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "oidc_forwarding"}
			tx := NewFabricTransaction(tc.transactionID)
			if _, err := re.Execute(participantContext(t, tx, loc, true), loc, "RETURN 1 AS n", nil, "Bearer tok", storage.RemoteAccessRead); err != nil {
				t.Fatalf("remote execute failed: %v", err)
			}
			wantCommit, wantRollback := int32(0), int32(1)
			var err error
			if tc.commit {
				err = tx.Commit(nil, nil)
				wantCommit, wantRollback = 1, 0
			} else {
				err = tx.Rollback(nil)
			}
			if err != nil {
				t.Fatalf("fabric finish failed: %v", err)
			}
			if openCount.Load() != 1 || execCount.Load() != 1 || commitCount.Load() != wantCommit || rollbackCount.Load() != wantRollback {
				t.Fatalf("unexpected lifecycle counts open=%d exec=%d commit=%d rollback=%d", openCount.Load(), execCount.Load(), commitCount.Load(), rollbackCount.Load())
			}
		})
	}
}

func TestRemoteFragmentExecutor_UserPasswordAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{
				map[string]any{
					"columns": []string{"x"},
					"data":    []any{map[string]any{"row": []any{1}}},
				},
			},
			"errors": []any{},
		})
	}))
	defer srv.Close()

	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := &LocationRemote{URI: srv.URL, DBName: "tenant", AuthMode: "user_password", User: "alice", Password: "secret"}
	res, err := re.Execute(context.Background(), loc, "RETURN 1 AS x", nil, "ignored-token", storage.RemoteAccessRead)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
}

func TestRemoteExecutionFailureBoundaries(t *testing.T) {
	type testcase struct {
		name          string
		useServer     bool
		uri           string
		dbName        string
		authMode      string
		query         string
		wantSubstring string
		errorMessage  string
	}

	cases := []testcase{
		{
			name:         "connection error",
			errorMessage: "expected connection error",
			useServer:    false,
			uri:          "http://127.0.0.1:1",
			dbName:       "db",
			authMode:     "",
			query:        "RETURN 1",
		},
		{
			name:          "query error",
			errorMessage:  "expected error from failed remote query",
			useServer:     true,
			dbName:        "tenant",
			authMode:      "oidc_forwarding",
			query:         "INVALID",
			wantSubstring: "remote execution",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uri := tc.uri
			if tc.useServer {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_ = json.NewEncoder(w).Encode(map[string]any{
						"results": []any{},
						"errors":  []any{map[string]any{"message": "query failed"}},
					})
				}))
				defer srv.Close()
				uri = srv.URL
			}

			re := NewRemoteFragmentExecutor()
			defer func() { _ = re.Close() }()

			loc := &LocationRemote{URI: uri, DBName: tc.dbName, AuthMode: tc.authMode}
			_, err := re.Execute(context.Background(), loc, tc.query, nil, "tok", storage.RemoteAccessRead)
			if err == nil {
				t.Fatal(tc.errorMessage)
			}
			if tc.wantSubstring != "" && !strings.Contains(err.Error(), tc.wantSubstring) {
				t.Fatalf("expected 'remote execution' in error, got: %v", err)
			}
		})
	}
}

// Both real HTTP and failed cleanup cases share one Close lifecycle owner:
// every participant/engine ends, every failure survives, and Close is idempotent.
func TestRemoteFragmentExecutor_CloseWithTxHandles(t *testing.T) {
	for _, fails := range []bool{false, true} {
		name := "clean_cleanup"
		if fails {
			name = "retained_cleanup_failures"
		}
		t.Run(name, func(t *testing.T) {
			re := NewRemoteFragmentExecutor()
			var handleErr, engineErr error
			var verifyCompletion func()
			if fails {
				handleErr = errors.New("remote tx handle rollback failed")
				engineErr = errors.New("remote engine close failed")
				handle := &countingRemoteTx{rollbackErr: handleErr}
				engineCloses := 0
				remote := newRemoteQueryEngineFixture(t, &LocationRemote{URI: "bolt://remote.invalid:7687", DBName: "nornic", AuthMode: "oidc_forwarding"}, &cleanupFailRemoteEngine{count: &engineCloses, closeErr: engineErr})
				re.txHandles["handle"] = &remoteParticipant{tx: handle}
				key := remote.install(t, re)
				verifyCompletion = func() {
					require.Equal(t, 1, handle.rollbacks)
					require.Equal(t, 1, engineCloses)
					re.mu.RLock()
					_, cached := re.engineCache[key]
					re.mu.RUnlock()
					require.False(t, cached, "Close released the exact cached engine")
				}
			} else {
				server := newRemoteTxServer(t, "tenant", "99", 1)
				loc := server.location("tenant")
				tx := NewFabricTransaction("tx-close-test")
				result, err := re.Execute(participantContext(t, tx, loc, true), loc, "RETURN 1 AS x", nil, "tok", storage.RemoteAccessRead)
				require.NoError(t, err)
				require.Equal(t, 1, result.RowCount(), "the actual statement returned its row")
			}
			err := re.Close()
			if fails {
				require.Error(t, err)
				require.ErrorIs(t, err, handleErr, "the participant's failure survives")
				require.ErrorIs(t, err, engineErr, "the engine's failure survives")
			} else {
				require.NoError(t, err)
			}
			if verifyCompletion != nil {
				verifyCompletion()
			}
			require.Empty(t, re.txHandles)
			require.Empty(t, re.engineCache)
			require.NoError(t, re.Close(), "completed cleanup is not repeated")
			if verifyCompletion != nil {
				verifyCompletion()
			}
		})
	}
}

// A statement whose context has no fabric transaction cannot bind its remote
// participant, so it fails naming the missing context and releases the handle
// it opened.
func TestRemoteFragmentExecutor_GetOrCreateTxHandle_MissingFabricTx(t *testing.T) {
	server := newRemoteTxServer(t, "tenant", "5", nil)
	re := NewRemoteFragmentExecutor()
	defer func() { _ = re.Close() }()

	loc := server.location("tenant")
	// Context with SubTransaction but WITHOUT FabricTransaction → should error.
	ctx := WithSubTransaction(context.Background(), &SubTransaction{ShardName: "test-shard", State: "open"})
	_, err := re.Execute(ctx, loc, "RETURN 1 AS x", nil, "tok", storage.RemoteAccessRead)
	if err == nil {
		t.Fatal("expected error for missing fabric transaction context")
	}
	if !strings.Contains(err.Error(), "fabric transaction context is missing") {
		t.Fatalf("expected missing fabric tx context error, got: %v", err)
	}
	if len(re.txHandles) != 0 {
		t.Fatalf("the unbound participant kept a handle: %v", re.txHandles)
	}
}

func TestRemoteEngineCachePreservesCredentialTuple(t *testing.T) {
	re := NewRemoteFragmentExecutor()
	defer re.Close()
	pairs := [][2]string{{"service", "password"}, {" service ", "password"}, {"service", " password "}, {"a:b", "c"}, {"a", "b:c"}}
	seen := make(map[remoteCypherEngine]bool)
	for _, pair := range pairs {
		loc := &LocationRemote{URI: "http://127.0.0.1:1", DBName: "tenant", AuthMode: "user_password", User: pair[0], Password: pair[1]}
		engine, err := re.getOrCreateEngine(loc, "ignored")
		if err != nil {
			t.Fatal("engine construction failed")
		}
		if seen[engine] {
			t.Fatal("distinct admitted credential tuple reused an engine")
		}
		seen[engine] = true
		repeated, err := re.getOrCreateEngine(loc, "also-ignored")
		if err != nil || repeated != engine {
			t.Fatal("identical credential tuple failed to reuse engine")
		}
	}
}

func TestRemoteEngineCacheIncludesOpaqueCredentialRevision(t *testing.T) {
	first := &LocationRemote{GraphName: "composite.r", URI: "http://example.invalid", AuthMode: "user_password", CredentialRevision: "ciphertext-revision-one"}
	second := *first
	second.CredentialRevision = "ciphertext-revision-two"
	if cacheKey(first, "") == cacheKey(&second, "") {
		t.Fatal("different stored connection revisions collapsed")
	}
	if cacheKey(first, "ignored") != cacheKey(first, "also-ignored") {
		t.Fatal("stored explicit identity changed with forwarded token")
	}
}
