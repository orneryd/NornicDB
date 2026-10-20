package server

import (
	"bytes"
	"context"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSolReview1483R3HTTPIdleRetiresSession(t *testing.T) {
	for _, transport := range []string{"http", "bolt"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			remote, remoteAuth := setupTestServer(t)
			remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
			router := remote.buildRouter()
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Header.Set("Authorization", remoteToken)
				router.ServeHTTP(w, r)
			}))
			defer peer.Close()
			uri := peer.URL
			if transport == "bolt" {
				uri = fabricLifetimeBolt(t, remote)
			}
			coordinator, _ := setupTestServer(t)
			require.NoError(t, coordinator.dbManager.CreateCompositeDatabase("r3expired", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: uri}}))
			manager := txsession.NewManager(60*time.Millisecond, coordinator.newExecutorForDatabase)
			manager.SetLifetimeContext(coordinator.requestsCtx)
			session, err := manager.Open(ctx, "r3expired")
			require.NoError(t, err)
			_, err = manager.ExecuteInSession(ctx, session, "USE r3expired.r CREATE (:R3Expired {k:7}) RETURN 7 AS k", nil)
			require.NoError(t, err)
			time.Sleep(120 * time.Millisecond)
			_, retained := manager.Get(session.ID)
			result, executeErr := manager.ExecuteInSession(ctx, session, "USE r3expired.r RETURN 8 AS k", nil)
			terminalErr := manager.RollbackAndDelete(ctx, session)
			t.Logf("expired session retained=%v, subsequent RUN result=%v error=%v, cleanup=%v", retained, result, executeErr, terminalErr)
			if retained {
				t.Error("idle expiry retained the active session and its executor/context")
			}
			if executeErr == nil {
				t.Error("a transaction past idle expiry still accepted remote work")
			}
			require.NoError(t, terminalErr, "expiry already completed rollback")
			require.Nil(t, session.Executor, "retired sessions release their executor")
			fresh, err := coordinator.newExecutorForDatabase("r3expired")
			require.NoError(t, err)
			count, err := fresh.Execute(ctx, "USE r3expired.r MATCH (n:R3Expired) RETURN count(n) AS c", nil)
			require.NoError(t, err)
			require.EqualValues(t, 0, count.Rows[0][0], "expiry rolled back the remote mutation")
		})
	}
}

func TestFabricHTTPIdleExpiryWaitsForActiveStatement(t *testing.T) {
	ctx := context.Background()
	remote, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	router := remote.buildRouter()
	entered, release := make(chan struct{}), make(chan struct{})
	var signal, unblock sync.Once
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if r.Method == http.MethodPost && strings.Contains(string(body), "R4ActiveIdle") {
			signal.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		r.Header.Set("Authorization", token)
		router.ServeHTTP(w, r)
	}))
	defer peer.Close()
	defer unblock.Do(func() { close(release) })
	coordinator, _ := setupTestServer(t)
	require.NoError(t, coordinator.dbManager.CreateCompositeDatabase("activeidle", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: peer.URL}}))
	manager := txsession.NewManager(60*time.Millisecond, coordinator.newExecutorForDatabase)
	manager.SetLifetimeContext(coordinator.requestsCtx)
	session, err := manager.Open(ctx, "activeidle")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := manager.ExecuteInSession(ctx, session, "USE activeidle.r CREATE (:R4ActiveIdle) RETURN 7 AS k", nil)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("remote statement did not start")
	}
	time.Sleep(120 * time.Millisecond)
	unblock.Do(func() { close(release) })
	require.NoError(t, <-done, "idle timeout must not cancel an active statement")
	_, ok := manager.Get(session.ID)
	require.True(t, ok, "successful statement refreshes its idle deadline")
	_, err = manager.CommitAndDelete(ctx, session)
	require.NoError(t, err)
	fresh, err := coordinator.newExecutorForDatabase("activeidle")
	require.NoError(t, err)
	result, err := fresh.Execute(ctx, "USE activeidle.r MATCH (n:R4ActiveIdle) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Rows[0][0])
}
