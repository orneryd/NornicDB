package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// Exercise the production lifetime transition with a remote commit in flight.
func TestCourseChangeServerRemoteLifetime(t *testing.T) {
	remote, remoteAuth := setupTestServer(t)
	remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
	remoteRouter := remote.buildRouter()
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/commit") {
			_, _ = io.Copy(io.Discard, r.Body)
			close(entered)
			select {
			case <-r.Context().Done():
				close(canceled)
			case <-release:
			}
			return
		}
		r.Header.Set("Authorization", remoteToken)
		remoteRouter.ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	defer close(release)
	server, auth := setupTestServer(t)
	server.config.Port = 0
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "lifetimeremote", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: endpoint.URL, AuthMode: "oidc_forwarding"}}))
	lifetime := server.requestsCtx
	require.NoError(t, server.Start())
	token := "Bearer " + getAuthToken(t, auth, "admin")
	post := func(path, body string) (*http.Response, error) {
		if !strings.HasPrefix(path, "http") {
			path = "http://" + server.listener.Addr().String() + path
		}
		req, err := http.NewRequest(http.MethodPost, path, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		return http.DefaultClient.Do(req)
	}
	response, err := post("/db/lifetimeremote/tx", `{"statements":[{"statement":"USE lifetimeremote.r CREATE (:Lifetime) RETURN 1"}]}`)
	require.NoError(t, err)
	var opened TransactionResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&opened))
	response.Body.Close()
	require.Empty(t, opened.Errors)
	require.NotEmpty(t, opened.Commit)
	done := make(chan struct{})
	go func() {
		defer close(done)
		response, _ := post(opened.Commit, `{"statements":[]}`)
		if response != nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("remote participant commit was not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopErr := server.Stop(ctx)
	require.ErrorIs(t, lifetime.Err(), context.Canceled)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("remote participant missed server lifetime cancellation")
	}
	require.NoError(t, stopErr)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP terminal owner did not drain")
	}
}
