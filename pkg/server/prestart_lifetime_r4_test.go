package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestServerStopR4PreStartOwnerRemoteCompletion(t *testing.T) {
	remote, remoteAuth := setupTestServer(t)
	remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
	remoteRouter := remote.buildRouter()
	entered, canceled, escape := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, enterOnce, cancelOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(escape) }) }
	defer release()
	remoteHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("Authorization", remoteToken)
		// Constructor procedure reads use /tx/commit too. Gate only the COMMIT of
		// an already-open explicit remote transaction, after enlistment. The
		// participant writes: since #1638 a read-only participant ends with a
		// detached rollback instead, which the owner deliberately does not cancel.
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commit") && !strings.HasSuffix(r.URL.Path, "/tx/commit") {
			// net/http starts its disconnect reader when the body reaches EOF.
			// Preserve the body for the rejected baseline's controlled escape.
			body, readErr := io.ReadAll(r.Body)
			closeErr := r.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Errorf("read held terminal request: read=%v close=%v", readErr, closeErr)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			enterOnce.Do(func() { close(entered) })
			select {
			case <-r.Context().Done():
				cancelOnce.Do(func() { close(canceled) })
				return
			case <-escape:
			}
		}
		remoteRouter.ServeHTTP(w, r)
	}))
	t.Cleanup(remoteHTTP.Close)
	coordinator, authenticator := setupTestServerWithConfig(t, func(c *Config) {
		c.Address, c.Port, c.MCPEnabled = "127.0.0.1", 0, false
	})
	// The real constructor has enrolled its manager/providers before this real
	// Start. No replacement manager or buildRouter-only coordinator is installed.
	require.NoError(t, coordinator.Start())
	require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(t.Context(), "r4owner", []multidb.ConstituentRef{{Alias: "remote", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: remoteHTTP.URL, AuthMode: "oidc_forwarding"}}))
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	client := &http.Client{Timeout: 5 * time.Second}
	send := func(endpoint, query string) (TransactionResponse, string, error) {
		statements := []map[string]string{}
		if query != "" {
			statements = append(statements, map[string]string{"statement": query})
		}
		payload, err := json.Marshal(map[string]interface{}{"statements": statements})
		if err != nil {
			return TransactionResponse{}, "", err
		}
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return TransactionResponse{}, "", err
		}
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			return TransactionResponse{}, "", err
		}
		defer response.Body.Close()
		var result TransactionResponse
		err = json.NewDecoder(response.Body).Decode(&result)
		io.Copy(io.Discard, response.Body)
		return result, response.Header.Get("Location"), err
	}
	opened, location, err := send("http://"+coordinator.Addr()+"/db/r4owner/tx", "")
	require.NoError(t, err)
	require.Empty(t, opened.Errors)
	require.NotEmpty(t, location)
	enlisted, _, err := send(location, "USE r4owner.remote CREATE (:R4OwnerCommit) RETURN 1 AS value")
	require.NoError(t, err)
	require.Empty(t, enlisted.Errors)
	type terminalResult struct {
		response TransactionResponse
		err      error
	}
	committed := make(chan terminalResult, 1)
	go func() { result, _, err := send(opened.Commit, ""); committed <- terminalResult{result, err} }()
	stopR2Await(t, entered)
	budget, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	stopErr := coordinator.Stop(budget)
	t.Logf("bounded Stop outcome: %v", stopErr)
	select {
	case <-canceled:
	case <-time.After(200 * time.Millisecond):
		t.Error("Stop did not cancel the remote operation enrolled against the constructor's owner")
	}
	release() // controlled escape for the rejected baseline; no retained fixture work is abandoned
	select {
	case result := <-committed:
		t.Logf("remote terminal response: errors=%v transport=%v", result.response.Errors, result.err)
	case <-time.After(2 * time.Second):
		t.Fatal("remote terminal caller did not unwind")
	}
	final, end := context.WithTimeout(context.Background(), 2*time.Second)
	defer end()
	t.Logf("actual cleanup outcome: %v", coordinator.WaitStopped(final))
	require.NoError(t, final.Err(), "actual retained cleanup did not finish after the remote operation")
}
