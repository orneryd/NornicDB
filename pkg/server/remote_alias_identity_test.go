package server

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// aliasIdentityRemote is one synthetic remote registration: the principal it
// authenticates, the marker it answers and the probes it received.
type aliasIdentityRemote struct {
	user   string
	marker string
	mu     sync.Mutex
	probes int
}

func (r *aliasIdentityRemote) observe() {
	r.mu.Lock()
	r.probes++
	r.mu.Unlock()
}

func (r *aliasIdentityRemote) observed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.probes
}

// newIdentityRemoteServer authenticates synthetic remote registrations at a
// loopback proxy and delegates the entire transaction protocol to a real server.
// Each backing database contains distinct data; results cannot be supplied by
// request parameters or by a simulated transaction response.
func newIdentityRemoteServer(t *testing.T, remotes map[string]*aliasIdentityRemote, passwords map[string]string, probeMarker string) string {
	t.Helper()
	backend, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")
	for _, remote := range remotes {
		require.NoError(t, backend.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: remote.marker}))
		store, err := backend.dbManager.GetStorage(remote.marker)
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: "identity-marker", Labels: []string{"AliasIdentityProbe"}, Properties: map[string]any{"marker": remote.marker}})
		require.NoError(t, err)
	}
	router := backend.buildRouter()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		remote := remotes[user]
		if !ok || remote == nil || password != passwords[remote.marker] {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error("remote request body read failed")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var request struct {
			Statements []struct {
				Statement string `json:"statement"`
			} `json:"statements"`
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &request); err != nil {
				t.Error("remote request JSON invalid")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, statement := range request.Statements {
				if strings.Contains(statement.Statement, probeMarker) {
					remote.observe()
				}
			}
		}
		r.Header.Set("Authorization", token)
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL
}
