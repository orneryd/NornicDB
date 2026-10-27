package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPDescriptorMetadataUsesSafeDerivedView(t *testing.T) {
	const key = "synthetic-http-metadata-descriptor-key"
	const user = "public-http-user"
	const password = "synthetic-http-metadata-password"
	var accepted atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPassword, ok := r.BasicAuth()
		if !ok || gotUser != user || gotPassword != password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		accepted.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"columns":["count"],"data":[{"row":[0]}]}],"errors":[]}`))
	}))
	defer upstream.Close()
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", key)
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Auth.Enabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	plan, err := multidb.NewCatalogUpgradeV4("system", key)
	require.NoError(t, err)
	original, err := json.Marshal(map[string]any{"descriptor": map[string]any{"name": "descriptor", "type": "composite", "status": "online", "constituents": []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: upstream.URL + "/ordinary?token=synthetic-http-query-token", AuthMode: "user_password", User: user, Password: password}}}})
	require.NoError(t, err)
	data, _, err := plan.Transform(t.Context(), string(original))
	require.NoError(t, err)
	_, err = storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system").CreateNode(&storage.Node{ID: "databases:metadata", Labels: []string{"_System", "_Metadata"}, Properties: map[string]any{"type": "databases", "data": data}})
	require.NoError(t, err)
	serverCfg := DefaultConfig()
	serverCfg.ProcessConfig = cfg
	serverCfg.EmbeddingEnabled = false
	serverCfg.MCPEnabled = false
	server, err := New(db, nil, serverCfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Stop(ctx)
	})
	show := httptest.NewRequest(http.MethodPost, "/db/system/tx/commit", strings.NewReader(`{"statements":[{"statement":"SHOW CONSTITUENTS FOR COMPOSITE DATABASE descriptor"}]}`))
	show.Header.Set("Content-Type", "application/json")
	shown := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(shown, show)
	require.Equal(t, http.StatusOK, shown.Code)
	var response struct {
		Results []struct {
			Data []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []any `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(shown.Body.Bytes(), &response))
	require.Empty(t, response.Errors)
	require.Len(t, response.Results, 1)
	require.Len(t, response.Results[0].Data, 1)
	require.Equal(t, user, response.Results[0].Data[0].Row[7])
	require.NotContains(t, shown.Body.String(), password)
	require.NotContains(t, shown.Body.String(), "synthetic-http-query-token")
	require.Contains(t, shown.Body.String(), "/ordinary")
	info := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(info, httptest.NewRequest(http.MethodGet, "/db/descriptor", nil))
	require.Equal(t, http.StatusOK, info.Code)
	require.NotContains(t, info.Body.String(), password)
	require.NotContains(t, info.Body.String(), "synthetic-http-query-token")
	require.Contains(t, info.Body.String(), "/ordinary")
	require.Positive(t, accepted.Load(), "metadata peer must receive real authenticated requests")
}
