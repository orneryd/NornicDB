package multidb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDescriptorPersistedCatalogReopenAuthenticatesHTTPExactly(t *testing.T) {
	for _, rawQuery := range []string{"token=enc:v1:literal", "token=%zz&bad%key=keep"} {
		t.Run(rawQuery, func(t *testing.T) {
			const user = " uri-user "
			const password = " synthetic-descriptor-wire-password "
			var accepted atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotUser, gotPassword, ok := r.BasicAuth()
				if !ok || gotUser != user || gotPassword != password || r.URL.RawQuery != rawQuery {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.URL.Path != "/db/nornic/tx/commit" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var body struct {
					Statements []struct {
						Statement string `json:"statement"`
					} `json:"statements"`
				}
				if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Statements) != 1 || body.Statements[0].Statement != "RETURN 'ordinary-marker' AS marker" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				accepted.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"results":[{"columns":["marker"],"data":[{"row":["ordinary-marker"]}]}],"errors":[]}`))
			}))
			defer endpoint.Close()
			parsed, err := url.Parse(endpoint.URL)
			require.NoError(t, err)
			parsed.User = url.UserPassword(user, password)
			parsed.RawQuery = rawQuery
			catalog, err := json.Marshal(map[string]any{"descriptor": map[string]any{"name": "descriptor", "type": "composite", "status": "online", "constituents": []ConstituentRef{{Alias: "r", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: parsed.String()}}}})
			require.NoError(t, err)
			plan, err := NewCatalogUpgradeV4("system", "synthetic-wire-descriptor-key")
			require.NoError(t, err)
			secured, changed, err := plan.Transform(t.Context(), string(catalog))
			require.NoError(t, err)
			require.True(t, changed)
			dir := t.TempDir()
			for attempt := 0; attempt < 2; attempt++ {
				inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
				require.NoError(t, err)
				t.Cleanup(func() { _ = inner.Close() })
				system := storage.NewNamespacedEngine(inner, "system")
				if attempt == 0 {
					_, err = system.CreateNode(&storage.Node{ID: metadataNodeID, Labels: []string{"_System", "_Metadata"}, Properties: map[string]any{"type": "databases", "data": secured}})
					require.NoError(t, err)
				}
				cfg := DefaultConfig()
				cfg.RemoteCredentialEncryptionKey = "synthetic-wire-descriptor-key"
				var engines []*storage.RemoteEngine
				cfg.RemoteEngineFactory = func(ref ConstituentRef, token string) (storage.Engine, error) {
					engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, User: ref.User, Password: ref.Password, AuthToken: token})
					if engine != nil {
						engines = append(engines, engine)
					}
					return engine, err
				}
				manager, err := NewDatabaseManager(t.Context(), inner, cfg)
				require.NoError(t, err)
				t.Cleanup(func() {
					for _, engine := range engines {
						_ = engine.Close()
					}
					_ = manager.Close()
				})
				selected, err := manager.GetStorageWithAuth("descriptor.r", "")
				if err != nil {
					t.Fatal("persisted descriptor did not resolve")
				}
				remote, ok := selected.(*storage.RemoteEngine)
				require.True(t, ok)
				columns, rows, err := remote.QueryCypher(t.Context(), "RETURN 'ordinary-marker' AS marker", nil, storage.RemoteAccessRead)
				if err != nil {
					t.Fatal("persisted exact descriptor did not authenticate")
				}
				require.Equal(t, []string{"marker"}, columns)
				require.Equal(t, [][]interface{}{{"ordinary-marker"}}, rows)
				stored, err := system.GetNode(metadataNodeID)
				require.NoError(t, err)
				data, ok := stored.Properties["data"].(string)
				require.True(t, ok)
				require.Contains(t, data, "connection_v1")
				require.False(t, strings.Contains(data, "synthetic-descriptor-wire-password"))
				require.False(t, strings.Contains(data, "token=enc:v1:literal"))
				refs, err := manager.GetCompositeConstituents("descriptor")
				require.NoError(t, err)
				require.Len(t, refs, 1)
				require.Empty(t, refs[0].URI)
				require.NotEmpty(t, refs[0].ConnectionV1)
				view, err := manager.RemoteConstituentForDisplay(refs[0])
				require.NoError(t, err)
				if rawQuery == "token=enc:v1:literal" {
					require.Contains(t, view.URI, "127.0.0.1:")
				} else {
					require.Equal(t, observability.RedactedPlaceholder(), view.URI, "malformed URI display retains canonical fail-closed projection")
				}
				require.NotContains(t, view.URI, "synthetic-")
				require.Empty(t, view.Password)
				for _, engine := range engines {
					require.NoError(t, engine.Close())
				}
				require.NoError(t, manager.Close())
			}
			require.Equal(t, int32(2), accepted.Load())
		})
	}
}
