package cypher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFabricAutoCommitCapturesOnlyPlannedRemotes(t *testing.T) {
	for _, query := range []string{
		"USE participants.live RETURN 7 AS n",
		"CALL { USE participants.live RETURN 7 AS n } RETURN n",
		"USE participants.live RETURN 7 AS n UNION ALL USE participants.live RETURN 7 AS n",
	} {
		t.Run(query, func(t *testing.T) {
			var probes, unavailableCalls, unavailableProbes, procedureLoads atomic.Int64
			var available *httptest.Server
			available = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if respondRemoteNamespaceFixture(w, r) {
					probes.Add(1)
					return
				}
				if r.Method == http.MethodDelete || strings.HasSuffix(r.URL.Path, "/commit") {
					_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
					return
				}
				if strings.HasSuffix(r.URL.Path, "/tx") {
					_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}, "commit": available.URL + r.URL.Path + "/1/commit"})
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"n"}, "data": []any{map[string]any{"row": []any{7}}}}}, "errors": []any{}})
			}))
			t.Cleanup(available.Close)
			unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				unavailableCalls.Add(1)
				var request struct {
					Statements []struct {
						Statement string `json:"statement"`
					} `json:"statements"`
				}
				if json.NewDecoder(r.Body).Decode(&request) == nil && len(request.Statements) == 1 {
					statement := request.Statements[0].Statement
					if statement == "CALL db.info()" {
						unavailableProbes.Add(1)
					} else if strings.Contains(statement, procedureCatalogLabel) {
						procedureLoads.Add(1)
					}
				}
				http.Error(w, "unrelated remote unavailable", http.StatusServiceUnavailable)
			}))
			t.Cleanup(unavailable.Close)
			base := storage.NewMemoryEngine()
			manager, err := multidb.NewDatabaseManager(base, &multidb.Config{
				RemoteEngineFactory: func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
					return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
				},
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			require.NoError(t, manager.CreateCompositeDatabase("participants", []multidb.ConstituentRef{
				{Alias: "live", DatabaseName: "live_db", Type: "remote", AccessMode: "read_write", URI: available.URL, AuthMode: "oidc_forwarding"},
				{Alias: "down", DatabaseName: "down_db", Type: "remote", AccessMode: "read_write", URI: unavailable.URL, AuthMode: "oidc_forwarding"},
			}))
			view, err := manager.GetStorage("participants")
			require.NoError(t, err)
			executor := NewStorageExecutor(view)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			result, err := executor.Execute(context.Background(), query, nil)
			require.NoError(t, err, "an unused remote must not determine statement availability")
			expectedRows := 1
			if strings.Contains(query, "UNION ALL") {
				expectedRows = 2
			}
			require.Len(t, result.Rows, expectedRows)
			for _, row := range result.Rows {
				require.EqualValues(t, 7, row[0])
			}
			require.Zero(t, unavailableProbes.Load(), "logical BEGIN must not probe the unrelated constituent")
			// Executor construction independently loads persisted procedures from
			// composite storage. Keep that pre-existing traffic visible and separate
			// from this regression's newly introduced identity probes.
			require.Equal(t, procedureLoads.Load(), unavailableCalls.Load(), "unexpected unrelated remote traffic")
			t.Logf("unrelated procedure bootstrap calls=%d; logical-BEGIN probes=%d", procedureLoads.Load(), unavailableProbes.Load())
			require.Equal(t, int64(2), probes.Load(), "retain logical-BEGIN capture and physical participant validation, once per remote")
			_, err = executor.Execute(context.Background(), "USE participants.down RETURN 7 AS n", nil)
			require.Error(t, err, "a selected unavailable remote must still fail")
			require.Positive(t, unavailableProbes.Load(), "selected remote failures must remain visible")
		})
	}
}
