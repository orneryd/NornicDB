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
			var probes, unavailableCalls atomic.Int64
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
			require.Equal(t, int64(0), unavailableCalls.Load(), "no request may touch the unrelated constituent")
			require.Equal(t, int64(2), probes.Load(), "retain logical-BEGIN capture and physical participant validation, once per remote")
			_, err = executor.Execute(context.Background(), "USE participants.down RETURN 7 AS n", nil)
			require.Error(t, err, "a selected unavailable remote must still fail")
			require.Positive(t, unavailableCalls.Load())
		})
	}
}
