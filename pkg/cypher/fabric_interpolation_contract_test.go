package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Interpolation is a NornicDB extension without native grammar. The exact release
// and develop base controls establish this bounded existing contract: local
// constituents work; remote public Execute rejects the early internal helper.
// Do not extend the grammar or claim remote interpolation support in this fix.
func TestFabricLogicalInterpolationContract(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, remote := range []bool{false, true} {
				t.Run(fmt.Sprintf("normalize_%v/explicit_%v/remote_%v", normalize, explicit, remote), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					participant := NewStorageExecutor(newTestMemoryEngine(t))
					var mutex sync.Mutex
					var received []string
					var server *httptest.Server
					server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						mutex.Lock()
						defer mutex.Unlock()
						var request struct {
							Statements []struct {
								Statement  string
								Parameters map[string]interface{}
							}
						}
						if r.Method == http.MethodPost {
							_ = json.NewDecoder(r.Body).Decode(&request)
						}
						results := []interface{}{}
						errors := []interface{}{}
						for _, statement := range request.Statements {
							received = append(received, statement.Statement)
							result, err := participant.Execute(r.Context(), statement.Statement, statement.Parameters)
							if err != nil {
								errors = append(errors, map[string]interface{}{"code": "Neo.ClientError.Statement.SyntaxError", "message": err.Error()})
								break
							}
							data := []interface{}{}
							for _, row := range result.Rows {
								data = append(data, map[string]interface{}{"row": row})
							}
							results = append(results, map[string]interface{}{"columns": result.Columns, "data": data})
						}
						w.Header().Set("Content-Type", "application/json")
						_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": results, "errors": errors, "commit": server.URL + "/db/native/tx/1/commit"})
					}))
					defer server.Close()
					options := multidb.DefaultConfig()
					options.RemoteEngineFactory = func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
						return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: authToken})
					}
					manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), options)
					require.NoError(t, err)
					defer manager.Close()
					require.NoError(t, manager.CreateDatabase("scope52_local"))
					constituents := []multidb.ConstituentRef{{Alias: "one", DatabaseName: "scope52_local", Type: "local", AccessMode: "read_write"}, {Alias: "remote", DatabaseName: "native", Type: "remote", AccessMode: "read_write", URI: server.URL}}
					require.NoError(t, manager.CreateCompositeDatabase("scope52_cmp", constituents))
					store, err := manager.GetStorage("scope52_cmp")
					require.NoError(t, err)
					executor := NewStorageExecutor(store)
					executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
					ctx := context.Background()
					if explicit {
						_, err := executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer executor.Execute(ctx, "ROLLBACK", nil)
					}
					alias := "one"
					if remote {
						alias = "remote"
					}
					result, err := executor.Execute(ctx, `CYPHER 25 USE scope52_cmp.`+alias+` RETURN s"x{1}y" AS v`, nil)
					var rows [][]interface{}
					if result != nil {
						rows = result.Rows
					}
					mutex.Lock()
					queries := append([]string(nil), received...)
					mutex.Unlock()
					encoded, _ := json.Marshal(rows)
					if remote {
						require.Error(t, err)
						require.Contains(t, err.Error(), interpolateFunction)
					} else {
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{"x1y"}}, rows)
					}
					t.Logf("INTERPOLATION_CONTRACT remote=%v explicit=%v normalization=%v rows=%s error=%v participant=%q", remote, explicit, normalize, encoded, err, queries)
				})
			}
		}
	}
}
