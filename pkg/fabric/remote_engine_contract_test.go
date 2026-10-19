package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCatalogRemoteMixedCaseLookup(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"marker"}, "data": []any{map[string]any{"row": []any{"mixed-case-result"}}}}}, "errors": []any{}})
	}))
	defer remote.Close()
	config := multidb.DefaultConfig()
	config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
		return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
	}
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), config)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateCompositeDatabase("MixedCatalog", []multidb.ConstituentRef{{Alias: "Remote", DatabaseName: "nornic", URI: remote.URL, Type: "remote", AccessMode: "read"}}))
	catalog := NewCatalog()
	require.NoError(t, catalog.PopulateFromManager(manager))
	location, err := catalog.Resolve("MIXEDCATALOG.REMOTE")
	require.NoError(t, err)
	executor := NewRemoteFragmentExecutor()
	defer executor.Close()
	result, err := executor.Execute(context.Background(), location.(*LocationRemote), "RETURN 'mixed-case-result' AS marker", nil, "")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"mixed-case-result"}}, result.Rows)
}

type observedRemoteEngine struct {
	*storage.RemoteEngine
	queries int
	begins  int
	closes  int
}

func (e *observedRemoteEngine) QueryCypher(ctx context.Context, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	e.queries++
	return e.RemoteEngine.QueryCypher(ctx, query, params)
}

func (e *observedRemoteEngine) BeginCypherTx(ctx context.Context) (storage.RemoteCypherTx, error) {
	e.begins++
	return e.RemoteEngine.BeginCypherTx(ctx)
}

func (e *observedRemoteEngine) Close() error {
	e.closes++
	return e.RemoteEngine.Close()
}

func TestRemoteFactoryDecorators(t *testing.T) {
	for _, commit := range []bool{true, false} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			const password = "synthetic-decorator-password-491628"
			var opens, queries, commits, rollbacks atomic.Int32
			var remote *httptest.Server
			remote = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, pass, ok := r.BasicAuth()
				if !ok || user != "admin" || pass != password {
					t.Error("remote request lacked synthetic authentication; values withheld")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				response := map[string]any{"results": []any{}, "errors": []any{}}
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/db/nornic/tx":
					id := opens.Add(1)
					response["commit"] = fmt.Sprintf("%s/db/nornic/tx/%d/commit", remote.URL, id)
				case r.Method == http.MethodPost && r.URL.Path == "/db/nornic/tx/commit":
					queries.Add(1)
					response["results"] = []any{map[string]any{"columns": []string{"marker"}, "data": []any{map[string]any{"row": []any{"decorated-result"}}}}}
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/commit"):
					commits.Add(1)
				case r.Method == http.MethodDelete:
					rollbacks.Add(1)
				case r.Method == http.MethodPost:
					queries.Add(1)
					response["results"] = []any{map[string]any{"columns": []string{"marker"}, "data": []any{map[string]any{"row": []any{"decorated-result"}}}}}
				default:
					http.NotFound(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer remote.Close()
			config := multidb.DefaultConfig()
			config.RemoteCredentialEncryptionKey = "synthetic-decorator-key-617483"
			var wrapped *observedRemoteEngine
			factoryCalls := 0
			config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
				factoryCalls++
				engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, User: ref.User, Password: ref.Password, AuthToken: token})
				if err != nil {
					return nil, err
				}
				wrapped = &observedRemoteEngine{RemoteEngine: engine}
				return wrapped, nil
			}
			manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), config)
			require.NoError(t, err)
			defer manager.Close()
			require.NoError(t, manager.CreateCompositeDatabase("decorator_contract", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", URI: remote.URL, Type: "remote", AccessMode: "read", AuthMode: "user_password", User: "admin", Password: password}}))
			catalog := NewCatalog()
			require.NoError(t, catalog.PopulateFromManager(manager))
			location, err := catalog.Resolve("decorator_contract.r")
			require.NoError(t, err)
			loc := location.(*LocationRemote)
			executor := NewRemoteFragmentExecutor()
			defer executor.Close()
			result, err := executor.Execute(context.Background(), loc, "RETURN 'decorated-result' AS marker", nil, "")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"decorated-result"}}, result.Rows)
			transaction := NewFabricTransaction("decorator-lifetime")
			sub, err := transaction.GetOrOpen(participantKeyFromLocation(loc), false)
			require.NoError(t, err)
			ctx := WithSubTransaction(WithFabricTransaction(context.Background(), transaction), sub)
			for query := 0; query < 2; query++ {
				result, err := executor.Execute(ctx, loc, "RETURN 'decorated-result' AS marker", nil, "")
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"decorated-result"}}, result.Rows)
			}
			if commit {
				require.NoError(t, transaction.Commit(nil, nil))
			} else {
				require.NoError(t, transaction.Rollback(nil))
			}
			require.Equal(t, 1, factoryCalls)
			require.Equal(t, 1, wrapped.queries)
			require.Equal(t, 1, wrapped.begins)
			require.EqualValues(t, 1, opens.Load())
			require.EqualValues(t, 3, queries.Load())
			if commit {
				require.EqualValues(t, 1, commits.Load())
				require.EqualValues(t, 0, rollbacks.Load())
			} else {
				require.EqualValues(t, 0, commits.Load())
				require.EqualValues(t, 1, rollbacks.Load())
			}
			require.NoError(t, executor.Close())
			require.Equal(t, 1, wrapped.closes)
		})
	}
}
