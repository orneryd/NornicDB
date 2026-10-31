package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture/remotequery"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
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
	// The manager's Close releases only the inner store, and a remote engine
	// can only close a completed transport. The test therefore owns the exact
	// factory return and closes it after the executor stops naming it.
	var factoryEngines []*storage.RemoteEngine
	config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
		engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
		if err != nil {
			return nil, err
		}
		factoryEngines = append(factoryEngines, engine)
		return engine, nil
	}
	t.Cleanup(func() {
		for _, engine := range factoryEngines {
			_ = engine.Close()
		}
	})
	manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), config)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "MixedCatalog", []multidb.ConstituentRef{{Alias: "Remote", DatabaseName: "nornic", URI: remote.URL, Type: "remote", AccessMode: "read"}}))
	catalog := NewCatalog()
	require.NoError(t, catalog.PopulateFromManager(manager))
	location, err := catalog.Resolve("MIXEDCATALOG.REMOTE")
	require.NoError(t, err)
	executor := NewRemoteFragmentExecutor()
	defer executor.Close()
	result, err := executor.Execute(context.Background(), location.(*LocationRemote), "RETURN 'mixed-case-result' AS marker", nil, "", storage.RemoteAccessRead)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"mixed-case-result"}}, result.Rows)
}

type observedRemoteEngine struct {
	remotequery.Observer
	begins int
	closes int
}

func (e *observedRemoteEngine) BeginCypherTx(ctx context.Context, mode storage.RemoteAccessMode) (storage.RemoteCypherTx, error) {
	e.begins++
	return e.RemoteEngine.BeginCypherTx(ctx, mode)
}

func (e *observedRemoteEngine) Close() error {
	e.closes++
	return e.RemoteEngine.Close()
}

func TestRemoteFactoryDecorators(t *testing.T) {
	for _, ending := range []string{"commit", "write_commit", "rollback", "early_rollback"} {
		t.Run(ending, func(t *testing.T) {
			commit := ending == "commit" || ending == "write_commit"
			mode := storage.RemoteAccessRead
			if ending == "write_commit" {
				mode = storage.RemoteAccessWrite
			}
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
			var factoryEngines []storage.Engine
			factoryCalls := 0
			config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
				factoryCalls++
				engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, User: ref.User, Password: ref.Password, AuthToken: token})
				if err != nil {
					return nil, err
				}
				// The test asserts the decorator's close below; the concrete
				// factory return is also released explicitly when the executor
				// no longer names it.
				wrapped = &observedRemoteEngine{Observer: remotequery.Observer{RemoteEngine: engine}}
				factoryEngines = append(factoryEngines, wrapped)
				return wrapped, nil
			}
			t.Cleanup(func() {
				for _, engine := range factoryEngines {
					_ = engine.Close()
				}
			})
			manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), config)
			require.NoError(t, err)
			fixtureowner.Own(t, manager)
			require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "decorator_contract", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "nornic", URI: remote.URL, Type: "remote", AccessMode: "read", AuthMode: "user_password", User: "admin", Password: password}}))
			catalog := NewCatalog()
			require.NoError(t, catalog.PopulateFromManager(manager))
			location, err := catalog.Resolve("decorator_contract.r")
			require.NoError(t, err)
			loc := location.(*LocationRemote)
			executor := NewRemoteFragmentExecutor()
			defer executor.Close()
			result, err := executor.Execute(context.Background(), loc, "RETURN 'decorated-result' AS marker", nil, "", storage.RemoteAccessRead)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"decorated-result"}}, result.Rows)
			transaction := NewFabricTransaction("decorator-lifetime")
			transaction.BindContext(context.Background())
			sub, err := transaction.GetOrOpen(ParticipantKeyFromLocation(loc), false)
			require.NoError(t, err)
			statementCtx, cancelStatement := context.WithCancel(context.Background())
			defer cancelStatement()
			ctx := WithSubTransaction(WithFabricTransaction(statementCtx, transaction), sub)
			for query := 0; query < 2; query++ {
				result, err := executor.Execute(ctx, loc, "RETURN 'decorated-result' AS marker", nil, "", mode)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"decorated-result"}}, result.Rows)
			}
			cancelStatement()
			if ending == "early_rollback" {
				require.NoError(t, executor.RollbackParticipants(context.Background()))
			}
			if commit {
				require.NoError(t, transaction.Commit(nil, nil))
			} else {
				require.NoError(t, transaction.Rollback(nil))
			}
			require.Equal(t, 1, factoryCalls)
			require.Equal(t, 1, wrapped.Calls)
			require.Equal(t, 1, wrapped.begins)
			require.EqualValues(t, 1, opens.Load())
			require.EqualValues(t, 3, queries.Load())
			if ending == "write_commit" {
				require.EqualValues(t, 1, commits.Load())
				require.EqualValues(t, 0, rollbacks.Load())
			} else {
				require.EqualValues(t, 0, commits.Load())
				require.EqualValues(t, 1, rollbacks.Load())
			}
			require.NoError(t, executor.Close())
			require.Equal(t, 1, wrapped.closes)
			if !commit {
				require.EqualValues(t, 1, rollbacks.Load(), "early rollback, coordinator rollback and close share one completion")
			}
		})
	}
}
