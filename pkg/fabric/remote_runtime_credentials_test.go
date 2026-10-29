package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCatalogRemoteRuntimeCredentials(t *testing.T) {
	for _, uriAuth := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "uri"}[uriAuth], func(t *testing.T) {
			const password = "synthetic-catalog-password-48371926"
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, pass, ok := r.BasicAuth()
				require.True(t, ok)
				require.Equal(t, "admin", user)
				require.Equal(t, password, pass)
				_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{"columns": []string{"marker"}, "data": []any{map[string]any{"row": []any{"real-catalog-result"}}}}}, "errors": []any{}})
			}))
			defer remote.Close()
			config := multidb.DefaultConfig()
			config.RemoteCredentialEncryptionKey = "synthetic-catalog-key-93164827"
			// The manager's Close releases only the inner store, and a remote
			// engine can only close a completed transport. The test therefore
			// owns the exact factory return and closes it after the executor
			// stops naming it.
			var factoryEngines []*storage.RemoteEngine
			config.RemoteEngineFactory = func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
				engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, User: ref.User, Password: ref.Password, AuthToken: token})
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
			defer manager.Close()
			ref := multidb.ConstituentRef{Alias: "r", DatabaseName: "nornic", URI: remote.URL, Type: "remote", AccessMode: "read", AuthMode: "user_password", User: "admin", Password: password}
			if uriAuth {
				ref.URI = "http://admin:" + password + "@" + remote.Listener.Addr().String()
				ref.AuthMode = ""
				ref.User = ""
				ref.Password = ""
			}
			require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "catalog_runtime", []multidb.ConstituentRef{ref}))
			catalog := NewCatalog()
			require.NoError(t, catalog.PopulateFromManager(manager))
			loc, err := catalog.Resolve("catalog_runtime.r")
			require.NoError(t, err)
			runtime := loc.(*LocationRemote)
			require.Empty(t, runtime.Password)
			executor := NewRemoteFragmentExecutor()
			defer executor.Close()
			result, err := executor.Execute(context.Background(), runtime, "RETURN 'real-catalog-result' AS marker", nil, "", storage.RemoteAccessRead)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"real-catalog-result"}}, result.Rows)
			for _, ending := range []string{"commit", "rollback"} {
				transaction := NewFabricTransaction("synthetic-terminal-identity")
				participant, err := transaction.GetOrOpen(ParticipantKeyFromLocation(runtime), false)
				require.NoError(t, err)
				require.NoError(t, transaction.BindParticipantCallbacks(participant.ShardName, ParticipantCallbacks{Commit: func(*SubTransaction) error { return fmt.Errorf("synthetic commit rejection") }, Rollback: func(*SubTransaction) error { return fmt.Errorf("synthetic rollback rejection") }}))
				if ending == "commit" {
					err = transaction.Commit(nil, nil)
				} else {
					err = transaction.Rollback(nil)
				}
				require.Error(t, err)
				require.NotContains(t, err.Error(), password)
				require.Contains(t, err.Error(), "catalog_runtime.r")
			}
		})
	}
}

func TestRemoteResolvedAliasesKeepDistinctCredentials(t *testing.T) {
	executor := NewRemoteFragmentExecutor()
	defer executor.Close()
	transaction := NewFabricTransaction("synthetic-alias-identities")
	var participants []*SubTransaction
	for _, alias := range []string{"first", "second"} {
		location := &LocationRemote{GraphName: "composite." + alias, URI: "http://127.0.0.1:1", DBName: "nornic", AuthMode: "user_password"}
		location.ResolveEngine = func(string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: location.URI, Database: location.DBName, User: alias, Password: "synthetic-distinct-password-61827394"})
		}
		engine, err := executor.getOrCreateEngine(location, "")
		require.NoError(t, err)
		again, err := executor.getOrCreateEngine(location, "")
		require.NoError(t, err)
		require.Same(t, engine, again)
		location.URI = "http://admin:synthetic-uri-secret-91736482@127.0.0.1:1"
		participant, err := transaction.GetOrOpen(ParticipantKeyFromLocation(location), false)
		require.NoError(t, err)
		require.Equal(t, "remote:composite."+alias, participant.ShardName)
		participants = append(participants, participant)
	}
	require.NotSame(t, participants[0], participants[1], "aliases with distinct credentials must not share a remote transaction")
	require.Len(t, executor.engineCache, 2, "constituents with different identities at one endpoint must not share a connection")
}
