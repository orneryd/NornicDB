package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type requestOwnedTestEngine struct {
	storage.Engine
	reads, closed int
}

func (e *requestOwnedTestEngine) GetInnerEngine() storage.Engine { return e.Engine }
func (e *requestOwnedTestEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.reads++
	return e.Engine.GetNodesByLabel(label)
}
func (e *requestOwnedTestEngine) Close() error { e.closed++; return e.Engine.Close() }

func TestHTTPRemoteResourceOwnershipAndDenial(t *testing.T) {
	s, users := setupTestServer(t)
	var created []*requestOwnedTestEngine
	manager, err := multidb.NewDatabaseManager(s.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic", SystemDatabase: "system",
		RemoteEngineFactory: func(multidb.ConstituentRef, string) (storage.Engine, error) {
			e := &requestOwnedTestEngine{Engine: storage.NewMemoryEngine()}
			created = append(created, e)
			return e, nil
		},
	})
	require.NoError(t, err)
	s.dbManager = manager
	require.NoError(t, manager.CreateCompositeDatabase("owned_comp", []multidb.ConstituentRef{{Alias: "remote", DatabaseName: "peer", Type: "remote", AccessMode: "read_write", URI: "http://unused.invalid"}}))
	ctx := context.Background()
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, "viewer", []string{"owned_comp", "peer"}))
	_, err = users.CreateUser("remote_reader", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, "remote_reader")
	for i := 0; i < 2; i++ {
		response := makeRequest(t, s, http.MethodPost, "/db/owned_comp.remote/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "RETURN 1"}}}, token)
		var body TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Empty(t, body.Errors, response.Body.String())
		require.Len(t, created, i+1, "response metadata must not construct another remote")
		require.Equal(t, 1, created[i].closed, "request owner releases its engine once")
		require.Zero(t, created[i].reads, "statement construction must not scan the remote catalog")
		require.NotContains(t, s.executors, "owned_comp.remote", "closed request resources are never cached")
	}
	denied := makeRequest(t, s, http.MethodPost, "/db/owned_comp.remote/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "CREATE (:Refused)"}}}, token)
	require.Contains(t, denied.Body.String(), "Neo.ClientError.Security.Forbidden")
	require.Len(t, created, 2, "denial and warning metadata leave the factory untouched")
	for _, terminal := range []string{"commit", "rollback"} {
		begin := makeRequest(t, s, http.MethodPost, "/db/owned_comp.remote/tx", map[string]any{"statements": []map[string]any{{"statement": "RETURN 1"}}}, token)
		var opened TransactionResponse
		require.NoError(t, json.Unmarshal(begin.Body.Bytes(), &opened))
		require.Empty(t, opened.Errors, begin.Body.String())
		owned := created[len(created)-1]
		require.Zero(t, owned.closed, "BEGIN retains its resource")
		location, err := url.Parse(opened.Commit)
		require.NoError(t, err)
		path := strings.TrimSuffix(location.Path, "/commit")
		executed := makeRequest(t, s, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": "RETURN 2"}}}, token)
		var continued TransactionResponse
		require.NoError(t, json.Unmarshal(executed.Body.Bytes(), &continued))
		require.Empty(t, continued.Errors, executed.Body.String())
		require.Zero(t, owned.closed, "transaction statements retain the resource")
		if terminal == "commit" {
			ended := makeRequest(t, s, http.MethodPost, path+"/commit", map[string]any{"statements": []any{}}, token)
			var committed TransactionResponse
			require.NoError(t, json.Unmarshal(ended.Body.Bytes(), &committed))
			require.Empty(t, committed.Errors, ended.Body.String())
		} else {
			ended := makeRequest(t, s, http.MethodDelete, path, nil, token)
			require.Less(t, ended.Code, 300, ended.Body.String())
		}
		require.Equal(t, 1, owned.closed, terminal)
		require.Zero(t, owned.reads, "transaction construction does not scan the remote catalog")
	}
	require.Len(t, created, 4, "each transaction owns exactly one remote resource")
}
