package cypher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type remotePolicyObserver struct {
	*storage.RemoteEngine
	mode   storage.RemoteAccessMode
	source string
	calls  int
}

func (e *remotePolicyObserver) QueryCypher(ctx context.Context, source string, params map[string]interface{}, mode storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
	e.mode, e.source = mode, source
	e.calls++
	return e.RemoteEngine.QueryCypher(ctx, source, params, mode)
}

func TestRemoteCompilerPolicyPublicDispatch(t *testing.T) {
	for _, tc := range []struct {
		source string
		mode   storage.RemoteAccessMode
	}{
		{"MATCH (n) SET\tn.x=1 RETURN n", storage.RemoteAccessWrite},
		{"MATCH (n) SET/*gap*/n.x=1 RETURN n", storage.RemoteAccessWrite},
		{"MATCH (n) REMOVE\tn.x RETURN n", storage.RemoteAccessWrite},
		{"MATCH (n) REMOVE/*gap*/n.x RETURN n", storage.RemoteAccessWrite},
		{"RETURN 'CREATE' AS value", storage.RemoteAccessRead},
		{"RETURN 'SET x=1' AS value", storage.RemoteAccessRead},
		{"MATCH (n) RETURN n", storage.RemoteAccessRead},
		{"CREATE (n:Person)", storage.RemoteAccessWrite},
		{"MERGE (n:Person {id: 1})", storage.RemoteAccessWrite},
		{"MATCH (n) DELETE n", storage.RemoteAccessWrite},
		{"MATCH (n) SET n.x = 1", storage.RemoteAccessWrite},
		{"CALL db.labels()", storage.RemoteAccessWrite},
		{"CALL opaque.remote()", storage.RemoteAccessWrite},
		{"EXPLAIN CREATE (n)", storage.RemoteAccessRead},
		{"PROFILE CREATE (n)", storage.RemoteAccessWrite},
		{"FOREIGN REMOTE DIALECT", storage.RemoteAccessWrite},
	} {
		t.Run(tc.source, func(t *testing.T) {
			var sent string
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Statements []struct {
						Statement string `json:"statement"`
					} `json:"statements"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				require.Len(t, request.Statements, 1)
				sent = request.Statements[0].Statement
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"results":[{"columns":["marker"],"data":[{"row":[17]}]}],"errors":[]}`))
			}))
			defer peer.Close()
			engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: peer.URL, Database: "remote"})
			require.NoError(t, err)
			observed := &remotePolicyObserver{RemoteEngine: engine}
			defer observed.Close()
			catalog := fabric.NewCatalog()
			catalog.Register("comp.remote", &fabric.LocationRemote{DBName: "remote", URI: peer.URL, ResolveEngine: func(string) (storage.Engine, error) { return observed, nil }})
			remote := fabric.NewRemoteFragmentExecutor()
			defer remote.Close()
			executor := fabric.NewFabricExecutor(catalog, nil, remote)
			var compiledSource string
			executor.SetRemoteStatementPolicy(func(ctx context.Context, source string) (fabric.RemoteStatementPolicy, error) {
				compiledSource = source
				return readRemoteStatementPolicy(ctx, source)
			})
			result, err := executor.Execute(t.Context(), nil, &fabric.FragmentExec{Input: &fabric.FragmentInit{}, Query: tc.source, GraphName: "comp.remote"}, nil, "")
			require.NoError(t, err)
			require.Equal(t, []string{"marker"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(17)}}, result.Rows)
			require.Equal(t, tc.mode, observed.mode)
			require.Equal(t, tc.source, compiledSource)
			require.Equal(t, compiledSource, observed.source)
			require.Equal(t, observed.source, sent)
			require.Equal(t, 1, observed.calls)
		})
	}
}

func TestRemoteCompilerPolicyUsesPostImportSourceAndCancellation(t *testing.T) {
	var compiled string
	catalog := fabric.NewCatalog()
	catalog.Register("comp.remote", &fabric.LocationRemote{DBName: "remote", URI: "http://unused.invalid"})
	remote := fabric.NewRemoteFragmentExecutor()
	defer remote.Close()
	executor := fabric.NewFabricExecutor(catalog, nil, remote)
	stop := context.Canceled
	executor.SetRemoteStatementPolicy(func(ctx context.Context, source string) (fabric.RemoteStatementPolicy, error) {
		compiled = source
		policy, err := readRemoteStatementPolicy(ctx, source)
		require.NoError(t, err)
		require.Equal(t, storage.RemoteAccessRead, policy.AccessMode)
		return policy, stop
	})
	ctx := fabric.WithRecordBindings(t.Context(), map[string]interface{}{"value": int64(7)})
	_, err := executor.Execute(ctx, nil, &fabric.FragmentExec{Input: &fabric.FragmentInit{}, Query: "RETURN value", ScopeImports: []string{"value"}, GraphName: "comp.remote"}, nil, "")
	require.ErrorIs(t, err, stop)
	require.Contains(t, compiled, "WITH $")
	require.Equal(t, "WITH $__fabric_import_0 AS value CALL (value) { RETURN value } RETURN *", compiled)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = readRemoteStatementPolicy(canceled, "RETURN 1")
	require.ErrorIs(t, err, context.Canceled)
}
