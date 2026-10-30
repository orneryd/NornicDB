package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFabricRemoteEntityGraphFunctions(t *testing.T) {
	wire, err := os.ReadFile("../storage/testdata/remote_entity_response.json")
	require.NoError(t, err)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, _ := io.ReadAll(r.Body)
		if strings.HasSuffix(r.URL.Path, "/tx") && strings.Contains(string(body), `"statements":[]`) {
			_, _ = w.Write([]byte(`{"commit":"` + server.URL + r.URL.Path + `/commit","errors":[]}`))
			return
		}
		if r.Method == http.MethodDelete {
			_, _ = w.Write([]byte(`{"results":[],"errors":[]}`))
			return
		}
		_, _ = w.Write(wire)
	}))
	t.Cleanup(server.Close)
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	mgr, err := multidb.NewDatabaseManager(t.Context(), base, &multidb.Config{
		RemoteEngineFactory: func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "remote_graph", []multidb.ConstituentRef{{Alias: "source", DatabaseName: "remote", Type: "remote", AccessMode: "read_write", URI: server.URL, AuthMode: "oidc_forwarding"}}))
	store, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	for _, tt := range []struct {
		expression string
		want       interface{}
	}{
		{"labels(node)", []interface{}{"Doc"}},
		{"type(relationship)", "LINKS"},
		{"elementId(node)", "4:remote:node"},
		{"elementId(relationship)", "5:remote:rel"},
		{"properties(node)", map[string]interface{}{"id": "local-node", "name": "Remote"}},
		{"properties(relationship)", map[string]interface{}{"id": "local-rel", "weight": int64(2)}},
		{"keys(node)", []interface{}{"id", "name"}},
		{"keys(relationship)", []interface{}{"id", "weight"}},
		{"id(node)", nil}, // HTTP has always preserved only elementId at top level.
		{"id(relationship)", nil},
		{"startNode(relationship)", nil},
		{"endNode(relationship)", nil},
	} {
		t.Run(tt.expression, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), `USE remote_graph CALL { USE remote_graph.source MATCH (node)-[relationship]->() RETURN node, relationship, {} AS ordinary } RETURN `+tt.expression+` AS value`, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, tt.want, result.Rows[0][0])
		})
	}

}

func TestRemoteHTTPGraphValueParameterBinding(t *testing.T) {
	wire, err := os.ReadFile("../storage/testdata/remote_entity_response.json")
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(wire)
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN graph values", nil, storage.RemoteAccessRead)
	require.NoError(t, err)
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, query := range []string{
		"RETURN elementId($entity) AS id",
		"WITH $entity AS entity RETURN elementId(entity) AS id",
		"WITH $nested AS nested RETURN elementId(nested.entity) AS id",
		"WITH $entities AS entities UNWIND entities AS entity RETURN elementId(entity) AS id",
		"UNWIND $entities AS entity RETURN elementId(entity) AS id",
		"RETURN elementId($nested.entity) AS id",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), query, map[string]interface{}{"entity": rows[0][0], "entities": []interface{}{rows[0][0]}, "nested": map[string]interface{}{"entity": rows[0][0]}})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, "4:remote:node", result.Rows[0][0])
		})
	}
}

func TestRemoteEntityCacheIdentity(t *testing.T) {
	wire, err := os.ReadFile("../storage/testdata/remote_entity_response.json")
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(wire)
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN graph values", nil, storage.RemoteAccessRead)
	require.NoError(t, err)
	node := rows[0][0].(storage.RemoteEntityValue)
	ordinary := map[string]interface{}(node)
	public := storage.PublicRemoteMap(ordinary)
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	result, err := exec.Execute(context.Background(), "WITH $m AS m RETURN properties(m),m{.*},m[$key]", map[string]interface{}{"m": ordinary, "key": "\x00remoteIdentity"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{public, public, nil}}, result.Rows)
	user := map[string]interface{}{"\x00remoteIdentity": "user"}
	result, err = exec.Execute(context.Background(), "RETURN $m[$key]", map[string]interface{}{"m": user, "key": "\x00remoteIdentity"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"user"}}, result.Rows)

	for _, reverse := range []bool{false, true} {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		values := []interface{}{node, ordinary}
		wants := []interface{}{[]interface{}{"id", "name"}, []interface{}{"elementId", "labels", "properties"}}
		if reverse {
			values[0], values[1] = values[1], values[0]
			wants[0], wants[1] = wants[1], wants[0]
		}
		for index, value := range values {
			for run := 0; run < 2; run++ {
				result, err := exec.Execute(context.Background(), "RETURN keys($entity) AS value", map[string]interface{}{"entity": value})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{wants[index]}}, result.Rows)
			}
		}
	}
}

func TestRemoteHTTPGraphValueParameterFunctions(t *testing.T) {
	wire, err := os.ReadFile("../storage/testdata/remote_entity_response.json")
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(wire)
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN graph values", nil, storage.RemoteAccessRead)
	require.NoError(t, err)
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, tt := range []struct {
		expression string
		want       interface{}
	}{
		{"labels($node)", []interface{}{"Doc"}}, {"type($relationship)", "LINKS"},
		{"elementId($node)", "4:remote:node"}, {"elementId($relationship)", "5:remote:rel"},
		{"id($node)", nil}, {"id($relationship)", nil},
		{"keys($node)", []interface{}{"id", "name"}}, {"keys($relationship)", []interface{}{"id", "weight"}},
		{"properties($node)", map[string]interface{}{"id": "local-node", "name": "Remote"}},
		{"properties($relationship)", map[string]interface{}{"id": "local-rel", "weight": int64(2)}},
		{"startNode($relationship)", nil}, {"endNode($relationship)", nil},
	} {
		t.Run(tt.expression, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), "RETURN "+tt.expression+" AS value", map[string]interface{}{"node": rows[0][0], "relationship": rows[0][1]})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tt.want}}, result.Rows)
		})
	}
}
