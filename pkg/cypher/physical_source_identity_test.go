package cypher

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPhysicalSourceIdentityAcrossNativeAndRemoteRows(t *testing.T) {
	for _, remoteValues := range []bool{false, true} {
		name := "native"
		if remoteValues {
			name = "transport"
		}
		t.Run(name, func(t *testing.T) {
			wire, err := os.ReadFile("../storage/testdata/remote_entity_response.json")
			require.NoError(t, err)
			sources := map[string]storage.Engine{}
			remotes := map[string]*storage.RemoteEngine{}
			for _, alias := range []string{"left", "right"} {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					if remoteValues {
						_, _ = io.Copy(w, bytes.NewReader(wire))
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []interface{}{map[string]interface{}{
						"columns": []string{"n"}, "data": []interface{}{map[string]interface{}{"row": []interface{}{map[string]interface{}{"id": "shared", "labels": []string{"N"}, "properties": map[string]interface{}{"source": alias}}}}},
					}}, "errors": []interface{}{}})
				}))
				t.Cleanup(server.Close)
				remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote_db"})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, remote.Close()) })
				sources[alias], remotes[alias] = remote, remote
			}
			var first, second, same interface{}
			if remoteValues {
				for _, alias := range []string{"left", "right"} {
					_, rows, err := remotes[alias].QueryCypher(context.Background(), "RETURN n", nil)
					require.NoError(t, err)
					entity := rows[0][0].(storage.RemoteEntityValue)
					if alias == "left" {
						first = entity
						_, again, err := remotes[alias].QueryCypher(context.Background(), "RETURN n", nil)
						require.NoError(t, err)
						same = again[0][0]
					} else {
						second = entity
					}
				}
			} else {
				composite := storage.NewCompositeEngine(sources, map[string]string{"left": "remote_db", "right": "remote_db"}, map[string]string{"left": "read", "right": "read"})
				composite.SetCompositeName("comp")
				nodes, err := composite.AllNodes()
				require.NoError(t, err)
				require.Len(t, nodes, 2)
				first, second = nodes[0], nodes[1]
				other := storage.NewCompositeEngine(sources, map[string]string{"left": "remote_db", "right": "remote_db"}, map[string]string{"left": "read", "right": "read"})
				other.SetCompositeName("other")
				for _, node := range other.GetAllNodes() {
					if node.IdentityKey() == nodes[0].IdentityKey() {
						same = node
					}
				}
				require.NotNil(t, same)
			}
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			params := map[string]interface{}{"a": first, "b": second, "same": same, "nodes": []interface{}{first, second, same}}
			result, err := executor.Execute(context.Background(), "RETURN $a=$b,$a=$same,$a IN [$b]", params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{false, true, false}}, result.Rows)
			for _, testCase := range []struct {
				right interface{}
				equal bool
			}{{second, false}, {same, true}, {second, false}} {
				result, err = executor.Execute(context.Background(), "RETURN $left=$right", map[string]interface{}{"left": first, "right": testCase.right})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.equal}}, result.Rows, "source identity survives repeated parameter/cache admission")
			}
			result, err = executor.Execute(context.Background(), "UNWIND $nodes AS n RETURN count(DISTINCT n)", params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
			result, err = executor.Execute(context.Background(), "UNWIND $nodes AS n WITH DISTINCT n RETURN n ORDER BY n", params)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			public, err := json.Marshal(result.Rows)
			require.NoError(t, err)
			require.NotContains(t, string(public), "http://")
			require.NotContains(t, string(public), "remotePhysicalSource")
		})
	}
}
