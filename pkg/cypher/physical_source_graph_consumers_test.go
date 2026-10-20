package cypher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Both actual HTTP sources deliberately reuse every local node/relationship ID
// and the same database name. Public consumers must keep their graphs apart.
func TestPhysicalSourceGraphConsumers(t *testing.T) {
	var constituents []multidb.ConstituentRef
	for _, alias := range []string{"left", "right"} {
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if request.Method == http.MethodDelete {
				_, _ = w.Write([]byte(`{"results":[],"errors":[]}`))
				return
			}
			var body struct {
				Statements []struct {
					Statement  string                 `json:"statement"`
					Parameters map[string]interface{} `json:"parameters"`
				} `json:"statements"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				http.Error(w, "invalid test request", http.StatusBadRequest)
				return
			}
			if len(body.Statements) == 0 {
				response := map[string]interface{}{"results": []interface{}{}, "errors": []interface{}{}}
				if strings.HasSuffix(request.URL.Path, "/tx") {
					response["commit"] = server.URL + request.URL.Path + "/fixture/commit"
				}
				_ = json.NewEncoder(w).Encode(response)
				return
			}
			if len(body.Statements) != 1 {
				http.Error(w, "invalid test request", http.StatusBadRequest)
				return
			}
			statement := body.Statements[0].Statement
			id, _ := body.Statements[0].Parameters["id"].(string)
			rows := []interface{}{}
			column := "n"
			add := func(value interface{}) { rows = append(rows, map[string]interface{}{"row": []interface{}{value}}) }
			node := func(id string) map[string]interface{} {
				return map[string]interface{}{"id": id, "labels": []string{"N"}, "properties": map[string]interface{}{"source": alias, "id": id}}
			}
			edge := func(start, end string) map[string]interface{} {
				return map[string]interface{}{"id": start + "-" + end, "type": "R", "startNodeElementId": start, "endNodeElementId": end, "properties": map[string]interface{}{"source": alias, "w": 1}}
			}
			switch {
			case statement == "MATCH (n:`_ProcedureCatalog`) RETURN n":
				// The remote graph contains no stored procedure definitions.
			case strings.Contains(statement, "RETURN r"):
				column = "r"
				outgoing := strings.Contains(statement, "(n)-[r]->()")
				incoming := strings.Contains(statement, "()-[r]->(n)")
				for _, pair := range [][2]string{{"shared", "bridge"}, {"bridge", "target"}} {
					if (outgoing && pair[0] != id) || (incoming && pair[1] != id) {
						continue
					}
					add(edge(pair[0], pair[1]))
				}
			case statement == "MATCH (n) WHERE n.id = $id RETURN n LIMIT 1":
				for _, key := range []string{"shared", "bridge", "target"} {
					if id == key {
						add(node(key))
					}
				}
			case statement == "MATCH (n) RETURN n":
				for _, key := range []string{"shared", "bridge", "target"} {
					add(node(key))
				}
			default:
				t.Errorf("unexpected storage query %q", statement)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"results": []interface{}{map[string]interface{}{"columns": []string{column}, "data": rows}}, "errors": []interface{}{}})
		}))
		t.Cleanup(server.Close)
		constituents = append(constituents, multidb.ConstituentRef{Alias: alias, DatabaseName: "remote_db", URI: server.URL, Type: "remote", AccessMode: "read"})
	}
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	manager, err := multidb.NewDatabaseManager(base, &multidb.Config{
        RemoteEngineFactory: func(ref multidb.ConstituentRef, token string) (storage.Engine, error) {
            return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: token})
        },
    })
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateCompositeDatabase("comp", constituents))
	composite, err := manager.GetStorage("comp")
	require.NoError(t, err)
	params := make(map[string]interface{})
	for _, alias := range []string{"left", "right"} {
		for _, id := range []string{"shared", "bridge", "target"} {
			node, err := composite.GetNode(storage.NodeID("4:comp." + alias + ":" + id))
			require.NoError(t, err)
			params[alias+"_"+id] = node
		}
	}
	params["roots"] = []interface{}{params["left_shared"], params["right_shared"]}
	selected, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	executor := NewStorageExecutor(selected)
	executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := context.Background()
	execute := func(t *testing.T, query string) *ExecuteResult {
		t.Helper()
		result, err := executor.Execute(ctx, "USE comp "+query, params)
		require.NoError(t, err, query)
		public, err := json.Marshal(result.Rows)
		require.NoError(t, err)
		require.NotContains(t, string(public), "http://")
		require.NotContains(t, string(public), "\\u0000")
		return result
	}
	for _, procedure := range []string{"dijkstra", "aStar", "allSimplePaths"} {
		t.Run(procedure, func(t *testing.T) {
			tail := "'R>','w'"
			if procedure == "allSimplePaths" {
				tail = "'R>',3"
			}
			result := execute(t, "CALL apoc.algo."+procedure+"($left_shared,$right_target,"+tail+") YIELD path RETURN path")
			require.Empty(t, result.Rows)
			result = execute(t, "CALL apoc.algo."+procedure+"($left_shared,$left_target,"+tail+") YIELD path RETURN path")
			require.Equal(t, [][]interface{}{{[]storage.NodeID{"4:comp.left:shared", "4:comp.left:bridge", "4:comp.left:target"}}}, result.Rows)
		})
	}
	for _, procedure := range []string{"expandConfig", "spanningTree"} {
		result := execute(t, "CALL apoc.path."+procedure+"($roots,{relationshipFilter:'R>',minLevel:2,maxLevel:2}) YIELD path RETURN count(DISTINCT path)")
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	}
	result := execute(t, "CALL apoc.path.subgraphAll($roots,{relationshipFilter:'R>',maxLevel:2}) YIELD nodes,relationships RETURN size(nodes),size(relationships)")
	require.Equal(t, [][]interface{}{{int64(6), int64(4)}}, result.Rows)
	for _, query := range []string{
		"UNWIND $roots AS root CALL apoc.neighbors.tohop(root,'R>',1) YIELD node RETURN elementId(root),elementId(node) ORDER BY elementId(root)",
		"UNWIND $roots AS root CALL apoc.neighbors.byhop(root,'R>',1) YIELD nodes UNWIND nodes AS node RETURN elementId(root),elementId(node) ORDER BY elementId(root)",
	} {
		require.Equal(t, [][]interface{}{{"4:comp.left:shared", "4:comp.left:bridge"}, {"4:comp.right:shared", "4:comp.right:bridge"}}, execute(t, query).Rows)
	}
	result = execute(t, "RETURN apoc.node.degree($left_shared),apoc.nodes.connected($left_shared,$left_bridge),apoc.nodes.connected($left_shared,$right_bridge),gds.alpha.linkprediction.commonNeighbors($left_shared,$left_target),gds.alpha.linkprediction.commonNeighbors($left_shared,$right_target)")
	require.Equal(t, [][]interface{}{{int64(1), true, false, float64(1), float64(0)}}, result.Rows)
	for _, procedure := range []string{"commonNeighbors", "adamicAdar", "resourceAllocation", "jaccard", "predict"} {
		result := execute(t, "CALL gds.linkPrediction."+procedure+".stream({sourceNode:$left_shared,algorithm:'common_neighbors',topologyWeight:1.0,semanticWeight:0.0}) YIELD node1,node2,score RETURN node1,node2")
		require.Equal(t, [][]interface{}{{"4:comp.left:shared", "4:comp.left:target"}}, result.Rows)
	}
}
