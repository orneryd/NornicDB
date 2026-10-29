package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCurrentHTTPPeerPreservesPositionalProvenance(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "automatic", true: "explicit"}[explicit], func(t *testing.T) {
			server := &Server{}
			node := &storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]interface{}{}}
			other := &storage.Node{ID: "m", Labels: []string{"Doc"}, Properties: map[string]interface{}{}}
			edge := &storage.Edge{ID: "r", Type: "R", StartNode: "n", EndNode: "m", Properties: map[string]interface{}{}}
			path := &cypher.PathResult{Nodes: []*storage.Node{node, other}, Relationships: []*storage.Edge{edge}, Length: 1}
			var peer *httptest.Server
			peer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					_, _ = w.Write([]byte(`{"results":[],"errors":[]}`))
					return
				}
				if strings.HasSuffix(r.URL.Path, "/tx") {
					_ = json.NewEncoder(w).Encode(map[string]interface{}{"commit": peer.URL + "/db/neo4j/tx/1/commit", "results": []interface{}{}, "errors": []interface{}{}})
					return
				}
				var request TransactionRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				require.Len(t, request.Statements, 1)
				response := &TransactionResponse{Results: []QueryResult{}, Errors: []QueryError{}}
				result := &cypher.ExecuteResult{Columns: []string{"v", "r", "p"}, Rows: [][]interface{}{{[]interface{}{map[string]interface{}{}, node}, edge, path}}}
				server.appendStatementResult(response, result, "neo4j", false, request.Statements[0].ResultDataContents)
				require.NoError(t, json.NewEncoder(w).Encode(response))
			}))
			defer peer.Close()
			remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: peer.URL, Database: "neo4j"})
			require.NoError(t, err)
			defer remote.Close()
			var rows [][]interface{}
			if explicit {
				tx, openErr := remote.BeginCypherTx(context.Background(), storage.RemoteAccessWrite)
				require.NoError(t, openErr)
				defer tx.Rollback(context.Background())
				_, rows, err = tx.QueryCypher(context.Background(), "RETURN v", nil)
			} else {
				_, rows, err = remote.QueryCypher(context.Background(), "RETURN v", nil, storage.RemoteAccessRead)
			}
			require.NoError(t, err)
			require.Len(t, rows, 1)
			nested := rows[0][0].([]interface{})
			require.IsType(t, map[string]interface{}{}, nested[0])
			require.IsType(t, storage.RemoteEntityValue{}, nested[1])
			forwarded := rows[0][1].(storage.RemoteEntityValue)
			id, start, end := forwarded.LegacyIDs()
			require.Equal(t, server.hashStringToInt64("r"), id)
			require.Equal(t, server.hashStringToInt64("n"), start)
			require.Equal(t, server.hashStringToInt64("m"), end)
			returnedPath := rows[0][2].(storage.RemotePathValue)
			nodes, edges, valid := returnedPath.Parts()
			require.True(t, valid)
			require.Len(t, nodes, 2)
			require.Len(t, edges, 1)
		})
	}
}

func TestHTTPRelationshipGraphPreservesNumericEndpoints(t *testing.T) {
	s := &Server{}
	graph := &GraphResult{}
	edge := &storage.Edge{ID: "r", Type: "R", StartNode: "a", EndNode: "b", Properties: map[string]interface{}{}}
	_, _ = s.transactionHTTPValue(edge, "neo4j", &transactionHTTPValueState{graph: graph})
	b, err := json.Marshal(graph)
	require.NoError(t, err)
	var decoded struct {
		Relationships []map[string]interface{} `json:"relationships"`
	}
	require.NoError(t, json.Unmarshal(b, &decoded))
	require.Len(t, decoded.Relationships, 1)
	require.Equal(t, strconv.FormatInt(s.hashStringToInt64("a"), 10), decoded.Relationships[0]["startNode"])
	require.Equal(t, strconv.FormatInt(s.hashStringToInt64("b"), 10), decoded.Relationships[0]["endNode"])
}

func TestHTTPRESTMalformedPathDoesNotInventEntities(t *testing.T) {
	s := &Server{}
	state := &transactionHTTPValueState{rest: true}
	require.Nil(t, s.transactionHTTPRESTPath([]interface{}{int64(1)}, "neo4j", state))
}
