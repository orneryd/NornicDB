package server

// gh745_element_id_test.go — regression tests for #745 §2: HTTP row values
// and row meta must name the database the entity actually lives in, not a
// fixed "nornicdb".

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGh745_HTTPCompositeQueryIdentities(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	require.NoError(t, server.dbManager.CreateDatabase("identity_leaf"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("identity_root", []multidb.ConstituentRef{
		{Alias: "leaf", DatabaseName: "identity_leaf", Type: "local", AccessMode: "read_write"},
	}))
	created := makeRequest(t, server, http.MethodPost, "/db/identity_leaf/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (:Identity {name:'a'})-[:LINK {name:'r'}]->(:Identity {name:'b'})"}},
	}, token)
	var setup TransactionResponse
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &setup))
	require.Empty(t, setup.Errors)
	projection := "RETURN a, r, b, [a,r], {node:a,edge:r}, p, elementId(a), elementId(r), elementId(b), elementId(startNode(r)), elementId(endNode(r))"
	for _, endpoint := range []string{"/db/identity_root/tx/commit", "/db/identity_root/tx"} {
		for _, query := range []string{
			"USE identity_root.leaf MATCH p=(a:Identity)-[r:LINK]->(b:Identity) " + projection,
			"CALL { USE identity_root.leaf MATCH p=(a:Identity)-[r:LINK]->(b:Identity) RETURN a,r,b,p } " + projection,
		} {
			t.Run(endpoint+query, func(t *testing.T) {
				recorder := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{
					"statements": []map[string]any{{"statement": query, "resultDataContents": []string{"row", "graph"}}},
				}, token)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
				require.Empty(t, response.Errors)
				require.Len(t, response.Results, 1)
				require.Len(t, response.Results[0].Data, 1)
				data := response.Results[0].Data[0]
				require.Len(t, data.Meta, 13)
				for index := 0; index < 3; index++ {
					require.Equal(t, data.Row[6+index], data.Meta[index].(map[string]interface{})["elementId"])
				}
				require.Equal(t, data.Meta[0], data.Meta[3])
				require.Equal(t, data.Meta[1], data.Meta[4])
				require.ElementsMatch(t, []interface{}{data.Meta[0], data.Meta[1]}, data.Meta[5:7])
				require.Equal(t, []interface{}{data.Meta[0], data.Meta[1], data.Meta[2]}, data.Meta[7])
				require.Equal(t, []interface{}{nil, nil, nil, nil, nil}, data.Meta[8:])
				require.Len(t, data.Graph.Nodes, 2)
				require.Len(t, data.Graph.Relationships, 1)
				require.ElementsMatch(t, []interface{}{data.Row[6], data.Row[8]}, []interface{}{data.Graph.Nodes[0].ElementID, data.Graph.Nodes[1].ElementID})
				relationship := data.Graph.Relationships[0]
				require.Equal(t, data.Row[7], relationship.ElementID)
				require.Equal(t, data.Row[9], relationship.StartNode)
				require.Equal(t, data.Row[10], relationship.EndNode)
				if response.Commit != "" {
					rollback := makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(response.Commit, "/commit"), nil, token)
					require.Equal(t, http.StatusOK, rollback.Code)
				}
			})
		}
	}
}

func TestGh745_HTTPCanonicalRemoteEntityIdentities(t *testing.T) {
	server := &Server{}
	node := &storage.Node{ID: "4:remote:n", Properties: map[string]interface{}{"name": "a"}}
	target := &storage.Node{ID: "4:remote:m", Properties: map[string]interface{}{"name": "b"}}
	edge := &storage.Edge{ID: "5:remote:r", StartNode: node.ID, EndNode: target.ID, Properties: map[string]interface{}{"name": "r"}}
	response := &TransactionResponse{}
	server.appendStatementResult(response, &cypher.ExecuteResult{
		Columns: []string{"nodes", "edge"}, Rows: [][]interface{}{{[]*storage.Node{node, target}, edge}},
	}, "coordinator", false, []string{"row", "graph"})
	data := response.Results[0].Data[0]
	require.Equal(t, "4:remote:n", data.Meta[0].(map[string]interface{})["elementId"])
	require.Equal(t, "4:remote:m", data.Meta[1].(map[string]interface{})["elementId"])
	require.Equal(t, "5:remote:r", data.Meta[2].(map[string]interface{})["elementId"])
	require.Equal(t, "4:remote:n", data.Graph.Nodes[0].ElementID)
	require.Equal(t, "5:remote:r", data.Graph.Relationships[0].ElementID)
	require.Equal(t, "4:remote:n", data.Graph.Relationships[0].StartNode)
	require.Equal(t, "4:remote:m", data.Graph.Relationships[0].EndNode)
}

func TestGh745_HTTPElementIDUsesRequestDatabase(t *testing.T) {
	s := &Server{}
	node := &storage.Node{ID: "n1", Labels: []string{"EI"}, Properties: map[string]interface{}{"k": int64(1)}}
	edge := &storage.Edge{ID: "e1", Type: "R", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{}}

	converted := s.convertRowToNeo4jFormat([]interface{}{node, edge}, "otherdb")
	require.Len(t, converted, 2)

	nodeMap, ok := converted[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, node.Properties, nodeMap)

	edgeMap, ok := converted[1].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, edge.Properties, edgeMap)

	// The row meta echoes the same canonical element id.
	_, nodeMetadata := s.transactionHTTPValue(node, "otherdb")
	_, edgeMetadata := s.transactionHTTPValue(edge, "otherdb")
	meta := append(nodeMetadata, edgeMetadata...)
	require.Len(t, meta, 2)
	nodeMeta, ok := meta[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "4:otherdb:n1", nodeMeta["elementId"])
	require.Equal(t, "node", nodeMeta["type"])
	edgeMeta, ok := meta[1].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "5:otherdb:e1", edgeMeta["elementId"])
	require.Equal(t, "relationship", edgeMeta["type"])
}

func TestGh745_HTTPCompositeElementIDUsesConstituentDatabase(t *testing.T) {
	// A composite request's row values and meta must name the constituent
	// that actually holds each entity (#745 §3), not the composite root.
	server, _ := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("cmp_745_a"))
	require.NoError(t, server.dbManager.CreateDatabase("cmp_745_b"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("cmp_745", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "cmp_745_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "cmp_745_b", Type: "local", AccessMode: "read_write"},
	}))

	storeA, err := server.dbManager.GetStorage("cmp_745.a")
	require.NoError(t, err)
	_, err = storeA.CreateNode(&storage.Node{ID: "cmp_745_a:n1", Labels: []string{"L"}, Properties: map[string]interface{}{"k": int64(1)}})
	require.NoError(t, err)
	_, err = storeA.CreateNode(&storage.Node{ID: "cmp_745_a:n2", Labels: []string{"L"}, Properties: map[string]interface{}{"k": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, storeA.CreateEdge(&storage.Edge{ID: "cmp_745_a:e1", Type: "R", StartNode: "cmp_745_a:n1", EndNode: "cmp_745_a:n2", Properties: map[string]interface{}{}}))

	node := &storage.Node{ID: "cmp_745_a:n1", Labels: []string{"L"}, Properties: map[string]interface{}{"k": int64(1)}}
	edge := &storage.Edge{ID: "cmp_745_a:e1", Type: "R", StartNode: "cmp_745_a:n1", EndNode: "cmp_745_a:n2", Properties: map[string]interface{}{}}

	converted := server.convertRowToNeo4jFormat([]interface{}{node, edge}, "cmp_745")
	require.Len(t, converted, 2)
	nodeMap, ok := converted[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, node.Properties, nodeMap)
	edgeMap, ok := converted[1].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, edge.Properties, edgeMap)

	_, nodeMetadata := server.transactionHTTPValue(node, "cmp_745")
	_, edgeMetadata := server.transactionHTTPValue(edge, "cmp_745")
	meta := append(nodeMetadata, edgeMetadata...)
	require.Equal(t, "4:cmp_745_a:cmp_745_a:n1", meta[0].(map[string]interface{})["elementId"])
	require.Equal(t, "5:cmp_745_a:cmp_745_a:e1", meta[1].(map[string]interface{})["elementId"])

	// An entity no constituent holds falls back to the request database.
	stray := &storage.Node{ID: "nobody:n1", Labels: []string{"L"}}
	_, strayMetadata := server.transactionHTTPValue(stray, "cmp_745")
	require.Equal(t, "4:cmp_745:nobody:n1", strayMetadata[0].(map[string]interface{})["elementId"])
}
