package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGDSMixedProjectionIdentityWire(t *testing.T) {
	s, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")
	db := s.dbManager.DefaultDatabaseName()
	engine, seedErr := s.dbManager.GetStorage(db)
	require.NoError(t, seedErr)
	_, seedErr = engine.CreateNode(&storage.Node{ID: "1", Labels: []string{"ReviewMixedId"}})
	require.NoError(t, seedErr)
	peer := httptest.NewServer(s.buildRouter())
	defer peer.Close()
	post := func(path, query string) TransactionResponse {
		t.Helper()
		statements := []map[string]interface{}{}
		if query != "" {
			statements = append(statements, map[string]interface{}{"statement": query})
		}
		body, err := json.Marshal(map[string]interface{}{"statements": statements})
		require.NoError(t, err)
		req, err := http.NewRequest("POST", peer.URL+path, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := peer.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		var out TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		require.Empty(t, out.Errors, "%s", query)
		return out
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, s.dbManager)
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	defer func() { _ = bs.Close(); <-done }()
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(context.Background())
	ctx := context.Background()
	for _, route := range []string{"http-auto", "http-explicit", "bolt-auto", "bolt-explicit"} {
		t.Run(route, func(t *testing.T) {
			path := "/db/" + db + "/tx/commit"
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
			defer session.Close(ctx)
			var tx neo4j.ExplicitTransaction
			if route == "http-explicit" {
				opened := post("/db/"+db+"/tx", "")
				u, err := url.Parse(opened.Commit)
				require.NoError(t, err)
				path = strings.TrimSuffix(u.Path, "/commit")
				defer post(path+"/commit", "")
			}
			if route == "bolt-explicit" {
				tx, err = session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer tx.Commit(ctx)
			}
			run := func(q string) [][]interface{} {
				t.Helper()
				if strings.HasPrefix(route, "http") {
					out := post(path, q)
					rows := [][]interface{}{}
					for _, r := range out.Results[0].Data {
						rows = append(rows, r.Row)
					}
					return rows
				}
				var result neo4j.ResultWithContext
				var err error
				if tx != nil {
					result, err = tx.Run(ctx, q, nil)
				} else {
					result, err = session.Run(ctx, q, nil)
				}
				require.NoError(t, err)
				records, err := result.Collect(ctx)
				require.NoError(t, err)
				rows := [][]interface{}{}
				for _, r := range records {
					rows = append(rows, r.Values)
				}
				return rows
			}
			graph := strings.ReplaceAll(route, "-", "_")
			run("CREATE (a:R1571 {name:'alpha'}), (b:R1571 {name:'beta'}), (c:R1571 {name:'shared'}), (a)-[:R1571]->(c), (b)-[:R1571]->(c)")
			run(fmt.Sprintf("CALL gds.graph.project('%s','R1571','R1571')", graph))
			rows := run(fmt.Sprintf("CALL gds.fastRP.stream('%s',{embeddingDimension:4}) YIELD nodeId RETURN nodeId, nodeId + ':ok', gds.util.asNode(nodeId).name", graph))
			require.NotEmpty(t, rows)
			for _, r := range rows {
				require.IsType(t, "", r[0])
				require.Equal(t, r[0].(string)+":ok", r[1])
				require.NotNil(t, r[2])
			}
			for _, proc := range []string{"adamicAdar", "commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
				rows = run(fmt.Sprintf("MATCH (a:R1571 {name:'alpha'}), (b:R1571 {name:'beta'}) WITH a,b LIMIT 1 CALL gds.linkPrediction.%s.stream('%s',{sourceNode:id(a),targetNodes:[id(b)]}) YIELD node1,node2 RETURN node1,node2,node1 + ':ok',gds.util.asNode(node2).name", proc, graph))
				require.Len(t, rows, 1)
				require.IsType(t, "", rows[0][0])
				require.IsType(t, "", rows[0][1])
				require.Equal(t, rows[0][0].(string)+":ok", rows[0][2])
				require.Equal(t, "beta", rows[0][3])
			}
			rows = run("SHOW PROCEDURES YIELD name,signature WHERE name = 'gds.fastRP.stream' OR name STARTS WITH 'gds.linkPrediction.' RETURN name,signature")
			require.Len(t, rows, 7)
			for _, r := range rows {
				require.Contains(t, r[1], " :: ANY")
				require.NotContains(t, r[1], "nodeId :: INTEGER")
				require.NotContains(t, r[1], "node1 :: INTEGER")
			}
			run(fmt.Sprintf("CALL gds.graph.drop('%s')", graph))
			ints := "ints_" + graph
			run(fmt.Sprintf("UNWIND [[1,3],[2,3],[3,4]] AS e RETURN gds.graph.project('%s',e[0],e[1])", ints))
			integerRows := run(fmt.Sprintf("CALL gds.fastRP.stream('%s',{embeddingDimension:4}) YIELD nodeId RETURN nodeId,nodeId + 1", ints))
			require.Len(t, integerRows, 4)
			checkInteger := func(value interface{}) {
				if strings.HasPrefix(route, "http") {
					require.IsType(t, float64(0), value)
				} else {
					require.IsType(t, int64(0), value)
				}
			}
			for _, r := range integerRows {
				checkInteger(r[0])
				checkInteger(r[1])
			}
			for _, proc := range []string{"adamicAdar", "commonNeighbors", "resourceAllocation", "preferentialAttachment", "jaccard", "predict"} {
				integerRows = run(fmt.Sprintf("CALL gds.linkPrediction.%s.stream('%s',{sourceNode:1,targetNodes:[2]}) YIELD node1,node2 RETURN node1,node2,node1 + 1", proc, ints))
				require.Len(t, integerRows, 1, proc)
				for _, v := range integerRows[0] {
					checkInteger(v)
				}
			}
			run(fmt.Sprintf("CALL gds.graph.drop('%s')", ints))
			types := map[string]interface{}{}
			for _, order := range []string{"[n,1]", "[1,n]"} {
				mixed := "mixed_" + graph
				run(fmt.Sprintf("MATCH (n:ReviewMixedId) UNWIND %s AS source RETURN gds.graph.project('%s',source,null)", order, mixed))
				result := run(fmt.Sprintf("CALL gds.fastRP.stream('%s',{embeddingDimension:4}) YIELD nodeId RETURN nodeId", mixed))
				require.Len(t, result, 1)
				types[order] = result[0][0]
				t.Logf("MIXED route=%s order=%s id=%#v type=%T", route, order, result[0][0], result[0][0])
				run(fmt.Sprintf("CALL gds.graph.drop('%s')", mixed))
			}
			require.Equal(t, types["[1,n]"], types["[n,1]"], "row order must not change the output ID type")
		})
	}
}
