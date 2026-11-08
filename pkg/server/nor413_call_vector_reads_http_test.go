package server

// NOR-413 over HTTP: auto-commit statements that match nodes and read vectors
// through CALL answer as in an explicit transaction and as expected.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor413HTTPRun runs statement auto-commit (tx/commit) or in an explicit
// transaction (opened with the statement, then committed) and returns its
// result's rows as JSON.
func nor413HTTPRun(t *testing.T, server *Server, token string, explicit bool, statement string, params map[string]interface{}) []string {
	t.Helper()
	body := map[string]interface{}{"statements": []map[string]interface{}{{"statement": statement, "parameters": params}}}
	path := "/db/nornic/tx/commit"
	if explicit {
		path = "/db/nornic/tx"
	}
	recorder := makeRequest(t, server, http.MethodPost, path, body, token)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
	require.Empty(t, response.Errors, recorder.Body.String())
	require.Len(t, response.Results, 1, recorder.Body.String())
	if explicit {
		parts := strings.Split(response.Commit, "/")
		require.GreaterOrEqual(t, len(parts), 2, response.Commit)
		commit := makeRequest(t, server, http.MethodPost, fmt.Sprintf("/db/nornic/tx/%s/commit", parts[len(parts)-2]), map[string]interface{}{"statements": []map[string]interface{}{}}, token)
		require.Equal(t, http.StatusOK, commit.Code, commit.Body.String())
	}
	rows := make([]string, 0, len(response.Results[0].Data))
	for _, row := range response.Results[0].Data {
		encoded, err := json.Marshal(row.Row)
		require.NoError(t, err)
		rows = append(rows, string(encoded))
	}
	return rows
}

func TestNOR413HTTPVectorReadingCallStatementsKeepResults(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	store, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
	require.NoError(t, err)
	_, err = store.CreateNode(t.Context(), &storage.Node{ID: "nor413-hub", Labels: []string{"Hub"}, Properties: map[string]interface{}{"k": int64(0)}})
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		id := storage.NodeID(fmt.Sprintf("nor413-doc-%d", i))
		vector := make([]float32, 1024)
		vector[i] = 1
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"k": int64(i), "title": fmt.Sprintf("doc %d", i), "vec": []interface{}{float64(i), 1.0}}, ChunkEmbeddings: [][]float32{vector}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID("nor413-has-" + string(id)), StartNode: "nor413-hub", EndNode: id, Type: "HAS"}))
	}
	require.NoError(t, server.db.BuildSearchIndexes(context.Background()))

	axis3 := make([]interface{}, 1024)
	for i := range axis3 {
		axis3[i] = 0.0
	}
	axis3[3] = 1.0
	params := map[string]interface{}{"q": axis3, "p": []interface{}{3.0, 1.0}}
	// A statement without MATCH never read nodes without vectors: it is the
	// retrieval's reference answer.
	retrieveOracle := nor413HTTPRun(t, server, token, false, "CALL db.retrieve({query: 'doc', embedding: $q, limit: 4}) YIELD node, score RETURN 0, node.title, score ORDER BY score DESC, node.title", params)
	require.NotEmpty(t, retrieveOracle)
	for _, tc := range []struct {
		name, query string
		want        []string
	}{
		{"call-subquery", "MATCH (h:Hub {k: 0}) CALL { WITH h MATCH (h)-[:HAS]->(d:Doc) WITH d ORDER BY d.k RETURN collect(d.title) AS titles } RETURN h.k, titles",
			[]string{`[0,["doc 0","doc 1","doc 2","doc 3"]]`}},
		{"read-procedure", "MATCH (:Hub)-[:HAS]->(d:Doc {k: 3}) CALL db.labels() YIELD label WHERE label = 'Doc' RETURN d.title, d.has_embedding, label",
			[]string{`["doc 3",true,"Doc"]`}},
		{"property-vector", "MATCH (n:Doc {k: 3}) CALL { WITH n RETURN n.vec AS vec, vector.similarity.cosine(n.vec, $p) AS sim } RETURN n.title, vec, sim > 0.999999",
			[]string{`["doc 3",[3,1],true]`}},
		{"whole-node", "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL { WITH n RETURN n AS m } RETURN m.title, m.vec, m.has_embedding, properties(m) = properties(n)",
			[]string{`["doc 3",[3,1],true,true]`}},
		{"whole-node-value", "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL { WITH n RETURN n AS m } RETURN m", nil},
		{"retrieve", "MATCH (h:Hub {k: 0}) CALL db.retrieve({query: 'doc', embedding: $q, limit: 4}) YIELD node, score RETURN h.k, node.title, score ORDER BY score DESC, node.title", retrieveOracle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			autocommit := nor413HTTPRun(t, server, token, false, tc.query, params)
			if tc.want != nil {
				require.Equal(t, tc.want, autocommit)
			} else {
				require.NotEmpty(t, autocommit)
			}
			require.Equal(t, autocommit, nor413HTTPRun(t, server, token, true, tc.query, params), "auto-commit answers as a transaction does")
		})
	}
}
