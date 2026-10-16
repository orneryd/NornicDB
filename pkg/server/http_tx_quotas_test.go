package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestHTTPTransactionQuotas(t *testing.T) {
	server, users := setupTestServer(t)
	_, err := users.CreateUser("quota_writer", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, "quota_writer")
	request := func(endpoint string, queries ...string) TransactionResponse {
		t.Helper()
		statements := make([]map[string]any, 0, len(queries))
		for _, query := range queries {
			statements = append(statements, map[string]any{"statement": query})
		}
		recorder := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": statements}, token)
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
		return response
	}
	for _, mode := range []string{"autocommit", "explicit"} {
		for _, tc := range []struct {
			name         string
			limits       multidb.Limits
			setup        []string
			queries      []string
			nodes, edges int64
			message      string
		}{
			{name: "nodes", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}, setup: []string{"CREATE (:Quota)"}, queries: []string{"CREATE (:Quota)"}, nodes: 1, message: "max_nodes"},
			{name: "bytes", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: 1}}, queries: []string{"CREATE (:Quota {payload: 'too large'})"}, message: "max_bytes"},
			{name: "edges", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxEdges: 1}}, setup: []string{"CREATE (a:Quota)-[:R]->(b:Quota)"}, queries: []string{"MATCH (a:Quota), (b:Quota) CREATE (a)-[:R]->(b)"}, nodes: 2, edges: 1, message: "max_edges"},
			{name: "batch", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}, queries: []string{"UNWIND [1, 2] AS id CREATE (:Quota {id: id})"}, message: "max_nodes"},
			{name: "multi", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}, queries: []string{"CREATE (:Quota {id: 1})", "CREATE (:Quota {id: 2})"}, message: "max_nodes"},
			{name: "rate", limits: multidb.Limits{Rate: multidb.RateLimits{MaxWritesPerSecond: 1}}, queries: []string{"UNWIND [1, 2] AS id CREATE (:Quota {id: id})"}, message: "max_writes_per_second"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				db := "quota_" + mode + "_" + tc.name
				require.NoError(t, server.dbManager.CreateDatabase(db))
				require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "editor", db, true, true))
				require.NoError(t, server.dbManager.SetDatabaseLimits(db, &tc.limits))
				endpoint := "/db/" + db + "/tx/commit"
				if len(tc.setup) > 0 {
					require.Empty(t, request(endpoint, tc.setup...).Errors)
				}
				if mode == "explicit" {
					opened := request(strings.TrimSuffix(endpoint, "/commit"))
					require.Empty(t, opened.Errors)
					require.NotEmpty(t, opened.Commit)
					for _, query := range tc.queries {
						require.Empty(t, request(strings.TrimSuffix(opened.Commit, "/commit"), query).Errors)
					}
					endpoint = opened.Commit
				}
				queries := tc.queries
				if mode == "explicit" {
					queries = nil
				}
				rejected := request(endpoint, queries...)
				require.NotEmpty(t, rejected.Errors)
				require.Contains(t, strings.ToLower(rejected.Errors[0].Message), tc.message)
				require.Nil(t, rejected.Receipt)
				read := request("/db/"+db+"/tx/commit", "MATCH (n:Quota) RETURN count(n)")
				require.Empty(t, read.Errors)
				require.EqualValues(t, tc.nodes, read.Results[0].Data[0].Row[0])
				engine, err := server.dbManager.GetStorage(db)
				require.NoError(t, err)
				edges, err := engine.EdgeCount()
				require.NoError(t, err)
				require.Equal(t, tc.edges, edges)
			})
		}
	}
}
