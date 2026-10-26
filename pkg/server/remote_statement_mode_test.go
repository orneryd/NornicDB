package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteDynamicHTTPModePreservesStandardUpdateCounters(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	endpoint := httptest.NewServer(server.buildRouter())
	defer endpoint.Close()
	for _, tc := range []struct {
		name, body, mode string
		updates          bool
	}{
		{"read", "RETURN 1 AS value", "r", false},
		{"write", "CREATE (:U5HTTPMode) RETURN 1 AS value", "w", true},
		{"write without counters", "UNWIND [] AS x CREATE (:U5HTTPMode) RETURN count(*) AS value", "w", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: endpoint.URL, Database: "nornic", AuthToken: token})
			require.NoError(t, err)
			defer func() { require.NoError(t, engine.Close()) }()
			tx, err := engine.BeginCypherTx(context.Background())
			require.NoError(t, err)
			query := "CALL apoc.cypher.run($body,{}) YIELD value RETURN value"
			_, _, err = tx.QueryCypher(context.Background(), query, map[string]interface{}{"body": tc.body})
			require.NoError(t, err)
			mode, ok := tx.(storage.RemoteCypherMode)
			require.True(t, ok)
			write, known := mode.ResolvedWriteMode()
			require.True(t, known)
			require.Equal(t, tc.mode != "r", write)
			require.NoError(t, tx.Rollback(context.Background()))
			response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]interface{}{"statements": []interface{}{map[string]interface{}{"statement": query, "parameters": map[string]interface{}{"body": tc.body}, "includeStats": true}}}, token)
			require.Equal(t, http.StatusOK, response.Code)
			var result TransactionResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
			require.Empty(t, result.Errors)
			require.Len(t, result.Results, 1)
			require.Equal(t, tc.mode, result.Results[0].QueryType)
			native, err := server.db.GetCypherExecutor().Execute(context.Background(), query, map[string]interface{}{"body": tc.body})
			require.NoError(t, err)
			expected := queryStatsFromResult(native)
			require.Equal(t, expected.ContainsUpdates, result.Results[0].Stats.ContainsUpdates, "mode must preserve execution counters")
			if !tc.updates {
				require.False(t, result.Results[0].Stats.ContainsUpdates, "mode must not fabricate mutation counters")
			}
		})
	}
}
func TestRemoteHTTPModeLeavesCountersIndependent(t *testing.T) {
	for _, mode := range []string{"r", "w", "rw", "s"} {
		for _, created := range []int{0, 1} {
			result := &cypher.ExecuteResult{Metadata: map[string]interface{}{"resolved_query_type": mode}, Stats: &cypher.QueryStats{NodesCreated: created}}
			stats := queryStatsFromResult(result)
			var response TransactionResponse
			(&Server{}).appendStatementResult(&response, result, "nornic", true, nil)
			require.Equal(t, mode, response.Results[0].QueryType)
			require.Equal(t, created > 0, stats.ContainsUpdates)
			require.Equal(t, created, stats.NodesCreated)
		}
	}
}

func TestRemotePeriodicCoordinatorMetadataHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	endpoint := httptest.NewServer(server.buildRouter())
	defer endpoint.Close()
	for _, procedure := range []string{"apoc.periodic.iterate", "apoc.periodic.commit"} {
		for _, clearMetadata := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clear=%v", procedure, clearMetadata), func(t *testing.T) {
				engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: endpoint.URL, Database: "nornic", AuthToken: token})
				require.NoError(t, err)
				defer engine.Close()
				tx, err := engine.BeginCypherTx(storage.WithCoordinatorOwnedTransaction(context.Background()))
				require.NoError(t, err)
				defer tx.Rollback(context.Background())
				if clearMetadata {
					_, _, err = tx.QueryCypher(context.Background(), "CALL tx.setMetaData({})", nil)
					require.NoError(t, err)
				}
				body := "CALL " + procedure + "('UNWIND [1,2,3] AS x RETURN x','CREATE (:U5PeriodicRemote)',{batchSize:1})"
				if procedure == "apoc.periodic.commit" {
					body = "CALL apoc.periodic.commit('CREATE (:U5PeriodicRemote) RETURN 1',{})"
				}
				_, _, err = tx.QueryCypher(context.Background(), "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", map[string]interface{}{"body": body})
				require.ErrorContains(t, err, "unsupported query type in transaction")
				result, err := server.db.GetCypherExecutor().Execute(context.Background(), "MATCH (n:U5PeriodicRemote) RETURN count(n) AS count", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			})
		}
	}
}
