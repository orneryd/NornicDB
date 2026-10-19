package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestHTTPWriteProcedureStatementRollback(t *testing.T) {
	failure := errors.New("u5 HTTP rejected after write")
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name: "u5http.writefail", Signature: "u5http.writefail() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite,
		Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	}, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		_, err := exec.Execute(ctx, "CREATE (:U5HTTPWrite) RETURN 1 AS value", nil)
		if err != nil { return nil, err }
		return nil, failure
	}))
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name: "u5http.fail", Signature: "u5http.fail() :: (rejected :: INTEGER)", Mode: cypher.ProcedureModeRead,
		Returns: []cypher.ProcedureColumn{{Name: "rejected", Type: "INTEGER"}},
	}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) { return nil, failure }))
	srv, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	client := peer.Client()
	client.Timeout = 10*time.Second
	post := func(query string) TransactionResponse {
		body, err := json.Marshal(map[string]interface{}{"statements": []interface{}{map[string]interface{}{
			"statement": query, "parameters": map[string]interface{}{"statement": "CREATE (:U5HTTPWrite) RETURN 1 AS value"},
		}}})
		require.NoError(t, err)
		request, err := http.NewRequest(http.MethodPost, peer.URL+"/db/"+srv.dbManager.DefaultDatabaseName()+"/tx/commit", bytes.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		var result TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
		return result
	}
	for _, query := range []string{
		"CALL u5http.writefail() YIELD value RETURN value",
		"CALL apoc.cypher.doIt($statement,{}) YIELD value CALL u5http.fail() YIELD rejected RETURN value",
	} {
		t.Run(query, func(t *testing.T) {
			failed := post(query)
			require.NotEmpty(t, failed.Errors)
			require.Contains(t, failed.Errors[0].Message, failure.Error())
			read := post("MATCH (n:U5HTTPWrite) RETURN count(n)")
			require.Empty(t, read.Errors)
			require.Len(t, read.Results, 1)
			require.Len(t, read.Results[0].Data, 1)
			require.EqualValues(t, 0, read.Results[0].Data[0].Row[0])
		})
	}
}
