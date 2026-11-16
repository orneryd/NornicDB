package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

const httpNilResultProcedureName = "custom.nilResultHTTP"

// A registered procedure whose handler returns no result at all answers an
// HTTP /tx request with its declared, empty result: the transport serializes
// columns and no rows instead of dereferencing a missing result. The same
// request carries a row-returning control statement in the same batch.
//
// The server package cannot reach the cypher package's scoped registration
// cleanup, so this transport test keeps the existing serialized convention:
// ClearUserProcedures removes every user procedure at the end of the test.
func TestHTTPProcedureNilResultServesEmptyResult(t *testing.T) {
	t.Cleanup(cypher.ClearUserProcedures)
	require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name:      httpNilResultProcedureName,
		Signature: httpNilResultProcedureName + "() :: (value :: INTEGER)",
		Mode:      cypher.ProcedureModeRead,
		MinArgs:   0,
		MaxArgs:   0,
	}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
		return nil, nil
	}))

	server, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")

	response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL " + httpNilResultProcedureName + "()"},
			{"statement": "CALL " + httpNilResultProcedureName + "() YIELD value RETURN value"},
			{"statement": "RETURN 1 AS control"},
		},
	}, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Empty(t, body.Errors)
	require.Len(t, body.Results, 3)

	require.Equal(t, []string{"value"}, body.Results[0].Columns)
	require.Empty(t, body.Results[0].Data, "the empty procedure result carries no rows")

	require.Equal(t, []string{"value"}, body.Results[1].Columns)
	require.Empty(t, body.Results[1].Data, "YIELD of the empty procedure result carries no rows")

	// A row-returning statement in the same batch keeps its own row; the JSON
	// transport decodes its number as float64.
	require.Equal(t, []string{"control"}, body.Results[2].Columns)
	require.Len(t, body.Results[2].Data, 1)
	require.Equal(t, []interface{}{float64(1)}, body.Results[2].Data[0].Row)
}
