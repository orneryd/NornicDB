package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPExplicitCommitKeepsInstalledConstraintOwner(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(path string, statements ...string) TransactionResponse {
		list := make([]map[string]any, 0, len(statements))
		for _, statement := range statements {
			list = append(list, map[string]any{"statement": statement})
		}
		recorder := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": list}, token)
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response), recorder.Body.String())
		return response
	}
	// Stage before DDL so statement-time validation cannot mask the commit
	// check. The pending node is invisible to the DDL's existing-data scan.
	open := post("/db/nornic/tx", "CREATE (:K {replacement:2})")
	require.Empty(t, open.Errors)
	require.Contains(t, open.Commit, "/db/")
	require.Empty(t, post("/db/nornic/tx/commit", "CREATE CONSTRAINT owner_key FOR (n:K) REQUIRE (n.required) IS NODE KEY").Errors)
	_, err := server.db.GetCypherExecutor().Execute(context.Background(), "CREATE (:K {replacement:2})", nil)
	require.Error(t, err)
	var violation *storage.ConstraintViolationError
	require.True(t, errors.As(err, &violation))
	require.Equal(t, []string{"required"}, violation.Properties)
	violation.Properties[0] = "replacement"
	response := post(open.Commit[strings.Index(open.Commit, "/db/"):])
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", response.Errors[0].Code)
	valid := post("/db/nornic/tx", "CREATE (:K {required:2})")
	require.Empty(t, valid.Errors)
	require.Empty(t, post(valid.Commit[strings.Index(valid.Commit, "/db/"):]).Errors)
	store, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	nodes, err := store.GetNodesByLabel("K")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
}
