package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestHTTPReadOnlyRoleCannotMutateAfterMatch(t *testing.T) {
	server, authenticator := setupTestServer(t)
	adminToken := getAuthToken(t, authenticator, "admin")
	viewerToken := getAuthToken(t, authenticator, "reader")

	request := func(token, statement string) TransactionResponse {
		recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		return response
	}
	requireForbidden := func(t *testing.T, response TransactionResponse) {
		t.Helper()
		require.Len(t, response.Errors, 1)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code)
	}

	seed := request(adminToken, "CREATE (:SecurityProbe {value: 'before'})")
	require.Empty(t, seed.Errors)

	t.Run("direct write control", func(t *testing.T) {
		requireForbidden(t, request(viewerToken, "CREATE (:SecurityProbe {value: 'unauthorized'})"))
	})

	for name, statement := range map[string]string{
		"MATCH SET":           "MATCH (n:SecurityProbe) SET n.value = 'after' RETURN n.value",
		"MATCH SET newline":   "MATCH (n:SecurityProbe) SET\nn.value = 'after' RETURN n.value",
		"MATCH REMOVE tab":    "MATCH (n:SecurityProbe) REMOVE\tn.value RETURN n",
		"MATCH DETACH DELETE": "MATCH (n:SecurityProbe) DETACH DELETE n",
		"OPTIONAL MATCH SET":  "OPTIONAL MATCH (n:SecurityProbe) SET n.value = 'after' RETURN n.value",
		"UNWIND CREATE":       "UNWIND [1] AS value CREATE (:SecurityProbe {value: value})",
		"WITH CREATE":         "WITH 1 AS value CREATE (:SecurityProbe {value: value})",
		"CALL write procedure": "CALL db.create.setNodeVectorProperty(" +
			"'missing-node', 'embedding', [1.0])",
	} {
		t.Run(name, func(t *testing.T) {
			requireForbidden(t, request(viewerToken, statement))
		})
	}

	read := request(viewerToken, "MATCH (n:SecurityProbe) RETURN n.value")
	require.Empty(t, read.Errors)
	require.Len(t, read.Results, 1)
	require.Len(t, read.Results[0].Data, 1)
	require.Equal(t, []interface{}{"before"}, read.Results[0].Data[0].Row)
}

func TestHTTPReadOnlyRoleCannotMutateInExplicitTransaction(t *testing.T) {
	server, authenticator := setupTestServer(t)
	viewerToken := getAuthToken(t, authenticator, "reader")
	authorization := "Bearer " + viewerToken

	openTransaction := func() string {
		recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{
			"statements": []map[string]any{},
		}, authorization)
		require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())

		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		parts := strings.Split(response.Commit, "/")
		require.GreaterOrEqual(t, len(parts), 2)
		return parts[len(parts)-2]
	}
	requireForbidden := func(response TransactionResponse) {
		require.Len(t, response.Errors, 1)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code)
	}

	t.Run("open statement", func(t *testing.T) {
		recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{
			"statements": []map[string]any{{"statement": "MATCH (n) SET n.value = 'unauthorized'"}},
		}, authorization)
		require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		requireForbidden(response)

		parts := strings.Split(response.Commit, "/")
		require.GreaterOrEqual(t, len(parts), 2)
		txID := parts[len(parts)-2]
		rollback := makeRequest(t, server, http.MethodDelete, fmt.Sprintf("/db/nornic/tx/%s", txID), nil, authorization)
		require.Equal(t, http.StatusOK, rollback.Code, rollback.Body.String())
	})

	t.Run("execute statement", func(t *testing.T) {
		txID := openTransaction()
		recorder := makeRequest(t, server, http.MethodPost, fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]any{
			"statements": []map[string]any{{"statement": "MATCH (n) SET n.value = 'unauthorized'"}},
		}, authorization)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		requireForbidden(response)

		rollback := makeRequest(t, server, http.MethodDelete, fmt.Sprintf("/db/nornic/tx/%s", txID), nil, authorization)
		require.Equal(t, http.StatusOK, rollback.Code, rollback.Body.String())
	})

	t.Run("commit statement", func(t *testing.T) {
		txID := openTransaction()
		recorder := makeRequest(t, server, http.MethodPost, fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]any{
			"statements": []map[string]any{{"statement": "MATCH (n) DETACH DELETE n"}},
		}, authorization)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		requireForbidden(response)
	})

}

func TestHTTPQueryPermissionModesMatchBolt(t *testing.T) {
	server, authenticator := setupTestServer(t)
	_, err := authenticator.CreateUser("editor", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)

	for name, testCase := range map[string]struct {
		username  string
		statement string
	}{
		"schema requires schema permission": {
			username:  "editor",
			statement: "CREATE INDEX security_probe FOR (n:SecurityProbe) ON (n.value)",
		},
		"DBMS procedure requires admin permission": {
			username:  "reader",
			statement: "CALL dbms.components()",
		},
	} {
		t.Run(name, func(t *testing.T) {
			token := getAuthToken(t, authenticator, testCase.username)
			recorder := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
				"statements": []map[string]any{{"statement": testCase.statement}},
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

			var response TransactionResponse
			require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
			require.Len(t, response.Errors, 1)
			require.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code)
		})
	}
}
