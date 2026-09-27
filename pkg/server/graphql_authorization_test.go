package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type graphQLAuthorizationResponse struct {
	Data struct {
		Cypher *struct {
			Rows [][]any `json:"rows"`
		} `json:"cypher"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func executeGraphQLAuthorizationRequest(t *testing.T, server *Server, token, operation, statement string) graphQLAuthorizationResponse {
	t.Helper()
	recorder := makeRequest(t, server, http.MethodPost, "/graphql", map[string]any{
		"query":     operation,
		"variables": map[string]any{"statement": statement},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, recorder.Code)

	var response graphQLAuthorizationResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	return response
}

func TestGraphQLCypherEnforcesViewerPermissions(t *testing.T) {
	server, authenticator := setupTestServer(t)
	viewerToken := getAuthToken(t, authenticator, "reader")
	store, err := server.dbManager.GetDefaultStorage()
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "graphql-authorization-protected",
		Labels: []string{"GraphQLAuthorizationProtected"},
	})
	require.NoError(t, err)

	mutation := `mutation Execute($statement: String!) {
		executeCypher(input: {statement: $statement}) { rowCount }
	}`
	response := executeGraphQLAuthorizationRequest(t, server, viewerToken, mutation, "CREATE (:DeniedMutation)")
	require.NotEmpty(t, response.Errors)
	require.Contains(t, response.Errors[0].Message, "write permission")

	query := `query Execute($statement: String!) {
		cypher(input: {statement: $statement}) { rows rowCount }
	}`
	for name, testCase := range map[string]struct {
		statement  string
		permission string
	}{
		"direct create":               {statement: "CREATE (:DeniedDirect)", permission: "write"},
		"match delete":                {statement: "MATCH (n:GraphQLAuthorizationProtected) DETACH DELETE n", permission: "write"},
		"optional match":              {statement: "OPTIONAL MATCH (n:GraphQLAuthorizationProtected) SET n.denied = true", permission: "write"},
		"unwind create":               {statement: "UNWIND [1] AS value CREATE (:DeniedUnwind {value: value})", permission: "write"},
		"with create":                 {statement: "WITH 1 AS value CREATE (:DeniedWith {value: value})", permission: "write"},
		"call dynamic":                {statement: "CALL apoc.cypher.run('CREATE (:DeniedDynamic)', {})", permission: "write"},
		"schema":                      {statement: "DROP INDEX denied_index IF EXISTS", permission: "schema"},
		"admin":                       {statement: "CALL dbms.info()", permission: "admin"},
		"admin DDL":                   {statement: "DROP DATABASE nornic", permission: "admin"},
		"out of scope USE":            {statement: "USE forbidden RETURN 1", permission: "read"},
		"out of scope lower-case USE": {statement: "use forbidden return 1", permission: "read"},
	} {
		t.Run(name, func(t *testing.T) {
			response := executeGraphQLAuthorizationRequest(t, server, viewerToken, query, testCase.statement)
			require.NotEmpty(t, response.Errors)
			require.Contains(t, response.Errors[0].Message, testCase.permission+" permission")
		})
	}

	response = executeGraphQLAuthorizationRequest(t, server, viewerToken, query,
		"MATCH (n:GraphQLAuthorizationProtected) RETURN count(n) AS count")
	require.Empty(t, response.Errors)
	require.NotNil(t, response.Data.Cypher)
	require.Equal(t, [][]any{{map[string]any{"value": float64(1)}}}, response.Data.Cypher.Rows)

	nodes, err := store.GetNodesByLabel("GraphQLAuthorizationProtected")
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	adminToken := getAuthToken(t, authenticator, "admin")
	response = executeGraphQLAuthorizationRequest(t, server, adminToken, query, "CREATE (:AllowedAdmin)")
	require.Empty(t, response.Errors)
	nodes, err = store.GetNodesByLabel("AllowedAdmin")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
}
