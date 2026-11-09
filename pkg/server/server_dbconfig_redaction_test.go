package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

// syntheticCredentialURIValue is a deliberately invalid numeric setting value
// that carries URL credentials. It is synthetic test data only; nothing here
// reads or prints real configuration.
const syntheticCredentialURIValue = "postgres://diaguser:diagpass-secret@db.example.internal:5432/nornic"

// requireNoSubmittedCredentials asserts that an admin config error response
// neither reveals the submitted credential-bearing URI nor any of its secret
// components, while remaining an informative 400.
func requireNoSubmittedCredentials(t *testing.T, response *httptest.ResponseRecorder) *TransactionResponse {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	for _, secret := range []string{
		syntheticCredentialURIValue,
		"diaguser:diagpass-secret",
		"diagpass-secret",
		"diaguser",
	} {
		require.NotContains(t, response.Body.String(), secret,
			"admin config error response leaked submitted credential material")
	}

	var body TransactionResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Len(t, body.Errors, 1)
	require.Equal(t, "Neo.ClientError.General.BadRequest", body.Errors[0].Code)
	require.NotEmpty(t, body.Errors[0].Message)
	return &body
}

// TestAdminDbConfigErrorsDoNotEchoSubmittedCredentials drives the public admin
// PUT route: both the normalization error path and the unregistered-stemmer
// path accept arbitrary operator input, so neither may echo it back.
func TestAdminDbConfigErrorsDoNotEchoSubmittedCredentials(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	t.Run("normalization error", func(t *testing.T) {
		response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
			map[string]any{"overrides": map[string]string{
				"db.nornic.query_plan_cache.max_entries": syntheticCredentialURIValue,
			}}, "Bearer "+token)
		body := requireNoSubmittedCredentials(t, response)
		// Informative, non-secret diagnostics survive redaction.
		require.Contains(t, body.Errors[0].Message, "invalid nonnegative number")
	})

	t.Run("unregistered stemmer", func(t *testing.T) {
		response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
			map[string]any{"overrides": map[string]string{
				"db.nornic.search.bm25.enabled": "true",
				"db.nornic.search.bm25.stemmer": syntheticCredentialURIValue,
			}}, "Bearer "+token)
		body := requireNoSubmittedCredentials(t, response)
		require.Contains(t, body.Errors[0].Message, "is not registered")
	})

	// The rejected requests must not have persisted the credential material.
	overrides := server.dbConfigStore.GetOverrides("nornic")
	for _, value := range overrides {
		require.NotContains(t, value, "diagpass-secret")
		require.NotContains(t, value, "diaguser")
	}
}

// TestDerivedConfigDiagnosticKeepsNonSecretContext pins that redaction of
// derived diagnostics preserves setting names and allowed-value diagnostics.
func TestDerivedConfigDiagnosticKeepsNonSecretContext(t *testing.T) {
	raw := `invalid value for db.nornic.search.bm25.warming: got ` + syntheticCredentialURIValue + ` (allowed: startup,lazy)`
	redacted := observability.RedactText(raw)
	require.NotContains(t, redacted, "diagpass-secret")
	require.NotContains(t, redacted, "diaguser")
	require.Contains(t, redacted, "invalid value for db.nornic.search.bm25.warming")
	require.Contains(t, redacted, "allowed: startup,lazy")
}
