package server

// Author: pr1826-base3 test lane (Claude Opus 5.5), 23 Nov 2026.
// SHOW USERS WITH AUTH under the ANTLR parser: reproduction, the grammar's
// verdict, and the same HTTP flow with only those statements admitted by the
// default parser (the test-side bypass of the ANTLR admission).

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
)

var pr1826b3WithAuth = []string{
	"SHOW USERS WITH AUTH YIELD user, provider, auth WHERE user = 'analyst'",
	"SHOW USER WITH AUTH YIELD user, provider WHERE user = 'analyst_new'",
	"SHOW USERS WITH AUTH",
}

// What the ANTLR grammar says about each statement, with SHOW USERS without
// WITH AUTH as the control.
func TestPR1826B3ShowUsersWithAuthGrammar(t *testing.T) {
	for _, statement := range append(append([]string{}, pr1826b3WithAuth...),
		"SHOW USERS YIELD user WHERE user = 'analyst'", "SHOW USERS") {
		_, err := antlr.ParseSyntax(t.Context(), statement, antlr.StatementEntry, false, cyphertext.AcceptedSource)
		t.Logf("PR1826B3GRAMMAR statement=%q antlrParseSyntax=%v", statement, err)
	}
}

// The upstream flow (TestFineGrainedUserProviderHTTPFlow) with every
// SHOW USER[S] WITH AUTH first run as configured (its answer logged), then run
// with the default parser selected for that statement only. Every other
// statement runs under the configured parser.
func TestPR1826B3UserProviderBypass(t *testing.T) {
	configured := config.GetParserType()
	t.Logf("PR1826B3BYPASS configured parser=%s", configured)
	server, users := setupTestServer(t)
	require.NoError(t, server.initializeFineGrainedRuntime(t.Context()))
	admin := "Bearer " + getAuthToken(t, users, "admin")
	post := func(statement string) TransactionResponse {
		t.Helper()
		response := makeRequest(t, server, http.MethodPost, "/db/system/tx/commit", TransactionRequest{
			Statements: []StatementRequest{{Statement: statement}},
		}, admin)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var result TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		return result
	}
	run := func(statement string, success bool) TransactionResponse {
		t.Helper()
		result := post(statement)
		if success {
			require.Empty(t, result.Errors, statement)
		} else {
			require.NotEmpty(t, result.Errors, statement)
		}
		return result
	}
	show := func(statement string) TransactionResponse {
		t.Helper()
		asConfigured := post(statement)
		t.Logf("PR1826B3BYPASS parser=%s statement=%q errors=%v", configured, statement, asConfigured.Errors)
		config.SetParserType(config.ParserTypeNornic)
		defer config.SetParserType(configured)
		result := run(statement, true)
		t.Logf("PR1826B3BYPASS parser=%s statement=%q rows=%v", config.GetParserType(), statement, result.Results[0].Data)
		return result
	}
	run("CREATE USER analyst SET AUTH 'oidc-work' { SET ID 'employee-12' }", true)
	query := "SHOW USERS WITH AUTH YIELD user, provider, auth WHERE user = 'analyst'"
	rows := show(query).Results[0].Data
	require.Len(t, rows, 1)
	require.Equal(t, []any{"analyst", "oidc-work", map[string]any{"id": "employee-12"}}, rows[0].Row)
	run("ALTER USER analyst REMOVE ALL AUTH", false)
	run("ALTER USER analyst REMOVE AUTH 'oidc-work' SET AUTH 'oidc-next' { SET ID 'profile-3' }", true)
	rows = show(query).Results[0].Data
	require.Len(t, rows, 1)
	require.Equal(t, []any{"analyst", "oidc-next", map[string]any{"id": "profile-3"}}, rows[0].Row)
	run("RENAME USER analyst TO analyst_new", true)
	rows = show("SHOW USER WITH AUTH YIELD user, provider WHERE user = 'analyst_new'").Results[0].Data
	require.Len(t, rows, 1)
	require.Equal(t, []any{"analyst_new", "oidc-next"}, rows[0].Row)
	run("DROP USER analyst_new", true)
}
