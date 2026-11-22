package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

// Expected headers were observed on a fresh Neo4j 5.26.30 community reference.
// This verifies real authenticated HTTP metadata and nonexecution, separately
// from the embedded executor tests.
func TestNOR335HTTPExplainAdministrativeMetadataAndNoEffects(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			server, authenticator := setupTestServer(t)
			auth := "Bearer " + getAuthToken(t, authenticator, "admin")
			run := func(t *testing.T, query string) TransactionResponse {
				t.Helper()
				response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": query}}}, auth)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				var body TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				t.Logf("query=%q errors=%v", query, body.Errors)
				return body
			}
			for _, test := range []struct {
				name, query string
				columns     []string
			}{
				{"procedures", "SHOW PROCEDURES", []string{"name", "description", "mode", "worksOnSystem"}},
				{"procedures_projected", "SHOW PROCEDURES YIELD name RETURN name", []string{"name"}},
				{"functions_projected", "SHOW FUNCTIONS YIELD name", []string{"name"}},
				{"indexes", "SHOW INDEXES", []string{"id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount"}},
				{"users_control", "SHOW USERS", []string{"user", "roles", "passwordChangeRequired", "suspended", "home"}},
			} {
				t.Run(test.name, func(t *testing.T) {
					t.Run("ordinary_control", func(t *testing.T) {
						body := run(t, test.query)
						require.Empty(t, body.Errors)
						require.Len(t, body.Results, 1)
						require.Equal(t, test.columns, body.Results[0].Columns)
					})
					t.Run("explain", func(t *testing.T) {
						body := run(t, "EXPLAIN "+test.query)
						require.Empty(t, body.Errors)
						require.Len(t, body.Results, 1)
						require.Equal(t, test.columns, body.Results[0].Columns)
						require.Empty(t, body.Results[0].Data)
						require.NotEmpty(t, body.Results[0].Plan)
					})
				})
			}
			absent := func(t *testing.T) {
				body := run(t, "SHOW INDEXES YIELD name WHERE name='nor335_http_i' RETURN name")
				require.Empty(t, body.Errors)
				require.Len(t, body.Results, 1)
				require.Empty(t, body.Results[0].Data)
			}
			t.Run("schema_ddl_no_effect", func(t *testing.T) {
				absent(t)
				t.Cleanup(func() { absent(t) })
				body := run(t, "EXPLAIN CREATE INDEX nor335_http_i FOR (n:NOR335HTTP) ON (n.p)")
				require.Empty(t, body.Errors)
				require.Len(t, body.Results, 1)
				require.Empty(t, body.Results[0].Columns)
				require.Empty(t, body.Results[0].Data)
				require.NotEmpty(t, body.Results[0].Plan)
			})
			for _, query := range []string{"EXPLAIN SHOW PROCEDURES YIELD noSuchColumn RETURN noSuchColumn", "EXPLAIN CREATE INDEX nor335_invalid FOR (n:NOR335HTTP) ON ()"} {
				t.Run(query, func(t *testing.T) {
					t.Cleanup(func() {
						body := run(t, "SHOW INDEXES YIELD name WHERE name='nor335_invalid' RETURN name")
						require.Empty(t, body.Errors)
						require.Len(t, body.Results, 1)
						require.Empty(t, body.Results[0].Data)
					})
					body := run(t, query)
					require.NotEmpty(t, body.Errors)
				})
			}
		})
	}
}

// Reuse the established actual HTTP privilege-state control on both parser
// settings: EXPLAIN creates no grant, while PROFILE executes the command only
// for its admitted administrator.
func TestNOR335HTTPAdministrationExplainNoPrivilegeEffect(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			TestExplainAndProfileOfAdministrationCommands(t)
		})
	}
}
