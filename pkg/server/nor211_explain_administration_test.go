package server

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"testing"
)

// Run this public-wire regression in separate NORNICDB_PARSER=nornic and antlr
// processes. EXPLAIN returns a plan without invoking a catalog mutation.
func TestNOR211HTTPExplainDatabaseCreation(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	t.Logf("parser=%s", config.GetParserType())
	control, _ := systemProcedureResponse(t, server, admin, "system", "CREATE DATABASE nor211_http_existing", false)
	require.Empty(t, control.Errors)
	identity, created := server.dbManager.DatabaseIdentity("nor211_http_existing")
	require.NotEmpty(t, identity)
	for _, database := range []string{"system", server.dbManager.DefaultDatabaseName()} {
		for _, command := range []struct{ statement, name string }{
			{"CREATE DATABASE nor211_http_standard", "nor211_http_standard"},
			{"CREATE DATABASE nor211_http_standard IF NOT EXISTS", "nor211_http_standard"},
			{"CREATE OR REPLACE DATABASE nor211_http_existing", "nor211_http_existing"},
			{"CREATE COMPOSITE DATABASE nor211_http_comp ALIAS r FOR DATABASE nornic", "nor211_http_comp"},
		} {
			body, raw := systemProcedureResponse(t, server, admin, database, "EXPLAIN "+command.statement, false)
			require.Empty(t, body.Errors, raw)
			require.Len(t, body.Results, 1, raw)
			require.Equal(t, []string{}, body.Results[0].Columns, raw)
			require.Empty(t, body.Results[0].Data, raw)
			require.NotNil(t, body.Results[0].Plan, raw)
			if command.name != "nor211_http_existing" {
				require.False(t, server.dbManager.Exists(command.name), command.name)
				require.False(t, server.dbManager.IsCompositeDatabase(command.name), command.name)
			}
			afterIdentity, afterCreated := server.dbManager.DatabaseIdentity("nor211_http_existing")
			require.Equal(t, identity, afterIdentity, "EXPLAIN must not replace the existing database")
			require.Equal(t, created, afterCreated)
		}
	}
}

// Neo4j's database-creation commands return no columns and no records, and the
// HTTP wire keeps that empty declared schema: the created identity reaches
// access grants through the typed administration outcome, not a result row.
func TestNOR211HTTPDatabaseCreationOutput(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	t.Logf("parser=%s", config.GetParserType())
	defaultDatabase := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "nor211_http_replaced"}))
	identity, created := server.dbManager.DatabaseIdentity("nor211_http_replaced")

	for _, command := range []struct {
		database  string
		statement string
	}{
		{"system", "CREATE DATABASE nor211_http_new"},
		{"system", "CREATE DATABASE nor211_http_new IF NOT EXISTS"},
		{"system", "CREATE OR REPLACE DATABASE nor211_http_replaced"},
		{defaultDatabase, "CREATE COMPOSITE DATABASE nor211_http_composite ALIAS r FOR DATABASE nornic"},
	} {
		t.Run(command.statement, func(t *testing.T) {
			body, raw := systemProcedureResponse(t, server, admin, command.database, command.statement, false)
			require.Empty(t, body.Errors, raw)
			require.Len(t, body.Results, 1, raw)
			require.Equal(t, []string{}, body.Results[0].Columns, raw)
			require.Empty(t, body.Results[0].Data, raw)
			require.Nil(t, body.Results[0].Plan, raw)
		})
	}

	require.True(t, server.dbManager.Exists("nor211_http_new"))
	require.True(t, server.dbManager.IsCompositeDatabase("nor211_http_composite"))
	replacedIdentity, replacedCreated := server.dbManager.DatabaseIdentity("nor211_http_replaced")
	require.NotEqual(t, identity, replacedIdentity, "CREATE OR REPLACE DATABASE replaced the existing database")
	require.NotEqual(t, created, replacedCreated)
}
