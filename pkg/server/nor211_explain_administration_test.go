package server

import (
	"github.com/orneryd/nornicdb/pkg/config"
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
			require.Equal(t, []string{"name"}, body.Results[0].Columns, raw)
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
