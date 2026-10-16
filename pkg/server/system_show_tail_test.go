package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPSystemShowTailReturnsNoGraphDataOrClauseResults(t *testing.T) {
	server, users := setupTestServer(t)
	token := getAuthToken(t, users, "admin")
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	const marker = "system-show-tail-private-value"
	_, err = system.CreateNode(&storage.Node{
		ID: "show-tail-user", Labels: []string{"_User"},
		Properties: map[string]interface{}{"username": "show-tail-user", "password_hash": marker},
	})
	require.NoError(t, err)
	for _, explicit := range []bool{false, true} {
		for _, route := range []struct{ database, prefix string }{
			{"system", ""},
			{"nornic", "USE system "},
		} {
			for _, statement := range []string{
				"SHOW PROCEDURES YIELD name RETURN name, COUNT { (u:_User) } AS c LIMIT 1",
				"SHOW PROCEDURES YIELD name RETURN name, COLLECT { MATCH (u:_User) RETURN u.password_hash } AS hashes LIMIT 1",
				"SHOW SETTINGS YIELD name RETURN name, [(u:_User)-->() | u.password_hash] AS hashes LIMIT 1",
				"SHOW PROCEDURES YIELD name WITH name LIMIT 1 RETURN [(u:_User) | u.password_hash] AS hashes",
				"SHOW SETTINGS YIELD name WITH name LIMIT 1 RETURN EXISTS { (u:_User) WHERE u.password_hash <> '' } AS e",
				"SHOW FUNCTIONS YIELD name UNWIND [1] AS x RETURN x",
				"SHOW PROCEDURES YIELD name CALL { RETURN 1 AS one } RETURN one",
				"SHOW PROCEDURES YIELD name CREATE (:ShowTailWrite) RETURN name",
			} {
				body, raw := systemProcedureResponse(t, server, token, route.database, route.prefix+statement, explicit)
				require.Len(t, body.Errors, 1, raw)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", body.Errors[0].Code, statement)
				require.Empty(t, body.Results, statement)
				require.NotContains(t, raw, marker)
			}
			// A valid USE of another database in an explicit transaction is
			// refused by the transaction rule; the invalid tails above still
			// report their syntax errors before that rule applies.
			if explicit && route.database != "system" {
				continue
			}
			body, raw := systemProcedureResponse(t, server, token, route.database, route.prefix+"SHOW PROCEDURES YIELD name ORDER BY name LIMIT 1 RETURN name", explicit)
			require.Empty(t, body.Errors, raw)
			require.Len(t, body.Results, 1)
			require.Len(t, body.Results[0].Data, 1)
		}
	}
	written, err := system.GetNodesByLabel("ShowTailWrite")
	require.NoError(t, err)
	require.Empty(t, written, "no continuation clause ran")
	user, err := system.GetNode("show-tail-user")
	require.NoError(t, err)
	require.Equal(t, marker, user.Properties["password_hash"])
}
