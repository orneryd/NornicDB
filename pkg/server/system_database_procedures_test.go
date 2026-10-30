package server

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// systemProcedureResponse sends statement to database's /tx/commit endpoint,
// or through an explicit transaction (open, then commit) when explicit, and
// returns the decoded response and its raw body.
func systemProcedureResponse(t *testing.T, server *Server, token, database, statement string, explicit bool) (TransactionResponse, string) {
	t.Helper()
	request := map[string]any{"statements": []map[string]any{{"statement": statement}}}
	path := "/db/" + database + "/tx/commit"
	if explicit {
		path = "/db/" + database + "/tx"
	}
	response := makeRequest(t, server, "POST", path, request, "Bearer "+token)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	raw := response.Body.String()
	if location := response.Header().Get("Location"); explicit && location != "" {
		parsed, err := url.Parse(location)
		require.NoError(t, err)
		committed := makeRequest(t, server, "POST", parsed.Path+"/commit", map[string]any{"statements": []map[string]any{}}, "Bearer "+token)
		raw += committed.Body.String()
	}
	return body, raw
}

// The system database runs only system procedures (those marked
// WorksOnSystem), as in Neo4j: any other procedure fails with Neo4j's
// SemanticError on every route that reaches the system database, so a
// statement there never reads the records the system database keeps.
func TestSystemDatabaseRunsOnlySystemProcedures(t *testing.T) {
	server, users := setupTestServer(t)
	viewer := getAuthToken(t, users, "reader")
	// An administrator, so the system database's policy, not a missing
	// permission, is what refuses each statement.
	admin := getAuthToken(t, users, "admin")
	export := "CALL apoc.export.json.all(null, {stream: true}) YIELD data RETURN data"
	routes := []struct{ name, database, statement string }{
		{"direct", "system", export},
		{"USE system", "nornic", "USE system " + export},
		{"apoc.cypher.run USE system", "nornic", "CALL apoc.cypher.run('USE system " + strings.ReplaceAll(export, "'", "\\'") + "', {}) YIELD value RETURN value"},
		{"CALL subquery USE system", "nornic", "CALL { USE system " + export + " } RETURN data"},
		{"apoc.cypher.run on system", "system", "CALL apoc.cypher.run('RETURN 1 AS one', {}) YIELD value RETURN value"},
		{"apoc.cypher.runMany on system", "system", "CALL apoc.cypher.runMany('RETURN 1 AS one;', {}) YIELD row RETURN row"},
		{"apoc.cypher.doitall on system", "system", "CALL apoc.cypher.doitall('RETURN 1 AS one', {}) YIELD value RETURN value"},
		{"apoc.periodic.iterate on system", "system", "CALL apoc.periodic.iterate('RETURN 1 AS x', 'RETURN x', {}) YIELD batches RETURN batches"},
		{"db.create.setNodeVectorProperty on system", "system", "CALL db.create.setNodeVectorProperty(null, 'v', [1.0])"},
	}
	for _, explicit := range []bool{false, true} {
		for _, route := range routes {
			name := route.name + map[bool]string{false: " (auto-commit)", true: " (explicit transaction)"}[explicit]
			t.Run(name, func(t *testing.T) {
				body, raw := systemProcedureResponse(t, server, admin, route.database, route.statement, explicit)
				require.NotEmpty(t, body.Errors, raw)
				require.Equal(t, "Neo.ClientError.Statement.SemanticError", body.Errors[0].Code, raw)
				require.NotContains(t, raw, "password", "no system record is returned")
			})
		}
	}

	// The transaction log procedures are NornicDB's own and don't run on
	// the system database, whose log holds the records it keeps.
	for _, statement := range []string{"CALL db.txlog.entries()", "CALL db.txlog.byTxId('1')"} {
		body, raw := systemProcedureResponse(t, server, admin, "system", statement, false)
		require.NotEmpty(t, body.Errors, raw)
		require.Equal(t, "Neo.ClientError.Statement.SemanticError", body.Errors[0].Code, raw)
	}

	// System procedures run there, and see none of the records the system
	// database keeps: Neo4j's system database lists no labels or property
	// keys to db.labels() / db.propertyKeys().
	for _, call := range []struct {
		statement string
		empty     bool
	}{
		{"CALL dbms.components() YIELD name RETURN name", false},
		{"CALL db.ping() YIELD success RETURN success", false},
		{"CALL db.labels() YIELD label RETURN label", true},
		{"CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey", true},
		{"CALL db.schema.nodeTypeProperties() YIELD propertyName RETURN propertyName", true},
	} {
		t.Run(call.statement, func(t *testing.T) {
			body, raw := systemProcedureResponse(t, server, viewer, "system", call.statement, false)
			require.Empty(t, body.Errors, raw)
			require.Len(t, body.Results, 1, raw)
			if call.empty {
				require.Empty(t, body.Results[0].Data, raw)
			} else {
				require.NotEmpty(t, body.Results[0].Data, raw)
			}
		})
	}
}
