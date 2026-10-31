package cypher

// The statement-kind contract: classifyStatement alone decides whether a
// statement is a graph query or a command, which command it is and which
// permission it needs. These public-path checks cover, on both parsers, the
// command kinds earlier corrections got wrong: the catalog commands' owner
// diagnostics, pre-name DROP … IF EXISTS, USE-prefixed schema commands,
// CREATE DATABASE … OPTIONS as a write, and SHOW commands with tails, which
// must not enter the graph scope compiler.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Catalog, SHOW and DROP commands are read by their command owners on either
// parser, with the messages the default parser always gave.
func TestStatementKindCommandOwnerDiagnostics(t *testing.T) {
	cases := []struct{ statement, message string }{
		{"DROP DATABASE", "invalid DROP DATABASE syntax: database name expected"},
		{"CREATE DATABASE", "invalid CREATE DATABASE syntax: database name expected"},
		{"ALTER DATABASE tenant_a SET LIMT max_nodes = 1", "invalid ALTER DATABASE syntax: SET LIMIT clause expected"},
		{"CREATE ALIAS a FOR DATABASE", "invalid CREATE ALIAS syntax: database name expected"},
		{"DROP ALIAS", "invalid DROP ALIAS syntax: alias name expected"},
		{"CREATE COMPOSITE DATABASE", "invalid CREATE COMPOSITE DATABASE syntax: database name expected"},
		{"DROP COMPOSITE DATABASE", "invalid DROP COMPOSITE DATABASE syntax: database name expected"},
		{"ALTER COMPOSITE DATABASE", "invalid ALTER COMPOSITE DATABASE syntax: database name expected"},
		{"SHOW WHATEVER", "unsupported query type: SHOW"},
		{"DROP USER", "invalid DROP clause: DROP USER"},
		{"SHOW INDEXES YIELD", "invalid SHOW command: expected WHERE, or YIELD"},
		{"USE system DROP DATABASE foo", localization.CypherCommandRoutingUseAdministrationCommand().Fallback},
	}
	forEachParser(t, func(t *testing.T) {
		_, exec, manager := newDatabaseCommandFixture(t)
		exec.SetDatabaseManager(manager)
		for _, tc := range cases {
			_, err := exec.Execute(t.Context(), tc.statement, nil)
			require.Error(t, err, tc.statement)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError", tc.statement)
			require.Contains(t, err.Error(), tc.message, tc.statement)
		}
	})
}

// Schema commands NornicDB reads beyond the grammar run on either parser:
// pre-name IF EXISTS, and a USE-prefixed schema command, which is a schema
// command and not a graph query.
func TestStatementKindSchemaCommandsRunOnBothParsers(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		for _, statement := range []string{
			"DROP CONSTRAINT IF EXISTS kind_missing_constraint",
			"DROP INDEX IF EXISTS kind_missing_index",
			"CREATE CONSTRAINT kind_unique IF NOT EXISTS FOR (n:KindLabel) REQUIRE n.id IS UNIQUE",
			"DROP CONSTRAINT kind_unique IF EXISTS",
		} {
			_, err := exec.Execute(t.Context(), statement, nil)
			require.NoError(t, err, statement)
		}
		class := classifyStatement("USE nornic CREATE CONSTRAINT kind_use FOR (n:KindLabel) REQUIRE n.id IS UNIQUE")
		require.True(t, class.schema && class.commandOwned && !class.graphQuery, "USE-prefixed schema command: %+v", class)
	})
}

// The permission a statement needs comes from its kind, read from its body:
// EXPLAIN and USE prefixes don't hide an administration or schema command.
func TestStatementKindPermissions(t *testing.T) {
	cases := []struct {
		statement                  string
		read, write, schema, admin bool
	}{
		{statement: "CREATE DATABASE kind_db", read: true, admin: true},
		{statement: "CREATE DATABASE kind_db OPTIONS {seedURI: 's3://bucket/seed'}", read: true, admin: true},
		{statement: "CREATE OR REPLACE DATABASE kind_db", read: true, admin: true},
		{statement: "EXPLAIN CREATE DATABASE kind_db", read: true, admin: true},
		{statement: "DROP DATABASE kind_db", read: true, admin: true},
		{statement: "GRANT EXECUTE PROCEDURE * ON DBMS TO kind_role", read: true, admin: true},
		{statement: "SHOW USERS", read: true, admin: true},
		{statement: "CREATE INDEX kind_idx FOR (n:KindLabel) ON (n.p)", read: true, schema: true},
		{statement: "USE nornic CREATE INDEX kind_idx FOR (n:KindLabel) ON (n.p)", read: true, schema: true},
		{statement: "DROP CONSTRAINT kind_c", read: true, schema: true},
		{statement: "SHOW INDEXES", read: true},
		{statement: "MATCH (n) RETURN n", read: true},
		{statement: "CREATE (:KindLabel)", write: true},
	}
	forEachParser(t, func(t *testing.T) {
		for _, tc := range cases {
			got := QueryPermissionRequirements(t.Context(), tc.statement, RawSource)
			require.Equal(t, tc.read, got.Read, "%s read", tc.statement)
			require.Equal(t, tc.write, got.Write, "%s write", tc.statement)
			require.Equal(t, tc.schema, got.Schema, "%s schema", tc.statement)
			require.Equal(t, tc.admin, got.Admin, "%s admin", tc.statement)
		}
	})
}

// CREATE DATABASE … OPTIONS is an administration CREATE: it writes, so a
// READ procedure cannot run it, as with CREATE DATABASE without options.
func TestStatementKindReadProcedureRefusesCreateDatabaseOptions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, manager.Close()) })
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "system"))
		exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
		for _, declaration := range []string{
			"MODE READ AS CREATE DATABASE kind_options_db OPTIONS {seedURI: 's3://bucket/seed'}",
			"MODE READ AS CREATE DATABASE kind_plain_db",
		} {
			_, err := exec.Execute(t.Context(), "CREATE PROCEDURE custom.kindReadDatabase() "+declaration, nil)
			require.Error(t, err, declaration)
			require.Contains(t, err.Error(), localization.CypherProceduresReadContainsWrite().Fallback, declaration)
			require.False(t, manager.Exists("kind_options_db"))
			require.False(t, manager.Exists("kind_plain_db"))
		}
	})
}

// SHOW commands with YIELD / WHERE / RETURN tails run as commands, with their
// declared columns, and never as graph queries.
func TestStatementKindShowTails(t *testing.T) {
	cases := []struct {
		statement string
		columns   []string
		rows      int
	}{
		{"SHOW PROCEDURES YIELD name WHERE name = 'db.labels' RETURN name", []string{"name"}, 1},
		{"SHOW FUNCTIONS YIELD name WHERE name = 'abs' RETURN name", []string{"name"}, 1},
		{"SHOW INDEXES YIELD name RETURN name", []string{"name"}, -1},
		{"SHOW CONSTRAINTS YIELD name RETURN name", []string{"name"}, -1},
	}
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		for _, tc := range cases {
			result, err := exec.Execute(t.Context(), tc.statement, nil)
			require.NoError(t, err, tc.statement)
			require.Equal(t, tc.columns, result.Columns, tc.statement)
			if tc.rows >= 0 {
				require.Len(t, result.Rows, tc.rows, tc.statement)
			}
		}
	})
}

// The owner's command identity names the command each dispatcher runs.
func TestStatementKindCommandIdentity(t *testing.T) {
	cases := []struct {
		statement string
		command   statementCommand
		schema    schemaCommand
	}{
		{"MATCH (n) RETURN n", commandNone, schemaCommandNone},
		{"CREATE (n)", commandNone, schemaCommandNone},
		{"SHOW CONSTRAINTS", commandShowConstraints, schemaCommandNone},
		{"SHOW FULLTEXT INDEXES", commandShowIndexes, schemaCommandNone},
		{"TERMINATE TRANSACTIONS 'tx-1'", commandTerminateTransactions, schemaCommandNone},
		{"SHOW SERVERS", commandShowServers, schemaCommandNone},
		{"ALTER DATABASE kind SET LIMIT max_nodes = 1", commandAlterDatabaseLimit, schemaCommandNone},
		{"CREATE OR REPLACE DATABASE kind", commandCreateOrReplaceDatabase, schemaCommandNone},
		{"CREATE PROCEDURE custom.p() MODE READ AS RETURN 1 AS x", commandCreateProcedure, schemaCommandNone},
		{"CREATE DECAY PROFILE kind OPTIONS {halfLife: 1}", commandCreateKnowledgePolicy, schemaCommandNone},
		{"CREATE VECTOR INDEX kind FOR (n:L) ON (n.v)", commandSchema, schemaCreateVectorIndex},
		{"DROP INDEX kind", commandSchema, schemaDropIndex},
		{"DROP ROLE kind", commandDropRole, schemaCommandNone},
		{"DROP WHATEVER", commandInvalidDrop, schemaCommandNone},
		{"EXPLAIN SHOW PROCEDURES", commandShowProcedures, schemaCommandNone},
	}
	for _, tc := range cases {
		class := classifyStatement(tc.statement)
		require.Equal(t, tc.command, class.command, tc.statement)
		require.Equal(t, tc.schema, class.schemaCommand, tc.statement)
	}
}
