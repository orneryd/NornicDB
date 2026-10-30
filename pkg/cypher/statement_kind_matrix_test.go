package cypher

// The statement-kind conformance matrix. Its rows are generated from the
// owner's command table (statementCommandFacts): a command the table names
// without a row here fails TestStatementKindMatrixCoversEveryCommand. Each row's
// expectations come from Neo4j 5.26's documented semantics, not from the
// owner: the entitlement the command needs (schema or admin beyond read and
// write) and whether it changes state. Every row runs through the public
// entries on both parsers and with query normalization on and off: the
// statement itself, behind a leading comment, a CYPHER 5 preamble, PROFILE,
// EXPLAIN (which changes nothing) and USE, and as the body of a READ
// procedure (refused exactly when the statement changes state).

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type kindMatrixRow struct {
	statement     string
	schema, admin bool // entitlement needed beyond read/write (Neo4j)
	changes       bool // the statement changes state (Neo4j)
}

// kindMatrix: Neo4j-anchored expectations per command identity.
var kindMatrix = map[statementCommand]kindMatrixRow{
	commandShowConstraints:                 {statement: "SHOW CONSTRAINTS"},
	commandShowIndexes:                     {statement: "SHOW LOOKUP INDEXES"},
	commandShowKnowledgePolicies:           {statement: "SHOW DECAY PROFILES"},
	commandShowProcedures:                  {statement: "SHOW PROCEDURES"},
	commandShowFunctions:                   {statement: "SHOW FUNCTIONS"},
	commandShowCompositeDatabases:          {statement: "SHOW COMPOSITE DATABASES"},
	commandShowConstituents:                {statement: "SHOW CONSTITUENTS FOR COMPOSITE DATABASE c"},
	commandShowDefaultDatabase:             {statement: "SHOW DEFAULT DATABASE"},
	commandShowUsers:                       {statement: "SHOW USERS", admin: true},
	commandShowCurrentUser:                 {statement: "SHOW CURRENT USER"},
	commandShowTransactions:                {statement: "SHOW TRANSACTIONS", admin: true},
	commandTerminateTransactions:           {statement: "TERMINATE TRANSACTIONS 'tx-1'", admin: true, changes: true},
	commandShowPrivileges:                  {statement: "SHOW PRIVILEGES", admin: true},
	commandShowUnsupportedAdministration:   {statement: "SHOW ROLES"},
	commandShowUserDefined:                 {statement: "SHOW USER DEFINED FUNCTIONS"},
	commandShowServers:                     {statement: "SHOW SERVERS"},
	commandShowDatabases:                   {statement: "SHOW DATABASES"},
	commandShowDatabase:                    {statement: "SHOW DATABASE nornic"},
	commandShowAliases:                     {statement: "SHOW ALIASES"},
	commandShowSettings:                    {statement: "SHOW SETTINGS"},
	commandShowLimits:                      {statement: "SHOW LIMITS"},
	commandShowAdministrationOther:         {statement: "SHOW SUPPORTED PRIVILEGES"},
	commandShowOther:                       {statement: "SHOW WHATEVER"},
	commandTerminateOther:                  {statement: "TERMINATE WHATEVER"},
	commandAlterDatabase:                   {statement: "ALTER DATABASE kind SET ACCESS READ ONLY", admin: true, changes: true},
	commandAlterDatabaseLimit:              {statement: "ALTER DATABASE kind SET LIMIT max_nodes = 1", admin: true, changes: true},
	commandAlterDatabaseOption:             {statement: "ALTER DATABASE kind SET OPTION k 1", admin: true, changes: true},
	commandProcedurePrivilege:              {statement: "GRANT EXECUTE PROCEDURE * ON DBMS TO kind_role", admin: true, changes: true},
	commandUnsupportedPrivilege:            {statement: "GRANT ROLE kind_role TO kind_user", admin: true, changes: true},
	commandMalformedPrivilege:              {statement: "DENY WHATEVER", admin: true, changes: true},
	commandCreateProcedure:                 {statement: "CREATE PROCEDURE custom.kindMatrix() MODE READ AS RETURN 1 AS x", admin: true, changes: true},
	commandCreateKnowledgePolicy:           {statement: "CREATE DECAY PROFILE kind_matrix OPTIONS {halfLife: 1}", admin: true, changes: true},
	commandAlterKnowledgePolicy:            {statement: "ALTER DECAY PROFILE kind_matrix SET DECAY FLOOR 0.1", admin: true, changes: true},
	commandDropKnowledgePolicy:             {statement: "DROP DECAY PROFILE kind_matrix", admin: true, changes: true},
	commandRetentionPolicy:                 {statement: "CREATE RETENTION POLICY kind FOR (n:L) DURATION 1d"},
	commandShowRetentionPolicy:             {statement: "SHOW RETENTION BINDINGS"},
	commandDropRetentionPolicy:             {statement: "DROP RETENTION POLICY kind"},
	commandSchema:                          {statement: "CREATE LOOKUP INDEX kind_lookup FOR (n) ON EACH labels(n)", schema: true, changes: true},
	commandCreateSchemaOther:               {statement: "CREATE BTREE INDEX kind FOR (n:L) ON (n.p)", changes: true},
	commandCreateCompositeDatabase:         {statement: "CREATE COMPOSITE DATABASE kind_comp", admin: true, changes: true},
	commandCreateOrReplaceDatabase:         {statement: "CREATE OR REPLACE DATABASE kind_db", admin: true, changes: true},
	commandCreateOrReplaceOther:            {statement: "CREATE OR REPLACE INDEX kind FOR (n:L) ON (n.p)", changes: true},
	commandCreateDatabase:                  {statement: "CREATE DATABASE kind_db", admin: true, changes: true},
	commandCreateDatabaseOptions:           {statement: "CREATE DATABASE kind_db OPTIONS {seedURI: 's3://bucket/seed'}", admin: true, changes: true},
	commandCreateAlias:                     {statement: "CREATE ALIAS kind_alias FOR DATABASE nornic", admin: true, changes: true},
	commandDropCompositeDatabase:           {statement: "DROP COMPOSITE DATABASE kind_comp", admin: true, changes: true},
	commandDropDatabase:                    {statement: "DROP DATABASE kind_db", admin: true, changes: true},
	commandDropAlias:                       {statement: "DROP ALIAS kind_alias FOR DATABASE", admin: true, changes: true},
	commandDropProcedure:                   {statement: "DROP PROCEDURE custom.kindMatrix", admin: true, changes: true},
	commandDropRole:                        {statement: "DROP ROLE kind_role", changes: true},
	commandDropSchemaOther:                 {statement: "DROP FULLTEXT INDEX kind", changes: true},
	commandDropAdministration:              {statement: "DROP USER kind_user", changes: true},
	commandInvalidDrop:                     {statement: "DROP WHATEVER"},
	commandAlterCompositeDatabase:          {statement: "ALTER COMPOSITE DATABASE kind_comp DROP ALIAS a", admin: true, changes: true},
	commandAdministrationUnsupported:       {statement: "START DATABASE kind_db", changes: true},
	commandCreateAdministrationUnsupported: {statement: "CREATE USER kind_user SET PASSWORD 'kind-secret'", changes: true},
	commandBegin:                           {statement: "BEGIN"},
	commandCommit:                          {statement: "COMMIT TRANSACTION"},
	commandRollback:                        {statement: "ROLLBACK"},
	commandBeginScript:                     {statement: "BEGIN CREATE (:KindScript) COMMIT", changes: true},
}

// Graph statements: their effects are the grammar's (SHOW continuations too).
var kindMatrixGraph = []kindMatrixRow{
	{statement: "MATCH (n) RETURN n"},
	{statement: "CREATE (:KindMatrix)", changes: true},
	{statement: "MATCH (n) SET n.kind = 1", changes: true},
	{statement: "RETURN 1 AS x"},
}

// Every schema index kind needs the schema entitlement.
var kindMatrixSchemaForms = []string{
	"CREATE INDEX kind FOR (n:L) ON (n.p)",
	"CREATE RANGE INDEX kind FOR (n:L) ON (n.p)",
	"CREATE TEXT INDEX kind FOR (n:L) ON (n.p)",
	"CREATE POINT INDEX kind FOR (n:L) ON (n.p)",
	"CREATE LOOKUP INDEX kind FOR (n) ON EACH labels(n)",
	"CREATE FULLTEXT INDEX kind FOR (n:L) ON EACH [n.p]",
	"CREATE VECTOR INDEX kind FOR (n:L) ON (n.p)",
	"CREATE CONSTRAINT kind FOR (n:L) REQUIRE n.p IS UNIQUE",
	"DROP INDEX kind",
	"DROP CONSTRAINT kind",
}

func TestStatementKindMatrixCoversEveryCommand(t *testing.T) {
	for command := range statementCommandFacts {
		_, ok := kindMatrix[command]
		require.True(t, ok, "command %d has no conformance row", command)
	}
}

type kindEntry struct {
	name    string
	wrap    func(string) string
	explain bool
}

var kindEntries = []kindEntry{
	{name: "plain", wrap: func(s string) string { return s }},
	{name: "leading comment", wrap: func(s string) string { return "/* kind matrix */ " + s }},
	{name: "line comment", wrap: func(s string) string { return "// kind matrix\n" + s }},
	{name: "cypher 5 preamble", wrap: func(s string) string { return "CYPHER 5 " + s }},
	{name: "profile", wrap: func(s string) string { return "PROFILE " + s }},
	{name: "explain", wrap: func(s string) string { return "EXPLAIN " + s }, explain: true},
}

func forEachKindSetting(t *testing.T, run func(t *testing.T)) {
	forEachParser(t, func(t *testing.T) {
		for _, normalized := range []bool{true, false} {
			name := "normalization on"
			if !normalized {
				name = "normalization off"
			}
			t.Run(name, func(t *testing.T) {
				previous := config.IsCypherQueryNormalizationEnabled()
				config.SetCypherQueryNormalizationEnabled(normalized)
				t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
				run(t)
			})
		}
	})
}

func checkKindRow(t *testing.T, label string, want statementCommand, row kindMatrixRow) {
	t.Helper()
	for _, entry := range kindEntries {
		statement := entry.wrap(row.statement)
		require.Equal(t, want, classifyStatement(statement).command, "%s: %s", label, statement)
		got := QueryPermissionRequirements(t.Context(), statement, RawSource)
		require.Equal(t, row.schema, got.Schema, "%s: schema entitlement of %q", label, statement)
		require.Equal(t, row.admin, got.Admin, "%s: admin entitlement of %q", label, statement)
		require.Equal(t, row.changes && !entry.explain, got.ChangesState, "%s: state change of %q", label, statement)
	}
}

func TestStatementKindMatrixCommands(t *testing.T) {
	forEachKindSetting(t, func(t *testing.T) {
		for command, row := range kindMatrix {
			checkKindRow(t, "command", command, row)
		}
		for _, row := range kindMatrixGraph {
			checkKindRow(t, "graph", commandNone, row)
		}
		for _, statement := range kindMatrixSchemaForms {
			got := QueryPermissionRequirements(t.Context(), statement, RawSource)
			require.True(t, got.Schema, "schema entitlement of %q", statement)
			require.True(t, got.ChangesState, "state change of %q", statement)
		}
	})
}

// USE before a command: an administration command is refused; any other
// command keeps its identity and facts.
func TestStatementKindMatrixUseEntry(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for command, row := range kindMatrix {
			// Transaction control and BEGIN … COMMIT scripts have no USE form.
			if statementCommandFacts[command].transactionControl || command == commandBeginScript {
				continue
			}
			statement := "USE nornic " + row.statement
			class := classifyStatement(statement)
			require.Equal(t, command, class.command, statement)
			_, _, hasUse, err := parseUseClause(t.Context(), statement, false, RawSource)
			if class.administration {
				require.True(t, hasUse, statement)
				require.EqualError(t, err, localization.CypherCommandRoutingUseAdministrationCommand().Fallback, statement)
			}
		}
	})
}

// A READ procedure body is refused exactly when the statement changes state.
func TestStatementKindMatrixReadProcedureBodies(t *testing.T) {
	forEachKindSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
		check := func(row kindMatrixRow) {
			// A procedure body is one statement: no procedure DDL and no
			// transaction control or BEGIN … COMMIT script.
			if c := classifyStatement(row.statement); c.procedureDefinition || c.command == commandDropProcedure || c.transactionControl || c.command == commandBeginScript {
				return
			}
			_, err := exec.Execute(t.Context(), "CREATE OR REPLACE PROCEDURE custom.kindMatrixBody() MODE READ AS "+row.statement, nil)
			refused := err != nil && strings.Contains(err.Error(), localization.CypherProceduresReadContainsWrite().Fallback)
			require.Equal(t, row.changes, refused, "READ procedure body %q: %v", row.statement, err)
			if err == nil {
				_, dropErr := exec.Execute(t.Context(), "DROP PROCEDURE custom.kindMatrixBody", nil)
				require.NoError(t, dropErr)
			}
		}
		for _, row := range kindMatrix {
			check(row)
		}
		for _, row := range kindMatrixGraph {
			check(row)
		}
	})
}
