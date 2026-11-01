package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// TestStatementClassAdministration pins which statements count as Neo4j
// administration commands (refused after USE) and which are ordinary
// queries that share their first word (SHOW INDEXES, CREATE INDEX,
// CREATE (n), SHOW USER DEFINED FUNCTIONS, …).
func TestStatementClassAdministration(t *testing.T) {
	tests := []struct {
		statement string
		want      bool
	}{
		{"", false},
		{"   ", false},
		{"(n)", false},
		{"MATCH (n) RETURN n", false},
		{"GRANT ROLE r TO u", true},
		{"deny read {*} on graph * to r", true},
		{"REVOKE ROLE r FROM u", true},
		{"RENAME USER a TO b", true},
		{"START DATABASE foo", true},
		{"STOP DATABASE foo", true},
		{"ENABLE SERVER 'abc'", true},
		{"DRYRUN REALLOCATE DATABASES", true},
		{"DEALLOCATE DATABASES FROM SERVER 'a'", true},
		{"REALLOCATE DATABASES", true},
		{"// leading comment\nGRANT ROLE r TO u", true},
		{"SHOW", false},
		{"SHOW DATABASE foo", true},
		{"show databases", true},
		{"SHOW DEFAULT DATABASE", true},
		{"SHOW HOME DATABASE", true},
		{"SHOW SERVER 'a'", true},
		{"SHOW SERVERS", true},
		{"SHOW SUPPORTED PRIVILEGES", true},
		{"SHOW POPULATED ROLES", true},
		{"SHOW USERS", true},
		{"SHOW CURRENT USER", true},
		{"SHOW ROLE r PRIVILEGES", true},
		{"SHOW ROLES", true},
		{"SHOW PRIVILEGE", true},
		{"SHOW PRIVILEGES", true},
		{"SHOW ALIAS a FOR DATABASE", true},
		{"SHOW ALIASES FOR DATABASES", true},
		{"SHOW USER", true},
		{"SHOW USER bob PRIVILEGES", true},
		{"SHOW USER DEFINED FUNCTIONS", false},
		{"SHOW ALL", false},
		{"SHOW ALL ROLES", true},
		{"SHOW ALL ROLE", true},
		{"SHOW ALL PRIVILEGES", true},
		{"SHOW ALL PRIVILEGE", true},
		{"SHOW ALL FUNCTIONS", false},
		{"SHOW INDEXES", false},
		{"SHOW PROCEDURES", false},
		{"SHOW TRANSACTIONS", false},
		{"CREATE", false},
		{"CREATE (n) RETURN n", false},
		{"CREATE INDEX idx FOR (n:L) ON (n.p)", false},
		{"CREATE DATABASE foo", true},
		{"create composite database c", true},
		{"CREATE ALIAS a FOR DATABASE foo", true},
		{"CREATE USER u SET PASSWORD 'p'", true},
		{"CREATE ROLE r", true},
		{"CREATE OR REPLACE DATABASE foo", true},
		{"CREATE OR REPLACE", false},
		{"CREATE OR REPLACE INDEX", false},
		{"CREATE CURRENT USER", false},
		{"DROP DATABASE foo", true},
		{"DROP SERVER 'a'", true},
		{"DROP INDEX idx", false},
		{"DROP CONSTRAINT c", false},
		{"ALTER USER u SET PASSWORD 'p'", true},
		{"ALTER CURRENT USER SET PASSWORD FROM 'a' TO 'b'", true},
		{"ALTER CURRENT", false},
		{"ALTER CURRENT ROLE", false},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, classifyStatement(tt.statement).administration, tt.statement)
	}
}

func TestStatementClassAdministrationReservedWordsAreASCII(t *testing.T) {
	for _, statement := range []string{"CREATE UſER ordinary", "SHOW UſERS", "ALTER UſER ordinary SET PASSWORD 'synthetic'", "ſHOW DATABASES"} {
		require.False(t, classifyStatement(statement).administration, "non-ASCII keyword classified as administration: %q", statement)
	}
	for _, statement := range []string{"cReAtE uSeR `uſK`", "sHoW uSeRs", "aLtEr uSeR `uſK` SET PASSWORD 'synthetic'"} {
		require.True(t, classifyStatement(statement).administration, "ASCII keywords lost administration classification: %q", statement)
	}
}

// USE before an administration command is refused at the top level; the
// command is routed to the system database by itself.
func TestUseBeforeAdministrationCommandIsRefused(t *testing.T) {
	for _, query := range []string{"USE system SHOW USERS", "USE system CREATE DATABASE foo", "USE system GRANT ROLE r TO u"} {
		_, _, hasUse, err := parseUseClause(t.Context(), query, false, AcceptedSource)
		require.True(t, hasUse, query)
		require.EqualError(t, err, localization.CypherCommandRoutingUseAdministrationCommand().Fallback, query)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err), query)
		_, _, _, hasUse, err = readUseStatement(t.Context(), query, false)
		require.True(t, hasUse, query)
		require.EqualError(t, err, localization.CypherCommandRoutingUseAdministrationCommand().Fallback, query)
	}
}

// TestClassifyStatementMemoizesFullText pins the cost contract on the Execute
// path: asking again for one statement's class allocates nothing, including a
// lower-case leading word and a prefix, and each text keeps its own prefix facts.
func TestClassifyStatementMemoizesFullText(t *testing.T) {
	for _, statement := range []string{
		"match p = (n) return p",
		"explain match p = (n) return p",
		"cypher 25 use db match p = (n) return p",
	} {
		first := classifyStatement(statement)
		var again statementClass
		require.Zero(t, testing.AllocsPerRun(100, func() { again = classifyStatement(statement) }), statement)
		require.Equal(t, first, again, statement)
	}
	plain := classifyStatement("match p = (n) return p")
	require.Equal(t, prefixNone, plain.prefixMode)
	require.Equal(t, "MATCH", plain.startWord)
	require.Equal(t, "5", plain.version)
	explained := classifyStatement("explain match p = (n) return p")
	require.Equal(t, prefixExplain, explained.prefixMode)
	require.Equal(t, "EXPLAIN", explained.startWord)
	versioned := classifyStatement("cypher 25 use db match p = (n) return p")
	require.Equal(t, "25", versioned.version)
	require.Equal(t, "CYPHER", versioned.startWord)
}
