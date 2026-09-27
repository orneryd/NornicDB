package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUseClauseGrammar checks the USE clause as Neo4j 5.26's grammar reads
// it (#738). Each error case was run against Neo4j 5.26.30; the expected
// text is the start of Neo4j's message (NornicDB reports no position).
func TestUseClauseGrammar(t *testing.T) {
	valid := []struct {
		query, target, remaining string
	}{
		{"USE neo4j RETURN 1 AS x", "neo4j", "RETURN 1 AS x"},
		{"use neo4j return 1 as x", "neo4j", "return 1 as x"},
		{"USE `neo4j`RETURN 1 AS x", "neo4j", "RETURN 1 AS x"},
		{"USE `db``name` RETURN 1", "db`name", "RETURN 1"},
		{"USE neo4j/*c*/RETURN 1 AS x", "neo4j", "/*c*/RETURN 1 AS x"},
		{"USE neo4j//c\nRETURN 1 AS x", "neo4j", "//c\nRETURN 1 AS x"},
		{"USE /*c*/ neo4j RETURN 1 AS x", "neo4j", "RETURN 1 AS x"},
		{"USE GRAPH neo4j RETURN 1 AS x", "neo4j", "RETURN 1 AS x"},
		{"USE GRAPH/*c*/neo4j RETURN 1 AS x", "neo4j", "RETURN 1 AS x"},
		{"USE graph RETURN 1", "graph", "RETURN 1"},
		{"USE GRAPH graph RETURN 1", "graph", "RETURN 1"},
		{"USE neo4j  .  x RETURN 1", "neo4j.x", "RETURN 1"},
		{"USE neo4j.`x` RETURN 1", "neo4j.x", "RETURN 1"},
		{"USE `neo4j`.`x` RETURN 1", "neo4j.x", "RETURN 1"},
		{"USE `a-b` RETURN 1", "a-b", "RETURN 1"},
		{"USE neo4j ORDER BY 1 RETURN 1", "neo4j", "ORDER BY 1 RETURN 1"},
		{"USE neo4j OPTIONAL MATCH (n) RETURN 1 AS x", "neo4j", "OPTIONAL MATCH (n) RETURN 1 AS x"},
		{"USE neo4j SHOW INDEXES", "neo4j", "SHOW INDEXES"},
		{"USE neo4j SHOW USER DEFINED FUNCTIONS", "neo4j", "SHOW USER DEFINED FUNCTIONS"},
		{"USE neo4j SHOW ALL INDEXES", "neo4j", "SHOW ALL INDEXES"},
		{"USE neo4j TERMINATE TRANSACTION 'neo4j-transaction-1'", "neo4j", "TERMINATE TRANSACTION 'neo4j-transaction-1'"},
		{"USE neo4j CREATE OR REPLACE INDEX", "neo4j", "CREATE OR REPLACE INDEX"},
		{"USE neo4j CREATE CONSTRAINT uc IF NOT EXISTS FOR (n:UC) REQUIRE n.p IS UNIQUE", "neo4j", "CREATE CONSTRAINT uc IF NOT EXISTS FOR (n:UC) REQUIRE n.p IS UNIQUE"},
	}
	for _, tc := range valid {
		t.Run(tc.query, func(t *testing.T) {
			db, remaining, hasUse, err := parseLeadingUseClause(tc.query)
			require.NoError(t, err)
			require.True(t, hasUse)
			require.Equal(t, tc.target, db)
			require.Equal(t, tc.remaining, remaining)
		})
	}

	const administration = "The `USE` clause is not required for Administration Commands. Retry your query omitting the `USE` clause and it will be routed automatically."
	invalid := []struct {
		query, message string
	}{
		{"USE neo4j", "Query cannot conclude with USE GRAPH (must be a RETURN clause"},
		{"USE GRAPH neo4j", "Query cannot conclude with USE GRAPH"},
		{"USE graph", "Query cannot conclude with USE GRAPH"},
		{"USE neo4j ;", "Query cannot conclude with USE GRAPH"},
		{"USE neo4j EXPLAIN RETURN 1 AS x", "Invalid input 'EXPLAIN': expected a database name, '(', 'FOREACH', '.', 'ALTER', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF>"},
		{"USE neo4j PROFILE RETURN 1 AS x", "Invalid input 'PROFILE': expected a database name"},
		{"USE neo4j CYPHER 5 RETURN 1 AS x", "Invalid input 'CYPHER': expected a database name"},
		{"USE neo4j BEGIN", "Invalid input 'BEGIN': expected a database name"},
		{"USE neo4j COMMIT", "Invalid input 'COMMIT': expected a database name"},
		{"USE neo4j :USE neo4j", "Invalid input ':': expected a database name"},
		{"USE neo4j FOO RETURN 1", "Invalid input 'FOO': expected a database name"},
		{"USE nope FOO RETURN 1", "Invalid input 'FOO': expected a database name"},
		{"USE neo4j{a:1} RETURN 1", "Invalid input '{': expected a database name"},
		{"USE a-b RETURN 1", "Invalid input '-': expected a database name"},
		{"USE neo4jRETURN 1", "Invalid input '1': expected a database name"},
		{"USE 1abc RETURN 1", "Invalid input '1abc': expected an identifier, '(' or 'GRAPH'"},
		{"USE $db RETURN 1", "Invalid input '$': expected an identifier, '(' or 'GRAPH'"},
		{"USE neo4j USE neo4j RETURN 1 AS x", "USE clause must be either the first clause in a (sub-)query or preceded by an importing WITH clause in a sub-query."},
		{"USE neo4j SHOW USERS", administration},
		{"USE nope SHOW USERS", administration},
		{"USE neo4j SHOW DATABASES", administration},
		{"USE neo4j SHOW DATABASE neo4j YIELD name", administration},
		{"USE neo4j SHOW HOME DATABASE", administration},
		{"USE neo4j SHOW DEFAULT DATABASE", administration},
		{"USE neo4j SHOW SERVERS", administration},
		{"USE neo4j SHOW SUPPORTED PRIVILEGES", administration},
		{"USE neo4j SHOW POPULATED ROLES", administration},
		{"USE neo4j SHOW ALL ROLES", administration},
		{"USE neo4j SHOW ALL ROLE", administration},
		{"USE neo4j SHOW PRIVILEGE", administration},
		{"USE neo4j SHOW USER neo4j PRIVILEGES", administration},
		{"USE neo4j SHOW USER PRIVILEGES", administration},
		{"USE neo4j SHOW CURRENT USER", administration},
		{"USE neo4j SHOW ROLES", administration},
		{"USE neo4j SHOW ALIASES FOR DATABASE", administration},
		{"USE neo4j CREATE ROLE r1", administration},
		{"USE neo4j DROP ROLE r1", administration},
		{"USE neo4j RENAME ROLE a TO b", administration},
		{"USE neo4j CREATE USER u1 SET PASSWORD 'x'", administration},
		{"USE neo4j ALTER USER u1 SET PASSWORD 'y'", administration},
		{"USE neo4j ALTER CURRENT USER SET PASSWORD FROM 'a' TO 'b'", administration},
		{"USE neo4j GRANT ROLE r1 TO u1", administration},
		{"USE neo4j REVOKE ROLE r1 FROM u1", administration},
		{"USE neo4j DENY ACCESS ON DATABASE neo4j TO r1", administration},
		{"USE neo4j START DATABASE neo4j", administration},
		{"USE neo4j STOP DATABASE x", administration},
		{"USE neo4j CREATE DATABASE x", administration},
		{"USE neo4j CREATE OR REPLACE DATABASE x", administration},
		{"USE neo4j DROP DATABASE x IF EXISTS", administration},
		{"USE neo4j ALTER DATABASE neo4j SET ACCESS READ WRITE", administration},
		{"USE neo4j CREATE ALIAS a FOR DATABASE neo4j", administration},
		{"USE neo4j ALTER ALIAS a SET DATABASE TARGET neo4j", administration},
		{"USE neo4j CREATE COMPOSITE DATABASE c", administration},
		{"USE neo4j DROP COMPOSITE DATABASE c", administration},
		{"USE neo4j ENABLE SERVER 'x'", administration},
		{"USE neo4j DRYRUN REALLOCATE DATABASES", administration},
		{"USE neo4j DEALLOCATE DATABASE FROM SERVER 'x'", administration},
	}
	for _, tc := range invalid {
		t.Run(tc.query, func(t *testing.T) {
			_, _, hasUse, err := parseLeadingUseClause(tc.query)
			require.True(t, hasUse)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.message)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		})
	}
}

// requireStatusCode checks the Neo4j status code an error reports to
// Bolt and HTTP clients.
func requireStatusCode(t *testing.T, err error, code string) {
	t.Helper()
	var classified interface{ BoltErrorCode() string }
	require.ErrorAs(t, err, &classified)
	require.Equal(t, code, classified.BoltErrorCode())
}

// TestSubqueryUseClauseGrammar checks a USE clause that starts a CALL { }
// subquery body, as Neo4j 5.26 reads it (#738).
func TestSubqueryUseClauseGrammar(t *testing.T) {
	use, remaining, hasUse, err := parseUseClause("USE neo4j RETURN 1 AS y", true)
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "neo4j", use.Name)
	require.Equal(t, "RETURN 1 AS y", remaining)

	for query, message := range map[string]string{
		"USE neo4j":                         "Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD.",
		"USE neo4j FOO":                     "Invalid input 'FOO': expected a database name, '(', 'FOREACH', '.', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or '}'",
		"USE neo4j SHOW USERS":              "Invalid input 'SHOW': expected a database name",
		"USE neo4j EXPLAIN RETURN 1":        "Invalid input 'EXPLAIN': expected a database name",
		"USE neo4j USE neo4j RETURN 1 AS y": "USE clause must be either the first clause in a (sub-)query",
	} {
		_, _, hasUse, err := parseUseClause(query, true)
		require.True(t, hasUse, query)
		require.ErrorContains(t, err, message, query)
	}
}

// TestDynamicGraphLookupOutsideComposite: graph.byName is only allowed on a
// composite database (Neo4j 5.26 SyntaxError on a standard database, #738).
func TestDynamicGraphLookupOutsideComposite(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, query := range []string{
		"USE graph.byName('nornic') RETURN 1 AS x",
		"CALL { USE graph.byName('nornic') RETURN 1 AS y } RETURN y",
	} {
		_, err := exec.Execute(t.Context(), query, nil)
		require.ErrorContains(t, err, "Dynamic graph lookup not allowed here. This feature is only available on composite databases.\nAttempted to access graph graph.byName(\"nornic\")", query)
	}
}
