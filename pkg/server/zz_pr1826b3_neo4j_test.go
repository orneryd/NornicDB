package server

// Author: pr1826-base3 test lane (Claude Opus 5.5), 23 Nov 2026.
// Neo4j 5.26.30 reference answers for the statements of defects 1 and 3. The
// reference is reset under the job's exclusive lease; the users and nodes
// created here are removed again at the end.

import (
	"os"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestPR1826B3Neo4jReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("NORNICDB_NEO4J_REFERENCE_URI is required (--ref neo-ref)")
	}
	driver, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(t.Context()) })
	run := func(database, statement string, params map[string]any) {
		t.Helper()
		result, err := neo4jdriver.ExecuteQuery(t.Context(), driver, statement, params,
			neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase(database))
		if err != nil {
			t.Logf("PR1826B3NEO db=%s statement=%q error=%v", database, statement, err)
			return
		}
		rows := make([][]any, 0, len(result.Records))
		for _, record := range result.Records {
			rows = append(rows, record.Values)
		}
		t.Logf("PR1826B3NEO db=%s statement=%q columns=%v rows=%v counters=%+v", database, statement, result.Keys, rows, result.Summary.Counters())
	}
	run("system", "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil)

	// Defect 1: SHOW USER[S] WITH AUTH, with a user present.
	run("system", "CREATE USER pr1826b3_analyst IF NOT EXISTS SET PASSWORD 'pr1826b3-secret-1' CHANGE NOT REQUIRED", nil)
	run("system", "CREATE USER pr1826b3_oidc IF NOT EXISTS SET AUTH 'oidc-work' { SET ID 'employee-12' }", nil)
	for _, statement := range []string{
		"SHOW USERS WITH AUTH YIELD user, provider, auth WHERE user = 'pr1826b3_analyst'",
		"SHOW USER WITH AUTH YIELD user, provider WHERE user = 'pr1826b3_analyst'",
		"SHOW USERS WITH AUTH YIELD user, provider, auth WHERE user = 'pr1826b3_oidc'",
		"SHOW USERS WITH AUTH",
		"SHOW USERS YIELD user WHERE user = 'pr1826b3_analyst'",
		"SHOW USERS",
	} {
		run("system", statement, nil)
	}
	run("system", "DROP USER pr1826b3_oidc IF EXISTS", nil)
	run("system", "DROP USER pr1826b3_analyst IF EXISTS", nil)

	// Defect 3: a statement that starts with SET or REMOVE, alone (the
	// grammar's answer) and as apoc.periodic.iterate's action.
	run("neo4j", "SET x.flag = 1", nil)
	run("neo4j", "REMOVE x.body", nil)
	run("neo4j", "EXPLAIN UNWIND $_batch AS _batch WITH _batch.x AS x SET x.flag = 1", map[string]any{"_batch": []any{}})
	run("neo4j", "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc.periodic' RETURN name", nil)
	for _, call := range []string{
		`CALL apoc.periodic.iterate("MATCH p=(n:PR1826B3Doc {key:'one'}) RETURN nodes(p)[0] AS x", "SET x.flag = 1", {batchSize: 10})`,
		`CALL apoc.periodic.iterate("MATCH (end:PR1826B3Doc {key:'one'}) RETURN coalesce(end, null) AS x", "SET x.flag = 1", {batchSize: 10})`,
		`CALL apoc.periodic.iterate("MATCH (x:PR1826B3Doc {key:'one'}) RETURN x", "REMOVE x.body", {batchSize: 10})`,
	} {
		run("neo4j", "MATCH (d:PR1826B3Doc) DETACH DELETE d", nil)
		run("neo4j", "CREATE (:PR1826B3Doc {key:'one', body:'b1'}), (:PR1826B3Doc {key:'two', body:'b2'})", nil)
		run("neo4j", call, nil)
		run("neo4j", "MATCH (d:PR1826B3Doc) RETURN d.key, d.flag, d.body ORDER BY d.key", nil)
	}
	run("neo4j", "MATCH (d:PR1826B3Doc) DETACH DELETE d", nil)
}
