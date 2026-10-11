package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apoc.custom installs a parameterised statement as custom.<name>, persisted
// with the database, as APOC does: callable with its declared defaults,
// listed, reloaded from the catalog, replaced and dropped (#1066).
func TestApocCustomProcedures(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	run := func(t *testing.T, query string, params map[string]interface{}) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	run(t, "CREATE (:Step {n: 1})-[:NEXT]->(:Step {n: 2})-[:NEXT]->(:Step {n: 3})", nil)

	run(t, `CALL apoc.custom.installProcedure('reach(start :: INTEGER, depth = 1 :: INTEGER) :: (n :: INTEGER)',
		'MATCH (s:Step {n: $start})-[:NEXT*1..10]->(t) WHERE t.n - s.n <= $depth RETURN t.n AS n ORDER BY n', 'neo4j', 'read', 'steps reachable from a start')`, nil)
	require.Equal(t, [][]interface{}{{int64(2)}}, run(t, "CALL custom.reach(1) YIELD n RETURN n", nil))
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, run(t, "CALL custom.reach($s, 2) YIELD n RETURN n", map[string]interface{}{"s": int64(1)}))

	listed := run(t, "CALL apoc.custom.list() YIELD name, mode, description, inputs, outputs RETURN name, mode, description, inputs, outputs", nil)
	require.Equal(t, [][]interface{}{{"reach", "read", "steps reachable from a start",
		[]interface{}{[]interface{}{"start", "integer"}, []interface{}{"depth", "integer", "1"}},
		[]interface{}{[]interface{}{"n", "integer"}}}}, listed)
	require.Len(t, run(t, "CALL apoc.custom.show() YIELD name RETURN name", nil), 1)

	// The APOC 4 form, replacing a procedure of the same name.
	run(t, `CALL apoc.custom.declareProcedure('reach(start :: INTEGER) :: (n :: INTEGER)', 'MATCH (s:Step {n: $start}) RETURN s.n AS n')`, nil)
	require.Equal(t, [][]interface{}{{int64(3)}}, run(t, "CALL custom.reach(3) YIELD n RETURN n", nil))

	// Persisted with the database: a new executor on the same store has it.
	ClearUserProcedures()
	reloaded := NewStorageExecutor(store)
	result, err := reloaded.Execute(ctx, "CALL custom.reach(2) YIELD n RETURN n", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

	run(t, "CALL apoc.custom.dropProcedure('reach')", nil)
	_, err = exec.Execute(ctx, "CALL custom.reach(1) YIELD n RETURN n", nil)
	require.Error(t, err)
	// The list shows custom procedures only, and skips records it can't read.
	run(t, "CREATE PROCEDURE plain.one() MODE READ AS RETURN 1 AS n", nil)
	run(t, "CREATE (:_ProcedureCatalog {record: 'not base64 ***'})", nil)
	require.Empty(t, run(t, "CALL apoc.custom.list() YIELD name RETURN name", nil))

	// Not inside an explicit transaction, as CREATE / DROP PROCEDURE.
	for _, query := range []string{"CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', 'RETURN 1 AS n')", "CALL apoc.custom.dropProcedure('p')"} {
		_, err := exec.Execute(WithTransactionControl(ctx), "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "transaction", query)
		_, err = exec.Execute(WithTransactionControl(ctx), "ROLLBACK", nil)
		require.NoError(t, err)
	}

	for query, message := range map[string]string{
		"CALL apoc.custom.installProcedure('not a signature', 'RETURN 1 AS n')":                                           "invalid custom procedure signature",
		"CALL apoc.custom.installProcedure('p(a = 1 :: INTEGER, b :: INTEGER)', 'RETURN 1 AS n')":                         "invalid custom procedure signature",
		"CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', '')":                                                  "body",
		"CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', 'CREATE (:X) RETURN 1 AS n')":                         "write",
		"CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', 'RETURN 1 AS n', 'neo4j', 'oops')":                    "invalid procedure mode",
		"UNWIND [1] AS s CALL apoc.custom.installProcedure(s, 'RETURN 1 AS n')":                                           "signature must be STRING",
		"UNWIND [1] AS s CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', s)":                                   "statement must be STRING",
		"UNWIND [1] AS s CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', 'RETURN 1 AS n', s)":                  "databaseName must be STRING",
		"UNWIND [1] AS s CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', 'RETURN 1 AS n', 'neo4j', s)":         "mode must be STRING",
		"UNWIND [1] AS s CALL apoc.custom.installProcedure('p() :: (n :: INTEGER)', 'RETURN 1 AS n', 'neo4j', 'read', s)": "description must be STRING",
		"UNWIND [1] AS s CALL apoc.custom.dropProcedure(s)":                                                               "name must be STRING",
		"UNWIND [1] AS s CALL apoc.custom.dropProcedure('p', s)":                                                          "databaseName must be STRING",
		"CALL apoc.custom.dropProcedure('never_installed')":                                                               "never_installed",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, message, query)
	}
}

func TestParseCustomSignature(t *testing.T) {
	parsed, ok := parseCustomSignature("custom.my.walk($start :: NODE, depth :: INTEGER = 2) :: VOID")
	require.True(t, ok)
	require.Equal(t, "my.walk", parsed.name)
	require.Equal(t, []string{"start", "depth"}, parsed.args)
	require.Equal(t, []string{"", "2"}, parsed.defaults)
	require.Empty(t, parsed.outputs)
	require.Equal(t, "custom.my.walk(start :: NODE, depth = 2 :: INTEGER) :: VOID", parsed.text())
	parsed, ok = parseCustomSignature("count()")
	require.True(t, ok)
	require.Equal(t, "custom.count() :: VOID", parsed.text())
	parsed, ok = parseCustomSignature("p(a :: INTEGER, ) :: (n :: INTEGER)")
	require.True(t, ok)
	require.Equal(t, []string{"a"}, parsed.args)
	for _, bad := range []string{"", "bad-name()", "custom.my..walk()", "(a :: INTEGER)", "p(a :: INTEGER, a :: INTEGER)", "p(1a :: INTEGER)", "p() :: n", "p() :: (n)", "p( :: (n :: INTEGER)", "p() junk"} {
		_, ok := parseCustomSignature(bad)
		require.False(t, ok, bad)
	}
}
