package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Constraint checks on the server's storage stack (Badger -> WAL -> Async ->
// Namespaced), #700. Neo4j rejects a write that breaks a constraint with
// Neo.ClientError.Schema.ConstraintValidationFailed when the statement runs,
// stores nothing of the statement, and keeps serving every client.

// requireConstraintFailure checks the code a client gets for err (the Bolt
// and HTTP mapping).
func requireConstraintFailure(t *testing.T, err error, statement string) {
	t.Helper()
	require.Error(t, err, statement)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", code, "%s: %v", statement, err)
}

func countOf(t *testing.T, exec *StorageExecutor, ctx context.Context, statement string) int64 {
	t.Helper()
	result, err := exec.Execute(ctx, statement, nil)
	require.NoError(t, err, statement)
	require.Len(t, result.Rows, 1, statement)
	return result.Rows[0][0].(int64)
}

// TestConstraintViolationFailsTheStatementAndNeverWedgesTheEngine: an
// auto-commit CREATE that breaks a DOMAIN or UNIQUE constraint fails, stores
// nothing, and deletes, SET and schema commands keep working afterwards (they
// used to fail with "flush incomplete" for every client until a restart).
func TestConstraintViolationFailsTheStatementAndNeverWedgesTheEngine(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	for _, ddl := range []string{
		"CREATE CONSTRAINT dA FOR (n:DA) REQUIRE n.s IN ['a', 'b']",
		"CREATE CONSTRAINT uK FOR (n:UK) REQUIRE n.k IS UNIQUE",
		"CREATE CONSTRAINT rK FOR ()-[r:RK]-() REQUIRE r.k IS UNIQUE",
	} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err, ddl)
	}
	for _, statement := range []string{
		"CREATE (:Plain {x: 1})",
		"CREATE (:DA {s: 'a'})",
		"CREATE (:UK {k: 1})",
		"CREATE (:P {id: 1})-[:RK {k: 1}]->(:P {id: 2})",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}

	for _, statement := range []string{
		"CREATE (:DA {s: 'c'})",
		"CREATE (:UK {k: 1})",
		"CREATE (:UK {k: 5}), (:UK {k: 5})",
		"CREATE (:Plain {x: 2}), (:UK {k: 1})",
		"CREATE (:P {id: 3})-[:RK {k: 1}]->(:P {id: 4})",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		requireConstraintFailure(t, err, statement)
	}

	// Nothing of a failed statement is stored.
	require.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:DA) RETURN count(n) AS c"))
	require.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:UK) RETURN count(n) AS c"))
	require.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:Plain) RETURN count(n) AS c"))
	require.EqualValues(t, 2, countOf(t, exec, ctx, "MATCH (n:P) RETURN count(n) AS c"))

	// The engine keeps serving writes, deletes and schema commands.
	for _, statement := range []string{
		"MATCH (n:Plain) SET n.y = 2",
		"MATCH (n:DA) DETACH DELETE n",
		"CREATE CONSTRAINT uZ FOR (n:UZ) REQUIRE n.z IS UNIQUE",
		"DROP CONSTRAINT uZ",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	require.EqualValues(t, 0, countOf(t, exec, ctx, "MATCH (n:DA) RETURN count(n) AS c"))
}

// TestExplicitTransactionConstraintViolationFailsTheStatement: in an explicit
// transaction the statement that breaks a constraint fails (not the COMMIT),
// the transaction is failed from then on, and nothing it wrote is stored.
// The same holds when another client committed the value after the
// transaction began.
func TestExplicitTransactionConstraintViolationFailsTheStatement(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT uK FOR (n:UK) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:UK {k: 1})", nil)
	require.NoError(t, err)

	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		tx := NewStorageExecutor(exec.storage)
		_, err = tx.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = tx.Execute(ctx, "CREATE (:B)", nil)
		require.NoError(t, err)
		_, err = tx.Execute(ctx, "CREATE (:UK {k: 1})", nil)
		requireConstraintFailure(t, err, "duplicate CREATE in explicit transaction")
		_, err = tx.Execute(ctx, "CREATE (:C)", nil)
		require.Error(t, err, "statement after the failure")
		_, _ = tx.Execute(ctx, end, nil)
		require.EqualValues(t, 0, countOf(t, exec, ctx, "MATCH (n) WHERE n:B OR n:C RETURN count(n) AS c"), end)
	}

	// A MERGE whose SET writes another node's value fails the statement and
	// stores nothing: neither the created node nor its SET.
	for _, statement := range []string{
		"MERGE (n:UK {k: 2}) SET n.k = 1",
		"MERGE (n:UK {k: 2}) ON CREATE SET n.k = 1",
		"MERGE (n:UK {k: 2}) SET n += {k: 1}",
		"CREATE (a:B) WITH a MERGE (n:UK {k: 2}) SET n.k = 1",
	} {
		tx := NewStorageExecutor(exec.storage)
		_, err = tx.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = tx.Execute(ctx, statement, nil)
		requireConstraintFailure(t, err, statement)
		_, _ = tx.Execute(ctx, "COMMIT", nil)
		require.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:UK) RETURN count(n) AS c"), statement)
		require.EqualValues(t, 0, countOf(t, exec, ctx, "MATCH (n:B) RETURN count(n) AS c"), statement)
	}

	// Another client commits k = 9 after the transaction began.
	tx := NewStorageExecutor(exec.storage)
	_, err = tx.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = tx.Execute(ctx, "MATCH (n:UK) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:UK {k: 9})", nil)
	require.NoError(t, err)
	_, err = tx.Execute(ctx, "CREATE (:UK {k: 9})", nil)
	requireConstraintFailure(t, err, "CREATE of a value committed after BEGIN")
	_, _ = tx.Execute(ctx, "ROLLBACK", nil)
	require.EqualValues(t, 1, countOf(t, exec, ctx, "MATCH (n:UK {k: 9}) RETURN count(n) AS c"))
}
