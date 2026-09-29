package cypher

// gh741_explicit_tx_staged_delete_test.go — regression tests for #741: in an
// explicit transaction, a relationship the transaction created and then removed
// (DELETE r or DETACH DELETE of an endpoint) must not fail COMMIT with
// "invalid edge: start or end node not found", and the positional
// (label, type) relationship counters must not drift.
//
// Pinned against neo4j:5.26.30-community (issue table).

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newGh741Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "gh741")
	return NewStorageExecutor(store)
}

// runTx executes statements in one explicit transaction and asserts COMMIT.
func runGh741Tx(t *testing.T, exec *StorageExecutor, ctx context.Context, statements ...string) {
	t.Helper()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	for _, stmt := range statements {
		_, err := exec.Execute(ctx, stmt, nil)
		require.NoError(t, err, "statement failed: %s", stmt)
	}
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err, "COMMIT must succeed")
}

func scalarResult(t *testing.T, exec *StorageExecutor, ctx context.Context, query string) int64 {
	t.Helper()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err, "query failed: %s", query)
	require.Len(t, result.Rows, 1, "expected one row: %s", query)
	require.Len(t, result.Rows[0], 1)
	value, ok := result.Rows[0][0].(int64)
	require.True(t, ok, "expected int64, got %T (%v)", result.Rows[0][0], result.Rows[0][0])
	return value
}

func TestGh741_DetachDeleteEndpointOfStagedRelationship(t *testing.T) {
	exec := newGh741Executor(t)
	ctx := context.Background()

	// CREATE (:B)-[:U]->(:C) then DETACH DELETE the end node, in one
	// transaction. Neo4j commits: 1 node, 0 relationships.
	runGh741Tx(t, exec, ctx,
		"CREATE (:B)-[:U]->(:C)",
		"MATCH (n:C) DETACH DELETE n",
	)
	require.EqualValues(t, 1, scalarResult(t, exec, ctx, "MATCH (n:B) RETURN count(n) AS c"))
	require.EqualValues(t, 0, scalarResult(t, exec, ctx, "MATCH ()-[r:U]->() RETURN count(r) AS c"))

	// Determinism: the same transaction shape must succeed again.
	runGh741Tx(t, exec, ctx,
		"CREATE (:B)-[:U]->(:C)",
		"MATCH (n:C) DETACH DELETE n",
	)
	require.EqualValues(t, 2, scalarResult(t, exec, ctx, "MATCH (n:B) RETURN count(n) AS c"))
	require.EqualValues(t, 0, scalarResult(t, exec, ctx, "MATCH ()-[r:U]->() RETURN count(r) AS c"))
}

func TestGh741_DeleteRelationshipThenEndpoint(t *testing.T) {
	exec := newGh741Executor(t)
	ctx := context.Background()

	// DELETE r first, then DELETE the end node, in one transaction.
	// Neo4j commits: 1 node, 0 relationships.
	runGh741Tx(t, exec, ctx,
		"CREATE (:B)-[:U]->(:C)",
		"MATCH ()-[r:U]->() DELETE r",
		"MATCH (n:C) DELETE n",
	)
	require.EqualValues(t, 1, scalarResult(t, exec, ctx, "MATCH (n:B) RETURN count(n) AS c"))
	require.EqualValues(t, 0, scalarResult(t, exec, ctx, "MATCH ()-[r:U]->() RETURN count(r) AS c"))
}

func TestGh741_DeleteRelationshipOnlyStillCommits(t *testing.T) {
	exec := newGh741Executor(t)
	ctx := context.Background()

	// Control: deleting only the relationship commits with 2 nodes, 0 rels.
	runGh741Tx(t, exec, ctx,
		"CREATE (:B)-[:U]->(:C)",
		"MATCH ()-[r:U]->() DELETE r",
	)
	require.EqualValues(t, 2, scalarResult(t, exec, ctx, "MATCH (n) RETURN count(n) AS c"))
	require.EqualValues(t, 0, scalarResult(t, exec, ctx, "MATCH ()-[r:U]->() RETURN count(r) AS c"))
}

func TestGh741_DetachDeleteKeepsPositionalCountersRight(t *testing.T) {
	exec := newGh741Executor(t)
	ctx := context.Background()

	// Setup (auto-commit), from the issue: one node with both labels.
	_, err := exec.Execute(ctx, "CREATE (:A:B {i: 2})", nil)
	require.NoError(t, err)

	runGh741Tx(t, exec, ctx,
		"MATCH (a:A), (b:B) WITH a, b LIMIT 1 CREATE (a)-[:U]->(b)",
		"MATCH (n:A) WITH n LIMIT 1 DETACH DELETE n",
	)

	// The positional counter behind MATCH (:A)-[r:U]->() must be 0: the only
	// :U relationship was a self-loop on the deleted node.
	require.EqualValues(t, 0, scalarResult(t, exec, ctx, "MATCH (:A)-[r:U]->() RETURN count(r) AS c"))
	require.EqualValues(t, 0, scalarResult(t, exec, ctx, "MATCH ()-[r:U]->() RETURN count(r) AS c"))
	require.EqualValues(t, 0, scalarResult(t, exec, ctx, "MATCH (n:A) RETURN count(n) AS c"))
}
