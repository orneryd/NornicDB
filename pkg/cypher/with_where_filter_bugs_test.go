// Regression tests for three Cypher evaluation defects observed against
// Neo4j 5.x community as the reference implementation. Each test states the
// Neo4j-measured result in its comment; a failure means NornicDB diverges from
// that reference.

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func setupWithWhereFixture(t *testing.T, store storage.Engine) {
	t.Helper()
	ctx := context.Background()
	exec := NewStorageExecutor(store)
	for _, q := range []string{
		`CREATE (f:Function {uid: 'fn1', name: 'Caller'})`,
		`CREATE (a:CloudAction {action: 's3:GetObject'})`,
		`CREATE (w:Workload {id: 'w1', name: 'checkout'})`,
		`MATCH (f:Function {uid: 'fn1'}) MATCH (a:CloudAction {action: 's3:GetObject'}) CREATE (f)-[:INVOKES_CLOUD_ACTION]->(a)`,
		`MATCH (f:Function {uid: 'fn1'}) MATCH (w:Workload {id: 'w1'}) CREATE (f)-[:RUNS_IN]->(w)`,
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "fixture query failed: %s", q)
	}
}

// A WHERE attached to a WITH must filter. Neo4j 5 returns 1; a label test in
// this clause position is currently dropped, so every node comes back.
func TestBug_WithAttachedWhereLabelTestIsIgnored(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	setupWithWhereFixture(t, store)

	result, err := exec.Execute(ctx, `MATCH (n) WITH n WHERE n:Workload RETURN count(*) AS c`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, 1, result.Rows[0][0],
		"WITH-attached WHERE must filter by label; Neo4j 5 returns 1")
}

// The same predicate placed before the WITH is the documented workaround and
// must keep working — it is the control for the test above.
func TestBug_WhereBeforeWithStillFilters(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	setupWithWhereFixture(t, store)

	result, err := exec.Execute(ctx, `MATCH (n) WHERE n:Workload WITH n RETURN count(*) AS c`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, 1, result.Rows[0][0])
}

// A function call on a bound variable inside a WITH-attached WHERE must be
// evaluated, not short-circuited to NULL. Neo4j 5 returns 1.
func TestBug_WithAttachedWhereFunctionCallEvaluatesNull(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	setupWithWhereFixture(t, store)

	result, err := exec.Execute(ctx,
		`MATCH (w:Workload) WITH w WHERE toUpper(w.name) = 'CHECKOUT' RETURN count(*) AS c`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, 1, result.Rows[0][0],
		"toUpper on a bound variable must evaluate; Neo4j 5 returns 1")
}

func TestBug_WithAttachedWhereFunctionCallOnComputedValues(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	setupWithWhereFixture(t, store)

	t.Run("function result is null after path traversal", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (f:Function)-[*1..2]->(impacted)
			WITH impacted WHERE coalesce(impacted.id, 'X') IS NULL
			RETURN count(*) AS c`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.EqualValues(t, 0, result.Rows[0][0],
			"a non-null coalesce result must not pass IS NULL; Neo4j 5 returns 0")
	})

	t.Run("function consumes a scalar WITH alias", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (w:Workload)
			WITH w.name AS name WHERE toUpper(name) = 'CHECKOUT'
			RETURN count(*) AS c`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.EqualValues(t, 1, result.Rows[0][0],
			"functions in a WITH-attached WHERE must retain scalar aliases; Neo4j 5 returns 1")
	})
}

// A disjunction of label tests in a WITH-attached WHERE is the shape real
// traversal whitelists use. Neo4j 5 returns 2 here (the Workload and the
// CloudAction), not 3.
func TestBug_WithAttachedWhereLabelDisjunction(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	setupWithWhereFixture(t, store)

	result, err := exec.Execute(ctx,
		`MATCH (n) WITH n WHERE n:Workload OR n:CloudAction RETURN count(*) AS c`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, 2, result.Rows[0][0],
		"a disjunction of label tests must filter; Neo4j 5 returns 2")
}
