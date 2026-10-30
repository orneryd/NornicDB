package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransactionalReviewOuterWritesRollback(t *testing.T) {
	for _, prefix := range []string{"", "PROFILE "} {
		t.Run(prefix, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
			_, err := e.Execute(context.Background(), prefix+`UNWIND [1,2] AS x CALL { WITH x CREATE (:ReviewBatch {v:x}) } IN TRANSACTIONS OF 1 ROWS CREATE (:ReviewBatch {v:100+x}) RETURN 1/$d AS z`, map[string]interface{}{"d": int64(0)})
			require.Error(t, err)
			got, err := e.Execute(context.Background(), `MATCH (n:ReviewBatch) RETURN n.v ORDER BY n.v`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, got.Rows)
		})
	}
}

func TestTransactionalReviewCommitBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, tail string
		want       int64
	}{
		{"bare_create", `WITH COUNT { MATCH (n) } AS c CALL { CREATE (:Later) } IN TRANSACTIONS OF $bad ROWS`, 1},
		{"runtime_skip", `WITH count(*) AS c SKIP $bad RETURN c`, 1},
		{"pass_through_skip", `WITH x SKIP $bad RETURN x`, 0},
		{"reverse_type", `WITH count(*) AS c CALL { CREATE (:Later) } IN TRANSACTIONS OF head(reverse(['bad'])) ROWS`, 0},
		{"tail_type", `WITH count(*) AS c CALL { CREATE (:Later) } IN TRANSACTIONS OF head(tail(['bad','bad'])) ROWS`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
			_, err := e.Execute(context.Background(), `UNWIND [1] AS x CALL { CREATE (a) } IN TRANSACTIONS OF 1 ROWS `+tc.tail, map[string]interface{}{"bad": int64(-1)})
			require.Error(t, err)
			got, err := e.Execute(context.Background(), `MATCH (n) RETURN count(n)`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, got.Rows)
		})
	}
}

func TestTransactionalReviewBareCreateEffects(t *testing.T) {
	for _, query := range []string{"CREATE (a)", "CREATE (a)-[:R]->(b)"} {
		effects, err := clauseEffectsForTest(context.Background(), query)
		require.NoError(t, err)
		if query == "CREATE (a)" {
			require.True(t, effects.chainWrites.anyNode, "a standalone bare CREATE creates a node")
		}
	}
}

func TestTransactionalReviewMutationReads(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
	_, err := e.Execute(context.Background(), `UNWIND [1,2] AS x CREATE (:Phase {v: COUNT { MATCH (:Phase) }})`, nil)
	require.NoError(t, err)
	got, err := e.Execute(context.Background(), `MATCH (n:Phase) RETURN n.v ORDER BY n.v`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}, {int64(0)}}, got.Rows)
}

func TestTransactionalReviewConsecutiveCreateReads(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
	_, err := e.Execute(context.Background(), `UNWIND [1,2] AS x CREATE (:Earlier) CREATE (:Later {v: COUNT { MATCH (:Earlier) }})`, nil)
	require.NoError(t, err)
	got, err := e.Execute(context.Background(), `MATCH (n:Later) RETURN n.v ORDER BY n.v`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(2)}}, got.Rows)
}

func TestTransactionalReviewBatchVisibility(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
	got, err := e.Execute(context.Background(), `UNWIND [1,2] AS x CALL { WITH x CREATE (:VisibleBatch {v:x}) } IN TRANSACTIONS OF 1 ROWS WITH count(*) AS c MATCH (n:VisibleBatch) RETURN n.v ORDER BY n.v`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, got.Rows)
}

func TestTransactionalReviewUpdatedImportedNode(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
	_, err := e.Execute(context.Background(), `CREATE (:Counter {v:0})`, nil)
	require.NoError(t, err)
	got, err := e.Execute(context.Background(), `MATCH (n:Counter) CALL { WITH n SET n.v=n.v+1 } IN TRANSACTIONS OF 1 ROWS RETURN n.v`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, got.Rows)
}

func TestTransactionalReviewCreateSelfReadPhases(t *testing.T) {
	for _, query := range []string{
		`UNWIND [1,2] AS x WITH x CREATE (:Phase {v: COUNT { MATCH (:Phase) }})`,
		`UNWIND [1,2] AS x WITH x WHERE x>0 CREATE (:Phase {v: COUNT { MATCH (:Phase) }})`,
	} {
		t.Run(query, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review"))
			_, err := e.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			got, err := e.Execute(context.Background(), `MATCH (n:Phase) RETURN n.v ORDER BY n.v`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}, {int64(0)}}, got.Rows)
		})
	}
}

func TestTransactionalReviewCompositeBatchRefused(t *testing.T) {
	e := newFabricCallRowsExecutor(t)
	_, err := e.Execute(context.Background(), `USE comp.one UNWIND [1,2] AS x CALL { WITH x CREATE (:ReviewBatch {v:x}) } IN TRANSACTIONS OF 1 ROWS`, nil)
	require.Error(t, err)
	var classified interface{ BoltErrorCode() string }
	require.ErrorAs(t, err, &classified)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode())
	result, err := e.Execute(context.Background(), `USE comp.one MATCH (n:ReviewBatch) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}
