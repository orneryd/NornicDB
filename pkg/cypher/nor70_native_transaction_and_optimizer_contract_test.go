package cypher

// NOR-70 grouped relationship aggregate coverage through Execute with both parsers.

import (
	"context"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor70PreparedFixtureCreate is the issue's prepared one-hop fixture exactly as
// written in NOR-70.
const nor70PreparedFixtureCreate = "CREATE (q:AggOwner {g: 'g'}), (p1:AggValue {y: 1}), (p2:AggValue {y: 2}), " +
	"(q)-[:AGG {x: 1}]->(p1), (q)-[:AGG {x: 2}]->(p2)"

// nor70FollowupNativeFixture returns a real BadgerEngine (in-memory) and an
// executor over a namespaced view of it. The engine is tracked and explicitly
// closed, so the backing Badger resources are actually released.
func nor70FollowupNativeFixture(t *testing.T) (storage.Engine, *StorageExecutor) {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "nor70_followup_native")
	return store, NewStorageExecutor(store)
}

// nor70FollowupBegin starts an explicit transaction through the public Execute
// route and checks the documented BEGIN result.
func nor70FollowupBegin(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	result, err := exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"status"}, result.Columns)
	require.Equal(t, [][]interface{}{{"Transaction started"}}, result.Rows)
}

// nor70FollowupCommit commits and fails the test if COMMIT reported an error.
func nor70FollowupCommit(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	_, err := exec.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
}

// TestNOR70NativeExplicitTransactionAggregateContracts runs the issue's two
// prepared contracts inside an explicit native Badger transaction: the fixture
// is written, read and aggregated in the transaction, isolated from a committed
// reader, then committed and re-read from a separate executor. The reversed
// aggregate order is checked in the same transaction so projection order is
// proven not to share a running input.
func TestNOR70NativeExplicitTransactionAggregateContracts(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		t.Run("prepared fixture written and aggregated in BEGIN/COMMIT", func(t *testing.T) {
			store, exec := nor70FollowupNativeFixture(t)
			ctx := context.Background()

			nor70FollowupBegin(t, exec)
			committed := false
			defer func() {
				if !committed {
					_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
				}
			}()

			_, err := exec.Execute(ctx, nor70PreparedFixtureCreate, nil)
			require.NoError(t, err)

			// The transaction is a real snapshot: a committed reader cannot
			// see its staged fixture before COMMIT.
			isolation, err := NewStorageExecutor(store).Execute(ctx, "MATCH (q:AggOwner) RETURN count(q)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, isolation.Rows,
				"a committed reader must not see the transaction's staged writes")

			original, err := exec.Execute(ctx, nor70OneHopPattern+" RETURN q.g, sum(r.x), avg(p.y)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", int64(3), float64(1.5)}}, original.Rows,
				"sum(r.x) and avg(p.y) must have independent values inside the transaction")
			t.Logf("NOR-70 in BEGIN/COMMIT: %q => %v", nor70OneHopPattern+" RETURN q.g, sum(r.x), avg(p.y)", original.Rows)

			reversed, err := exec.Execute(ctx, nor70OneHopPattern+" RETURN q.g, avg(p.y), sum(r.x)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", float64(1.5), int64(3)}}, reversed.Rows,
				"reversed projection order must not change either aggregate's input")

			nor70FollowupCommit(t, exec)
			committed = true

			after, err := NewStorageExecutor(store).Execute(ctx, nor70OneHopPattern+" RETURN q.g, sum(r.x), avg(p.y)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", int64(3), float64(1.5)}}, after.Rows,
				"the committed fixture must produce the same contract outside the transaction")
		})

		t.Run("null-only group after removing both p.y in BEGIN/COMMIT", func(t *testing.T) {
			store, exec := nor70FollowupNativeFixture(t)
			ctx := context.Background()

			_, err := exec.Execute(ctx, nor70PreparedFixtureCreate, nil)
			require.NoError(t, err)

			nor70FollowupBegin(t, exec)
			committed := false
			defer func() {
				if !committed {
					_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
				}
			}()

			_, err = exec.Execute(ctx, "MATCH (p:AggValue) REMOVE p.y", nil)
			require.NoError(t, err)

			nullOnly, err := exec.Execute(ctx, nor70OneHopPattern+" RETURN q.g, avg(p.y), collect(p.y)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", nil, []interface{}{}}}, nullOnly.Rows,
				"the group is retained with avg = null and collect = [] inside the transaction")
			t.Logf("NOR-70 null-only in BEGIN/COMMIT: %q => %v", nor70OneHopPattern+" RETURN q.g, avg(p.y), collect(p.y)", nullOnly.Rows)

			reversed, err := exec.Execute(ctx, nor70OneHopPattern+" RETURN q.g, avg(p.y), sum(r.x)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", nil, int64(3)}}, reversed.Rows,
				"the independent sum beside a null average keeps the group and its value")

			// The removal is still staged: a committed reader sees the values
			// this transaction removed.
			before, err := NewStorageExecutor(store).Execute(ctx, nor70OneHopPattern+" RETURN q.g, avg(p.y), sum(r.x)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", float64(1.5), int64(3)}}, before.Rows,
				"the transaction's REMOVE must not be visible before COMMIT")

			nor70FollowupCommit(t, exec)
			committed = true

			after, err := NewStorageExecutor(store).Execute(ctx, nor70OneHopPattern+" RETURN q.g, avg(p.y), collect(p.y)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", nil, []interface{}{}}}, after.Rows,
				"the committed null-only contract must hold outside the transaction")
		})

		t.Run("BEGIN … COMMIT script route", func(t *testing.T) {
			store, exec := nor70FollowupNativeFixture(t)
			ctx := context.Background()

			_, err := exec.Execute(ctx, nor70PreparedFixtureCreate, nil)
			require.NoError(t, err)

			script := "BEGIN\n" + nor70OneHopPattern + "\nRETURN q.g, sum(r.x), avg(p.y)\nCOMMIT"
			original, err := exec.Execute(ctx, script, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", int64(3), float64(1.5)}}, original.Rows,
				"the BEGIN … COMMIT script must return the aggregate result after commit")
			t.Logf("NOR-70 BEGIN/COMMIT script => %v", original.Rows)

			_, err = exec.Execute(ctx, "MATCH (p:AggValue) REMOVE p.y", nil)
			require.NoError(t, err)

			nullScript := "BEGIN\n" + nor70OneHopPattern + "\nRETURN q.g, avg(p.y), collect(p.y)\nCOMMIT"
			nullOnly, err := exec.Execute(ctx, nullScript, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", nil, []interface{}{}}}, nullOnly.Rows,
				"the null-only group must survive the BEGIN … COMMIT script route")

			after, err := NewStorageExecutor(store).Execute(ctx, nor70OneHopPattern+" RETURN q.g, avg(p.y), collect(p.y)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", nil, []interface{}{}}}, after.Rows,
				"the script route must commit the removal")
		})
	})
}

// nor70FollowupEdgeAggFixture seeds the detector-supported edge-property
// aggregate shape. P1 has two rated reviews, P2 has one review without a
// rating, P3 has one rated and one unrated review.
func nor70FollowupEdgeAggFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor70_followup_edge_agg"))
	_, err := exec.Execute(context.Background(), `CREATE
		(c1:Customer {id: 1}), (c2:Customer {id: 2}), (c3:Customer {id: 3}), (c4:Customer {id: 4}),
		(p1:Product {name: 'P1'}), (p2:Product {name: 'P2'}), (p3:Product {name: 'P3'}),
		(c1)-[:REVIEWED {rating: 4}]->(p1),
		(c2)-[:REVIEWED {rating: 6}]->(p1),
		(c3)-[:REVIEWED]->(p2),
		(c1)-[:REVIEWED {rating: 2}]->(p3),
		(c4)-[:REVIEWED]->(p3)`, nil)
	require.NoError(t, err)
	return exec
}

func TestNOR70ExecuteEdgeAggNullGroup(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := nor70FollowupEdgeAggFixture(t)
		ctx := context.Background()
		query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) " +
			"RETURN p.name, avg(r.rating), count(r) ORDER BY p.name"

		general, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		require.Equal(t, []string{"p.name", "avg(r.rating)", "count(r)"}, general.Columns)

		// The general path is the approved NOR-70 contract: every end node is
		// a group, a group with no values keeps avg = null, and count(r)
		// counts every matched relationship.
		require.Equal(t, [][]interface{}{
			{"P1", float64(5), int64(2)},
			{"P2", nil, int64(1)},
			{"P3", float64(2), int64(2)},
		}, general.Rows)

		window, err := exec.Execute(ctx, query+" SKIP 1 LIMIT 1", nil)
		require.NoError(t, err)
		require.Equal(t, general.Columns, window.Columns)
		require.Equal(t, [][]interface{}{{"P2", nil, int64(1)}}, window.Rows)
	})
}
