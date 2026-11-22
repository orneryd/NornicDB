package cypher

// NOR-70 follow-up adjudication (test-only). NOR-70:
// https://linear.app/nornicdb/issue/NOR-70/return-independent-aggregates-and-retain-groups-whose-values-are-null
//
// The outcome under test is unchanged: a one-hop grouped query calculates each
// aggregate independently, and a group whose aggregated property has no values
// is still returned (avg = null, collect = []). This file adds three things the
// existing reproduction file does not cover:
//
//  1. The prepared one-hop fixture and the prepared null-only fixture executed
//     in an explicit native transaction on a real BadgerEngine, through both
//     the BEGIN/COMMIT statement route and the BEGIN … COMMIT script route,
//     under both process-wide parsers. Staged isolation and post-commit
//     visibility are asserted so the transaction is proven to be a real
//     snapshot, not an empty wrapper.
//  2. Adjudication of the exported DetectQueryPattern + ExecuteOptimized pair
//     for the exact original NOR-70 queries. The detector is never bypassed:
//     the returned PatternInfo is passed to ExecuteOptimized unchanged. The
//     original shapes are outside the optimized detector's population, so
//     handled=false is the offered API delegating to the general path; the same
//     general path is then executed and must satisfy the approved contract.
//  3. The differently scoped, detector-supported PatternEdgePropertyAgg shape
//     (RETURN <end>.name, avg(<rel>.prop), count(<rel>)) measured over
//     relationships that lack the aggregated property. The required result is asserted against the general path; no defect count is invented,
//     because only this one supported query shape was measured.
//
// No production code is changed. Every assertion goes through the public
// NewStorageExecutor + Execute / DetectQueryPattern / ExecuteOptimized API.
// withCypherParsers runs each test in a child process for both parser
// selections, so no sibling package test inherits a process-wide setting.

import (
	"context"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor70PreparedFixtureCreateS3 is the issue's prepared one-hop fixture exactly as
// written in NOR-70.
const nor70PreparedFixtureCreateS3 = "CREATE (q:AggOwner {g: 'g'}), (p1:AggValue {y: 1}), (p2:AggValue {y: 2}), " +
	"(q)-[:AGG {x: 1}]->(p1), (q)-[:AGG {x: 2}]->(p2)"

// nor70FollowupNativeFixtureS3 returns a real BadgerEngine (in-memory) and an
// executor over a namespaced view of it. The engine is tracked and explicitly
// closed, so the backing Badger resources are actually released.
func nor70FollowupNativeFixtureS3(t *testing.T) (storage.Engine, *StorageExecutor) {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "nor70_followup_native")
	return store, NewStorageExecutor(store)
}

// nor70FollowupMemoryFixture seeds the prepared one-hop fixture in a fresh
// in-memory store and returns its executor.
func nor70FollowupMemoryFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor70_followup_memory"))
	_, err := exec.Execute(context.Background(), nor70PreparedFixtureCreateS3, nil)
	require.NoError(t, err)
	return exec
}

// nor70FollowupBeginS3 starts an explicit transaction through the public Execute
// route and checks the documented BEGIN result.
func nor70FollowupBeginS3(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	result, err := exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"status"}, result.Columns)
	require.Equal(t, [][]interface{}{{"Transaction started"}}, result.Rows)
}

// nor70FollowupCommitS3 commits and fails the test if COMMIT reported an error.
func nor70FollowupCommitS3(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	_, err := exec.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)
}

// TestNOR70NativeExplicitTransactionAggregateContractsS3 runs the issue's two
// prepared contracts inside an explicit native Badger transaction: the fixture
// is written, read and aggregated in the transaction, isolated from a committed
// reader, then committed and re-read from a separate executor. The reversed
// aggregate order is checked in the same transaction so projection order is
// proven not to share a running input.
func TestNOR70NativeExplicitTransactionAggregateContractsS3(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		t.Run("prepared fixture written and aggregated in BEGIN/COMMIT", func(t *testing.T) {
			store, exec := nor70FollowupNativeFixtureS3(t)
			ctx := context.Background()

			nor70FollowupBeginS3(t, exec)
			committed := false
			defer func() {
				if !committed {
					_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
				}
			}()

			_, err := exec.Execute(ctx, nor70PreparedFixtureCreateS3, nil)
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

			nor70FollowupCommitS3(t, exec)
			committed = true

			after, err := NewStorageExecutor(store).Execute(ctx, nor70OneHopPattern+" RETURN q.g, sum(r.x), avg(p.y)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", int64(3), float64(1.5)}}, after.Rows,
				"the committed fixture must produce the same contract outside the transaction")
		})

		t.Run("null-only group after removing both p.y in BEGIN/COMMIT", func(t *testing.T) {
			store, exec := nor70FollowupNativeFixtureS3(t)
			ctx := context.Background()

			_, err := exec.Execute(ctx, nor70PreparedFixtureCreateS3, nil)
			require.NoError(t, err)

			nor70FollowupBeginS3(t, exec)
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

			nor70FollowupCommitS3(t, exec)
			committed = true

			after, err := NewStorageExecutor(store).Execute(ctx, nor70OneHopPattern+" RETURN q.g, avg(p.y), collect(p.y)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"g", nil, []interface{}{}}}, after.Rows,
				"the committed null-only contract must hold outside the transaction")
		})

		t.Run("BEGIN … COMMIT script route", func(t *testing.T) {
			store, exec := nor70FollowupNativeFixtureS3(t)
			ctx := context.Background()

			_, err := exec.Execute(ctx, nor70PreparedFixtureCreateS3, nil)
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

// TestNOR70DetectQueryPatternAndExecuteOptimizedOriginalQueries adjudicates the
// exported detector/optimized pair with the actual detector. The detector's
// output is passed to ExecuteOptimized unchanged (nothing is hand-forced). The
// four original NOR-70 shapes are outside the optimized population, so
// handled=false is the offered contract delegating to the general path; the
// general path is then run through public Execute and must satisfy the approved
// contract.
func TestNOR70DetectQueryPatternAndExecuteOptimizedOriginalQueries(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		prepared := nor70FollowupMemoryFixture(t)
		nullOnly := nor70FollowupMemoryFixture(t)
		_, err := nullOnly.Execute(context.Background(), "MATCH (p:AggValue) REMOVE p.y", nil)
		require.NoError(t, err)

		cases := []struct {
			name  string
			exec  *StorageExecutor
			query string
			want  [][]interface{}
		}{
			{
				name:  "prepared fixture, sum then avg",
				exec:  prepared,
				query: nor70OneHopPattern + " RETURN q.g, sum(r.x), avg(p.y)",
				want:  [][]interface{}{{"g", int64(3), float64(1.5)}},
			},
			{
				name:  "prepared fixture, reversed aggregate order",
				exec:  prepared,
				query: nor70OneHopPattern + " RETURN q.g, avg(p.y), sum(r.x)",
				want:  [][]interface{}{{"g", float64(1.5), int64(3)}},
			},
			{
				name:  "null-only avg and collect",
				exec:  nullOnly,
				query: nor70OneHopPattern + " RETURN q.g, avg(p.y), collect(p.y)",
				want:  [][]interface{}{{"g", nil, []interface{}{}}},
			},
			{
				name:  "null-only beside an independent sum",
				exec:  nullOnly,
				query: nor70OneHopPattern + " RETURN q.g, sum(r.x), avg(p.y), collect(p.y)",
				want:  [][]interface{}{{"g", int64(3), nil, []interface{}{}}},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				info, err := DetectQueryPattern(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, PatternGeneric, info.Pattern,
					"the original NOR-70 shape is not in the optimized detector's population")
				require.False(t, info.IsOptimizable())

				result, handled := tc.exec.ExecuteOptimized(ctx, tc.query, info)
				require.False(t, handled,
					"the actual detector reports no optimized route for the original NOR-70 shape")
				require.Nil(t, result)
				t.Logf("detector: %q => pattern=%v handled=%v",
					tc.query, info.Pattern, handled)

				delegated, err := tc.exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, delegated.Rows,
					"handled=false must delegate to the general path, which must satisfy the approved contract")
			})
		}
	})
}

// nor70FollowupEdgeAggFixtureS3 seeds the detector-supported edge-property
// aggregate shape. P1 has two rated reviews, P2 has one review without a
// rating, P3 has one rated and one unrated review.
func nor70FollowupEdgeAggFixtureS3(t *testing.T) *StorageExecutor {
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

// TestNOR70OptimizedEdgeAggNullGroupConsequence measures the one supported
// edge-property aggregate shape the actual detector routes to ExecuteOptimized
// when relationships lack the aggregated property.
//
// Supported population measured here: exactly the detected
// PatternEdgePropertyAgg projection shape
//
//	MATCH (<start>)-[r:<TYPE>]->(p:Product)
//	RETURN p.name, avg(r.rating), count(r)
//
// over an end node whose relationships all lack r.rating, plus an end node with
// a mix of rated and unrated relationships. This is deliberately narrower than
// all uses of PatternEdgePropertyAgg; no count of affected queries is claimed.
//
// Observed consequence (pinned below), on the same data and query:
//
//   - the general path returns three groups: P1 avg 5 count 2, P2 avg null
//     count 1, P3 avg 2 count 2. That is the approved NOR-70 contract.
//   - the optimized path returns two groups. P2 (the null-only group) is
//     absent, and each returned row carries only the avg value even though
//     three columns are advertised, because the detector records only the
//     non-count aggregate in AggFunctions. count(r) is therefore not delivered
//     for this detected shape.
//
// The optimized result must preserve the approved rows and columns. This
// fail-first reproduction intentionally fails while the supported API drops
// the null-only group and omits count values; it changes no production code.
func TestNOR70OptimizedEdgeAggNullGroupConsequence(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := nor70FollowupEdgeAggFixtureS3(t)
		ctx := context.Background()
		query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) " +
			"RETURN p.name, avg(r.rating), count(r) ORDER BY p.name"

		info, err := DetectQueryPattern(ctx, query)
		require.NoError(t, err)
		require.Equal(t, PatternEdgePropertyAgg, info.Pattern,
			"the detector supports this edge-property aggregate shape")
		require.Equal(t, "rating", info.AggProperty)
		require.Equal(t, "r", info.RelVar)
		require.Equal(t, []string{"avg"}, info.AggFunctions,
			"the detector records the non-count aggregates the optimized executor emits")

		optimized, handled := exec.ExecuteOptimized(ctx, query, info)
		require.True(t, handled, "the detector's own PatternInfo must be handled by the optimized executor")
		require.NotNil(t, optimized)

		general, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)

		t.Logf("general columns/rows: %v / %v", general.Columns, general.Rows)
		t.Logf("optimized columns/rows: %v / %v", optimized.Columns, optimized.Rows)

		// The general path is the approved NOR-70 contract: every end node is
		// a group, a group with no values keeps avg = null, and count(r)
		// counts every matched relationship.
		require.Equal(t, [][]interface{}{
			{"P1", float64(5), int64(2)},
			{"P2", nil, int64(1)},
			{"P3", float64(2), int64(2)},
		}, general.Rows)

		// The exported, detector-supported API must meet the same query contract.
		require.Equal(t, general.Columns, optimized.Columns)
		require.Equal(t, general.Rows, optimized.Rows,
			"optimized edge aggregation must retain null-only groups and deliver each aggregate")
	})
}
