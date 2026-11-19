package cypher

// The exported DetectQueryPattern -> ExecuteOptimized pair documents a
// grouped edge-property aggregate contract:
//
//	MATCH (<start>)-[r:<TYPE>]->(<group>)
//	RETURN <group>.name, avg(r.prop), count(r) [, further aggregates on r.prop]
//
// The detector-population cases below pin that contract with exact typed
// results and columns, and each case is compared against the canonical Execute
// result for the same query and data. They deliberately assert `name`,
// `count(r)`, `count(*)`, window and aggregate ORDER BY behaviour rather than
// re-deriving the implementation's behaviour: the values follow Cypher
// semantics (avg is always a float, sum/min/max keep their integer type for
// integer inputs, sum of no values is integer 0, a group whose relationships
// all lack the property keeps its row with a null average, count counts matched
// rows, DESC puts null first) and the canonical pipeline is the second opinion.
//
// Failure boundaries keep the existing (nil, false) API: a detector-supported
// query whose evaluation fails (non-numeric aggregated value, storage read
// failure, canceled context) is declined rather than answered with a partial
// or fabricated row. The non-numeric case lives beside the migrated branch test
// in optimized_executors_test.go.

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// edgePropAggContractFixture is the population every case reads:
//
//	P1  4.5 + 5          mixed int/float group
//	P2  2 + 6            integer group
//	P3  two unrated      null-only group (avg/min/max null, count 2, sum 0)
//	P4  7                single-value group
//	O1  3                end node without the :Product label
//
// plus two isolations: a REVIEWED edge pointing Product -> Customer (opposite
// direction) and an OTHER edge carrying the same rating property.
const edgePropAggContractFixture = `CREATE
	(c1:Customer {name: 'C1'}), (c2:Customer {name: 'C2'}), (c3:Customer {name: 'C3'}),
	(p1:Product {name: 'P1'}), (p2:Product {name: 'P2'}), (p3:Product {name: 'P3'}), (p4:Product {name: 'P4'}),
	(o1:Other {name: 'O1'}),
	(c1)-[:REVIEWED {rating: 4.5}]->(p1),
	(c2)-[:REVIEWED {rating: 5}]->(p1),
	(c1)-[:REVIEWED {rating: 2}]->(p2),
	(c2)-[:REVIEWED {rating: 6}]->(p2),
	(c1)-[:REVIEWED]->(p3),
	(c2)-[:REVIEWED]->(p3),
	(c3)-[:REVIEWED {rating: 7}]->(p4),
	(p1)-[:REVIEWED {rating: 1}]->(c1),
	(c1)-[:OTHER {rating: 9}]->(p1),
	(c3)-[:REVIEWED {rating: 3}]->(o1)`

// TestEdgePropertyAggOptimizedBoundaries covers the supported detector
// population's boundaries: null-only and missing groups, every emitted column
// at its exact type, count(r) and count(*), grouping by the start or the end
// variable, direction/type/label/WHERE isolation, aggregate and name ORDER BY,
// and SKIP/LIMIT windows.
func TestEdgePropertyAggOptimizedBoundaries(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := newStatementExecutor(t, "edge_prop_agg_contract", edgePropAggContractFixture)
		ctx := context.Background()

		cases := []struct {
			name       string
			query      string
			wantCols   []string
			wantRows   [][]interface{}
			wantAggFns []string
			wantLimit  int
		}{
			{
				name: "end group keeps the null-only row and emits every column at its exact type",
				query: "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) " +
					"RETURN p.name, avg(r.rating), count(r), count(*), sum(r.rating), min(r.rating), max(r.rating) ORDER BY p.name",
				wantCols: []string{"p.name", "avg(r.rating)", "count(r)", "count(*)", "sum(r.rating)", "min(r.rating)", "max(r.rating)"},
				wantRows: [][]interface{}{
					{"P1", 4.75, int64(2), int64(2), 9.5, 4.5, int64(5)},
					{"P2", 4.0, int64(2), int64(2), int64(8), int64(2), int64(6)},
					{"P3", nil, int64(2), int64(2), int64(0), nil, nil},
					{"P4", 7.0, int64(1), int64(1), int64(7), int64(7), int64(7)},
				},
				wantAggFns: []string{"avg", "sum", "min", "max"},
			},
			{
				name:       "reversed direction groups the same product rows",
				query:      "MATCH (p:Product)<-[r:REVIEWED]-(c:Customer) RETURN p.name, avg(r.rating), count(r) ORDER BY p.name",
				wantCols:   []string{"p.name", "avg(r.rating)", "count(r)"},
				wantRows:   [][]interface{}{{"P1", 4.75, int64(2)}, {"P2", 4.0, int64(2)}, {"P3", nil, int64(2)}, {"P4", 7.0, int64(1)}},
				wantAggFns: []string{"avg"},
			},
			{
				name:       "unlabelled end node widens the grouped population",
				query:      "MATCH (c:Customer)-[r:REVIEWED]->(p) RETURN p.name, avg(r.rating), count(r) ORDER BY p.name",
				wantCols:   []string{"p.name", "avg(r.rating)", "count(r)"},
				wantRows:   [][]interface{}{{"O1", 3.0, int64(1)}, {"P1", 4.75, int64(2)}, {"P2", 4.0, int64(2)}, {"P3", nil, int64(2)}, {"P4", 7.0, int64(1)}},
				wantAggFns: []string{"avg"},
			},
			{
				name:       "grouping start variable counts the unrated relationship",
				query:      "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN c.name, avg(r.rating), count(r) ORDER BY c.name",
				wantCols:   []string{"c.name", "avg(r.rating)", "count(r)"},
				wantRows:   [][]interface{}{{"C1", 3.25, int64(3)}, {"C2", 5.5, int64(3)}, {"C3", 7.0, int64(1)}},
				wantAggFns: []string{"avg"},
			},
			{
				name:       "WHERE filters relationships before grouping",
				query:      "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) WHERE r.rating > 3 RETURN p.name, avg(r.rating), count(r) ORDER BY p.name",
				wantCols:   []string{"p.name", "avg(r.rating)", "count(r)"},
				wantRows:   [][]interface{}{{"P1", 4.75, int64(2)}, {"P2", 6.0, int64(1)}, {"P4", 7.0, int64(1)}},
				wantAggFns: []string{"avg"},
			},
			{
				name:       "relationship type isolates the aggregated edges",
				query:      "MATCH (c:Customer)-[r:OTHER]->(p:Product) RETURN p.name, avg(r.rating), count(r) ORDER BY p.name",
				wantCols:   []string{"p.name", "avg(r.rating)", "count(r)"},
				wantRows:   [][]interface{}{{"P1", 9.0, int64(1)}},
				wantAggFns: []string{"avg"},
			},
			{
				name:       "ORDER BY the average alias puts the null group first in DESC",
				query:      "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating) AS a, count(r) AS c ORDER BY a DESC",
				wantCols:   []string{"p.name", "a", "c"},
				wantRows:   [][]interface{}{{"P3", nil, int64(2)}, {"P4", 7.0, int64(1)}, {"P1", 4.75, int64(2)}, {"P2", 4.0, int64(2)}},
				wantAggFns: []string{"avg"},
			},
			{
				name:       "ORDER BY the count alias breaks ties with the name",
				query:      "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating) AS a, count(r) AS c ORDER BY c DESC, p.name",
				wantCols:   []string{"p.name", "a", "c"},
				wantRows:   [][]interface{}{{"P1", 4.75, int64(2)}, {"P2", 4.0, int64(2)}, {"P3", nil, int64(2)}, {"P4", 7.0, int64(1)}},
				wantAggFns: []string{"avg"},
			},
			{
				name:       "SKIP and LIMIT window the grouped rows",
				query:      "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r) ORDER BY p.name SKIP 1 LIMIT 2",
				wantCols:   []string{"p.name", "avg(r.rating)", "count(r)"},
				wantRows:   [][]interface{}{{"P2", 4.0, int64(2)}, {"P3", nil, int64(2)}},
				wantAggFns: []string{"avg"},
				wantLimit:  2,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				info, err := DetectQueryPattern(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, PatternEdgePropertyAgg, info.Pattern, "the shape belongs to the optimized population")
				require.True(t, info.IsOptimizable())
				require.Equal(t, "rating", info.AggProperty)
				require.Equal(t, "r", info.RelVar)
				require.Equal(t, tc.wantAggFns, info.AggFunctions)
				require.Equal(t, tc.wantLimit, info.Limit)

				optimized, handled := exec.ExecuteOptimized(ctx, tc.query, info)
				require.True(t, handled, "the detector's own PatternInfo must be handled")
				require.NotNil(t, optimized)
				require.Equal(t, tc.wantCols, optimized.Columns)
				require.Equal(t, tc.wantRows, optimized.Rows)
				for i, row := range optimized.Rows {
					require.Len(t, row, len(tc.wantCols), "row %d must carry every emitted column", i)
				}

				general, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, general.Columns, optimized.Columns, "canonical control")
				require.Equal(t, general.Rows, optimized.Rows, "canonical control")
			})
		}
	})
}

// TestEdgePropertyAggOptimizedEmptyPopulation: the supported shape over a store
// with no matching relationship still advertises its columns and reports no
// rows, exactly as the canonical route does.
func TestEdgePropertyAggOptimizedEmptyPopulation(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "edge_prop_agg_empty"))
		ctx := context.Background()
		query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r)"

		info, err := DetectQueryPattern(ctx, query)
		require.NoError(t, err)
		require.Equal(t, PatternEdgePropertyAgg, info.Pattern)

		optimized, handled := exec.ExecuteOptimized(ctx, query, info)
		require.True(t, handled)
		require.NotNil(t, optimized)
		require.Equal(t, []string{"p.name", "avg(r.rating)", "count(r)"}, optimized.Columns)
		require.Empty(t, optimized.Rows)

		general, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, general.Columns, optimized.Columns)
		require.Equal(t, general.Rows, optimized.Rows)
	})
}

// matchErrEngine (pipeline_executor_error_paths_test.go) fails every label and
// full-node read. The edge-property aggregate shape reads its start population
// that way, so a failing read must surface as an evaluation failure.

// TestEdgePropertyAggOptimizedStorageFailureDeclines: a storage read failure is
// not a result. The exported pair has no error return, so the failed query is
// declined and the caller keeps the general path, which reports the failure.
func TestEdgePropertyAggOptimizedStorageFailureDeclines(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "edge_prop_agg_failure")
		readFailure := errors.New("edge aggregate population read failed")
		exec := NewStorageExecutor(&matchErrEngine{Engine: store, err: readFailure})
		ctx := context.Background()
		query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r)"

		info, err := DetectQueryPattern(ctx, query)
		require.NoError(t, err)
		require.Equal(t, PatternEdgePropertyAgg, info.Pattern)

		result, handled := exec.ExecuteOptimized(ctx, query, info)
		require.False(t, handled, "a failed storage read must not be published as an optimized result")
		require.Nil(t, result)

		_, err = exec.Execute(ctx, query, nil)
		require.ErrorIs(t, err, readFailure)
	})
}

// TestEdgePropertyAggOptimizedCancellationDeclines: a canceled context is not a
// successful aggregate. The detector reports the cancellation and the optimizer
// declines instead of answering with the rows it managed to read.
func TestEdgePropertyAggOptimizedCancellationDeclines(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := newStatementExecutor(t, "edge_prop_agg_contract", edgePropAggContractFixture)
		query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r)"

		info, err := DetectQueryPattern(context.Background(), query)
		require.NoError(t, err)
		require.Equal(t, PatternEdgePropertyAgg, info.Pattern)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, detectErr := DetectQueryPattern(ctx, query)
		require.ErrorIs(t, detectErr, context.Canceled, "the detector reports the cancellation")

		result, handled := exec.ExecuteOptimized(ctx, query, info)
		require.False(t, handled, "a canceled aggregate must not be published as an optimized result")
		require.Nil(t, result)
	})
}

// TestEdgePropertyAggOptimizedUnsupportedShapesDecline pins the detector
// population: shapes outside it keep handled=false even though the canonical
// route answers them, so no query is silently answered by a narrower route.
func TestEdgePropertyAggOptimizedUnsupportedShapesDecline(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := newStatementExecutor(t, "edge_prop_agg_contract", edgePropAggContractFixture)
		ctx := context.Background()

		cases := []struct {
			name  string
			query string
		}{
			{
				name:  "aggregates over different properties",
				query: "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), sum(r.bonus)",
			},
			{
				name:  "no grouped name projection",
				query: "MATCH ()-[r:REVIEWED]->() RETURN avg(r.rating), count(r)",
			},
			{
				name:  "WITH clause outside the optimized population",
				query: "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) WITH p, r RETURN p.name, avg(r.rating), count(r)",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				info, err := DetectQueryPattern(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, PatternGeneric, info.Pattern, "the shape stays outside the optimized population")
				require.False(t, info.IsOptimizable())

				result, handled := exec.ExecuteOptimized(ctx, tc.query, info)
				require.False(t, handled)
				require.Nil(t, result)
			})
		}
	})
}
