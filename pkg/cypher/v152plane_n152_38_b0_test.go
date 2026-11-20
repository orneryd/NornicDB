package cypher

// NOR-70 public reproduction and audit:
// https://linear.app/nornicdb/issue/NOR-70/return-independent-aggregates-and-retain-groups-whose-values-are-null
//
// Outcome: a one-hop grouped query must calculate each aggregate
// independently and return a group even when the aggregated property has no
// values. The issue's prepared fixture is:
//
//	CREATE (q:AggOwner {g:'g'}), (p1:AggValue {y:1}), (p2:AggValue {y:2}),
//	       (q)-[:AGG {x:1}]->(p1), (q)-[:AGG {x:2}]->(p2)
//
//	MATCH (q:AggOwner)-[r:AGG]->(p:AggValue)
//	RETURN q.g, sum(r.x), avg(p.y)
//	=> ['g', 3, 1.5]
//
// After both p.y properties are removed:
//
//	MATCH (q:AggOwner)-[r:AGG]->(p:AggValue)
//	RETURN q.g, avg(p.y), collect(p.y)
//	=> ['g', null, []]
//
// This file changes no production code. Every assertion goes through the
// public NewStorageExecutor + Execute path; expected rows are Neo4j 5.26.30's
// values recorded on the issue. withCypherParsers runs the whole test under
// both process-wide parser selections, so the parser is restored by process
// exit and no sibling package test inherits it.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor70OneHopPattern is the exact relationship pattern from NOR-70.
const nor70OneHopPattern = "MATCH (q:AggOwner)-[r:AGG]->(p:AggValue)"

// nor70SeedPreparedFixture creates the issue's prepared one-hop fixture
// exactly as written above, in a fresh in-memory store.
func nor70SeedPreparedFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor70_prepared"))
	_, err := exec.Execute(t.Context(), "CREATE (q:AggOwner {g: 'g'}), (p1:AggValue {y: 1}), (p2:AggValue {y: 2}), "+
		"(q)-[:AGG {x: 1}]->(p1), (q)-[:AGG {x: 2}]->(p2)", nil)
	require.NoError(t, err)
	return exec
}

// nor70SeedIndependentFixture keeps the issue's one-hop shape but gives sum
// and avg different value distributions (x is 1,2; y is 10,20). A shared
// running total therefore cannot produce both correct answers.
func nor70SeedIndependentFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor70_independent"))
	_, err := exec.Execute(t.Context(), "CREATE (q:AggOwner {g: 'g'}), (p1:AggValue {y: 10}), (p2:AggValue {y: 20}), "+
		"(q)-[:AGG {x: 1}]->(p1), (q)-[:AGG {x: 2}]->(p2)", nil)
	require.NoError(t, err)
	return exec
}

// TestNOR70IndependentOneHopAggregatesPublic pins the issue's first prepared
// query and shows that each aggregate has its own input: sum(r.x) is 3 and
// avg(p.y) is 1.5, in either projection order, through the one-hop statement
// shape and through the general pipeline shapes (WHERE, WITH and explicit
// WITH aggregation). Normal and PROFILE execution agree; EXPLAIN accepts the
// same projection and reports the same columns without executing it.
func TestNOR70IndependentOneHopAggregatesPublic(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		t.Run("prepared fixture", func(t *testing.T) {
			exec := nor70SeedPreparedFixture(t)
			routes := []struct {
				name  string
				query string
				want  [][]interface{}
			}{
				{
					name:  "plain",
					query: nor70OneHopPattern + " RETURN q.g, sum(r.x), avg(p.y)",
					want:  [][]interface{}{{"g", int64(3), float64(1.5)}},
				},
				{
					name:  "reversed aggregate order",
					query: nor70OneHopPattern + " RETURN q.g, avg(p.y), sum(r.x)",
					want:  [][]interface{}{{"g", float64(1.5), int64(3)}},
				},
				{
					name:  "where",
					query: nor70OneHopPattern + " WHERE true RETURN q.g, sum(r.x), avg(p.y)",
					want:  [][]interface{}{{"g", int64(3), float64(1.5)}},
				},
				{
					name:  "with-star",
					query: nor70OneHopPattern + " WITH * RETURN q.g, sum(r.x), avg(p.y)",
					want:  [][]interface{}{{"g", int64(3), float64(1.5)}},
				},
				{
					name:  "with-projection",
					query: nor70OneHopPattern + " WITH q.g AS g, sum(r.x) AS s, avg(p.y) AS a RETURN g, s, a",
					want:  [][]interface{}{{"g", int64(3), float64(1.5)}},
				},
			}
			for _, route := range routes {
				for _, mode := range []string{"", "PROFILE "} {
					name := route.name
					if mode != "" {
						name += "/PROFILE"
					}
					t.Run(name, func(t *testing.T) {
						result, err := exec.Execute(t.Context(), mode+route.query, nil)
						require.NoError(t, err, mode+route.query)
						require.Equal(t, route.want, result.Rows, mode+route.query)
						if route.name == "plain" {
							t.Logf("NOR-70 prepared query %q => %v", mode+route.query, result.Rows)
						}
					})
				}
			}

			plain := routes[0].query
			normal, err := exec.Execute(t.Context(), plain, nil)
			require.NoError(t, err)
			explain, err := exec.Execute(t.Context(), "EXPLAIN "+plain, nil)
			require.NoError(t, err)
			require.Equal(t, normal.Columns, explain.Columns)
			require.Empty(t, explain.Rows, "EXPLAIN executes nothing")
		})

		t.Run("different sum and avg distributions", func(t *testing.T) {
			exec := nor70SeedIndependentFixture(t)
			cases := []struct {
				name  string
				query string
				want  [][]interface{}
			}{
				{
					name:  "sum then avg",
					query: nor70OneHopPattern + " RETURN q.g, sum(r.x), avg(p.y)",
					want:  [][]interface{}{{"g", int64(3), float64(15)}},
				},
				{
					name:  "avg then sum",
					query: nor70OneHopPattern + " RETURN q.g, avg(p.y), sum(r.x)",
					want:  [][]interface{}{{"g", float64(15), int64(3)}},
				},
			}
			for _, tc := range cases {
				for _, mode := range []string{"", "PROFILE "} {
					name := tc.name
					if mode != "" {
						name += "/PROFILE"
					}
					t.Run(name, func(t *testing.T) {
						result, err := exec.Execute(t.Context(), mode+tc.query, nil)
						require.NoError(t, err, mode+tc.query)
						require.Equal(t, tc.want, result.Rows, mode+tc.query)
						if tc.name == "sum then avg" {
							t.Logf("NOR-70 independent distribution %q => %v", mode+tc.query, result.Rows)
						}
					})
				}
			}
		})
	})
}

// TestNOR70NullOnlyOneHopGroupRetainedPublic removes both p.y properties and
// pins the issue's second prepared query: avg(p.y) is null and collect(p.y) is
// [] while the q.g group is still returned. The group also remains visible
// when another aggregate over r.x is projected beside it.
func TestNOR70NullOnlyOneHopGroupRetainedPublic(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		t.Run("prepared fixture after removing both p.y", func(t *testing.T) {
			exec := nor70SeedPreparedFixture(t)
			_, err := exec.Execute(t.Context(), "MATCH (p:AggValue) REMOVE p.y", nil)
			require.NoError(t, err)

			routes := []struct {
				name  string
				query string
				want  [][]interface{}
			}{
				{
					name:  "plain",
					query: nor70OneHopPattern + " RETURN q.g, avg(p.y), collect(p.y)",
					want:  [][]interface{}{{"g", nil, []interface{}{}}},
				},
				{
					name:  "where",
					query: nor70OneHopPattern + " WHERE true RETURN q.g, avg(p.y), collect(p.y)",
					want:  [][]interface{}{{"g", nil, []interface{}{}}},
				},
				{
					name:  "with-star",
					query: nor70OneHopPattern + " WITH * RETURN q.g, avg(p.y), collect(p.y)",
					want:  [][]interface{}{{"g", nil, []interface{}{}}},
				},
				{
					name:  "with-projection",
					query: nor70OneHopPattern + " WITH q.g AS g, avg(p.y) AS a, collect(p.y) AS c RETURN g, a, c",
					want:  [][]interface{}{{"g", nil, []interface{}{}}},
				},
				{
					name:  "independent aggregate beside null average",
					query: nor70OneHopPattern + " RETURN q.g, sum(r.x), avg(p.y), collect(p.y)",
					want:  [][]interface{}{{"g", int64(3), nil, []interface{}{}}},
				},
			}
			for _, route := range routes {
				for _, mode := range []string{"", "PROFILE "} {
					name := route.name
					if mode != "" {
						name += "/PROFILE"
					}
					t.Run(name, func(t *testing.T) {
						result, err := exec.Execute(t.Context(), mode+route.query, nil)
						require.NoError(t, err, mode+route.query)
						require.Equal(t, route.want, result.Rows, mode+route.query)
						if route.name == "plain" {
							t.Logf("NOR-70 null-only query %q => %v", mode+route.query, result.Rows)
						}
					})
				}
			}

			plain := routes[0].query
			normal, err := exec.Execute(t.Context(), plain, nil)
			require.NoError(t, err)
			explain, err := exec.Execute(t.Context(), "EXPLAIN "+plain, nil)
			require.NoError(t, err)
			require.Equal(t, normal.Columns, explain.Columns)
			require.Empty(t, explain.Rows, "EXPLAIN executes nothing")
		})

		t.Run("null-only group kept beside a valued group", func(t *testing.T) {
			exec := nor70SeedPreparedFixture(t)
			_, err := exec.Execute(t.Context(), "CREATE (:AggOwner {g: 'h'})-[:AGG {x: 4}]->(:AggValue)", nil)
			require.NoError(t, err)
			for _, mode := range []string{"", "PROFILE "} {
				t.Run("mode="+mode, func(t *testing.T) {
					result, err := exec.Execute(t.Context(), mode+nor70OneHopPattern+
						" RETURN q.g, sum(r.x), avg(p.y), count(p.y) ORDER BY q.g", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{
						{"g", int64(3), float64(1.5), int64(2)},
						{"h", int64(4), nil, int64(0)},
					}, result.Rows)
					if mode == "" {
						t.Logf("NOR-70 valued/null groups => %v", result.Rows)
					}
				})
			}
		})
	})
}
