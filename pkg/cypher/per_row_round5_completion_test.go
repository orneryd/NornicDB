package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"runtime/debug"
	"testing"
)

func TestPerRowRound5ApplyOrder(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		want        [][]interface{}
		message     string
	}{
		{name: "where_and_exists", query: "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND EXISTS { WITH x WHERE 1/x=1 RETURN x } RETURN x", message: "/ by zero"},
		{name: "where_and_count", query: "UNWIND [0,0] AS x WITH x WHERE x <> 0 AND COUNT { WITH x WHERE 1/x=1 RETURN x } > 0 RETURN x", message: "/ by zero"},
		{name: "return_and_exists", query: "UNWIND [0,0] AS x RETURN x <> 0 AND EXISTS { WITH x WHERE 1/x=1 RETURN x } AS v", message: "/ by zero"},
		{name: "where_not_exists_and", query: "UNWIND [0,0] AS x WITH x WHERE NOT EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x <> 0 RETURN x", want: [][]interface{}{}},
		{name: "case_exists_and", query: "UNWIND [0,0] AS x RETURN CASE WHEN EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x <> 0 THEN 1 ELSE 0 END AS v", want: [][]interface{}{{int64(0)}, {int64(0)}}},
		{name: "abs_case_exists_and", query: "UNWIND [0,0] AS x RETURN abs(CASE WHEN EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x <> 0 THEN 1 ELSE 0 END) AS v", want: [][]interface{}{{int64(0)}, {int64(0)}}},
		{name: "trim_case_exists_and", query: "UNWIND [0,0] AS x RETURN trim(toString(CASE WHEN EXISTS { WITH x WHERE 1/x=1 RETURN x } AND x <> 0 THEN 1 ELSE 0 END)) AS v", want: [][]interface{}{{"0"}, {"0"}}},
		{name: "chain_count", query: "UNWIND [0,0] AS x RETURN x < -1 < COUNT { WITH x WHERE 1/x=1 RETURN x } AS v", message: "/ by zero"},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.name, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				rows, err := runPerRowRound5Statements(t, tc.query)
				if tc.message != "" {
					requireRound3Neo4jError(t, err, tc.message)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, rows)
			})
		}
	}
}
func TestPerRowRound5WholePointSubquery(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN point({x: COUNT { RETURN 1 }, y: 0}) AS v", CypherPoint{SRID: 7203, X: 1, Y: 0}},
		{"RETURN toString(point({x: COUNT { RETURN 1 }, y: 0})) AS v", "point({x: 1.0, y: 0.0, crs: 'cartesian'})"},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.query, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				rows, err := runPerRowRound5Statements(t, tc.query)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.want}}, rows)
			})
		}
	}
}

func TestPerRowRound5ApplyOperandsEvaluatedOnce(t *testing.T) {
	plan := planLogicalExpression("x > 0 AND COUNT { RETURN 1 } > 0", nil)
	calls := [2]int{}
	value, ok, err := plan.evaluate(func(i int) (interface{}, bool, error) { calls[i]++; return i == 1, true, nil })
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, false, value)
	require.Equal(t, [2]int{1, 1}, calls)
	counts := map[string]int{}
	value, matched, ok, err := evaluateComparisonChain("x < 0 < COUNT { RETURN 1 }", func(text string, _ foldPosition) (interface{}, bool, error) {
		counts[text]++
		if text == "0" {
			return int64(0), true, nil
		}
		return int64(1), true, nil
	}, compareCypherPredicateValue, false)
	require.NoError(t, err)
	require.True(t, matched && ok)
	require.Equal(t, false, value)
	require.Equal(t, map[string]int{"x": 1, "0": 1, "COUNT { RETURN 1 }": 1}, counts)
}

type round5EndpointReadCounter struct {
	storage.Engine
	reads int
}

func (engine *round5EndpointReadCounter) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	engine.reads++
	return engine.Engine.GetEdge(id)
}
func TestPerRowRound5EndpointStorageReadBudget(t *testing.T) {
	counter := &round5EndpointReadCounter{Engine: newTestMemoryEngine(t)}
	store := storage.NewNamespacedEngine(counter, "r5endpoint")
	_, err := store.CreateNode(&storage.Node{ID: "a"})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b"})
	require.NoError(t, err)
	edge := &storage.Edge{ID: "r", Type: "R", StartNode: "a", EndNode: "b", Properties: map[string]interface{}{"k": "v"}}
	require.NoError(t, store.CreateEdge(edge))
	exec := NewStorageExecutor(store)
	counter.reads = 0
	require.NotNil(t, exec.relationshipEndpoint(edge, false))
	require.NotNil(t, exec.relationshipEndpoint(edge, true))
	require.Zero(t, counter.reads, "loaded relationship endpoints must not reread their record")
	require.NoError(t, store.DeleteEdge("r"))
	view := (&deletedEntityViews{}).edge(edge)
	require.Nil(t, exec.relationshipEndpoint(view, false))
	require.Equal(t, 1, counter.reads)
}

func TestPerRowRound5ExecutePatternAllocationBudget(t *testing.T) {
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	exec, _ := perRowTestExecutor(t)
	exec.cache = nil
	ctx := context.Background()
	for _, setup := range []string{"UNWIND range(1,100) AS i CREATE (:L100 {v:i})", "UNWIND range(1,200) AS i CREATE (:L200 {v:i})"} {
		_, err := exec.Execute(ctx, setup, nil)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		predicate string
		maximum   float64
		count     int64
	}{
		{"(n)-->() OR n.v = 1", 27, 1}, {"n.v = 0 OR (n)-[:R]->()", 28, 0},
	} {
		var allocations [2]float64
		for i, label := range []string{"L100", "L200"} {
			query := fmt.Sprintf("MATCH (n:%s) WHERE %s RETURN count(*) AS c", label, tc.predicate)
			run := func() {
				result, err := exec.Execute(ctx, query, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0][0] != tc.count {
					t.Fatalf("unexpected count: %v", result.Rows)
				}
			}
			for range 5 {
				run()
			}
			allocations[i] = testing.AllocsPerRun(10, run)
		}
		perRow := (allocations[1] - allocations[0]) / 100
		t.Logf("%s: %g allocations/row, maximum %g", tc.predicate, perRow, tc.maximum)
		// Separate one fixed allocation between differently sized executions
		// from the linear per-row cost. A one-per-row regression adds 100 and
		// still fails; the base budgets remain 27 and 28 allocations per row.
		require.LessOrEqual(t, allocations[1]-allocations[0], tc.maximum*100+1)
	}
}
func TestPerRowRound5ConstantSubqueryGuards(t *testing.T) {
	for _, tc := range []struct {
		expression string
		want       bool
	}{
		{"false AND COUNT { WITH x WHERE 1/x=1 RETURN x } > 0", false},
		{"COUNT { WITH x WHERE 1/x=1 RETURN x } > 0 AND false", false},
		{"EXISTS { WITH x WHERE 1/x=1 RETURN x } AND false", false},
		{"EXISTS { WITH x WHERE 1/x=1 RETURN x } OR true", true},
		{"false AND EXISTS { WITH x WHERE 1/x=1 RETURN x }", false},
		{"true OR EXISTS { WITH x WHERE 1/x=1 RETURN x }", true},
	} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.expression, text), func(t *testing.T) {
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				rows, err := runPerRowRound5Statements(t, "UNWIND [0,0] AS x RETURN "+tc.expression+" AS v")
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{tc.want}, {tc.want}}, rows)
			})
		}
	}
}
