package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMergeChain_OptionalMatchForeach_CreatesOnlyWhenMatched(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (a:TypeA {name: 'A1'})`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `CREATE (c:TypeC {name: 'C1'})`, nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MERGE (e:Entity {id: 'opt1'})
		WITH e
		OPTIONAL MATCH (a:TypeA {name: 'A1'})
		FOREACH (x IN CASE WHEN a IS NOT NULL THEN [1] ELSE [] END |
			MERGE (e)-[:REL_A]->(a)
		)
		WITH e
		OPTIONAL MATCH (b:TypeB {name: 'NONEXISTENT'})
		FOREACH (x IN CASE WHEN b IS NOT NULL THEN [1] ELSE [] END |
			MERGE (e)-[:REL_B]->(b)
		)
		WITH e
		OPTIONAL MATCH (c:TypeC {name: 'C1'})
		FOREACH (x IN CASE WHEN c IS NOT NULL THEN [1] ELSE [] END |
			MERGE (e)-[:REL_C]->(c)
		)
		RETURN e.id
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "opt1", result.Rows[0][0])

	relACount, err := exec.Execute(ctx, `MATCH (e:Entity {id: 'opt1'})-[:REL_A]->(:TypeA) RETURN count(*) as c`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), relACount.Rows[0][0])

	relBCount, err := exec.Execute(ctx, `MATCH (e:Entity {id: 'opt1'})-[:REL_B]->(:TypeB) RETURN count(*) as c`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), relBCount.Rows[0][0])

	relCCount, err := exec.Execute(ctx, `MATCH (e:Entity {id: 'opt1'})-[:REL_C]->(:TypeC) RETURN count(*) as c`, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), relCCount.Rows[0][0])
}

func TestForeach_ReplacesLoopVariable_NotMapKeys(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Regression: naive strings.ReplaceAll would turn "{i: i}" into "{1: 1}".
	_, err := exec.Execute(ctx, `FOREACH (i IN [1] | CREATE (:Item {i: i}))`, nil)
	require.NoError(t, err)

	got, err := exec.Execute(ctx, `MATCH (n:Item) RETURN n.i`, nil)
	require.NoError(t, err)
	require.Len(t, got.Rows, 1)
	require.Equal(t, int64(1), got.Rows[0][0])
}

func TestForeach_ComposedWritesRetainBindings(t *testing.T) {
	for _, testCase := range []struct {
		query       string
		parentCount int64
	}{
		{"CREATE (a:F {id: 1}) WITH a FOREACH (x IN [1, 2] | CREATE (:G {v: x}))", 1},
		{"UNWIND [1, 2] AS i FOREACH (x IN [i] | CREATE (:G {v: x}))", 0},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(testCase.query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(ctx, "MATCH (n:G) RETURN n.v ORDER BY n.v", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
				result, err = exec.Execute(ctx, "MATCH (a:F) RETURN count(a)", nil)
				require.NoError(t, err)
				require.Equal(t, testCase.parentCount, result.Rows[0][0])
			})
		}
	}
}

func TestForeach_ComposedMutationShapes(t *testing.T) {
	for _, testCase := range []struct {
		query      string
		checkQuery string
		want       [][]interface{}
	}{
		{"WITH 5 AS k FOREACH (x IN [1, 2] | CREATE (:G {v: x, k: k}))", "MATCH (n:G) RETURN n.v, n.k ORDER BY n.v", [][]interface{}{{int64(1), int64(5)}, {int64(2), int64(5)}}},
		{"CREATE (a:F {id: 1}) FOREACH (x IN [1, 2] | CREATE (:G {v: x}))", "MATCH (n:G) RETURN n.v ORDER BY n.v", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"CREATE (a:F {id: 1}) WITH a, 5 AS k FOREACH (x IN [1, 2] | CREATE (:G {v: x, k: k}))", "MATCH (n:G) RETURN n.v, n.k ORDER BY n.v", [][]interface{}{{int64(1), int64(5)}, {int64(2), int64(5)}}},
		{"CREATE (a:F {id: 1}) FOREACH (x IN [1] | SET a.touched = x)", "MATCH (a:F) RETURN a.id, a.touched", [][]interface{}{{int64(1), int64(1)}}},
		{"CREATE (a:F {id: 1}) FOREACH (x IN [1, 2] | CREATE (a)-[:R]->(:G {v: x}))", "MATCH (a:F)-[:R]->(n:G) RETURN n.v ORDER BY n.v", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"FOREACH (x IN ['a | b', 'c'] | CREATE (:H {v: x}))", "MATCH (n:H) RETURN n.v ORDER BY n.v", [][]interface{}{{"a | b"}, {"c"}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, testCase.checkQuery, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.want, result.Rows)
		})
	}
}

func TestForeach_PipelineValidation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, clause := range []string{
		"FOREACH", "FOREACH (i [1] | CREATE (:F))", "FOREACH (1 IN [1] | CREATE (:F))",
		"FOREACH (i IN [1] CREATE (:F))", "FOREACH (i IN [1] | )",
		"FOREACH (i IN [1] | RETURN i)", "FOREACH (i IN [1] | SET missing.x = i)",
	} {
		t.Run(clause, func(t *testing.T) {
			_, err := exec.pipelineApplyForeach(context.Background(), []pipelineRow{{}}, clause)
			require.Error(t, err)
		})
	}
}

func TestForeach_UnsupportedUpdateRollsBack(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:F {id: 1}) FOREACH (x IN [1] | MATCH (b:F) DELETE b)", nil)
	require.Error(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:F) RETURN count(a)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])
}
