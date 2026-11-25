package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var computedPatternBenchmarkRows any

// A computed pattern adds row-local parameters without re-converting the
// statement's potentially large parameter list for every input row.
func TestComputedPatternBindingReusesStatementParameterRows(t *testing.T) {
	rows := []interface{}{
		map[string]interface{}{"tags": []string{"first", "second"}},
		map[string]interface{}{"tags": []string{"third"}},
	}
	ctx := withQueryParams(t.Context(), map[string]interface{}{"rows": rows})
	statementRows := parameterRowValues(ctx)
	converted := statementRows["$rows"].([]interface{})
	require.NotEqual(t, reflect.ValueOf(rows).Pointer(), reflect.ValueOf(converted).Pointer(), "fixture must require nested conversion")
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	pattern := "(n:C69ParameterCache {id: wanted})"
	for _, wanted := range []int64{7, 8} {
		bound, _, err := exec.bindPatternProperties(ctx, pattern, map[string]interface{}{"wanted": wanted})
		require.NoError(t, err)
		boundRows := parameterRowValues(bound)
		require.Equal(t, reflect.ValueOf(converted).Pointer(), reflect.ValueOf(boundRows["$rows"]).Pointer(), "statement parameter conversion must be reused")
		require.Equal(t, wanted, boundRows["$__nornic_pattern_property_0"])
		require.Equal(t, reflect.ValueOf(converted).Pointer(), reflect.ValueOf(parameterRowValues(ctx)["$rows"]).Pointer(), "binding must not mutate the parent row values")
	}
	cypher25 := context.WithValue(t.Context(), cypherVersionKey{}, "25")
	bound, _, err := exec.bindPatternProperties(cypher25, pattern, map[string]interface{}{"wanted": int64(9)})
	require.NoError(t, err)
	require.True(t, rowIsCypher25(parameterRowValues(bound)), "row-local parameters must retain the statement's Cypher 25 mark")
}

func BenchmarkComputedPatternBindingStatementRows(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows_%d", size), func(b *testing.B) {
			rows := make([]interface{}, size)
			for i := range rows {
				rows[i] = map[string]interface{}{"tags": []string{"first", "second"}}
			}
			ctx := withQueryParams(b.Context(), map[string]interface{}{"rows": rows})
			exec := NewStorageExecutor(newTestMemoryEngine(b))
			pattern := "(n:C69ParameterCache {id: wanted})"
			row := map[string]interface{}{"wanted": int64(7)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bound, _, err := exec.bindPatternProperties(ctx, pattern, row)
				if err != nil {
					b.Fatal(err)
				}
				computedPatternBenchmarkRows = parameterRowValues(bound)["$rows"]
			}
		})
	}
}

func TestComputedMergeActionsRetainParameterValues(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "c69_merge"))
		rows := []interface{}{
			map[string]interface{}{"id": "n0", "value": "first", "carried": true},
			map[string]interface{}{"id": "n1", "value": "second", "carried": true},
		}
		params := map[string]interface{}{"rows": rows}
		result, err := exec.Execute(t.Context(), "UNWIND $rows AS row MERGE (n:C69Merge {id: row.id}) ON CREATE SET n.value = row.value RETURN count(n)", params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
		rows[0].(map[string]interface{})["value"] = "updated"
		rows[1].(map[string]interface{})["value"] = "ignored"
		rows[1].(map[string]interface{})["carried"] = false
		result, err = exec.Execute(t.Context(), "UNWIND $rows AS row MERGE (n:C69Merge {id: row.id}) ON MATCH SET n.value = CASE WHEN row.carried THEN row.value ELSE n.value END RETURN count(n)", params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
		result, err = exec.Execute(t.Context(), "UNWIND $rows AS row MATCH (n:C69Merge {id: row.id}) SET n.value = CASE WHEN row.carried THEN row.value ELSE n.value END RETURN count(n)", params)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
		result, err = exec.Execute(t.Context(), "MATCH (n:C69Merge) RETURN n.id, n.value ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"n0", "updated"}, {"n1", "second"}}, result.Rows)
	})
}

// BenchmarkComputedMergeActionPublicPath keeps the public MERGE/CASE path in
// the cost evidence; the binding microbenchmark isolates its causal owner.
func BenchmarkComputedMergeActionPublicPath(b *testing.B) {
	for _, size := range []int{100, 1000} {
		b.Run(fmt.Sprintf("rows_%d", size), func(b *testing.B) {
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(b), "c69_public")
			exec := NewStorageExecutor(engine)
			if _, err := exec.Execute(b.Context(), "CREATE INDEX c69_merge_bench_id FOR (n:C69MergeBench) ON (n.id)", nil); err != nil {
				b.Fatal(err)
			}
			rows := make([]interface{}, size)
			nodes := make([]*storage.Node, size)
			for i := range rows {
				id := fmt.Sprintf("n%d", i)
				rows[i] = map[string]interface{}{"id": id, "value": "ready", "carried": true}
				nodes[i] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"C69MergeBench"}, Properties: map[string]interface{}{"id": id, "value": "ready"}}
			}
			params := map[string]interface{}{"rows": rows}
			if err := engine.BulkCreateNodes(b.Context(), nodes); err != nil {
				b.Fatal(err)
			}
			const query = "UNWIND $rows AS row MERGE (n:C69MergeBench {id: row.id}) ON MATCH SET n.value = CASE WHEN row.carried THEN row.value ELSE n.value END RETURN count(n)"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(b.Context(), query, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || !reflect.DeepEqual(result.Rows[0], []interface{}{int64(size)}) {
					b.Fatalf("computed MERGE count: %v", result.Rows)
				}
			}
		})
	}
}
