package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The built-in apoc.coll.flatten, apoc.coll.frequencies and apoc.map.clean
// give APOC's results, in a statement and in both evaluators (#1164).
// Expected values are Neo4j 5.26.30 with APOC's.
func TestBuiltInApocResultsMatchAPOC(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_results"))
	ctx := context.Background()
	for expression, want := range map[string]interface{}{
		// One level unless recursive.
		"apoc.coll.flatten([[1, [2]], [3]])":            []interface{}{int64(1), []interface{}{int64(2)}, int64(3)},
		"apoc.coll.flatten([[1, [2]], [3]], true)":      []interface{}{int64(1), int64(2), int64(3)},
		"apoc.coll.flatten([1, [2, [3]], [[4]]])":       []interface{}{int64(1), int64(2), []interface{}{int64(3)}, []interface{}{int64(4)}},
		"apoc.coll.flatten([1, [2, [3]], [[4]]], true)": []interface{}{int64(1), int64(2), int64(3), int64(4)},
		"apoc.coll.flatten(null)":                       []interface{}{},
		// {item, count} in first-seen order; 1 and 1.0 count together.
		"apoc.coll.frequencies([3, 1, 2, 3])": []interface{}{
			map[string]interface{}{"item": int64(3), "count": int64(2)},
			map[string]interface{}{"item": int64(1), "count": int64(1)},
			map[string]interface{}{"item": int64(2), "count": int64(1)},
		},
		"apoc.coll.frequencies([1, 1.0, [1], [1]])": []interface{}{
			map[string]interface{}{"item": int64(1), "count": int64(2)},
			map[string]interface{}{"item": []interface{}{int64(1)}, "count": int64(2)},
		},
		"apoc.coll.frequencies(['b', 'a', 'b', null])": []interface{}{
			map[string]interface{}{"item": "b", "count": int64(2)},
			map[string]interface{}{"item": "a", "count": int64(1)},
			map[string]interface{}{"item": nil, "count": int64(1)},
		},
		"apoc.coll.frequencies([])": []interface{}{},
		// Null values go; a listed value goes only as the same kind.
		"apoc.map.clean({a: 1, b: null, c: ''}, ['a'], [''])": map[string]interface{}{},
		"apoc.map.clean({a: 1, b: null, c: 2}, [], [])":       map[string]interface{}{"a": int64(1), "c": int64(2)},
		"apoc.map.clean({a: 1, b: 'x', c: 2}, [], [2, 'x'])":  map[string]interface{}{"a": int64(1)},
		"apoc.map.clean({a: 2, b: 2.0}, [], [2])":             map[string]interface{}{"b": 2.0},
	} {
		result, err := exec.Execute(ctx, "RETURN "+expression+" AS v", nil)
		require.NoError(t, err, expression)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, expression)
		require.Equal(t, want, exec.evaluateExpressionWithContext(ctx, expression, nil, nil), expression)
	}
	for expression, want := range map[string]interface{}{
		"apoc.coll.flatten([[1, [2]], [3]])":       []interface{}{int64(1), []interface{}{int64(2)}, int64(3)},
		"apoc.coll.flatten([[1, [2]], [3]], true)": []interface{}{int64(1), int64(2), int64(3)},
	} {
		value, ok, err := exec.evaluateRowValue(expression, nil)
		require.NoError(t, err, expression)
		require.True(t, ok, expression)
		require.Equal(t, want, value, expression)
	}
	_, ok, _ := exec.evaluateRowValue("apoc.coll.flatten([1], true, 3)", nil)
	require.False(t, ok)
	require.Nil(t, exec.evaluateExpressionWithContext(ctx, "apoc.coll.flatten([1], true, 3)", nil, nil))
}

// apoc.cypher.runMany gives each statement's rows, numbered from 0, then a
// row -1 with the statement's statistics, as APOC does (#1164).
func TestApocCypherRunManyStatisticsRows(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_run_many"))
	statements := "CREATE (n:ZZRM {x: 1});\nMATCH (n:ZZRM) SET n.y = 2 RETURN n.y AS y;\nMATCH (n:ZZRM) DELETE n;"
	result, err := exec.Execute(context.Background(), "CALL apoc.cypher.runMany($statements, {}) YIELD row, result RETURN row, result", map[string]interface{}{"statements": statements})
	require.NoError(t, err)
	require.Len(t, result.Rows, 4)
	statistics := func(index int) map[string]interface{} {
		require.Equal(t, int64(-1), result.Rows[index][0])
		values := result.Rows[index][1].(map[string]interface{})
		require.Contains(t, values, "time")
		delete(values, "time")
		return values
	}
	counts := func(changes map[string]int64) map[string]interface{} {
		values := map[string]interface{}{}
		for _, key := range []string{"rows", "nodesCreated", "nodesDeleted", "relationshipsCreated", "relationshipsDeleted", "propertiesSet", "labelsAdded", "labelsRemoved", "indexesAdded", "indexesRemoved", "constraintsAdded", "constraintsRemoved"} {
			values[key] = changes[key]
		}
		return values
	}
	require.Equal(t, counts(map[string]int64{"nodesCreated": 1, "propertiesSet": 1, "labelsAdded": 1}), statistics(0))
	require.Equal(t, []interface{}{int64(0), map[string]interface{}{"y": int64(2)}}, result.Rows[1])
	require.Equal(t, counts(map[string]int64{"rows": 1, "propertiesSet": 1}), statistics(2))
	require.Equal(t, counts(map[string]int64{"nodesDeleted": 1}), statistics(3))

	// config {statistics: false} leaves the statistics rows out, as in APOC.
	result, err = exec.Execute(context.Background(), "CALL apoc.cypher.runMany('RETURN 1 AS v;', {}, {statistics: false}) YIELD row, result RETURN row, result", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0), map[string]interface{}{"v": int64(1)}}}, result.Rows)
	_, err = exec.Execute(context.Background(), "CALL apoc.cypher.runMany('RETURN 1 AS v;', {}, 1) YIELD row RETURN row", nil)
	require.ErrorContains(t, err, "config")
}
