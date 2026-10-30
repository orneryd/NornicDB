package cypher

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A Go time list is a list of DateTimes on every SET route: a map entry is
// normalized before it is checked, as a direct property value is.
func TestSetGoTimeListsInEveryRoute(t *testing.T) {
	london, ok := loadPinnedTemporalLocation("Europe/London")
	require.True(t, ok)
	instant := time.Unix(1792888200, 123456789).In(london)
	values := map[string]interface{}{
		"list":             []time.Time{instant},
		"pointer list":     []*time.Time{&instant},
		"any list":         []interface{}{instant},
		"any pointer list": []interface{}{&instant},
	}
	routes := []struct{ name, setup, query, entity string }{
		{"map", "", "MERGE (n:P) SET n += $m", "n"},
		{"replace", "", "MERGE (n:P) SET n = $m", "n"},
		{"on create", "", "MERGE (n:P) ON CREATE SET n += $m", "n"},
		{"on match", "CREATE (:P)", "MERGE (n:P) ON MATCH SET n += $m", "n"},
		{"match", "CREATE (:P)", "MATCH (n:P) SET n += $m", "n"},
		{"unwind", "", "UNWIND [$m] AS m MERGE (n:P) SET n += m", "n"},
		{"unwind property", "", "UNWIND [$m] AS m MERGE (n:P) SET n.v = m.v", "n"},
		{"map property", "", "MERGE (n:P) SET n.v = $m.v", "n"},
		{"property", "", "MERGE (n:P) SET n.v = $v", "n"},
		{"relationship map", "", "MERGE (:A)-[r:R]->(:B) SET r += $m", "r"},
		{"relationship replace", "", "MERGE (:A)-[r:R]->(:B) SET r = $m", "r"},
	}
	// Neo4j 5.26.30, the same list of one DateTime on every route.
	want := [][]interface{}{{"2026-10-25T01:30:00.123456789+01:00[Europe/London]", int64(1792888200)}}
	for name, value := range values {
		for _, route := range routes {
			t.Run(name+"/"+route.name, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "go_time_lists"))
				ctx := context.Background()
				if route.setup != "" {
					_, err := exec.Execute(ctx, route.setup, nil)
					require.NoError(t, err)
				}
				params := map[string]interface{}{"v": value, "m": map[string]interface{}{"v": value}}
				projection := fmt.Sprintf(" RETURN toString(%[1]s.v[0]), %[1]s.v[0].epochSeconds", route.entity)
				result, err := exec.Execute(ctx, route.query+projection, params)
				require.NoError(t, err)
				require.Equal(t, want, result.Rows)
			})
		}
	}
	// The int/float list coercion applies to the normalized value that is
	// stored (Neo4j 5.26.30: [1.0, 2.5]).
	for _, query := range []string{
		"MERGE (n:P) SET n += $c RETURN n.l",
		"MERGE (n:P) SET n = $c RETURN n.l",
		"MERGE (n:P) SET n += {l: [1, 2.5]} RETURN n.l",
		"MERGE (n:P) SET n.l = $c.l RETURN n.l",
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "go_time_lists_coerce"))
		parameter := []interface{}{1, 2.5}
		result, err := exec.Execute(context.Background(), query, map[string]interface{}{"c": map[string]interface{}{"l": parameter}})
		require.NoError(t, err, query)
		items, list := cypherListValue(result.Rows[0][0])
		require.True(t, list, query)
		require.Equal(t, []interface{}{float64(1), 2.5}, items, query)
		// The caller's parameter is not rewritten.
		require.Equal(t, []interface{}{1, 2.5}, parameter, query)
	}
}

// setPropertyMapValue hands back the source map itself when no value is
// rewritten, and a copy holding the stored values when one is; the source
// is never changed. The UNWIND relationship batch checks its row values the
// same way.
func TestSetStorableMapValues(t *testing.T) {
	plain := map[string]interface{}{"s": "x", "i": int64(1)}
	props, err := setPropertyMapValue(plain, "+=")
	require.NoError(t, err)
	require.Equal(t, reflect.ValueOf(plain).Pointer(), reflect.ValueOf(props).Pointer())

	source := map[string]interface{}{"s": "x", "l": []interface{}{1, 2.5}}
	props, err = setPropertyMapValue(source, "+=")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"s": "x", "l": []interface{}{float64(1), 2.5}}, props)
	require.Equal(t, map[string]interface{}{"s": "x", "l": []interface{}{1, 2.5}}, source)

	node := &storage.Node{ID: "n", Properties: map[string]interface{}{"i": 1}}
	props, err = setPropertyMapValue(node, "=")
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"i": int64(1)}, props)
	require.Equal(t, map[string]interface{}{"i": 1}, node.Properties)

	_, err = setPropertyMapValue(map[string]interface{}{"l": []interface{}{1, "a"}}, "+=")
	requireStatusCode(t, err, "Neo.ClientError.Statement.TypeError")

	setters := []relationshipVectorSetterSpec{
		{relVar: "r", propName: "v", rowField: "f"},
		{relVar: "r", propName: "a", rowField: "a"},
		{relVar: "r", propName: "w", rowField: "missing"},
		{relVar: "r", propName: "x", rowField: "null"},
	}
	stored, ok := normalizeRelationshipBatchRowProperties(map[string]interface{}{"a": 1, "f": []interface{}{1, 2.5}, "null": nil}, setters)
	require.True(t, ok)
	require.Equal(t, map[string]interface{}{"a": int64(1), "f": []interface{}{float64(1), 2.5}, "v": []interface{}{float64(1), 2.5}}, stored)
	stored["v"].([]interface{})[0] = 0.0
	require.Equal(t, []interface{}{float64(1), 2.5}, stored["f"], "a setter's value is its own copy")
	_, ok = normalizeRelationshipBatchRowProperties(map[string]interface{}{"f": []interface{}{1, "a"}}, setters)
	require.False(t, ok)
}

// A temporal constructor's error inside a SET source is the statement's
// error, not a TypeError for the null it leaves (Neo4j 5.26.30:
// Neo.ClientError.Statement.ArgumentError on every route).
func TestSetSourceKeepsExpressionError(t *testing.T) {
	text := "2026-10-25T01:30:00+02:00[Europe/London]"
	for _, route := range []struct{ setup, query string }{
		{"", "MERGE (n:P) SET n += {t: datetime($s)} RETURN n.t"},
		{"", "MERGE (n:P) SET n = {t: datetime($s)} RETURN n.t"},
		{"CREATE (:P)", "MATCH (n:P) SET n += {t: datetime($s)} RETURN n.t"},
		{"", "MERGE (:A)-[r:R]->(:B) SET r += {t: datetime($s)} RETURN r.t"},
		{"", "UNWIND [$s] AS s MERGE (n:P) SET n += {t: datetime(s)} RETURN n.t"},
		{"", "MERGE (n:P) SET n.l = [datetime($s)] RETURN n.l"},
		{"", "MERGE (n:P) SET n[toString(datetime($s))] = 1 RETURN n"},
		{"", "MERGE (n:P) SET n:$(toString(datetime($s))) RETURN n"},
	} {
		t.Run(route.query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "invalid_offset_set"))
			ctx := context.Background()
			if route.setup != "" {
				_, err := exec.Execute(ctx, route.setup, nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, route.query, map[string]interface{}{"s": text})
			require.Error(t, err)
			requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
			require.Contains(t, err.Error(), "Timezone and offset do not match: "+text)
		})
	}
}
