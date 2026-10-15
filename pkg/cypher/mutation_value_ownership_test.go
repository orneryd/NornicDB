package cypher

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Literal results verified with Neo4j 5.26.30. A parameter named m is
// independent of the row variable m, including when SET reads both scopes.
func TestMutationRowAndParameterNamespaces(t *testing.T) {
	for _, query := range []string{
		"UNWIND $rows AS m MERGE (n:P {id:m.id}) SET n += m RETURN m.id, n.id ORDER BY m.id",
		"UNWIND $rows AS m MERGE (n:P {id:m.id}) ON CREATE SET n += m RETURN m.id, n.id ORDER BY m.id",
		"UNWIND $rows AS m MERGE (n:P {id:m.id}) SET n.id = m.id RETURN m.id, n.id ORDER BY m.id",
	} {
		t.Run(query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "namespaces"))
			rows := []interface{}{map[string]interface{}{"id": "x"}, map[string]interface{}{"id": "y"}}
			params := map[string]interface{}{"rows": rows, "m": map[string]interface{}{"id": "x"}}
			result, err := exec.Execute(context.Background(), query, params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"x", "x"}, {"y", "y"}}, result.Rows)
			result, err = exec.Execute(context.Background(), "MATCH (n:P) RETURN n.id ORDER BY n.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"x"}, {"y"}}, result.Rows)
			require.Equal(t, "x", params["m"].(map[string]interface{})["id"])
			require.Equal(t, "y", rows[1].(map[string]interface{})["id"])
		})
	}
	for _, operator := range []string{"+=", "="} {
		t.Run("explicit parameter "+operator, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "explicit_parameter"))
			result, err := exec.Execute(context.Background(),
				"UNWIND [{id:'row'}] AS m MERGE (n:P) SET n "+operator+" $m RETURN m.id, n.id",
				map[string]interface{}{"m": map[string]interface{}{"id": "parameter"}})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"row", "parameter"}}, result.Rows)
		})
	}
}

func TestSetListCopyOnChange(t *testing.T) {
	for name, list := range map[string]interface{}{
		"integers": []interface{}{int64(1), int64(2)},
		"floats":   []interface{}{1.0, 2.5},
		"strings":  []interface{}{"a", "b"},
		"empty":    []interface{}{},
		"typed":    []int64{1, 2},
	} {
		t.Run(name, func(t *testing.T) {
			source := map[string]interface{}{"v": list}
			stored, changed, err := storablePropertyValue(list)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, reflect.ValueOf(list).Pointer(), reflect.ValueOf(stored).Pointer())
			props, err := setPropertyMapValue(source, "+=")
			require.NoError(t, err)
			require.Equal(t, reflect.ValueOf(source).Pointer(), reflect.ValueOf(props).Pointer())
		})
	}
	for _, list := range [][]interface{}{{int64(1), 2.5}, {int(1), int(2)}} {
		original := append([]interface{}(nil), list...)
		stored, changed, err := storablePropertyValue(list)
		require.NoError(t, err)
		require.True(t, changed)
		require.NotEqual(t, reflect.ValueOf(list).Pointer(), reflect.ValueOf(stored).Pointer())
		require.Equal(t, original, list)
		if _, mixed := list[1].(float64); mixed {
			require.Equal(t, []interface{}{1.0, 2.5}, stored)
		}
	}
}

func TestOffsetMismatchLocalized(t *testing.T) {
	const input = "2026-10-25T01:30:00+02:00[Europe/London]"
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "offset_localized"))
	for _, query := range []string{"RETURN datetime($s)", "MERGE (n:P) SET n += {t:datetime($s)} RETURN n"} {
		_, err := exec.Execute(context.Background(), query, map[string]interface{}{"s": input})
		require.Error(t, err)
		var localized *localization.LocalizedError
		require.True(t, errors.As(err, &localized))
		require.Contains(t, err.Error(), "Timezone and offset do not match: "+input)
		require.Equal(t, "Neo.ClientError.Statement.ArgumentError", err.(interface{ BoltErrorCode() string }).BoltErrorCode())
	}
}

func BenchmarkSetNormalizedListMap(b *testing.B) {
	source := map[string]interface{}{"v": []interface{}{int64(1), int64(2)}}
	node := &storage.Node{Properties: make(map[string]interface{})}
	b.ReportAllocs()
	for b.Loop() {
		props, err := setPropertyMapValue(source, "+=")
		if err != nil {
			b.Fatal(err)
		}
		setNodeProperty(node, "v", props["v"])
	}
}
