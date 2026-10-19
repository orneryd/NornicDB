package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUndeferAggregateMapRows(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate-map"))
	result, err := exec.Execute(context.Background(), "UNWIND range(1, 5) AS i RETURN collect({i: i, p: $p}) AS xs", map[string]interface{}{"p": "p"})
	require.NoError(t, err)
	want := make([]interface{}, 5)
	for i := range want {
		want[i] = map[string]interface{}{"i": int64(i + 1), "p": "p"}
	}
	require.Equal(t, [][]interface{}{{want}}, result.Rows)
}

func BenchmarkUndeferAggregateMap(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "aggregate-map"))
	exec.cache = nil
	params := map[string]interface{}{"limit": int64(10)}
	const query = "UNWIND range(1, 200) AS a UNWIND range(1, 40) AS b WITH a % 50 AS group, a, b RETURN group, collect({thing: b, having: a, weight: 1.0 / (a + b)}) AS shared ORDER BY group LIMIT $limit"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(context.Background(), query, params); err != nil {
			b.Fatal(err)
		}
	}
}
