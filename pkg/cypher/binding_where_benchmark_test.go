package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkFilterBindingsByWhere_CompiledJoin(b *testing.B) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	bindings := make([]binding, 0, 1024)
	for i := 0; i < 1024; i++ {
		key := "k" + strconv.Itoa(i%32)
		bindings = append(bindings, binding{
			"o": &storage.Node{ID: storage.NodeID("o-" + strconv.Itoa(i)), Properties: map[string]interface{}{"joinKey": key, "status": "active"}},
			"t": &storage.Node{ID: storage.NodeID("t-" + strconv.Itoa(i)), Properties: map[string]interface{}{"joinKey": key, "status": "active"}},
		})
	}
	params := map[string]interface{}{"keys": []interface{}{"k1", "k2", "k3", "k4"}}
	whereClause := "o.joinKey IN $keys AND t.joinKey = o.joinKey AND o.status IS NOT NULL AND t.status IS NOT NULL"
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = exec.filterBindingsByWhere(ctx, bindings, whereClause, params)
	}
}

func BenchmarkFilterBindingsByWhere_GenericFallback(b *testing.B) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	bindings := make([]binding, 0, 1024)
	for i := 0; i < 1024; i++ {
		bindings = append(bindings, binding{
			"n": &storage.Node{ID: storage.NodeID("n-" + strconv.Itoa(i)), Properties: map[string]interface{}{"name": "node-" + strconv.Itoa(i), "count": int64(i)}},
		})
	}
	whereClause := "size(n.name) > 0"
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = exec.filterBindingsByWhere(ctx, bindings, whereClause, nil)
	}
}
