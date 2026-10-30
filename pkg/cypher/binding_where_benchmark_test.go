package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkFilterBindingsByWhere_CompiledJoin(b *testing.B) {
	exec := &StorageExecutor{}
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
	if got := len(exec.filterBindingsByWhere(ctx, bindings, whereClause, params)); got != 128 {
		b.Fatalf("expected 128 bindings, got %d", got)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = exec.filterBindingsByWhere(ctx, bindings, whereClause, params)
	}
	b.StopTimer()
}

func BenchmarkFilterBindingsByWhere_SharedExpressionPlan(b *testing.B) {
	exec := &StorageExecutor{}
	bindings := make([]binding, 0, 1024)
	for i := 0; i < 1024; i++ {
		bindings = append(bindings, binding{
			"n": &storage.Node{ID: storage.NodeID("n-" + strconv.Itoa(i)), Properties: map[string]interface{}{"name": "node-" + strconv.Itoa(i), "count": int64(i)}},
		})
	}
	whereClause := "size(n.name) + n.count >= 0"
	ctx := context.Background()
	if plan := planRowPredicate(whereClause); plan == nil || !plan.complete {
		b.Fatal("workload must use a complete shared expression plan")
	}
	if got := len(exec.filterBindingsByWhere(ctx, bindings, whereClause, nil)); got != len(bindings) {
		b.Fatalf("expected %d bindings, got %d", len(bindings), got)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = exec.filterBindingsByWhere(ctx, bindings, whereClause, nil)
	}
	b.StopTimer()
}

func BenchmarkBindingWherePipelineHandlers(b *testing.B) {
	for _, handler := range []string{"binding", "with"} {
		for _, count := range []int{1, 32, 1024} {
			b.Run(handler+"/rows="+strconv.Itoa(count), func(b *testing.B) {
				exec := &StorageExecutor{}
				ctx := withExpressionFailureSlot(context.Background())
				clause := "size(n.name) + n.count >= 0"
				if plan := planRowPredicate(clause); plan == nil || !plan.complete {
					b.Fatal("workload must use a complete shared expression plan")
				}
				rows := make([]binding, count)
				values := make([]map[string]interface{}, count)
				for index := range rows {
					node := &storage.Node{ID: "node", Properties: map[string]interface{}{"name": "node", "count": int64(index)}}
					rows[index] = binding{"n": node}
					values[index] = map[string]interface{}{"n": node}
				}
				apply := func() int {
					if handler == "binding" {
						return len(exec.filterBindingsByWhere(ctx, rows, clause, nil))
					}
					accepted := 0
					for _, row := range values {
						keep, err := exec.evaluateWithWhere(ctx, clause, row)
						if err != nil {
							b.Fatal(err)
						}
						if keep {
							accepted++
						}
					}
					return accepted
				}
				if got := apply(); got != count {
					b.Fatalf("expected %d rows, got %d", count, got)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if got := apply(); got != count {
						b.Fatalf("expected %d rows, got %d", count, got)
					}
				}
				b.StopTimer()
			})
		}
	}
}

func BenchmarkGh728ContextWhere(b *testing.B) {
	exec := &StorageExecutor{}
	ctx := withExpressionFailureSlot(withQueryParams(context.Background(), map[string]interface{}{"offset": int64(1), "minimum": int64(0)}))
	nodes := map[string]*storage.Node{"n": {ID: "node", Properties: map[string]interface{}{"name": "node", "count": int64(1024)}}}
	for _, workload := range []struct{ name, clause string }{
		{"comparison", "n.count >= 0"},
		{"arithmetic", "size(n.name) + n.count >= 0"},
		{"parameters", "n.count + $offset >= $minimum"},
	} {
		b.Run(workload.name, func(b *testing.B) {
			if !exec.evaluateWhereForContext(ctx, workload.clause, nodes) {
				b.Fatal("context predicate must accept the row")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if !exec.evaluateWhereForContext(ctx, workload.clause, nodes) {
					b.Fatal("context predicate changed its result")
				}
			}
		})
	}
}

func BenchmarkGh728SharedComparisonHandlers(b *testing.B) {
	exec := &StorageExecutor{}
	ctx := context.Background()
	clause := "n.value >= $floor AND n.value < $limit"
	row := map[string]interface{}{
		"n":      &storage.Node{ID: "node", Properties: map[string]interface{}{"value": int64(3)}},
		"$floor": int64(1), "$limit": int64(5),
	}
	if !exec.evaluateRowPredicate(ctx, clause, row) {
		b.Fatal("comparison must accept the row")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if !exec.evaluateRowPredicate(ctx, clause, row) {
			b.Fatal("comparison must accept the row")
		}
	}
	b.StopTimer()
}
