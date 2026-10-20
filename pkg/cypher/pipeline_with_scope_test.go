package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestPipelineWithProjectionDoesNotAllocatePredicateScope(t *testing.T) {
	executor := &StorageExecutor{}
	ctx := context.Background()
	plan, ok := parsePipelineRowWith("WITH a AS b")
	if !ok {
		t.Fatal("projection must use the shared row operator")
	}
	row := pipelineRow{"a": int64(1), "hidden": int64(2), "$p": int64(3)}
	input := func(yield func(pipelineRow) bool) bool { return yield(row) }
	yield := func(projected pipelineRow) bool {
		if len(projected) != 2 || projected["b"] != int64(1) || projected["$p"] != int64(3) {
			t.Fatalf("incorrect projected row: %v", projected)
		}
		return true
	}
	allocations := testing.AllocsPerRun(100, func() {
		if !executor.pipelineWithRowSource(ctx, input, plan.withImports(ctx))(yield) {
			t.Fatal("projection source did not complete")
		}
	})
	// The borrowed projection and its source closure are allowed storage;
	// a second map for an absent WHERE must not be allocated and filled.
	if allocations > 3 {
		t.Fatalf("projection allocated %g objects, want <=3", allocations)
	}
	if len(row) != 3 || row["a"] != int64(1) || row["hidden"] != int64(2) || row["$p"] != int64(3) {
		t.Fatalf("projection changed the input: %v", row)
	}
}

func TestPipelineWithOrderingRetainsInputScope(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "with-scope")
	executor := NewStorageExecutor(engine)
	result, err := executor.Execute(context.Background(),
		"UNWIND [3,1,2] AS v WITH v AS x ORDER BY v RETURN collect(x) AS xs", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("incorrect ordered projection: %v", result.Rows)
	}
	values, ok := result.Rows[0][0].([]interface{})
	if !ok || len(values) != 3 || values[0] != int64(1) || values[1] != int64(2) || values[2] != int64(3) {
		t.Fatalf("ORDER BY lost the input binding v: %v", result.Rows)
	}
}
