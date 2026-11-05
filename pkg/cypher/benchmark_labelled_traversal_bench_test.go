package cypher

// Benchmarks for labelled-endpoint relationship traversal (#1110): following a
// relationship to a labelled endpoint previously loaded and double-copied every
// endpoint node. The graph mirrors the issue's suggested shape — 1 root, 16
// children, 22 grandchildren each (352 leaves) — with a ~500-byte text
// property on the leaves so the endpoint load cost is visible.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const traversalBenchLeafText = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

func seedLabelledTraversalBenchGraph(b *testing.B, exec *StorageExecutor, ctx context.Context) {
	b.Helper()
	_, err := exec.Execute(ctx, "CREATE (:Root {id: 1})", nil)
	if err != nil {
		b.Fatalf("seed root failed: %v", err)
	}
	for c := 1; c <= 16; c++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(`MATCH (r:Root {id: 1}) CREATE (r)<-[:A]-(:Child {id: %d})`, c), nil)
		if err != nil {
			b.Fatalf("seed child failed: %v", err)
		}
		for g := 1; g <= 22; g++ {
			_, err := exec.Execute(ctx, fmt.Sprintf(
				`MATCH (c:Child {id: %d}) CREATE (c)<-[:B]-(:Leaf {id: %d, text: $text})`,
				c, (c-1)*22+g), map[string]interface{}{"text": traversalBenchLeafText})
			if err != nil {
				b.Fatalf("seed leaf failed: %v", err)
			}
		}
	}
}

func newLabelledTraversalBenchmark(b *testing.B) (*StorageExecutor, context.Context) {
	b.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngine(b.TempDir()))
	if err != nil {
		b.Fatalf("badger engine failed: %v", err)
	}
	b.Cleanup(func() { base.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "bench"))
	// Disable the query result cache so every iteration measures traversal,
	// join and projection work instead of a cache hit.
	exec.cache = nil
	ctx := context.Background()
	seedLabelledTraversalBenchGraph(b, exec, ctx)
	return exec, ctx
}

// BenchmarkLabelledTraversalCount is the issue's relationship-based query: two
// labelled hops into 352 leaves, counted.
func BenchmarkLabelledTraversalCount(b *testing.B) {
	exec, ctx := newLabelledTraversalBenchmark(b)
	query := "MATCH (r:Root {id: 1})<-[:A]-(c:Child)<-[:B]-(l:Leaf) RETURN count(l)"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 || res.Rows[0][0] != int64(352) {
			b.Fatalf("expected count 352, got %v", res.Rows)
		}
	}
}

// BenchmarkLabelledTraversalIndexJoin is the index/label-scan-backed
// equivalent used as the performance reference: the same 352 leaves counted
// without any relationship traversal.
func BenchmarkLabelledTraversalIndexJoin(b *testing.B) {
	exec, ctx := newLabelledTraversalBenchmark(b)
	query := "MATCH (l:Leaf) WHERE l.id >= 1 AND l.id <= 352 RETURN count(l)"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) != 1 || res.Rows[0][0] != int64(352) {
			b.Fatalf("expected count 352, got %v", res.Rows)
		}
	}
}
