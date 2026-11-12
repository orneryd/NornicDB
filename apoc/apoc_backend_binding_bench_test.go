// Benchmarks for the bound APOC instances.
//
// They measure the two costs the migration promises to keep separate: admitting
// a handler from an independent backend instance (a constructor/registry cost,
// paid outside hot scans) and invoking an admitted handler per row (which must
// not pay for the binding).
package apoc

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	apocnode "github.com/orneryd/nornicdb/apoc/node"
	"github.com/orneryd/nornicdb/apoc/registry"
	apocrel "github.com/orneryd/nornicdb/apoc/rel"
	apocsearch "github.com/orneryd/nornicdb/apoc/search"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
)

// apocBenchmarkBackend returns an independent backend with size Person nodes.
func apocBenchmarkBackend(b *testing.B, size int) apocstorage.Storage {
	b.Helper()
	backend := apocstorage.NewInMemoryStorage()
	for i := 0; i < size; i++ {
		if _, err := backend.CreateNode(b.Context(), []string{"Person"}, map[string]interface{}{
			"name": fmt.Sprintf("Person-%d", i),
			"rank": i,
		}); err != nil {
			b.Fatal(err)
		}
	}
	return backend
}

// BenchmarkBoundSearchPrefixSmallGraph measures one registered prefix search
// over a small graph on an instance built for that backend.
func BenchmarkBoundSearchPrefixSmallGraph(b *testing.B) {
	backend := apocBenchmarkBackend(b, 100)
	functions := apocsearch.New(backend)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		{
			nodes, apocCallError1 := functions.Prefix(ctx, "Person", "name", "Person-1")
			if apocCallError1 != nil {
				b.Fatalf("APOC helper failed: %v", apocCallError1)
			}
			if len(nodes) == 0 {
				b.Fatal("expected matches")
			}
		}
	}
}

// BenchmarkBoundSearchPrefixLargeGraph measures the same search over a large
// graph, so the per-node scan dominates the per-call binding.
func BenchmarkBoundSearchPrefixLargeGraph(b *testing.B) {
	backend := apocBenchmarkBackend(b, 20000)
	functions := apocsearch.New(backend)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		{
			nodes, apocCallError2 := functions.Prefix(ctx, "Person", "name", "Person-19999")
			if apocCallError2 != nil {
				b.Fatalf("APOC helper failed: %v", apocCallError2)
			}
			if len(nodes) != 1 {
				b.Fatalf("expected one match, got %d", len(nodes))
			}
		}
	}
}

// BenchmarkBoundHandlerPerRowSmallGraph measures the admitted handler of a bound
// instance once per row on a small graph.
func BenchmarkBoundHandlerPerRowSmallGraph(b *testing.B) {
	backend := apocBenchmarkBackend(b, 10)
	handler, err := registry.Compile(apocsearch.New(backend).Prefix, registry.OpaqueReads)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	args := []interface{}{"Person", "name", "Person-1"}
	prefix := []reflect.Value{reflect.ValueOf(ctx)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := handler.CallWithPolicy(args, prefix, registry.CypherArguments); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBoundHandlerPerRowLargeGraph measures the same admitted handler once
// per row on a large graph.
func BenchmarkBoundHandlerPerRowLargeGraph(b *testing.B) {
	backend := apocBenchmarkBackend(b, 20000)
	handler, err := registry.Compile(apocsearch.New(backend).Prefix, registry.OpaqueReads)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	args := []interface{}{"Person", "name", "Person-19999"}
	prefix := []reflect.Value{reflect.ValueOf(ctx)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := handler.CallWithPolicy(args, prefix, registry.CypherArguments); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIndependentInstanceAdmission measures admitting one handler from a
// freshly built instance, the cost the migration keeps out of the per-row path.
func BenchmarkIndependentInstanceAdmission(b *testing.B) {
	backend := apocBenchmarkBackend(b, 10)
	relationship := &apocstorage.Relationship{ID: 1, StartNode: 1, EndNode: 2, Type: "KNOWS"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := registry.Compile(apocrel.New(backend).StartNode, registry.OpaqueReads); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = relationship
}

// BenchmarkIndependentInstancePerRow measures invoking that independently
// admitted handler once per row: only the binding is measured here, so a per-row
// allocation introduced by instance binding would appear in allocs/op.
func BenchmarkIndependentInstancePerRow(b *testing.B) {
	backend := apocBenchmarkBackend(b, 10)
	relationship := &apocstorage.Relationship{ID: 1, StartNode: 1, EndNode: 2, Type: "KNOWS"}
	handler, err := registry.Compile(apocrel.New(backend).StartNode, registry.OpaqueReads)
	if err != nil {
		b.Fatal(err)
	}
	values := []reflect.Value{reflect.ValueOf(b.Context()), reflect.ValueOf(relationship)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := handler.InvokeValues(values); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFreeHelperPerRow is the reference for the two per-row benchmarks: a
// free helper of the same shape, admitted and invoked the same way.
func BenchmarkFreeHelperPerRow(b *testing.B) {
	relationship := &apocstorage.Relationship{ID: 1, StartNode: 1, EndNode: 2, Type: "KNOWS"}
	handler, err := registry.Compile(apocrel.ID, registry.CanonicalReads)
	if err != nil {
		b.Fatal(err)
	}
	values := []reflect.Value{reflect.ValueOf(relationship)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := handler.InvokeValues(values); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBoundNodeDegreeSmallGraph reads the degree of one node through a
// bound instance, the access pattern the per-node scan helpers use.
func BenchmarkBoundNodeDegreeSmallGraph(b *testing.B) {
	backend := apocBenchmarkBackend(b, 100)
	functions := apocnode.New(backend)
	node := &apocstorage.Node{ID: 1, Labels: []string{"Person"}}
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		{
			got, apocCallError3 := functions.Degree(ctx, node, "")
			if apocCallError3 != nil {
				b.Fatalf("APOC helper failed: %v", apocCallError3)
			}
			if got != 0 {
				b.Fatalf("unexpected degree %d", got)
			}
		}
	}
}
