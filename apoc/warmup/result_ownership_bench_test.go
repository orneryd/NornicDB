// Representative benchmarks for the values the warmup cache hands to callers.
//
// The single cached node read and the label index read are the two cached exits
// whose caller-owned result carries a property bag, so they bound the cost of
// result ownership in the cache. Timing is captured by the reviewer after the
// correction lands; run these alone (no test binaries of another package running
// at the same time):
//
//	go test ./apoc/warmup -run '^$' -bench 'BenchmarkWarmup' -benchmem
package warmup

import (
	"github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// BenchmarkWarmupGetCachedNodeReturnedValue measures one cached point read: the
// smallest public exit that hands a caller a cached property bag.
func BenchmarkWarmupGetCachedNodeReturnedValue(b *testing.B) {
	backend := storage.NewInMemoryStorage()
	created, err := backend.CreateNode(b.Context(), []string{"Person"}, testfixture.NestedGraphProperties(1))
	if err != nil {
		b.Fatal(err)
	}
	functions := New(backend)
	if _, err := functions.Run(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		node, ok := functions.GetCachedNode(created.ID)
		if !ok {
			b.Fatalf("node %d is not cached", created.ID)
		}
		if node.Properties["name"] != "node" {
			b.Fatalf("unexpected cached node: %v", node.Properties["name"])
		}
	}
}

// BenchmarkWarmupGetNodesByLabelReturnedValues measures the label index read of
// a 128 node warmup: every cached node's property bag becomes the caller's.
func BenchmarkWarmupGetNodesByLabelReturnedValues(b *testing.B) {
	const fixtureSize = 128
	backend := storage.NewInMemoryStorage()
	for index := 0; index < fixtureSize; index++ {
		if _, err := backend.CreateNode(b.Context(), []string{"Person"}, testfixture.NestedGraphProperties(index)); err != nil {
			b.Fatal(err)
		}
	}
	functions := New(backend)
	if _, err := functions.Run(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		nodes, ok := functions.GetNodesByLabel("Person")
		if !ok {
			b.Fatal("the label index is not warmed")
		}
		if len(nodes) != fixtureSize {
			b.Fatalf("unexpected cached node count: %d", len(nodes))
		}
	}
}
