// Representative benchmarks for the values APOC storage reads hand to callers.
//
// The point read and the whole-graph read are the two exits whose caller-owned
// result carries a full property bag, so they bound the cost of result
// ownership. Timing is captured by the reviewer after the correction lands; run
// these alone (no test binaries of another package running at the same time):
//
//	go test ./apoc/storage -run '^$' -bench 'BenchmarkInMemoryStorage' -benchmem
package storage

import (
	"github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
)

// BenchmarkInMemoryStorageGetNodeReturnedValue measures one point read: the
// smallest public exit that hands a caller a property bag.
func BenchmarkInMemoryStorageGetNodeReturnedValue(b *testing.B) {
	store := NewInMemoryStorage()
	created, err := store.CreateNode(b.Context(), []string{"Person"}, testfixture.NestedGraphProperties(1))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		node, err := store.GetNode(b.Context(), created.ID)
		if err != nil {
			b.Fatal(err)
		}
		if node.Properties["name"] != "node" {
			b.Fatalf("unexpected node: %v", node.Properties["name"])
		}
	}
}

// BenchmarkInMemoryStorageAllNodesReturnedValues measures the whole-graph read
// of a 256 node graph: every node's property bag becomes the caller's.
func BenchmarkInMemoryStorageAllNodesReturnedValues(b *testing.B) {
	const fixtureSize = 256
	store := NewInMemoryStorage()
	for index := 0; index < fixtureSize; index++ {
		if _, err := store.CreateNode(b.Context(), []string{"Person"}, testfixture.NestedGraphProperties(index)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		nodes, err := store.AllNodes(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != fixtureSize {
			b.Fatalf("unexpected node count: %d", len(nodes))
		}
	}
}
