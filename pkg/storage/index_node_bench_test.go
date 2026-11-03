package storage

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkCompositeIndexIndexNode isolates the canonical IndexNode preparation
// and publication cost at arity 1, 2 and 8. Every timed iteration files a node
// that is already filed under the same keys, so the measured work is the
// canonical key preparation plus the posting-map update, not map growth or
// caller-side property construction. The accepted-write lifetime is the
// non-cancellable one the context-free signature had, so the comparison
// isolates that work from a request's cancellation polling.
func BenchmarkCompositeIndexIndexNode(b *testing.B) {
	ctx := context.Background()
	for _, arity := range []int{1, 2, 8} {
		b.Run(fmt.Sprintf("arity=%d", arity), func(b *testing.B) {
			index := benchmarkCompositeIndex(arity)
			const population = 256
			ids := make([]NodeID, population)
			properties := make([]map[string]interface{}, population)
			for i := range population {
				ids[i] = NodeID(fmt.Sprintf("bench:n%d", i))
				properties[i] = benchmarkCompositeProperties(index.Properties, i)
				if err := index.IndexNode(ctx, ids[i], properties[i]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				slot := i % population
				if err := index.IndexNode(ctx, ids[slot], properties[slot]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkCompositeIndex(arity int) *CompositeIndex {
	properties := make([]string, arity)
	for i := range properties {
		properties[i] = fmt.Sprintf("p%d", i)
	}
	return &CompositeIndex{
		Name:        "bench",
		Label:       "Item",
		Properties:  properties,
		values:      make(map[interface{}][]NodeID),
		keysDirty:   true,
		fullIndex:   make(map[string][]NodeID),
		prefixIndex: make(map[string][]NodeID),
	}
}

func benchmarkCompositeProperties(properties []string, node int) map[string]interface{} {
	values := make(map[string]interface{}, len(properties))
	for i, name := range properties {
		values[name] = fmt.Sprintf("value-%d-%d", node, i)
	}
	return values
}
