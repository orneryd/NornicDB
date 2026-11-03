package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// discoveryBenchSchema builds the schema shape every discovery contract is
// measured against: `others` arity-1 indexes that never match, one arity-2
// index that never matches, then the arity-1 target on Person/name and the
// arity-2 target on Fact/(left,right). Map iteration order is unspecified, so
// the target's position in the scan is a sample of the same distribution for
// every variant compared.
func discoveryBenchSchema(tb testing.TB, others int) *SchemaManager {
	tb.Helper()
	sm := NewSchemaManager()
	for i := 0; i < others; i++ {
		require.NoError(tb, sm.AddPropertyIndex(fmt.Sprintf("other_%03d", i), fmt.Sprintf("Other%03d", i), []string{"value"}))
	}
	require.NoError(tb, sm.AddCompositeIndex("pair", "Pair", []string{"left", "right"}))
	require.NoError(tb, sm.AddPropertyIndex("person_name", "Person", []string{"name"}))
	require.NoError(tb, sm.AddCompositeIndex("fact_key", "Fact", []string{"left", "right"}))
	return sm
}

// BenchmarkIndexDiscoveryArity1Property measures the arity-1 existence and
// seekable discovery contracts on a common small schema and on a large one:
// HasPropertyIndex (seekable) and GetPropertyIndex (seekable plus index).
func BenchmarkIndexDiscoveryArity1Property(b *testing.B) {
	for _, others := range []int{0, 7, 63} {
		sm := discoveryBenchSchema(b, others)
		ctx := context.Background()
		b.Run(fmt.Sprintf("others=%d/has", others), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exists, err := sm.HasPropertyIndex(ctx, "Person", "name")
				if err != nil {
					b.Fatal(err)
				}
				if !exists {
					b.Fatal("index not discovered")
				}
			}
		})
		b.Run(fmt.Sprintf("others=%d/get", others), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx, exists, err := sm.GetPropertyIndex(ctx, "Person", "name")
				if err != nil {
					b.Fatal(err)
				}
				if !exists || idx == nil {
					b.Fatal("index not discovered")
				}
			}
		})
	}
}

// BenchmarkIndexDiscoveryExactProperties measures the exact-property discovery
// contract filledKeyIndex serves for node key and uniqueness validation.
func BenchmarkIndexDiscoveryExactProperties(b *testing.B) {
	for _, others := range []int{0, 7, 63} {
		sm := discoveryBenchSchema(b, others)
		properties := []string{"left", "right"}
		b.Run(fmt.Sprintf("others=%d", others), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx, err := sm.filledKeyIndex(context.Background(), "Fact", properties)
				if err != nil {
					b.Fatal(err)
				}
				if idx == nil {
					b.Fatal("index not discovered")
				}
			}
		})
	}
}

// BenchmarkIndexDiscoveryAnyLabel measures the label-existence discovery
// existence contract; current production maintenance uses property-specific discovery.
func BenchmarkIndexDiscoveryAnyLabel(b *testing.B) {
	for _, others := range []int{0, 7, 63} {
		sm := discoveryBenchSchema(b, others)
		b.Run(fmt.Sprintf("others=%d", others), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !sm.HasAnyPropertyIndexForLabel("Person") {
					b.Fatal("label index not discovered")
				}
			}
		})
	}
}

// BenchmarkIndexDiscoveryAbsentInLargeSchema measures the full scan a lookup
// pays when no index matches: every declared index is visited and the caller's
// request is observed on each visit, so a large schema is where the
// cancellation checks and the scan's own cost are visible.
func BenchmarkIndexDiscoveryAbsentInLargeSchema(b *testing.B) {
	sm := discoveryBenchSchema(b, 256)
	ctx := context.Background()
	b.Run("has_absent", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			exists, err := sm.HasPropertyIndex(ctx, "Person", "missing")
			if err != nil {
				b.Fatal(err)
			}
			if exists {
				b.Fatal("absent index reported as seekable")
			}
		}
	})
	b.Run("filled_key_absent", func(b *testing.B) {
		properties := []string{"left", "middle", "right"}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx, err := sm.filledKeyIndex(ctx, "Fact", properties)
			if err != nil {
				b.Fatal(err)
			}
			if idx != nil {
				b.Fatal("absent key index reported as filled")
			}
		}
	})
}

// BenchmarkPropertyIndexLookup is the ordinary equality seek a matched index
// serves after discovery: it isolates the lookup cost from the discovery cost.
func BenchmarkPropertyIndexLookup(b *testing.B) {
	sm := discoveryBenchSchema(b, 7)
	ctx := context.Background()
	const nodes = 128
	for i := 0; i < nodes; i++ {
		require.NoError(b, sm.PropertyIndexInsert("Person", "name", NodeID(fmt.Sprintf("p:%03d", i)), fmt.Sprintf("name-%03d", i%8)))
	}
	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ids, err := sm.PropertyIndexLookup(ctx, "Person", "name", "name-003")
			if err != nil {
				b.Fatal(err)
			}
			if len(ids) != nodes/8 {
				b.Fatalf("unexpected hit count %d", len(ids))
			}
		}
	})
	b.Run("miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ids, err := sm.PropertyIndexLookup(ctx, "Person", "name", "absent")
			if err != nil {
				b.Fatal(err)
			}
			if len(ids) != 0 {
				b.Fatalf("unexpected hit count %d", len(ids))
			}
		}
	})
}

// BenchmarkPropertyIndexLookupAnyLabel is the labelless-pattern seek: it
// collects every arity-1 index declaring the property on any label, then looks
// the value up in each one.
func BenchmarkPropertyIndexLookupAnyLabel(b *testing.B) {
	sm := discoveryBenchSchema(b, 7)
	ctx := context.Background()
	require.NoError(b, sm.PropertyIndexInsert("Person", "name", "p:1", "alice"))
	require.NoError(b, sm.PropertyIndexInsert("Other003", "value", "o:1", "alice"))
	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ids, err := sm.PropertyIndexLookupAnyLabel(ctx, "name", "alice")
			if err != nil {
				b.Fatal(err)
			}
			if len(ids) != 1 || ids[0] != "p:1" {
				b.Fatalf("unexpected ids %v", ids)
			}
		}
	})
	b.Run("absent_property", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ids, err := sm.PropertyIndexLookupAnyLabel(ctx, "unindexed", "alice")
			if err != nil {
				b.Fatal(err)
			}
			if len(ids) != 0 {
				b.Fatalf("unexpected ids %v", ids)
			}
		}
	})
}
