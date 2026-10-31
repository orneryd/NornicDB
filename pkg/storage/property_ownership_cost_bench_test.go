package storage

import "testing"

var benchmarkOwnedEdge *Edge

// The same benchmark is overlaid onto the release checkout. Values above the
// runtime's small-integer box cache expose unnecessary scalar reboxing, while
// the list case measures the newly required detached backing storage.
func BenchmarkEdgePropertyOwnership(b *testing.B) {
	for _, row := range []struct {
		name  string
		value any
	}{
		{"integer", int64(1000)},
		{"float", float64(1.25)},
		{"nested-list", map[string]any{"values": []int64{1000, 2000}}},
	} {
		b.Run(row.name, func(b *testing.B) {
			edge := &Edge{ID: "test:ownership-cost", Properties: map[string]any{"value": row.value}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkOwnedEdge = copyEdge(edge)
			}
		})
	}
}
