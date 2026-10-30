package knowledgepolicy

import (
	"fmt"
	"testing"
)

func BenchmarkAccumulator_IncrementAccess_SingleEntity(b *testing.B) {
	a := NewAccessAccumulator(true, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.IncrementAccess("n1")
	}
}

func BenchmarkAccumulator_IncrementAccess_Parallel(b *testing.B) {
	a := NewAccessAccumulator(true, 0)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a.IncrementAccess("super-node")
		}
	})
}

func BenchmarkAccumulator_IncrementAccess_Distributed(b *testing.B) {
	a := NewAccessAccumulator(true, 0)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := fmt.Sprintf("n%d", b.N%128)
		for pb.Next() {
			a.IncrementAccess(id)
		}
	})
}

func BenchmarkAccumulator_ReadThrough(b *testing.B) {
	a := NewAccessAccumulator(true, 0)
	for i := 0; i < 1000; i++ {
		a.IncrementAccess("n1")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.ReadThrough("n1", "accessCount", 0)
	}
}

func BenchmarkAccumulator_Disabled(b *testing.B) {
	a := NewAccessAccumulator(false, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.IncrementAccess("n1")
	}
}
