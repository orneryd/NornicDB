package cypher

import (
	"strconv"
	"strings"
	"testing"
)

func BenchmarkPostfixReceiverBoundary(b *testing.B) {
	for _, size := range []int{0, 128, 4096} {
		prefix := strings.Repeat("n + ", size)
		for _, receiver := range []string{"x[0]", "keys({a:1})[0]", "[1,2][0]", "x[0][0]", "'text'[0]"} {
			b.Run(strconv.Itoa(size)+"/"+receiver, func(b *testing.B) {
				expression := prefix + receiver
				open := strings.LastIndexByte(expression, '[')
				if start := rowSubscriptReceiverStart(expression, open); start != len(prefix) {
					b.Fatalf("receiver starts at %d, want %d", start, len(prefix))
				}
				b.ReportAllocs()
				for b.Loop() {
					if rowSubscriptReceiverStart(expression, open) != len(prefix) {
						b.Fatal("unstable receiver boundary")
					}
				}
			})
		}
	}
}

// Ordinary list literals must not trigger a prefix walk for every '['.
// Fourfold input growth exposes the former quadratic receiver-admission cost.
func BenchmarkStaticSubscriptListPopulation(b *testing.B) {
	for _, size := range []int{256, 1024, 4096} {
		query := "RETURN [" + strings.Repeat("[1],", size-1) + "[1]] AS v"
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := validateStaticPropertySubscripts(b.Context(), query, staticTypeScope{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
