package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// admissionScalingStatement has the same changing declaration/pattern density
// as the public grid fixture, without storage writes or path-search work.
func admissionScalingStatement(bytes, identity int) string {
	var text strings.Builder
	text.Grow(bytes + 96)
	text.WriteString("CREATE ")
	for index := 0; text.Len() < bytes; index++ {
		if index > 0 {
			text.WriteByte(',')
		}
		fmt.Fprintf(&text, "(n%d_%d:Grid {r:%d,c:%d})", identity, index, index%40, index/40)
		if index > 0 {
			fmt.Fprintf(&text, ",(n%d_%d)-[:NEXT]->(n%d_%d)", identity, index-1, identity, index)
		}
	}
	return text.String()
}

func TestSemanticAdmissionScalesWithStatementSize(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	identity := 0
	var previousBytes int
	var previousAllocs float64
	var previousElapsed time.Duration
	for _, bytes := range []int{10 << 10, 40 << 10, 160 << 10} {
		var elapsed time.Duration
		actualBytes := 0
		allocations := testing.AllocsPerRun(1, func() {
			identity++
			query := admissionScalingStatement(bytes, identity)
			actualBytes = len(query)
			start := time.Now()
			if err := executor.validateSemanticScopes(context.Background(), query); err != nil {
				t.Fatal(err)
			}
			elapsed = time.Since(start)
		})
		t.Logf("bytes=%d admission=%s allocations=%.0f", actualBytes, elapsed, allocations)
		if previousBytes > 0 {
			growth := float64(actualBytes) / float64(previousBytes)
			// Allocation growth is deterministic enough to catch reacquiring
			// the complete native tree for each pattern without a timing floor.
			if allocations > previousAllocs*growth*1.5 {
				t.Fatalf("superlinear admission allocations: %dB %.0f -> %dB %.0f", previousBytes, previousAllocs, actualBytes, allocations)
			}
		}
		if previousBytes > 0 {
			growth := float64(actualBytes) / float64(previousBytes)
			if elapsed > time.Duration(float64(previousElapsed)*growth*2.5)+time.Second {
				t.Fatalf("superlinear admission time: %dB %s -> %dB %s", previousBytes, previousElapsed, actualBytes, elapsed)
			}
		}
		previousBytes, previousAllocs, previousElapsed = actualBytes, allocations, elapsed
	}
}

func BenchmarkSemanticAdmissionScaling(b *testing.B) {
	for _, bytes := range []int{10 << 10, 40 << 10, 160 << 10} {
		b.Run(fmt.Sprintf("bytes_%d", bytes), func(b *testing.B) {
			executor := NewStorageExecutor(newTestMemoryEngine(b))
			b.ReportAllocs()
			b.SetBytes(int64(bytes))
			for index := 0; index < b.N; index++ {
				query := admissionScalingStatement(bytes, index)
				if err := executor.validateSemanticScopes(context.Background(), query); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
