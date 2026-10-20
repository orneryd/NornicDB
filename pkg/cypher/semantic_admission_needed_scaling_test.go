package cypher

import (
	"context"
	"testing"
	"time"
)

// The native-required routes must obey the same growth gate as the ordinary
// graph route. The payload and assertions are the existing scaling fixture.
func TestNeededStatementAdmissionScalesWithStatementSize(t *testing.T) {
	for _, shape := range []struct{ name, prefix, suffix string }{
		{"call", "CALL { ", " }"},
		{"nested_call", "CALL { CALL { ", " } }"},
		{"constituent", "USE comp.one ", ""},
	} {
		t.Run(shape.name, func(t *testing.T) {

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
					query := shape.prefix + admissionScalingStatement(bytes, identity) + shape.suffix
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
		})
	}
}
