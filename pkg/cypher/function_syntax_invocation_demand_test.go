package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Name-only visitors must not allocate inventories proportional to unrelated
// expression punctuation, including inputs too large for lexical retention.
func TestFunctionSyntaxInvocationDemandAllocations(t *testing.T) {
	for _, identifiers := range []int{2000, 4000} {
		for _, line := range []bool{false, true} {
			query := commentedFunctionScanInput(identifiers, line)
			require.Greater(t, len(query), 16384)
			calls := 0
			visit := func(name, normalized string) error {
				if name != "coalesce" || normalized != "coalesce" {
					t.Fatalf("unexpected call %q (%q)", name, normalized)
				}
				calls++
				return nil
			}
			allocations := testing.AllocsPerRun(10, func() {
				calls = 0
				if err := forEachFunctionCall(query, visit); err != nil || calls != 1 {
					t.Fatalf("calls=%d error=%v", calls, err)
				}
			})
			require.Zero(t, allocations, "identifiers=%d line=%t", identifiers, line)
		}
	}
}
