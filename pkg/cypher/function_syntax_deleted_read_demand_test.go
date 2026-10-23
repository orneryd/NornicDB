package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxDeletedReadDemandAllocations(t *testing.T) {
	expression := strings.Repeat("/* don't range('bad'), ( */ 0 + ", 1200) + "labels(n) + n.name"
	require.Greater(t, len(expression), 16384)
	want := []deletedEntityRead{{variable: "n"}, {variable: "n"}}
	require.Equal(t, want, deletedEntityReadsIn(expression))
	allocations := testing.AllocsPerRun(20, func() {
		reads := deletedEntityReadsIn(expression)
		if len(reads) != 2 || reads[0] != want[0] || reads[1] != want[1] {
			t.Fatalf("unexpected reads: %v", reads)
		}
	})
	require.LessOrEqual(t, allocations, float64(4), "unrelated comments must not build a full lexical inventory")
}

func TestFunctionSyntaxDeletedReadDemandOrder(t *testing.T) {
	for _, expression := range []string{
		"a.p + labels(/* x */ n) + properties(`r`) + z.q",
		"a.p + labels(keys(n)) + properties(`r`) + z.q",
		"a.p + `labels`(n,) + properties(`r`) + z.q",
	} {
		// Function inputs use the established argument policy; this also
		// covers nested calls, quoted names and an omitted trailing item.
		want := []deletedEntityRead{{variable: "a"}, {variable: "n"}, {variable: "r", relationshipOnly: true}, {variable: "z"}}
		if strings.Contains(expression, "keys(n)") {
			want[1].relationshipOnly = true
		}
		require.Equal(t, want, deletedEntityReadsIn(expression), expression)
	}
}
