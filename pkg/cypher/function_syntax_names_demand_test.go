package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Query metadata asks about all six aggregate names in one walk. The caller's
// temporary name list must stay borrowed, including above the retention bound.
func TestFunctionSyntaxNameSetDemandAllocations(t *testing.T) {
	text := strings.Repeat("/* quoted-looking ' data */ ", 700) + "RETURN coalesce(null,1),date('2020-01-01')"
	require.Greater(t, len(text), 16384)
	for _, sample := range []string{text, strings.ToUpper(text)} {
		require.Zero(t, testing.AllocsPerRun(10, func() {
			if lexicalHasAnyFunction(sample, []string{"COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT"}) {
				t.Fatal("ordinary function became an aggregate")
			}
		}))
	}
}
