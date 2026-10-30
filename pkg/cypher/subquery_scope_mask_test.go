package cypher

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Scope masking must not turn a grouped subquery into an apparent node
// named EXISTS/COUNT/COLLECT, or hide a keyword-named map projection.
func TestSubqueryScopeMaskPreservesOperandKinds(t *testing.T) {
	for _, tc := range []struct{ name, expression string }{
		{"exists", "(EXISTS { MATCH (x)-->(y) } AND (a)-->(b))"},
		{"count", "(COUNT { (x)-->(y) } > 0 AND (a)-->(b))"},
		{"collect", "(COLLECT { MATCH (x) RETURN x } = [] AND (a)-->(b))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			masked := maskSubqueryBodies(originalBraceView(tc.expression))
			require.Len(t, masked, len(tc.expression))
			require.Equal(t, []string{"a", "b"}, extractNodeVariables(originalBraceView(masked)))
		})
	}
	for _, expression := range []string{
		"count {.a}", "exists {.*}", "collect {MATCH: 1}",
		"'EXISTS { MATCH (n) }'", "`count` {.a}",
		`"COUNT { MATCH (n) }"`, "EXISTS { MATCH (n)",
	} {
		require.Equal(t, expression, maskSubqueryBodies(originalBraceView(expression)))
	}
}
