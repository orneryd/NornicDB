package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Boolean consumers must not retain a statement's full syntax just to rule
// out a function or rewrite. The comment forces a lexical decision for every
// rewrite family, and the input is beyond the shared cache's retention bound.
func TestFunctionSyntaxBooleanDemandAllocations(t *testing.T) {
	text := strings.Repeat("/* WHERE vector ) : quoted-looking ' data */ ", 600) +
		"RETURN date.truncate('day',date('2020-01-02')), point.distance(point({x:0,y:0}),point({x:3,y:4}))"
	require.Greater(t, len(text), 16384)
	for _, sample := range []struct {
		name, text string
	}{
		{"lower", text},
		{"upper", strings.ToUpper(text)},
	} {
		t.Run(sample.name, func(t *testing.T) {
			for _, check := range []struct {
				name string
				run  func(string) bool
			}{
				{"function", func(text string) bool { return containsFunctionCallNamed(text, "rand") }},
				{"vector", mayUseVectorCall},
				{"pattern", mayUsePatternPredicate},
				{"label", mayTestExpressionLabels},
			} {
				t.Run(check.name, func(t *testing.T) {
					require.False(t, check.run(sample.text))
					require.Zero(t, testing.AllocsPerRun(10, func() {
						if check.run(sample.text) {
							t.Fatal("comment or ordinary call became a rewrite")
						}
					}))
				})
			}
		})
	}
}
