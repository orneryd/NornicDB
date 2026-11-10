package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The header of a scalar-coercing iteration is read at its IN.
func TestCoercingIterationDeclarationReadsTheHeaderAtItsIN(t *testing.T) {
	long := strings.Repeat("WITH [y IN [1, 2] | y + 1] AS padding, 'FOREACH (x IN' AS text\n", 40)
	for _, tc := range []struct {
		name, text string
		occurrence int
		coerces    bool
	}{
		{"foreach header", "MATCH (n) FOREACH (x IN n.list | SET n.seen = x)", 0, true},
		{"foreach header with a quoted name and a comment", "FOREACH ( `x y` /* each */ IN [1] | CREATE (:N))", 0, true},
		{"for header", "FOR x IN [1, 2] RETURN x", 0, true},
		{"membership inside a foreach", "FOREACH (x IN [1] | SET n.a = (x IN [2]))", 1, false},
		{"list comprehension", "RETURN [x IN [1, 2] | x] AS a", 0, false},
		{"membership", "MATCH (n) WHERE n.a IN [1] RETURN n", 0, false},
		{"a string that reads like a header", "RETURN 'FOREACH (x' IN ['a'] AS a", 0, false},
		{"a property named like the keyword", "MATCH (n) WHERE n.for IN [1] RETURN n", 0, false},
		{"a name that ends like the keyword", "MATCH (nfor) WHERE nfor IN [1] RETURN nfor", 0, false},
		{"foreach header after a long prefix", long + "FOREACH (x IN [1] | CREATE (:N))", 40, true},
		{"membership after a long prefix", long + "RETURN 1 IN [1] AS a", 40, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var found []int
			require.NoError(t, forEachListOperand(context.Background(), tc.text, func(_, in int) error {
				found = append(found, in)
				return nil
			}))
			require.Greater(t, len(found), tc.occurrence, "list operands found: %v", found)
			for range 2 {
				require.Equal(t, tc.coerces, coercingIterationDeclaration(context.Background(), tc.text, found[tc.occurrence]))
			}
		})
	}
}
