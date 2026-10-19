package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpressionIdentityLexicalContract(t *testing.T) {
	for _, sample := range []struct{ expression, name string }{
		{"/* before */ n /* after */", "n"},
		{"(/* before */ (n) /* after */)", "n"},
		{"((// ' \" , ) range('bad')\n n))", "n"},
		{"((`a/*data*/b`))", "a/*data*/b"},
		{"((`a//data`))", "a//data"},
		{"((`a``b`))", "a`b"},
		{"(n/* no joining */x)", ""},
		{"(n /* after */) + 0", ""},
		{"(n/* after */.x)", ""},
		{"('n/*quoted*/')", ""},
		{"($n/* after */)", ""},
		{"(n) OR (n)", ""},
	} {
		t.Run(sample.expression, func(t *testing.T) {
			require.Equal(t, sample.name, simpleSemanticIdentifier(unwrapOuterParens(sample.expression)))
		})
	}
}
