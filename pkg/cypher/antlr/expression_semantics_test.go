package antlr

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Pin the complete grammar enumeration behind SemanticExpression. A grammar
// change must reassess which constructors are transparent, not extend a generic
// single-child heuristic that can accidentally discard real semantics.
func TestSemanticExpressionGrammarWrappers(t *testing.T) {
	grammar, err := os.ReadFile("CypherParser.g4")
	require.NoError(t, err)
	for rule, production := range map[string]string{
		"expression":                "xorExpression (OR xorExpression)*",
		"xorExpression":             "andExpression (XOR andExpression)*",
		"andExpression":             "notExpression (AND notExpression)*",
		"notExpression":             "NOT* comparisonExpression | NOT* existsSubquery",
		"comparisonExpression":      "addSubExpression (comparisonSigns addSubExpression)*",
		"addSubExpression":          "multDivExpression ((PLUS | SUB) multDivExpression)*",
		"multDivExpression":         "powerExpression ((MULT | DIV | MOD) powerExpression)*",
		"powerExpression":           "unaryAddSubExpression (CARET unaryAddSubExpression)*",
		"unaryAddSubExpression":     "(PLUS | SUB)? atomicExpression",
		"atomicExpression":          "propertyOrLabelExpression (stringExpression | listExpression | nullExpression | typePredicate | normalizationPredicate | labelPredicate)*",
		"propertyOrLabelExpression": "propertyExpression (COLON labelExpression)*",
		"propertyExpression":        "atom (DOT name)*",
		"parenthesizedExpression":   "LPAREN expression RPAREN",
		"atom":                      "listComprehension | literal | parameter | mapProjection | caseExpression | reduceExpression | allReduceExpression | countAll | countSubquery | collectSubquery | patternComprehension | filterWith | relationshipsChainPattern | parenthesizedExpression | functionInvocation | symbol | subqueryExist",
	} {
		match := regexp.MustCompile(`(?m)^` + rule + `\s*:\s*([^;]+);`).FindSubmatch(grammar)
		require.Len(t, match, 2, rule)
		require.Equal(t, production, strings.Join(strings.Fields(string(match[1])), " "), rule)
	}
	for _, expression := range []string{"x", "`long name`", "``", "`WHERE`", "`a``b`", "1", "x+1", "x.value", "toString(x)", "+x", "-x", "NOT x", "$x", "x IS NULL", "x[0]", "x:Label", "[x]", "{value:x}"} {
		for depth := 0; depth <= 3; depth++ {
			written := strings.Repeat("(", depth) + expression + strings.Repeat(")", depth)
			t.Run(written, func(t *testing.T) {
				syntax, err := ParseSyntax(context.Background(), written, ExpressionEntry, false, RawSource)
				require.NoError(t, err)
				core := syntax.Root().SemanticExpression()
				require.True(t, core.Valid())
				require.Equal(t, expression, core.Text())
				require.Equal(t, written, syntax.Root().Text(), "semantic views never rewrite presentation")
			})
		}
	}
	require.False(t, (SyntaxNode{}).SemanticExpression().Valid())
}
