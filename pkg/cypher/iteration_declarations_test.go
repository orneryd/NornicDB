package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// referenceIterationDeclaration is the per-IN check iterationDeclarations
// replaced: it looks back from the IN through every FOREACH and FOR keyword
// before it. The one-pass answer must match it for every IN.
func referenceIterationDeclaration(text string, in int) bool {
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	for from := 0; from < in; {
		start := keywordIndexFrom(text, "FOREACH", from, opts)
		if start < 0 || start >= in {
			break
		}
		open := queryGapEnd(text, start+len("FOREACH"))
		if open < len(text) && text[open] == '(' {
			_, end, ok := scanIdentifierToken(text, queryGapEnd(text, open+1))
			if ok && queryGapEnd(text, end) == in {
				return true
			}
		}
		from = start + len("FOREACH")
	}
	for from := 0; from < in; {
		start := keywordIndexFrom(text, "FOR", from, opts)
		if start < 0 || start >= in {
			return false
		}
		nameStart := queryGapEnd(text, start+len("FOR"))
		if nameStart < in {
			if _, nameEnd, ok := scanIdentifierToken(text, nameStart); ok && queryGapEnd(text, nameEnd) == in {
				return true
			}
		}
		from = start + len("FOR")
	}
	return false
}

func TestIterationDeclarationsMatchPerINCheck(t *testing.T) {
	queries := []string{
		"FOREACH (x IN 5 | CREATE (:N {v: x}))",
		"FOREACH(x IN [1,2] | SET n.v = x) RETURN 1 IN [1]",
		"FOREACH ( x /* c */ IN $list | CREATE (:N))",
		"UNWIND [1] AS i FOREACH (y IN i | CREATE (:N)) RETURN [z IN [1] | z IN [1]]",
		"FOR x IN 5 RETURN x",
		"FOR x IN [1, 2] FOR y IN x RETURN x IN [1], y",
		"FOR\n  x\n  IN $p RETURN x",
		"RETURN 'FOREACH (x IN 5 | ' AS s, 1 IN [1]",
		"// FOR x IN 5\nRETURN 2 IN [2]",
		"/* FOREACH (x IN 1 | */ RETURN 3 IN [3]",
		"MATCH (n) WHERE n.for IN [1] RETURN n.foreach IN [2]",
		"WITH 1 AS for RETURN for IN [1]",
		"RETURN [for IN [1, 2] | for IN [1]] AS x",
		"MATCH (n) FOREACH (a IN CASE WHEN n.x IN [1] THEN [n] ELSE [] END | FOREACH (b IN [1] | SET a.v = b))",
		"CALL { FOREACH (x IN 1 | CREATE (:N)) } RETURN any(v IN [1] WHERE v IN [1])",
		"RETURN reduce(s = 0, x IN [1] | s + x), all(y IN [1] WHERE y IN [1])",
		"FOREACH x IN 5", "FOR", "FOR x", "FOREACH (", "FOREACH (x", "FOR x IN",
	}
	for _, q := range queries {
		declarations := iterationDeclarations{text: q}
		ins := 0
		require.NoError(t, forEachListOperand(q, func(_, in int) error {
			ins++
			require.Equal(t, referenceIterationDeclaration(q, in), declarations.declares(in), "%q at IN %d", q, in)
			return nil
		}))
		if strings.Contains(strings.ToUpper(q), " IN ") {
			require.Positive(t, ins, q)
		}
	}
}

// A statement with many INs is validated in one pass over its iteration
// keywords, not one look-back per IN.
func TestValidateListOperandsLargeStatementWithFor(t *testing.T) {
	var b strings.Builder
	b.WriteString("FOR x IN 5 ")
	for i := 0; i < 2000; i++ {
		b.WriteString("WITH x WHERE x IN [1, 2] AND any(f IN [x] WHERE f IN [x]) ")
	}
	b.WriteString("RETURN x")
	q := b.String()
	require.NoError(t, validateListOperands(q, nil))
	declarations := iterationDeclarations{text: q}
	require.True(t, declarations.declares(strings.Index(q, "IN 5")))
	require.False(t, declarations.declares(strings.Index(q, "IN [1, 2]")))
	require.Error(t, validateListOperands(q+" UNION RETURN 1 IN 5 AS x", nil))
}
