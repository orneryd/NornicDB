package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/functionsyntaxmatrix"
	"github.com/stretchr/testify/require"
)

var functionSyntaxReaderInputs = []string{
	"RETURN coalesce(/* don't split , */ null, custom.fn(1)), 1",
	"CALL db.labels() YIELD label RETURN count(label)",
	"CALL db . labels YIELD label RETURN label",
	"CALL db.`my proc`(/* , */ 1, 'a,b') YIELD x RETURN sum(x)",
	"USE g.sub MATCH (n) RETURN toUpper(n.name)",
	"MATCH (n:Person)-[:KNOWS]->(m) WHERE exists { MATCH (m)--() } RETURN collect(m.name)",
	"RETURN COUNT { MATCH (n) RETURN count(n) } AS c, sum(1)",
	"RETURN COLLECT { MATCH (n) RETURN max(n.v) } AS c",
	"UNWIND [1,2] AS x RETURN {c:count(x), valid:(null):A} AS v",
	"UNWIND [1,2] AS x RETURN {`c`/* don't split , */:count(x)} AS v",
	"UNWIND [1,2] AS x RETURN {c// count('bad'), range('bad')\n:count(x)} AS v",
	"RETURN [x IN range(1,3) | x][1..size([1])]",
	"RETURN x IS :: DATE, x IS TYPED LOCAL DATETIME, x IS :: ZONED DATETIME",
	"RETURN `my fn`(1), n.`prop`(2), `a``b`(3)",
	"RETURN 'count(' + \"sum(\" + n.`x(`",
	"RETURN f(1, g(2, [3, {a: h(4)}]), // line ,\n 5)",
	"RETURN f(1",
	"RETURN f(1]) + g)",
	"RETURN toLower (x), size　(y)",
	"RETURN a.b.c(1) + $p(2) + x.y(3)",
	"CALL { MATCH (n) RETURN count(n) AS c } RETURN c",
	"CALL (x) { RETURN x } RETURN 1",
	"RETURN count(DISTINCT x), percentileCont(x, 0.5)",
	"MATCH p = shortestPath((a)-[*]-(b)) RETURN length(p)",
	"RETURN /* unclosed count(1)",
	"RETURN `unclosed(1)",
	"RETURN 1 / 2 / f(3)",
	"MATCH (n) WHERE n.name STARTS WITH ('a') AND NOT (n)--() RETURN avg(n.v)",
}

// An unclosed scalar body makes the boolean reader fall back to the full
// aggregate reader, so it is compared with that reader instead.
const unclosedScalarReaderInput = "RETURN 1 + COUNT { MATCH (n) RETURN max(n) "

// pad keeps a text beyond the retention bound without changing any call,
// argument or comment, so every reader takes its own lexical path.
func paddedReaderInput(text string) string {
	return text + "\n" + strings.Repeat(" ", 16385)
}

func uncachedFunctionSyntax(text string) *functionSyntax {
	syntax := &functionSyntax{text: text, leading: lexicalLeadingTriviaEnd(text)}
	scanFunctionSyntax(syntax, false)
	for index := range syntax.calls {
		syntax.calls[index].syntax = syntax
	}
	return syntax
}

// Every narrow reader steps through the same tokens and call classifier as
// the retained facts. Their answers must equal what the facts imply.
func TestFunctionSyntaxReadersAgreeWithFacts(t *testing.T) {
	inputs := append([]string{unclosedScalarReaderInput}, functionSyntaxReaderInputs...)
	for _, cases := range [][]functionsyntaxmatrix.Case{functionsyntaxmatrix.Cases(), functionsyntaxmatrix.NativeCases()} {
		for _, item := range cases {
			inputs = append(inputs, item.Query)
		}
	}
	for _, input := range inputs {
		text := paddedReaderInput(input)
		syntax := uncachedFunctionSyntax(text)

		type visited struct {
			name, normalized string
			open             int
		}
		var expected, actual []visited
		names := map[string]bool{}
		aggregate := false
		for index := 0; index < len(text); {
			call, ok := syntax.nextCall(index, true, false, false)
			if !ok {
				break
			}
			expected = append(expected, visited{call.name, call.normalized, call.open})
			names[call.name] = true
			index = call.open + 1
		}
		for index := 0; index < len(text); {
			call, ok := syntax.nextCall(index, false, false, false)
			if !ok {
				break
			}
			if lexicalAggregateName(call.name) && call.close(text) >= 0 {
				aggregate = true
				break
			}
			index = call.open + 1
		}
		require.NoError(t, lexicalFunctionInvocations(text, func(name, normalized string, open int) error {
			actual = append(actual, visited{name, normalized, open})
			return nil
		}))
		require.Equal(t, expected, actual, "invocations: %q", input)
		if input == unclosedScalarReaderInput {
			aggregate = len(findAggregateSpans(text)) > 0
		}
		require.Equal(t, aggregate, hasLexicalAggregate(text), "aggregate: %q", input)
		for name := range names {
			want := make([]string, 0)
			for index := 0; index < len(text); {
				call, ok := syntax.nextCall(index, true, false, false)
				if !ok {
					break
				}
				index = call.open + 1
				if !strings.EqualFold(call.name, name) {
					continue
				}
				close := call.close(text)
				if close < 0 {
					break
				}
				want = append(want, syntax.source(syntaxSpan{call.open + 1, close}))
				index = close + 1
			}
			require.Equal(t, want, lexicalNamedArguments(text, name), "named %s: %q", name, input)
		}
		if strings.IndexByte(text, '/') >= 0 {
			require.Equal(t, syntax.render(syntaxSpan{0, len(text)}), lexicalExpressionText(text), "render: %q", input)
		}
	}
}

// The name-only walk is the hot static-check path: it keeps no facts.
func TestFunctionSyntaxInvocationWalkDoesNotAllocate(t *testing.T) {
	text := paddedReaderInput("RETURN " + strings.Repeat("coalesce(/* don't split , */ null, custom.fn(1)),", 512) + "1")
	require.Zero(t, testing.AllocsPerRun(10, func() {
		if err := lexicalFunctionInvocations(text, func(_, _ string, _ int) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}))
}
