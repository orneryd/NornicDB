package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/functionsyntaxmatrix"
	"github.com/stretchr/testify/require"
)

// Sparse requests are retention policies on the existing lexer. Compare them
// with its complete inventory, both on cache hits and above the cache bound.
// Public route tests keep the independently checked Neo4j outcomes unchanged.
func TestFunctionSyntaxDemandMatchesInventory(t *testing.T) {
	texts := []string{
		"sum(0) + COUNT { RETURN max(1) AS n } + sum(2)",
		"COUNT { RETURN COUNT { RETURN max(1) AS n } AS n }",
		"COUNT { RETURN max(1)",
		"COUNT { RETURN COUNT { RETURN max(1) AS n }",
		"sum(COUNT { RETURN max(1) AS n })",
		"sum(count(1)",
		"count(1] + sum(2)",
		"range(range(1,2),3) + range(4,5)",
		"range(1,2 + range(3,4)",
		"CALL range(count(1)) YIELD v RETURN v",
		"USE range(count(1)) RETURN v",
		"RETURN (n):count(1), {`c`:count(1)}, xs[..count(1)]",
		"RETURN substring(/* don't range('bad'), ) */ 'okay',0,2)",
		"RETURN count('not /* trivia */'), /* unfinished",
	}
	for _, sample := range functionsyntaxmatrix.Cases() {
		texts = append(texts, sample.Query)
	}
	prefix := "/* " + strings.Repeat("trivia ", 2400) + " */ "
	for index, text := range texts {
		for _, padding := range []string{"", prefix} {
			text := padding + text
			syntax := readFunctionSyntax(text)
			require.Equal(t, len(findAggregateSpans(text)) > 0, containsAggregateFunc(text), "aggregate %d: %s", index, text)
			require.Equal(t, syntax.render(syntaxSpan{0, len(text)}), lexicalExpressionText(text), "render %d", index)
			for _, name := range []string{"range", "coalesce", "substring", "count"} {
				want := make([]string, 0)
				for at := 0; at < len(text); {
					call, ok := syntax.nextCall(at, true, false, false)
					if !ok {
						break
					}
					at = call.open + 1
					if !strings.EqualFold(call.name, name) {
						continue
					}
					if call.close(text) < 0 {
						break
					}
					want = append(want, syntax.source(syntaxSpan{call.open + 1, call.close(text)}))
					at = call.close(text) + 1
				}
				require.Equal(t, want, namedFunctionArguments(text, name), "%s %d", name, index)
			}
		}
	}
}

func TestFunctionSyntaxLongAdapterView(t *testing.T) {
	expression := strings.Repeat("/* don't range('bad'), ( */ 0 + ", 600) + "labels(n) + properties(r) + n.name"
	require.Greater(t, len(expression), 16384)
	require.Equal(t, []deletedEntityRead{{variable: "n"}, {variable: "r", relationshipOnly: true}, {variable: "n"}}, deletedEntityReadsIn(expression))
	_, retained := functionSyntaxCache.get(functionSyntaxKey{expression, false})
	require.False(t, retained, "the reference walk keeps no facts")
	for _, text := range []string{"n.adapterView + 1", "labels(n) + keys(r) + n.adapterView"} {
		calls := newCallCursor(text)
		_, called := calls.callAt(0)
		require.Equal(t, strings.HasPrefix(text, "labels"), called, text)
		deletedEntityReadsIn(text)
		_, retained := functionSyntaxCache.get(functionSyntaxKey{text, false})
		require.False(t, retained, "a reference walk needs no full facts: %s", text)
	}
}

// Narrow readers keep only their own state. They never build or retain a
// call inventory, whatever the input size.
func TestFunctionSyntaxSparseRetention(t *testing.T) {
	block := functionFactConsumerInput(4000, "block")
	require.Len(t, lexicalNamedArguments(block, "coalesce"), 4000)
	subqueries := functionFactConsumerInput(4000, "subqueries")
	require.False(t, hasLexicalAggregate(subqueries))
	for _, text := range []string{block, subqueries} {
		_, retained := functionSyntaxCache.get(functionSyntaxKey{text, false})
		require.False(t, retained, "a narrow reader must not build retained facts")
	}
	// The boolean reader stops when count(1) closes, before the unclosed
	// trailing body that would need the full reader's fallback.
	text := "count(1) + COUNT { RETURN " + strings.Repeat("coalesce(null,1) + ", 4000)
	require.True(t, hasLexicalAggregate(text))
	require.Zero(t, testing.AllocsPerRun(10, func() {
		if !hasLexicalAggregate(text) {
			t.Fatal("outer aggregate missed")
		}
	}))
}

// These inputs exceed the retention bound. The boolean/absent-name requests
// need no owned results: bounded transient syntax must allocate nothing.
func TestFunctionSyntaxSparseStateDoesNotAllocate(t *testing.T) {
	text := functionFactConsumerInput(2000, "subqueries")
	require.Greater(t, len(text), 16384)
	require.False(t, containsAggregateFunc(text))
	require.Zero(t, testing.AllocsPerRun(10, func() {
		if containsAggregateFunc(text) {
			t.Fatal("inner aggregate escaped")
		}
	}))
	text = functionFactConsumerInput(2000, "block")
	require.Zero(t, testing.AllocsPerRun(10, func() {
		if len(namedFunctionArguments(text, "missing")) != 0 {
			t.Fatal("unrequested arguments retained")
		}
	}))
}

func TestFunctionSyntaxSparseNestingOverflow(t *testing.T) {
	prefix := "/* " + strings.Repeat("padding ", 2200) + " */ "
	for _, depth := range []int{16, 64, 128} {
		body := strings.Repeat("coalesce([{x:", depth) + "sum(1)" + strings.Repeat("}],null)", depth)
		text := prefix + body
		require.Greater(t, len(text), 16384)
		require.Equal(t, len(findAggregateSpans(text)) > 0, containsAggregateFunc(text))
		arguments := namedFunctionArguments(text, "coalesce")
		require.Len(t, arguments, 1)
		require.Equal(t, body[len("coalesce("):len(body)-1], arguments[0])
		require.Equal(t, body, lexicalExpressionText(text))
	}
}

func TestFunctionSyntaxExpressionViewGrouping(t *testing.T) {
	for _, text := range []string{"NOT (n)-[:R]->()", "NOT /* a function-like note() */ (n)-[:R]->()"} {
		calls := newCallCursor(text)
		_, call := calls.callAt(0)
		require.False(t, call, "NOT owns an operator operand, not function arguments")
		require.Equal(t, []string{"n"}, expressionFreeVariables(text))
	}
}

func TestFunctionSyntaxLegacyNameSpacing(t *testing.T) {
	for _, sample := range []struct {
		text string
		want bool
	}{
		{"count (n)", true},
		{"count\t\r\n(n)", true},
		{"count\u00a0(n)", false},
		{"count /* \u00a0 don't split range('bad'), ( */ (n)", true},
		{"count // \u00a0 range('bad'), (\n(n)", true},
	} {
		require.Equal(t, sample.want, matchFuncStart(sample.text, "count"), sample.text)
		require.Equal(t, sample.want, matchFuncStartAndSuffix(sample.text, "count"), sample.text)
		arguments, open := extractFuncArgsLen(sample.text, "count")
		_, _, suffix := extractFuncArgsWithSuffix(sample.text, "count")
		require.Equal(t, sample.want, open >= 0, sample.text)
		require.Equal(t, sample.want, suffix, sample.text)
		if sample.want {
			require.Equal(t, "n", arguments)
		}
		// The general reader keeps Unicode query trivia. Only the legacy
		// matcher's ASCII name-gap compatibility policy is restricted.
		name, inner, parsed := parseFunctionCallWS(sample.text)
		require.True(t, parsed, sample.text)
		require.Equal(t, "count", name)
		require.Equal(t, "n", strings.TrimSpace(inner))
	}
}

func TestFunctionSyntaxUnicodeCallRoutes(t *testing.T) {
	runFunctionSyntaxRoutes(t, []functionsyntaxmatrix.Case{
		{ID: "unicode-upper", Query: "RETURN toUpper\u00a0('ok') AS v", Rows: `[["OK"]]`},
		{ID: "unicode-count", Query: "UNWIND [1,2] AS n RETURN count\u00a0(n) AS v", Rows: `[[2]]`},
		{ID: "unicode-coalesce", Query: "RETURN coalesce\u00a0(null,7) AS v", Rows: `[[7]]`},
	})
}
