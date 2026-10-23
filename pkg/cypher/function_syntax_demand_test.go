package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/functionsyntaxmatrix"
	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxRewriteDemand(t *testing.T) {
	for _, sample := range []struct {
		text string
		want functionRewriteKind
	}{
		{"RETURN vector([1,2],2,INTEGER)", functionRewriteVector},
		{"RETURN `vector` /* note */ ([1,2],2,INTEGER)", functionRewriteVector},
		{"RETURN custom.vector([1,2],2,INTEGER)", 0},
		{"MATCH (n WHERE n.k=1) RETURN n", functionRewritePattern},
		{"MATCH (n) WHERE n.k=1 RETURN n", 0},
		{"RETURN [x IN [1,2] WHERE x>0 | x]", 0},
		{"RETURN all(x IN [1,2] WHERE x>0)", 0},
		{"RETURN endNode(r) /* note */ :Def", functionRewriteLabel},
		{"RETURN endNode(r)::INTEGER", 0},
		{"RETURN {v:count(1)}", 0},
		{"RETURN 'vector(1), (n WHERE n.k=1), f():L'", 0},
		{"RETURN coalesce(/* vector(1), WHERE, ):L */ null,1)", 0},
	} {
		for _, prefix := range []string{"", "/* " + strings.Repeat("padding ", 2400) + " */ "} {
			text := prefix + sample.text
			require.Equal(t, sample.want != 0, lexicalMayRewrite(text, functionRewriteAll), text)
			syntax := readFunctionSyntax(text)
			require.Equal(t, sample.want, syntax.rewrite, text)
			for _, kind := range []functionRewriteKind{functionRewriteVector, functionRewritePattern, functionRewriteLabel} {
				require.Equal(t, sample.want&kind != 0, lexicalMayRewrite(text, kind), "%d: %s", kind, text)
			}
		}
	}
}

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
			for _, kind := range []functionRewriteKind{functionRewriteVector, functionRewritePattern, functionRewriteLabel, functionRewriteAll} {
				require.Equal(t, syntax.rewrite&kind != 0, lexicalMayRewrite(text, kind), "rewrite %d: %s", kind, text)
			}
			type invocation struct {
				name, normalized string
				open             int
			}
			var wantCalls, gotCalls []invocation
			for at := 0; at < len(text); {
				call, ok := syntax.nextCall(at, true, false, false)
				if !ok {
					break
				}
				wantCalls = append(wantCalls, invocation{call.name, call.normalized, call.open})
				at = call.open + 1
			}
			err := lexicalFunctionInvocations(text, func(name, normalized string, open int) error {
				gotCalls = append(gotCalls, invocation{name, normalized, open})
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, wantCalls, gotCalls, "invocations %d: %s", index, text)
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
	view := functionSyntaxView{text: expression}
	first := view.facts()
	require.Same(t, first, view.facts())
	_, retained := functionSyntaxCache.get(functionSyntaxKey{expression, false})
	require.False(t, retained, "large facts must stay local to the walk")
	require.Equal(t, []deletedEntityRead{{variable: "n"}, {variable: "r", relationshipOnly: true}, {variable: "n"}}, deletedEntityReadsIn(expression))
	plain := functionSyntaxView{text: "n.name + 1"}
	_, called := plain.callAt(0)
	require.False(t, called)
	require.Nil(t, plain.syntax, "a call-free reference walk needs no full facts")
}

func TestFunctionSyntaxSparseRetention(t *testing.T) {
	for _, kind := range []string{"block", "subqueries"} {
		syntax := functionSyntax{text: functionFactConsumerInput(4000, kind)}
		request := functionSyntaxRequest{aggregate: true, activeOpen: -1}
		if kind == "block" {
			request.aggregate, request.functionName = false, "coalesce"
		}
		scanFunctionSyntax(&syntax, false, &request, nil)
		require.Empty(t, syntax.calls, "a sparse query must not retain a call inventory")
		require.Empty(t, syntax.arguments)
		require.Empty(t, syntax.quoted)
		require.Empty(t, syntax.rootPunctuation)
		require.Empty(t, syntax.comments)
		require.Empty(t, syntax.groups, "transient nesting must not escape into retained facts")
		if kind == "block" {
			require.Len(t, request.arguments, 4000)
		} else {
			require.False(t, request.found)
		}
	}
	text := "count(1) + COUNT { RETURN " + strings.Repeat("coalesce(null,1) + ", 4000)
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{aggregate: true, activeOpen: -1}
	scanFunctionSyntax(&syntax, false, &request, nil)
	require.True(t, request.found)
	require.False(t, request.unclosedScalar, "the boolean request stops before the unclosed trailing body")
	require.Empty(t, syntax.groups)
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
		view := functionSyntaxView{text: text}
		_, call := view.callAt(0)
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
