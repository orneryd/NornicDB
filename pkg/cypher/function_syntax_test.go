package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionSyntaxStructure(t *testing.T) {
	text := `substring(/* don't split , ) */ toLower('OKAY'), {k:range(1,2), ` + "`a,b`" + `: [1,2]}, 2 /* , ' */)`
	syntax := readFunctionSyntax(text)
	require.Len(t, syntax.calls, 3)
	call, ok := functionCallAt(text, 0)
	require.True(t, ok)
	require.Equal(t, len(text)-1, call.close(text))
	require.Equal(t, []string{`toLower('OKAY')`, "{k:range(1,2), `a,b`: [1,2]}", "2"}, call.arguments(nil))
	require.Equal(t, []string{"'a,b'", "[1,2]", "{x:1,y:2}", ""}, readFunctionSyntax("'a,b', [1,2], {x:1,y:2}, /* ' , */").appendArguments(nil, 0, true))
	require.Equal(t, []string{"'a,b'", "[1,2]", "{x:1,y:2}"}, readFunctionSyntax("'a,b', [1,2], {x:1,y:2}, /* ' , */").appendArguments(nil, 0, false))
	require.True(t, hasValidStartKeyword("/* } { ' \" */ RETURN 'ok' AS v"))
}

func TestFunctionSyntaxQualifiedInventory(t *testing.T) {
	text := "RETURN apoc /* ' , */ . coll . sort(collect(x)), datetime.statement(), `custom`.`fn`(1)"
	var names []string
	require.NoError(t, forEachFunctionCall(text, func(name, _ string) error {
		names = append(names, name)
		return nil
	}))
	require.Equal(t, []string{"apoc.coll.sort", "collect", "datetime.statement", "custom.fn"}, names)
	name, arguments, ok := parseFunctionCallArguments("apoc.coll.sort(/* , */ collect(x))")
	require.True(t, ok)
	require.Equal(t, "apoc.coll.sort", name)
	require.Equal(t, []string{"collect(x)"}, arguments)
}

func TestFunctionSyntaxIdentityCase(t *testing.T) {
	text := "CALL Custom /* trivia */ . Proc() RETURN TOUPPER('a'), `Custom`.`Fn`(1)"
	var names, normalized []string
	for _, call := range readFunctionSyntax(text).calls {
		names = append(names, call.name)
		normalized = append(normalized, call.normalized)
	}
	require.Equal(t, []string{"Custom.Proc", "TOUPPER", "Custom.Fn"}, names)
	require.Equal(t, []string{"custom.proc", "toupper", "custom.fn"}, normalized)
}

// Preserve the legacy evaluator's supported binding forms. Plain filter is
// already rejected by public execution on the prerequisite; this checks its
// existing evaluator contract without adding statement grammar support.
func TestFunctionSyntaxLegacyEvaluation(t *testing.T) {
	executor := setupTestExecutor(t)
	for expression, expected := range map[string][]interface{}{
		"filter(/* don't split range('bad'), */ x IN [1,2] WHERE x > 1)": {int64(2)},
		"filter// don't split range('bad'),\n(x IN [1,2] WHERE x > 1)":   {int64(2)},
		"extract(/* don't split range('bad'), */ x IN [1,2] | x + 1)":    {int64(2), int64(3)},
		"extract// don't split range('bad'),\n(x IN [1,2] | x + 1)":      {int64(2), int64(3)},
	} {
		ctx := withExpressionFailureSlot(context.Background())
		result := executor.evaluateExpressionWithContext(ctx, expression, nil, nil)
		require.NoError(t, getExpressionFailure(ctx), expression)
		require.Equal(t, expected, result, expression)
	}
}

func TestFunctionSyntaxParenthesizedOperand(t *testing.T) {
	for _, text := range []string{
		"(head/* don't count ), range('bad') */([null]))",
		"(head// don't count ), range('bad')\n([null]))",
		"(head(/* don't count ), range('bad') */[null]))",
		"(head(// don't count ), range('bad')\n[null]))",
	} {
		inner, enclosed := stripEnclosingExpressionParentheses(text)
		require.True(t, enclosed, text)
		require.NotContains(t, inner, "don't", text)
		require.True(t, hasOuterParens(text), text)
		require.Equal(t, inner, unwrapOuterParens(text))
		_, _, called := parseFunctionCallWS(inner)
		require.True(t, called, inner)
	}
	for _, text := range []string{"(')') + ('(')", "(head([null])", "(// )\nnull"} {
		_, enclosed := stripEnclosingExpressionParentheses(text)
		require.False(t, enclosed, text)
		require.False(t, hasOuterParens(text), text)
	}
}

func TestFunctionSyntaxRootDelimiters(t *testing.T) {
	for _, prefix := range []string{
		"f('a,b') /* don't count , | ) */ ",
		"`a,b` // don't count , | )\n",
		"{k:[1,2]} ",
	} {
		require.Equal(t, len(prefix), findTopLevelByte(prefix+", tail", ','))
		require.Equal(t, len(prefix), rowTopLevelPipeIndex(prefix+"| tail"))
	}
}

func TestFunctionSyntaxGraphConsumers(t *testing.T) {
	text := "CALL db.labels RETURN head /* range('bad'), */ ([null]), (1), 'quote('"
	syntax := readFunctionSyntax(text)
	for index := 1; index < len(syntax.calls); index++ {
		require.LessOrEqual(t, syntax.calls[index-1].next, syntax.calls[index].next)
	}
	for _, call := range syntax.calls {
		if call.procedure {
			continue
		}
		found, ok := functionCallBeforeParen(text, call.open)
		require.True(t, ok)
		require.Equal(t, call.start, found.start)
		group, ok := syntax.groupAt(call.open)
		require.True(t, ok)
		require.Equal(t, call.close(text), group.close)
	}
	comment := strings.Index(text, "/*")
	require.Equal(t, strings.Index(text, "*/")+2, syntax.commentEndAt(comment))
	require.Equal(t, strings.Index(text, "*/")+2, syntax.commentEndAt(comment+3))
	_, called := functionCallBeforeParen(text, strings.Index(text, "('bad')"))
	require.False(t, called)
	_, grouped := syntax.groupAt(strings.Index(text, "('bad')"))
	require.False(t, grouped)
	_, called = functionCallBeforeParen(text, strings.Index(text, "(1)"))
	require.False(t, called)
	require.False(t, mayTestExpressionLabels("UNWIND [1,2] AS x RETURN {c// range('bad')\n:count(x)}"))
	statement := "RETURN /* head('bad') */ (null):A"
	clause, called := functionCallBeforeParen(statement, strings.Index(statement, "(null)"))
	require.True(t, called)
	require.False(t, clause.expressionCall())
	legacy := "filter(x IN [1,2] WHERE x > 1)"
	require.Equal(t, "filter", bindingFunctionBeforeParen(legacy, len("filter")))
	require.Empty(t, expressionFreeVariables(legacy))
}

func TestFunctionSyntaxGraphFactsRetained(t *testing.T) {
	query := "/* prefix */ RETURN [" + strings.Repeat("head([null]):A,", 2000) + "null] AS v"
	require.Greater(t, len(query), 16384)
	rewriter := &labelExpressionRewriter{query: query}
	facts := rewriter.callSyntax()
	require.NoError(t, rewriter.statement(0, len(query)))
	require.Same(t, facts, rewriter.callSyntax())
	require.Len(t, rewriter.edits, 2000)
}

func TestFunctionSyntaxNodePatternBoundary(t *testing.T) {
	executor := &StorageExecutor{}
	require.Empty(t, executor.splitNodePatterns("x"))
	require.Equal(t, []string{"(a {s:') , ('})", "(b)"}, executor.splitNodePatterns("(a {s:') , ('})-[:R]->(b)"))
	require.Equal(t, []string{"(a)", "(b)"}, executor.splitNodePatterns("/* (fake), */ (a), // (fake),\n(b)"))
	require.Equal(t, []string{"(c)"}, executor.splitNodePatterns("$('A'), $any(['B']), (c)"))
	require.Equal(t, []string{"(a"}, executor.splitNodePatterns("(a"))
}

func TestFunctionSyntaxUnclosedArguments(t *testing.T) {
	for _, text := range []string{"range(1,2", "substring('x',0,custom.fn(1,2"} {
		for _, argumentsOnly := range []bool{false, true} {
			syntax := readFunctionSyntaxFacts(text, argumentsOnly)
			for index, group := range syntax.groups {
				if group.close < 0 {
					require.Empty(t, syntax.argumentSpans(index), text)
				}
			}
		}
		_, called := standaloneFunctionCall(text)
		require.False(t, called, text)
	}
}

func TestFunctionSyntaxUncachedWholeTextWalk(t *testing.T) {
	// Exceed the retention bound: the walker must keep its lexical structure
	// rather than reparsing the entire statement for each successive call.
	text := "RETURN " + strings.Repeat("coalesce(/* don't split , */ null, custom.fn(1)),", 512) + "1"
	require.Greater(t, len(text), 16384)
	count := 0
	require.NoError(t, forEachFunctionCall(text, func(name, _ string) error {
		if count%2 == 0 {
			require.Equal(t, "coalesce", name)
		} else {
			require.Equal(t, "custom.fn", name)
		}
		count++
		return nil
	}))
	require.Equal(t, 1024, count)
}

func TestFunctionSyntaxUnicodeTrivia(t *testing.T) {
	for _, gap := range []string{"\u00a0", "\u2003", "/* ' , */\u00a0"} {
		text := "custom" + gap + "." + gap + "fn" + gap + "(1,2)"
		name, arguments, ok := parseFunctionCallArguments(text)
		require.True(t, ok, text)
		require.Equal(t, "custom.fn", name)
		require.Equal(t, []string{"1", "2"}, arguments)
		name, ok = canonicalCallableName("custom" + gap + "." + gap + "fn")
		require.True(t, ok)
		require.Equal(t, "custom.fn", name)
	}
}

func TestFunctionSyntaxArgumentFactsAgree(t *testing.T) {
	for _, text := range []string{
		"",
		"/* don't split , ) */",
		"toLower('a,b'), {k:range(1,2), `a,b`: [1,2]}, 2 /* , ' */",
		"'it\\'s,okay', \"a\\\"b,c\", `a``b,c`, 2",
		"COLLECT { CALL /* ' , */ custom /* ) */ . proc(1,2) RETURN 1 }, 2",
		"CALL custom /* ' , */ . proc(1,2), 3",
		"[1,2], /* ' , */",
		"[1, {x:2,y:3}",
		"1, // don't split , (\n 2",
	} {
		for _, keepEmpty := range []bool{false, true} {
			require.Equal(t,
				readFunctionSyntax(text).appendArguments(nil, 0, keepEmpty),
				readFunctionSyntaxFacts(text, true).appendArguments(nil, 0, keepEmpty),
				"text=%q keepEmpty=%v", text, keepEmpty)
		}
		require.Empty(t, readFunctionSyntaxFacts(text, true).calls)
	}
	// Property names that happen to be keywords do not introduce procedures.
	require.Empty(t, readFunctionSyntax("WITH {call:1} AS n WITH n.call AS v RETURN v").calls)
}
