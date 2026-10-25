package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var statementReaderExtraInputs = []string{
	"count(n)", "COUNT (n)", "count (n) + 1", "toLower(count (n))", "count (n)", "count /* c */ (n)", " count(n) ",
	"`count`(n)", "ſum(x)", "sum(x)[0]", "collect({a: 1})[..10]", "sum(n.val) + 1", "count(", "count(*)", "count()",
	"labels(n, )", "f(a,,b)", "f(/* only */)", "f( , )", "apoc.coll.sort(/* , */ collect(x))", "custom . fn (1,2)",
	"sum(0) + COUNT { RETURN max(1) AS n } + sum(2)", "COUNT { RETURN COUNT { RETURN max(1) AS n } AS n } + avg(2)",
	"COUNT { RETURN max(1) AS n } + COLLECT { RETURN min(2) AS n } + sum(3)", "COUNT { RETURN max(1)",
	"count(COUNT { RETURN max(1) AS n })", "sum(count(1)", "count(1] + sum(2)", "x IS :: count(1), y IS TYPED sum(2), z + // TYPED\nmax(3)",
	"CALL db.labels() YIELD x", "CALL db . labels YIELD x", "CALL `my`.proc(1, 'a,b') YIELD x RETURN x", "MATCH (n) CALL { RETURN 1 } RETURN n",
	"'a,b', [1,2], {x:1,y:2}, /* ' , */", "a, /* , */ b,", ",", "",
}

func statementReaderInputs() []string {
	return append(append([]string{}, statementReaderExtraInputs...), callCursorTestInputs()...)
}

// The references below are the facts-based readers the token readers
// replaced, kept to compare with.

func standaloneCallReference(text string) (functionCallSpan, int, bool) {
	if !strings.ContainsRune(text, '(') {
		return functionCallSpan{}, -1, false
	}
	syntax := uncachedFunctionSyntax(text)
	index := syntax.callIndex(syntax.leading)
	if index >= len(syntax.calls) {
		return functionCallSpan{}, -1, false
	}
	call := syntax.calls[index]
	if call.procedure || call.start != syntax.leading || call.close(text) < 0 || queryGapEnd(text, call.close(text)+1) != len(text) {
		return functionCallSpan{}, -1, false
	}
	return call, call.close(text), true
}

func aggregateSpansReference(expr string) []aggregateSpan {
	syntax := uncachedFunctionSyntax(expr)
	var spans []aggregateSpan
	for index := 0; index < len(expr); {
		call, ok := syntax.nextCall(index, false, false, false)
		if !ok {
			break
		}
		index = call.open + 1
		if !lexicalAggregateName(call.name) {
			continue
		}
		close := call.close(expr)
		if close < 0 {
			continue
		}
		spans = append(spans, aggregateSpan{start: call.start, end: close + 1})
		index = close + 1
	}
	return spans
}

func TestFunctionSyntaxStatementReadersMatchFacts(t *testing.T) {
	for _, input := range statementReaderInputs() {
		for _, text := range []string{input, paddedReaderInput(input)} {
			syntax := uncachedFunctionSyntax(text)
			if len(text) < 1024 {
				for at := 0; at < len(text); at++ {
					want, wanted := syntax.callAt(at)
					got, called := lexicalCallAt(text, at)
					if wanted != called || wanted && cursorCallOf(want) != cursorCallOf(got) {
						t.Fatalf("lexicalCallAt %d: %q", at, input)
					}
				}
			}
			want, wantClose, wanted := standaloneCallReference(text)
			got, gotClose, called := lexicalStandaloneCall(text)
			require.Equal(t, wanted, called, "standalone: %q", input)
			if wanted {
				require.Equal(t, cursorCallOf(want), cursorCallOf(got), "standalone: %q", input)
				require.Equal(t, wantClose, gotClose, "standalone close: %q", input)
				require.Equal(t, want.asciiNameSpacing(text), got.asciiNameSpacing(text), "spacing: %q", input)
				name, inner, ok := parseFunctionCallWS(text)
				require.Equal(t, []any{want.name, syntax.source(syntaxSpan{want.open + 1, wantClose}), true}, []any{name, inner, ok}, "parse: %q", input)
				name, arguments, ok := parseFunctionCallArguments(text)
				require.Equal(t, []any{want.name, want.arguments(nil), true}, []any{name, arguments, ok}, "arguments: %q", input)
				single := ""
				if spans := syntax.argumentSpans(want.group); len(spans) == 1 {
					single = syntax.render(spans[0])
				}
				require.Equal(t, single, singleFunctionArgument(text), "single: %q", input)
				for _, name := range []string{"count", "sum", "COUNT", "custom.fn", "apoc.coll.sort", "sumx", "su"} {
					wantMatch := want.asciiNameSpacing(text) && strings.EqualFold(want.name, name)
					require.Equal(t, wantMatch, isFunctionCallWS(text, name), "isFunctionCallWS %s: %q", name, input)
					if !lexicalLeadingNameMayBe(text, name) {
						require.False(t, strings.EqualFold(want.name, name), "leading name %s settled wrongly: %q", name, input)
					}
				}
			} else {
				_, _, ok := parseFunctionCallWS(text)
				require.False(t, ok, "parse: %q", input)
				require.Empty(t, singleFunctionArgument(text), "single: %q", input)
			}
			require.Equal(t, aggregateSpansReference(text), findAggregateSpans(text), "aggregate spans: %q", input)
			require.Equal(t, uncachedFunctionSyntax(text).source(syntaxSpan{0, len(text)}), lexicalSourceText(text), "source: %q", input)
			for _, keepEmpty := range []bool{false, true} {
				facts := &functionSyntax{text: text, leading: lexicalLeadingTriviaEnd(text)}
				scanFunctionSyntax(facts, true)
				want := facts.appendArguments(nil, 0, keepEmpty)
				if !strings.ContainsRune(text, ',') && !strings.Contains(text, "/*") && !strings.Contains(text, "//") {
					want = nil
					if value := strings.TrimSpace(text); value != "" {
						want = []string{value}
					}
				}
				require.Equal(t, want, appendFunctionArguments(nil, text, keepEmpty), "split %v: %q", keepEmpty, input)
			}
			var procedure functionCallSpan
			found := false
			for _, call := range syntax.calls {
				if call.procedure {
					procedure, found = call, true
					break
				}
			}
			got, ok := firstProcedureCall(text)
			require.Equal(t, found, ok, "procedure: %q", input)
			if found {
				require.Equal(t, cursorCallOf(procedure), cursorCallOf(got), "procedure: %q", input)
				name, named := lexicalProcedureNameAt(text, 0)
				wantName, wantNamed := "", false
				for _, call := range syntax.calls {
					if call.start == queryGapEnd(text, 0) {
						wantName, wantNamed = call.name, true
						break
					}
				}
				if !wantNamed {
					for _, call := range uncachedFunctionSyntax("CALL " + text).calls {
						if call.procedure && call.start == len("CALL ")+queryGapEnd(text, 0) {
							wantName, wantNamed = call.name, true
							break
						}
					}
				}
				require.Equal(t, []any{wantName, wantNamed}, []any{name, named}, "procedure name: %q", input)
				require.True(t, containsProcedureCallNamed(text, procedure.name), "procedure named: %q", input)
			}
			for _, call := range syntax.calls {
				if call.procedure {
					continue
				}
				want := !call.annotation && !call.syntaxKeyword
				require.Equal(t, want, lexicalFunctionCall(text, &call), "function call %q: %q", call.name, input)
			}
		}
	}
}
