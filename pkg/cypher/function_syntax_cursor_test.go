package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/functionsyntaxmatrix"
	"github.com/stretchr/testify/require"
)

var callCursorInputs = []string{
	"labels(n) + keys(r) + properties(m)",
	"labels(n,) + labels(,) + labels() + labels(n,,) + labels(n, m)",
	"labels( n /* c */ , /* d */ ) + keys(/* only */ r)",
	"labels(`a b`) + keys(`x``y`) + labels(n m) + labels(n m)",
	"labels(n.x) + labels((n)) + labels('n') + labels(n]) + properties($p) + labels(1)",
	"labels(labels(labels(n))) + keys(n) // keys(x)\n + properties(n",
	"NOT (n)-[:R]->() AND NOT /* note() */ (m)--() AND exists((n)--())",
	"a.b.c(1) + x . y (2) + `my fn`(3) + filter(x IN xs WHERE x > 1)",
	"CALL db.labels() YIELD label RETURN labels(label)",
}

type cursorCall struct {
	name, normalized     string
	start, nameEnd, open int
}

func cursorCallOf(call functionCallSpan) cursorCall {
	return cursorCall{call.name, call.normalized, call.start, call.nameEnd, call.open}
}

func callCursorTestInputs() []string {
	inputs := append(append([]string{unclosedScalarReaderInput}, callCursorInputs...), functionSyntaxReaderInputs...)
	for _, cases := range [][]functionsyntaxmatrix.Case{functionsyntaxmatrix.Cases(), functionsyntaxmatrix.NativeCases()} {
		for _, item := range cases {
			inputs = append(inputs, item.Query)
		}
	}
	return inputs
}

// The cursor replaces per-offset lookups in the retained facts. One forward
// sweep must give the facts' answer at every offset: the expression call
// starting there, the call a parenthesis opens, and the single-name argument
// the deleted-entity reader takes from call.arguments.
func TestFunctionSyntaxCallCursorMatchesFacts(t *testing.T) {
	for _, input := range callCursorTestInputs() {
		text := paddedReaderInput(input)
		syntax := uncachedFunctionSyntax(text)
		cursor := newCallCursor(text)
		for at := 0; at < len(text); at++ {
			want, wanted := syntax.callAt(at)
			wanted = wanted && want.expressionCall()
			got, called := cursor.callAt(at)
			if wanted != called || wanted && cursorCallOf(want) != cursorCallOf(got) {
				t.Fatalf("callAt %d: facts %v %+v, cursor %v %+v: %q", at, wanted, cursorCallOf(want), called, cursorCallOf(got), input)
			}
			if wanted {
				arguments := want.arguments(nil)
				variable := ""
				if len(arguments) == 1 {
					variable = simpleSemanticIdentifier(arguments[0])
				}
				require.Equal(t, variable, singleNameArgument(text, got), "argument %d: %q", at, input)
			}
			if text[at] == '(' {
				want, wanted := syntax.callBeforeParen(at)
				got, called := cursor.callOpenedAt(at)
				if wanted != called || wanted && cursorCallOf(want) != cursorCallOf(got) {
					t.Fatalf("callOpenedAt %d: facts %v %+v, cursor %v %+v: %q", at, wanted, cursorCallOf(want), called, cursorCallOf(got), input)
				}
			}
		}
	}
}

// A query behind the previous one restarts the walk instead of missing a
// call.
func TestFunctionSyntaxCallCursorRestarts(t *testing.T) {
	for _, input := range callCursorTestInputs() {
		syntax := uncachedFunctionSyntax(input)
		cursor := newCallCursor(input)
		for at := len(input) - 1; at >= 0; at-- {
			want, wanted := syntax.callAt(at)
			if _, called := cursor.callAt(at); called != (wanted && want.expressionCall()) {
				t.Fatalf("backward callAt %d: %q", at, input)
			}
		}
	}
}

// The reference walk over a long expression keeps no facts and allocates
// nothing of its own.
func TestFunctionSyntaxCallCursorDoesNotAllocate(t *testing.T) {
	text := paddedReaderInput("labels(n) + " + callCursorInputs[0])
	require.Zero(t, testing.AllocsPerRun(10, func() {
		cursor := newCallCursor(text)
		for at := 0; at < len(text); at++ {
			cursor.callAt(at)
		}
	}))
}

// deletedEntityReadsReference is the byte walk deletedEntityReadsIn replaced,
// over the full facts: a call lookup at every offset it visits.
func deletedEntityReadsReference(expression string) []deletedEntityRead {
	var reads []deletedEntityRead
	syntax := uncachedFunctionSyntax(expression)
	for index := 0; index < len(expression); {
		if call, ok := syntax.callAt(index); ok && call.expressionCall() {
			name := call.normalized
			if name == "labels" || name == "keys" || name == "properties" {
				arguments := call.arguments(nil)
				if len(arguments) == 1 {
					if variable := simpleSemanticIdentifier(arguments[0]); variable != "" {
						reads = append(reads, deletedEntityRead{variable: variable, relationshipOnly: name != "labels"})
					}
				}
			}
		}
		if end := queryCommentEnd(expression, index); end >= 0 {
			index = end
			continue
		}
		if end, quoted := skipCypherLexicalToken(expression, index); quoted {
			index = end
			continue
		}
		if character := expression[index]; !isIdentByte(character) || index > 0 && (isIdentByte(expression[index-1]) || expression[index-1] == '.') {
			index++
			continue
		}
		start := index
		for index < len(expression) && isIdentByte(expression[index]) {
			index++
		}
		next := queryGapEnd(expression, index)
		if next < len(expression) && expression[next] == '.' && next+1 < len(expression) && expression[next+1] != '.' && !propertyNullTestFollows(expression, next+1) {
			reads = append(reads, deletedEntityRead{variable: expression[start:index]})
		}
	}
	return reads
}

func TestFunctionSyntaxDeletedReadsMatchFacts(t *testing.T) {
	inputs := append(callCursorTestInputs(),
		"n\u00a0m.x + \u00a0.y + \u00a0n.z + 1abc.x + 0.5 + $p.x + `a`.x + `a`b.x + 's'.x",
		"n.x IS NULL + m . y + labels(n)\u00a0+ keys(\u00a0n\u00a0) + n..m",
		"labels(n) + `unclosed.x + m.y",
		"'unclosed + n.x",
		"/* unclosed n.x",
	)
	for _, input := range inputs {
		for _, text := range []string{input, paddedReaderInput(input)} {
			require.Equal(t, deletedEntityReadsReference(text), deletedEntityReadsIn(text), "%q", input)
		}
	}
}

// The call-span readers list what the retained facts list, in order.
func TestFunctionSyntaxCallSpansMatchFacts(t *testing.T) {
	type span struct {
		cursorCall
		procedure bool
	}
	for _, input := range callCursorTestInputs() {
		text := paddedReaderInput(input)
		syntax := uncachedFunctionSyntax(text)
		var want, got, wantFunctions, gotFunctions []span
		for _, call := range syntax.calls {
			want = append(want, span{cursorCallOf(call), call.procedure})
		}
		for index := 0; index < len(text); {
			call, ok := syntax.nextCall(index, true, false, false)
			if !ok {
				break
			}
			wantFunctions = append(wantFunctions, span{cursorCallOf(call), false})
			index = call.open + 1
		}
		lexicalCallSpans(text, func(call functionCallSpan) bool {
			got = append(got, span{cursorCallOf(call), call.procedure})
			return true
		})
		lexicalFunctionCallSpans(text, func(call functionCallSpan) bool {
			gotFunctions = append(gotFunctions, span{cursorCallOf(call), call.procedure})
			return true
		})
		require.Equal(t, want, got, "calls: %q", input)
		require.Equal(t, wantFunctions, gotFunctions, "function calls: %q", input)
	}
}

func mayUsePatternPredicateReference(query string) bool {
	syntax := uncachedFunctionSyntax(query)
	var openers []int
	for i := 0; i < len(query); i++ {
		if query[i] == '/' {
			if next := syntax.commentEndAt(i); next > i {
				i = next - 1
				continue
			}
		}
		switch c := query[i]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(query, i, c) - 1
		case '(', '[', '{':
			openers = append(openers, i)
		case ')', ']', '}':
			if len(openers) > 0 {
				openers = openers[:len(openers)-1]
			}
		case 'W', 'w':
			if len(openers) == 0 || !matchKeywordAt(query, i, "WHERE") {
				continue
			}
			open := openers[len(openers)-1]
			call, called := syntax.callBeforeParen(open)
			if query[open] == '{' || called && call.expressionCall() {
				continue
			}
			if topLevelKeywordIndex(query[open+1:i], "IN") >= 0 {
				continue
			}
			return true
		}
	}
	return false
}

// The label rewrite's quick checks answer what they answered over facts.
func TestFunctionSyntaxRewriteChecksMatchFacts(t *testing.T) {
	inputs := append(callCursorTestInputs(),
		"MATCH (n WHERE n.x > 1) RETURN n", "RETURN all(x IN l WHERE x > 1)", "RETURN [x IN l WHERE x > 1]",
		"MATCH (n) RETURN count { (n) WHERE n.x }", "RETURN f /* c */ (x WHERE y)", "RETURN (x WHERE y)",
		"RETURN f(x):A, (y) : B, g(z)::INTEGER", "RETURN {c// range('bad')\n:count(x)}",
		"RETURN vector([1], 2, FLOAT32), `VECTOR_norm`(v, EUCLIDEAN), x.vector(1), vectors(1)",
		"CALL vector_distance YIELD x RETURN x",
	)
	for _, input := range inputs {
		for _, text := range []string{input, paddedReaderInput(input)} {
			syntax := uncachedFunctionSyntax(text)
			labels := false
			for _, group := range syntax.groups {
				if group.open < 0 || text[group.open] != '(' || group.close < 0 {
					continue
				}
				j := queryGapEnd(text, group.close+1)
				labels = labels || j < len(text) && text[j] == ':' && (j+1 >= len(text) || text[j+1] != ':')
			}
			vector := false
			for _, call := range syntax.calls {
				vector = vector || call.normalized == "vector" || call.normalized == "vector_distance" || call.normalized == "vector_norm"
			}
			require.Equal(t, labels, mayTestExpressionLabels(text), "labels: %q", input)
			require.Equal(t, vector, mayUseVectorCall(text), "vector: %q", input)
			require.Equal(t, mayUsePatternPredicateReference(text), mayUsePatternPredicate(text), "pattern predicate: %q", input)
		}
	}
}
