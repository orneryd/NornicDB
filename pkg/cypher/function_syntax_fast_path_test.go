package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var fastPathExtraInputs = []string{
	"RETURN a / b, a/b/c, a //c\n/ d, x /* c */ / y, 'a//b' + \"/*\" + `x/*y`",
	"RETURN {k:count(x), `q`/* c */ : sum(y), l: [..count(1)]}, xs[..size(ys)], xs[1..count(z)], (n):count(1)",
	"RETURN a\f(1), b\v(2), c (3), d . e(4), f/* ( */(5), g//\n(6), h\u0085(7), i\x01(8), j\x7f(9)",
	"RETURN ñame(1), na me(2), `a b`(3), $p(4), a$b(5), 1abc(6), x.y.z(7), x..y(8)",
	"CALL db.labels() YIELD x USE g CALL apoc.x.y USE `g h`.c RETURN call(1), use(2), CALLx(3)",
	" \t\n\r\f\v   /* c */ // d\n x",
}

func fastPathInputs() []string {
	return append(append([]string{}, fastPathExtraInputs...), callCursorTestInputs()...)
}

// callableIdentifierTokenReference is the two-pass reader the one-pass
// callableIdentifierToken replaced.
func callableIdentifierTokenReference(text string, start int) (string, int, bool) {
	name, end, ok := scanIdentifierToken(text, start)
	if !ok || isCypherBacktickByte(text[start]) {
		return name, end, ok
	}
	for index := start; index < end; index++ {
		if text[index] >= 0x80 && queryWhitespaceAt(text, index) > 0 {
			return text[start:index], index, index > start
		}
	}
	return name, end, true
}

// The byte-level shortcuts settle only what the full readers would have
// settled the same way.
func TestFunctionSyntaxFastPathsMatchFullReaders(t *testing.T) {
	for _, input := range fastPathInputs() {
		// Every offset of the text itself; padding would only repeat the
		// trailing gap at each of its offsets.
		text := input
		for at := 0; at <= len(text); at++ {
			want, _ := readQueryGap(text, at)
			if got := queryGapEnd(text, at); got != want {
				t.Fatalf("queryGapEnd %d: %d, want %d: %q", at, got, want, input)
			}
			if at == len(text) {
				break
			}
			name, end, ok := callableIdentifierTokenReference(text, at)
			gotName, gotEnd, gotOK := callableIdentifierToken(text, at)
			if name != gotName || end != gotEnd || ok != gotOK {
				t.Fatalf("callableIdentifierToken %d: %q %d %v, want %q %d %v: %q", at, gotName, gotEnd, gotOK, name, end, ok, input)
			}
		}
		for _, text := range []string{input, paddedReaderInput(input)} {
			require.Equal(t, uncachedFunctionSyntax(text).render(syntaxSpan{0, len(text)}), lexicalExpressionText(text), "render: %q", input)
		}
	}
}

// Retained token group context agrees with an independent stack of opening
// offsets, including comments, unmatched closes and long expressions.
func TestFunctionSyntaxLazyNestingMatchesEager(t *testing.T) {
	for _, input := range fastPathInputs() {
		text := paddedReaderInput(input)
		facts := readExpressionLexicalFacts(text)
		var opens []int
		for _, token := range facts.tokens {
			if token.kind == lexicalName {
				want := -1
				if len(opens) > 0 {
					want = opens[len(opens)-1]
				}
				require.Equal(t, want, facts.groups[token.group].open, input)
			}
			if token.kind != lexicalPunctuation {
				continue
			}
			switch c := text[token.start]; c {
			case '(', '[', '{':
				opens = append(opens, token.start)
			case ')', ']', '}':
				if len(opens) > 0 {
					open := opens[len(opens)-1]
					if text[open] == '(' && c == ')' || text[open] == '[' && c == ']' || text[open] == '{' && c == '}' {
						opens = opens[:len(opens)-1]
					}
				}
			}
		}
	}
}
