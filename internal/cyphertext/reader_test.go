package cyphertext

import (
	"strings"
	"testing"
)

func readerTokens(text string) []string {
	var tokens []string
	reader := NewReader(text, 0)
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		tokens = append(tokens, token.Text(text))
	}
	return tokens
}
func TestReaderTokenBoundaries(t *testing.T) {
	for _, test := range []struct {
		text string
		want []string
	}{
		{"RETURN {a$b:1} AS out", []string{"RETURN", "{", "a$b", ":", "1", "}", "AS", "out"}},
		{"RETURN $a$b, $`x y`, $0", []string{"RETURN", "$a$b", ",", "$`x y`", ",", "$0"}},
		{"CALL /* { } */ () // }\n{ RETURN 1 }", []string{"CALL", "(", ")", "{", "RETURN", "1", "}"}},
		{"RETURN '{a}' AS `x}`, \"y\\\"}\"", []string{"RETURN", "'{a}'", "AS", "`x}`", ",", "\"y\\\"}\""}},
		{"RETURN {`a``b`:1}", []string{"RETURN", "{", "`a``b`", ":", "1", "}"}},
		{"RETURN é$中 AS　x", []string{"RETURN", "é$中", "AS", "x"}},
		{"n{.a,.*}", []string{"n", "{", ".", "a", ",", ".", "*", "}"}},
		{"RETURN 'open", []string{"RETURN", "'open"}},
		{"RETURN 1 /* open", []string{"RETURN", "1"}},
	} {
		if got := readerTokens(test.text); strings.Join(got, "|") != strings.Join(test.want, "|") {
			t.Errorf("%q: tokens %q, want %q", test.text, got, test.want)
		}
	}
}

func TestReaderTokenKinds(t *testing.T) {
	text := "w 'S' `Q` $p ;"
	reader := NewReader(text, 0)
	for _, want := range []TokenKind{Word, String, QuotedName, Parameter, Symbol} {
		token, ok := reader.Next()
		if !ok || token.Kind != want {
			t.Fatalf("token %+v ok=%v, want kind %d", token, ok, want)
		}
	}
	if _, ok := reader.Next(); ok {
		t.Fatal("tokens after the end")
	}
}
