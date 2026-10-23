package cyphertext

import (
	"reflect"
	"testing"
)

func TestReaderTriviaAndOpaqueTokens(t *testing.T) {
	text := " \t\f\v\u2003/* ' ` CALL { */ CALL // () {\r\n ( ) { `ca``ll` : '/* : } */', \"a\\\"b\" }"
	reader := NewReader(text, 0)
	var got []string
	var kinds []TokenKind
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		got = append(got, token.Text(text))
		kinds = append(kinds, token.Kind)
	}
	want := []string{"CALL", "(", ")", "{", "`ca``ll`", ":", "'/* : } */'", ",", "\"a\\\"b\"", "}"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens=%q want=%q", got, want)
	}
	if kinds[0] != Word || kinds[4] != QuotedName || kinds[6] != String || kinds[8] != String {
		t.Fatalf("kinds=%v", kinds)
	}
	reader.Seek(0)
	first, _ := reader.Next()
	lookahead := reader
	next, _ := lookahead.Next()
	actual, _ := reader.Next()
	if !first.Is(text, "CALL") || next != actual {
		t.Fatal("lookahead changed reader progress")
	}
	for _, source := range []string{"", " /* only */ // end", "/* unterminated"} {
		reader := NewReader(source, 0)
		if _, ok := reader.Next(); ok {
			t.Fatalf("unexpected token in %q", source)
		}
	}
	for _, source := range []string{"'unterminated", "`unterminated", "\"unterminated"} {
		reader := NewReader(source, 0)
		token, ok := reader.Next()
		if !ok || token.End != len(source) {
			t.Fatalf("unterminated token=%v", token)
		}
	}
	if WhitespaceAt("x", -1) != 0 || WhitespaceAt("x", 1) != 0 || WhitespaceAt("é", 0) != 0 || WhitespaceAt("x", 0) != 0 {
		t.Fatal("nonspace classified as whitespace")
	}
}

func TestReaderCommaSegmentsAndTriviaBounds(t *testing.T) {
	for _, test := range []struct {
		text string
		want []string
	}{
		{"", nil}, {" /* empty */ ", nil}, {",", []string{"", ""}},
		{"a: [1,2], b: {k: ','}, c: f(1,2),", []string{"a: [1,2]", " b: {k: ','}", " c: f(1,2)", ""}},
		{"`a,b`: 1 // ,\n, 'x,y': 2", []string{"`a,b`: 1 // ,\n", " 'x,y': 2"}},
	} {
		segments := CommaSegments(test.text)
		var got []string
		for part, ok := segments.Next(); ok; part, ok = segments.Next() {
			got = append(got, part)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%q segments=%q want=%q", test.text, got, test.want)
		}
		if _, ok := segments.Next(); ok {
			t.Fatal("exhausted segments yielded another entry")
		}
	}
	for _, test := range []struct{ text, want string }{
		{" // only", ""}, {" /* before */ a /* middle */ : '/* data */' // after", "a /* middle */ : '/* data */'"}, {"é", "é"},
	} {
		if got := TrimTrivia(test.text); got != test.want {
			t.Fatalf("trim=%q want=%q", got, test.want)
		}
	}
}
