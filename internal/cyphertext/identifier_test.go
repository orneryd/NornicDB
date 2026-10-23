package cyphertext

import "testing"

func TestCanonicalIdentifierAndParameterTokens(t *testing.T) {
	for _, text := range []string{"a$b", "a$b$c", "a$", "é$中", "a€b", "`a$b`", "`ca``ll`", "name_42"} {
		end, valid := IdentifierEnd(text, 0)
		if !valid || end != len(text) {
			t.Fatalf("name %q end=%d valid=%v", text, end, valid)
		}
		reader := NewReader(text, 0)
		span, ok := reader.Next()
		if !ok || span.Start != 0 || span.End != end || span.Kind != Word && span.Kind != QuotedName {
			t.Fatalf("name %q token=%+v", text, span)
		}
		if _, more := reader.Next(); more {
			t.Fatalf("name %q split into multiple tokens", text)
		}
		if _, decodedEnd, ok := ScanIdentifier(text, 0); !ok || decodedEnd != end {
			t.Fatalf("decoder disagrees on %q", text)
		}
	}
	for _, text := range []string{"$name", "$0", "$`a b`", "$foo$bar"} {
		reader := NewReader(text, 0)
		span, ok := reader.Next()
		if !ok || span.Kind != Parameter || span.Start != 0 || span.End != len(text) {
			t.Fatalf("parameter %q token=%+v", text, span)
		}
	}
	// Check the entire ASCII continuation alphabet against the name owner.
	for c := byte(0); c < 128; c++ {
		text := "a" + string(c) + "z"
		end, _ := IdentifierEnd(text, 0)
		reader := NewReader(text, 0)
		span, _ := reader.Next()
		if span.End != end {
			t.Fatalf("ASCII %d reader end=%d name end=%d", c, span.End, end)
		}
	}
}

func TestOwnedRawSpansAndLexicalValidity(t *testing.T) {
	text := "a /* comment */ $p `quoted``name` 's'"
	reader := NewReader(text, 0)
	comments, whitespace, parameters := 0, 0, 0
	for span, ok := reader.NextRaw(); ok; span, ok = reader.NextRaw() {
		switch span.Kind {
		case Comment:
			comments++
		case Whitespace:
			whitespace++
		case Parameter:
			parameters++
		}
		if span.Unterminated {
			t.Fatalf("closed source marked unclosed: %+v", span)
		}
	}
	if comments != 1 || whitespace != 4 || parameters != 1 {
		t.Fatalf("raw counts comment=%d whitespace=%d parameter=%d", comments, whitespace, parameters)
	}
	if _, bad := reader.UnterminatedAt(); bad {
		t.Fatal("closed source has a lexical error")
	}
	for _, source := range []string{"/* unclosed", "'unclosed", "`unclosed"} {
		reader := NewReader(source, 0)
		for _, ok := reader.Next(); ok; _, ok = reader.Next() {
		}
		if at, bad := reader.UnterminatedAt(); !bad || at != 0 {
			t.Fatalf("unclosed %q at=%d bad=%v", source, at, bad)
		}
	}
	for _, source := range []string{"'MATCH'", "`MATCH`", "/* MATCH */"} {
		reader := NewReaderPolicy(source, 0, LexicalPolicy{})
		found := false
		for span, ok := reader.Next(); ok; span, ok = reader.Next() {
			found = found || span.Kind == Word && span.Text(source) == "MATCH"
		}
		if !found {
			t.Fatalf("configured visibility lost %q", source)
		}
	}
	if IsWhitespaceRune('\u0085') {
		t.Fatal("NEL is not Cypher trivia")
	}
}
