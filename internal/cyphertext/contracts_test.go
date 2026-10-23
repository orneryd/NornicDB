package cyphertext

import (
	"reflect"
	"strings"
	"testing"
)

func TestUnicodeEscapesRespectOwnedStringSpans(t *testing.T) {
	for _, source := range []string{
		`RETURN 'plain'`, `RETURN '\u0041'`, `RETURN '\U0001F600'`,
		`RETURN '\n\t'`, `RETURN '\\u12G4'`, "RETURN `\\uZZZZ`",
		`RETURN 1 /* '\uZZZZ' */`, "RETURN 1 // '\\uZZZZ'\n",
		"RETURN '\\", // unfinished quotation is a separate lexical fact
	} {
		if at, invalid := InvalidUnicodeEscape(source); invalid {
			t.Errorf("valid string data %q rejected at %d", source, at)
		}
	}
	for _, source := range []string{`RETURN '\uZZZZ'`, `RETURN '\u123'`, `RETURN '\uD800'`, `RETURN '\U00110000'`, `RETURN '\UFFFFFFFF'`} {
		if at, invalid := InvalidUnicodeEscape(source); !invalid || at != 8 {
			t.Errorf("invalid escape %q at=%d invalid=%v", source, at, invalid)
		}
	}
}

func TestOwnedLiteralSegmentsAndEscapeParity(t *testing.T) {
	for _, test := range []struct {
		source, want string
		valid        bool
	}{
		{"plain", "plain", true}, {`\{a\}`, "{a}", true}, {`line\n`, `line\n`, true},
		{`slash\\`, `slash\\`, true}, {`trailing\`, `trailing\`, true}, {"unescaped}", "", false},
	} {
		if got, valid := InterpolationLiteral(test.source); got != test.want || valid != test.valid {
			t.Errorf("segment %q=(%q,%v), want(%q,%v)", test.source, got, valid, test.want, test.valid)
		}
	}
	for _, test := range []struct {
		source  string
		escaped bool
	}{{"", false}, {"a", false}, {`\`, true}, {`\\`, false}, {`\\\`, true}, {`x\\\\`, false}} {
		if got := BackslashEscaped(test.source, len(test.source)); got != test.escaped {
			t.Errorf("escape parity %q=%v", test.source, got)
		}
	}
	for _, test := range []struct {
		source string
		whole  bool
	}{{"", false}, {"'", false}, {"`name`", false}, {"''", true}, {`'a\'b'`, true}, {`"a"`, true}, {`'a' + 'b'`, false}, {"'unfinished", false}} {
		if got := IsWholeString(test.source); got != test.whole {
			t.Errorf("whole string %q=%v", test.source, got)
		}
	}
}

func TestIdentifierGrammarAndSpanBoundaries(t *testing.T) {
	for _, test := range []struct {
		r           rune
		start, part bool
	}{
		{'a', true, true}, {'中', true, true}, {'_', true, true}, {'Ⅲ', true, true},
		{'9', false, true}, {'$', false, true}, {'€', false, true}, {'\u0301', false, true},
		{'\u200c', false, true}, {'\u0085', false, true}, {'!', false, false}, {' ', false, false},
	} {
		if IdentifierStartRune(test.r) != test.start || IdentifierPartRune(test.r) != test.part {
			t.Errorf("name character U+%04X start=%v part=%v", test.r, IdentifierStartRune(test.r), IdentifierPartRune(test.r))
		}
	}
	for _, test := range []struct {
		source string
		at     int
	}{{"", 0}, {"a", -1}, {"a", 1}, {"$a", 0}, {" a", 0}, {"\u2003a", 0}, {"`open", 0}} {
		if _, valid := IdentifierEnd(test.source, test.at); valid {
			t.Errorf("invalid name boundary %q at %d accepted", test.source, test.at)
		}
		if _, _, valid := ScanIdentifier(test.source, test.at); valid {
			t.Errorf("invalid decoded name boundary %q at %d accepted", test.source, test.at)
		}
	}
	if end, valid := IdentifierEnd("a\u2003b", 0); !valid || end != 1 {
		t.Errorf("UTF-8 trivia swallowed by name: end=%d valid=%v", end, valid)
	}
	for _, test := range []struct {
		source string
		words  []string
	}{
		{"12\u2003a", []string{"12", "a"}}, {"$12$x", []string{"$12$x"}}, {"$-", []string{"$", "-"}},
	} {
		reader := NewReader(test.source, 0)
		var words []string
		for span, ok := reader.Next(); ok; span, ok = reader.Next() {
			words = append(words, span.Text(test.source))
		}
		if !reflect.DeepEqual(words, test.words) {
			t.Errorf("tokens of %q=%q want=%q", test.source, words, test.words)
		}
	}
}

func TestStructuralSpansKeepOpaqueTokensIntact(t *testing.T) {
	source := "{a:'}', b:{c:1}, /* } */ d:`}` } tail"
	if close := FindMatchingDelimiter(source, 0, '{', '}'); close != strings.LastIndex(source, "}") {
		t.Fatalf("matching brace=%d", close)
	}
	for _, test := range []struct {
		source      string
		at          int
		open, close rune
	}{{"", 0, '{', '}'}, {source, -1, '{', '}'}, {source, 1, '{', '}'}, {"{ '}' /* } */", 0, '{', '}'}} {
		if close := FindMatchingDelimiter(test.source, test.at, test.open, test.close); close != -1 {
			t.Errorf("unmatched delimiter %q at %d closed at %d", test.source, test.at, close)
		}
	}
	text := "first /* hidden */ `last` "
	previous := PreviousToken(text, len(text))
	if previous.Text(text) != "`last`" || previous.Kind != QuotedName {
		t.Fatalf("previous token=%+v", previous)
	}
	if got := PreviousToken(text, 0); got != (Token{}) {
		t.Errorf("token before source: %+v", got)
	}
	if got := PreviousToken(text, len("first")); got.Text(text) != "first" {
		t.Errorf("token at exact boundary: %+v", got)
	}
	for _, test := range []struct {
		source  string
		at, end int
		found   bool
	}{{"'/*opaque*/'", 0, 12, true}, {"/*comment*/ x", 0, 11, true}, {"/+", 0, 0, false}, {"word", 0, 0, false}, {"word", -1, -1, false}} {
		if end, ok := SkipToken(test.source, test.at); end != test.end || ok != test.found {
			t.Errorf("opaque span %q at %d=(%d,%v)", test.source, test.at, end, ok)
		}
	}
	for _, at := range []int{-1, len(text)} {
		if _, ok := TokenAt(text, at); ok {
			t.Errorf("token outside source at %d", at)
		}
	}
	if token, ok := TokenAt(text, 0); !ok || token.Text(text) != "first" {
		t.Errorf("token at start=%+v found=%v", token, ok)
	}
}

func TestNormalizationPrecheckSharesLexicalFacts(t *testing.T) {
	arrow := func(left, right byte) bool { return left == '-' && right == '>' }
	after := func(text string, at int) bool { return strings.HasPrefix(text[at:], "AS") }
	before := func(text string, at int) bool { return strings.HasSuffix(text[:at], "AS") }
	for _, test := range []struct {
		source  string
		rewrite bool
	}{
		{"RETURN 1", false}, {"RETURN {a:1}", false}, {"RETURN /2", false},
		{"RETURN  1", true}, {"MATCH (a)- >(b)", true}, {"RETURN (1)AS x", true},
		{"AS`x`", true}, {"RETURN 'x'AS v", true}, {"RETURN 1 /* comment */", true},
		{"RETURN 1 // comment", true}, {"RETURN\t1", true}, {"RETURN é", true},
	} {
		if got := MayNeedNormalization(test.source, arrow, after, before); got != test.rewrite {
			t.Errorf("normalization precheck %q=%v want=%v", test.source, got, test.rewrite)
		}
	}
	for _, test := range []struct {
		source string
		at     int
	}{{"", 0}, {"x", -1}, {"x", 1}, {"x", 0}, {"`x`", 0}} {
		if _, _, closed := ScanStringToken(test.source, test.at); closed {
			t.Errorf("non-string boundary %q at %d accepted", test.source, test.at)
		}
	}
}
