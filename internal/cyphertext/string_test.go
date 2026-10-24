package cyphertext

import (
	"strings"
	"testing"
)

func TestIsWholeString(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"", false},
		{"'", false},
		{"ab", false},
		{"'abc'", true},
		{`"abc"`, true},
		{"'a''b'", true},
		{`'a\'b'`, true},
		{"'a' + 'b'", false},
		{"'abc", false},
		{`'a\`, false},
	} {
		if got := IsWholeString(tc.raw); got != tc.want {
			t.Errorf("IsWholeString(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}

func TestDecodeString(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want string
		ok   bool
	}{
		{"a", "", false},
		{"'abc", "", false},
		{"`a`", "", false},
		{"'abc'", "abc", true},
		{"'a''b'", "a'b", true},
		{"'a'b'", "a'b", true},
		{`'a\'b'`, "a'b", true},
		{`"a\"b"`, `a"b`, true},
		{`'a\\b'`, `a\b`, true},
		{`'\n\r\t\b\f'`, "\n\r\t\b\f", true},
		{`'\q'`, `\q`, true},
		{`'A'`, "A", true},
		{`'é'`, "é", true},
		{`'\U0001F600'`, "\U0001F600", true},
		{`'😀'`, "\U0001F600", true},
		{`'\uD83D'`, "", false},
		{`'\uD83Dxxxxxx'`, "", false},
		{`'\uD83DA'`, "", false},
		{`'\uDC00'`, "", false},
		{`'\U00110000'`, "", false},
		{`'\u00G1'`, "", false},
	} {
		got, ok := DecodeString(tc.raw)
		if got != tc.want || ok != tc.ok {
			t.Errorf("DecodeString(%q) = %q, %v, want %q, %v", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

func TestParseUnicodeEscape(t *testing.T) {
	for _, tc := range []struct {
		text         string
		start, width int
		want         uint32
		ok           bool
	}{
		{"00e9", 0, 4, 0xe9, true},
		{"00E9", 0, 4, 0xE9, true},
		{"x0041", 1, 4, 0x41, true},
		{"0041", -1, 4, 0, false},
		{"0041", 0, 0, 0, false},
		{"004", 0, 4, 0, false},
		{"0x41", 0, 4, 0, false},
	} {
		got, ok := ParseUnicodeEscape(tc.text, tc.start, tc.width)
		if got != tc.want || ok != tc.ok {
			t.Errorf("ParseUnicodeEscape(%q, %d, %d) = %#x, %v, want %#x, %v", tc.text, tc.start, tc.width, got, ok, tc.want, tc.ok)
		}
	}
}

func TestQuoteClasses(t *testing.T) {
	for _, tc := range []struct {
		character                                byte
		quote, stringQuote, single, double, tick bool
	}{
		{'\'', true, true, true, false, false},
		{'"', true, true, false, true, false},
		{'`', true, false, false, false, true},
		{'a', false, false, false, false, false},
	} {
		got := [5]bool{IsQuote(tc.character), IsStringQuote(tc.character), IsSingleQuote(tc.character), IsDoubleQuote(tc.character), IsBacktick(tc.character)}
		if want := [5]bool{tc.quote, tc.stringQuote, tc.single, tc.double, tc.tick}; got != want {
			t.Errorf("quote classes of %q = %v, want %v", tc.character, got, want)
		}
	}
}

func TestScanQuotedText(t *testing.T) {
	for _, tc := range []struct {
		text   string
		start  int
		end    int
		closed bool
	}{
		{"'a''b' x", 0, 6, true},
		{`'a\'b' x`, 0, 6, true},
		{"`a\\`", 0, 4, true},
		{"'abc", 0, 4, false},
		{"x", 0, 0, false},
		{"'a'", -1, -1, false},
		{"'a'", 3, 3, false},
	} {
		end, closed := ScanQuotedText(tc.text, tc.start)
		if end != tc.end || closed != tc.closed {
			t.Errorf("ScanQuotedText(%q, %d) = %d, %v, want %d, %v", tc.text, tc.start, end, closed, tc.end, tc.closed)
		}
	}
}

func TestScanStringToken(t *testing.T) {
	for _, tc := range []struct {
		text              string
		start, end        int
		hasEscape, closed bool
	}{
		{"'a''b'", 0, 3, false, true},
		{`'a\'b'`, 0, 6, true, true},
		{`"a\nb`, 0, 5, true, false},
		{"`a`", 0, 0, false, false},
		{"'a'", -1, -1, false, false},
	} {
		end, hasEscape, closed := ScanStringToken(tc.text, tc.start)
		if end != tc.end || hasEscape != tc.hasEscape || closed != tc.closed {
			t.Errorf("ScanStringToken(%q, %d) = %d, %v, %v, want %d, %v, %v", tc.text, tc.start, end, hasEscape, closed, tc.end, tc.hasEscape, tc.closed)
		}
	}
}

func TestScanInterpolatedText(t *testing.T) {
	text := "'a{b'c}d' z"
	matching := func(open int) int { return open + strings.IndexByte(text[open:], '}') }
	if end, closed := ScanInterpolatedText(text, 0, matching); end != 9 || !closed {
		t.Errorf("ScanInterpolatedText skipping the expression = %d, %v, want 9, true", end, closed)
	}
	if end, closed := ScanInterpolatedText(text, 0, func(int) int { return -1 }); end != len(text) || closed {
		t.Errorf("ScanInterpolatedText with an unclosed expression = %d, %v, want %d, false", end, closed, len(text))
	}
}

func TestCommentEnd(t *testing.T) {
	for _, tc := range []struct {
		text  string
		index int
		want  int
	}{
		{"// x\ny", 0, 4},
		{"// x\ry", 0, 4},
		{"// x", 0, 4},
		{"/* x */y", 0, 7},
		{"/* x", 0, 4},
		{"/x", 0, -1},
		{"x", 0, -1},
		{"ab/", 2, -1},
		{"ab", 0, -1},
	} {
		if got := CommentEnd(tc.text, tc.index); got != tc.want {
			t.Errorf("CommentEnd(%q, %d) = %d, want %d", tc.text, tc.index, got, tc.want)
		}
	}
}

func TestSkipToken(t *testing.T) {
	for _, tc := range []struct {
		text    string
		index   int
		end     int
		skipped bool
	}{
		{"'a' b", 0, 3, true},
		{"`a` b", 0, 3, true},
		{"// c\nx", 0, 4, true},
		{"/* c */x", 0, 7, true},
		{"x", 0, 0, false},
		{"x", -1, -1, false},
		{"x", 1, 1, false},
	} {
		end, skipped := SkipToken(tc.text, tc.index)
		if end != tc.end || skipped != tc.skipped {
			t.Errorf("SkipToken(%q, %d) = %d, %v, want %d, %v", tc.text, tc.index, end, skipped, tc.end, tc.skipped)
		}
	}
}

func TestSkipTokenRunes(t *testing.T) {
	for _, tc := range []struct {
		text    string
		index   int
		end     int
		skipped bool
	}{
		{`'a\'b' x`, 0, 6, true},
		{"'a''b' x", 0, 6, true},
		{"'ab", 0, 3, true},
		{"`a\\`", 0, 4, true},
		{"'é' x", 0, 3, true},
		{"// c\nx", 0, 4, true},
		{"// c\rx", 0, 4, true},
		{"/* c */x", 0, 7, true},
		{"/* c", 0, 4, true},
		{"/x", 0, 0, false},
		{"x", 0, 0, false},
		{"x", -1, -1, false},
		{"x", 1, 1, false},
	} {
		end, skipped := SkipTokenRunes([]rune(tc.text), tc.index)
		if end != tc.end || skipped != tc.skipped {
			t.Errorf("SkipTokenRunes(%q, %d) = %d, %v, want %d, %v", tc.text, tc.index, end, skipped, tc.end, tc.skipped)
		}
	}
	// Rune indexes, not bytes: after a multi-byte letter the quoted token
	// starts at rune 4.
	if end, skipped := SkipTokenRunes([]rune("é a 'b'"), 4); end != 7 || !skipped {
		t.Errorf("SkipTokenRunes after a multi-byte letter = %d, %v, want 7, true", end, skipped)
	}
}
