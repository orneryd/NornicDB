package cyphertext

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// IsWhitespace is the shared lexer's White_Space classification.
func IsWhitespace(r rune) bool { return unicode.IsSpace(r) }

// NameStart is the shared lexer's Letter classification. Semantic identifier
// validation may reject a name after tokenization; whitespace never joins it.
func NameStart(r rune) bool {
	return r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= utf8.RuneSelf && !IsWhitespace(r)
}

// NamePart is the shared lexer's LetterOrDigit classification.
func NamePart(r rune) bool { return NameStart(r) || r >= '0' && r <= '9' || r == '$' }

// RuneAt decodes the character containing a byte cursor. Existing forward
// and reverse byte scanners can retain their source offsets without treating
// continuation bytes of Unicode whitespace as identifier characters.
func RuneAt(text string, index int) rune {
	if index < 0 || index >= len(text) {
		return -1
	}
	if text[index] < utf8.RuneSelf {
		return rune(text[index])
	}
	for index > 0 && !utf8.RuneStart(text[index]) {
		index--
	}
	r, _ := utf8.DecodeRuneInString(text[index:])
	return r
}

// NameStartAt reports whether the character at a source byte cursor starts a name.
func NameStartAt(text string, index int) bool { return NameStart(RuneAt(text, index)) }

// NamePartAt reports whether the character at a source byte cursor continues a name.
func NamePartAt(text string, index int) bool { return NamePart(RuneAt(text, index)) }

// IsWhitespaceAt reports whitespace at either a forward or reverse byte cursor.
func IsWhitespaceAt(text string, index int) bool { return IsWhitespace(RuneAt(text, index)) }

// TrimWhitespaceSpan narrows span of text to exclude leading and trailing
// whitespace. It does not skip comments or other trivia.
func TrimWhitespaceSpan(text string, span Span) Span {
	left := strings.TrimLeftFunc(text[span.Start:span.End], IsWhitespace)
	span.Start = span.End - len(left)
	span.End = span.Start + len(strings.TrimRightFunc(left, IsWhitespace))
	return span
}

// WhitespaceAt returns the byte width of Cypher whitespace beginning at index.
func WhitespaceAt(text string, index int) int {
	if index < 0 || index >= len(text) {
		return 0
	}
	r, width := utf8.DecodeRuneInString(text[index:])
	if IsWhitespace(r) {
		return width
	}
	return 0
}

// KeywordBoundary reports a name boundary on both sides of an ASCII keyword.
func KeywordBoundary(text string, start, end int) bool {
	return !NamePartAt(text, start-1) && !NamePartAt(text, end)
}

// CanonicalSpaceAt tests the emitted ASCII separator, when deciding whether
// a normalization edit is needed rather than classifying source whitespace.
func CanonicalSpaceAt(text string, index int) bool {
	return index >= 0 && index < len(text) && text[index] == ' '
}

// NonCanonicalWhitespace identifies source separators needing normalization.
func NonCanonicalWhitespace(r rune) bool { return IsWhitespace(r) && r != ' ' }

// CutWhitespace splits at the first source separator, consuming its full rune.
func CutWhitespace(text string) (before, after string, found bool) {
	index := strings.IndexFunc(text, IsWhitespace)
	if index < 0 {
		return text, "", false
	}
	return text[:index], text[index+WhitespaceAt(text, index):], true
}

// WhitespaceReplacement preserves existing ASCII layout and maps Unicode
// line separators to newline and the other whitespace characters to space.
func WhitespaceReplacement(r rune) (rune, bool) {
	switch r {
	case '\u0085', '\u2028', '\u2029':
		return '\n', true
	case ' ', '\t', '\n', '\r':
		return 0, false
	default:
		if IsWhitespace(r) {
			return ' ', true
		}
		return 0, false
	}
}

// ValidNameStart reports whether a non-ASCII character can start an
// unquoted name in Neo4j 5.26: a letter, a letter number (Ⅰ) or a
// connector punctuation (‿), not a digit, mark or currency sign.
func ValidNameStart(r rune) bool {
	return unicode.IsLetter(r) || unicode.In(r, unicode.Nl, unicode.Pc)
}

// ValidNamePart reports whether a non-ASCII character can continue an
// unquoted name in Neo4j 5.26 (Java's identifier part): a start character, a
// digit, a combining mark, a currency sign (a€), a format character, or a
// C1 control.
func ValidNamePart(r rune) bool {
	return NamePart(r) && (ValidNameStart(r) || unicode.In(r, unicode.Nd, unicode.Mn, unicode.Mc, unicode.Sc, unicode.Cf) ||
		r >= 0x80 && r <= 0x9F)
}
