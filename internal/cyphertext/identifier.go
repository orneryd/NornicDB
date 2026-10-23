package cyphertext

import (
	"strings"
	"unicode"
)

// IdentifierStartByte and IdentifierPartByte are the canonical UTF-8 byte
// boundaries of unquoted Cypher names. Unicode validity is checked separately;
// a dollar sign continues a name but does not start one.
func IdentifierStartByte(c byte) bool {
	return c >= 0x80 || c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func IdentifierPartByte(c byte) bool {
	return IdentifierStartByte(c) || c >= '0' && c <= '9' || c == '$'
}

// IdentifierStartRune and IdentifierPartRune own the Unicode name grammar;
// statement validators use these same predicates after UTF-8 tokenization.
func IdentifierStartRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.In(r, unicode.Nl, unicode.Pc)
}
func IdentifierPartRune(r rune) bool {
	return IdentifierStartRune(r) || unicode.In(r, unicode.Nd, unicode.Mn, unicode.Mc, unicode.Sc, unicode.Cf) || r >= 0x80 && r <= 0x9f
}

// IdentifierEnd returns the end of one unquoted or backtick-quoted name.
// It reads only its span, so tokenization and decoded-name consumers share
// exactly the same alphabet and quotation rules without allocating.
func IdentifierEnd(text string, start int) (int, bool) {
	if start < 0 || start >= len(text) {
		return start, false
	}
	if IsBacktick(text[start]) {
		end, closed := ScanQuotedText(text, start)
		return end, closed
	}
	if !IdentifierStartByte(text[start]) || WhitespaceAt(text, start) > 0 {
		return start, false
	}
	end := start + 1
	for end < len(text) && IdentifierPartByte(text[end]) {
		if text[end] >= 0x80 && WhitespaceAt(text, end) > 0 {
			break
		}
		end++
	}
	return end, true
}

// ScanIdentifier decodes the symbolic name at start using the tokenizer's span.
func ScanIdentifier(text string, start int) (string, int, bool) {
	end, ok := IdentifierEnd(text, start)
	if !ok {
		return "", start, false
	}
	if IsBacktick(text[start]) {
		return strings.ReplaceAll(text[start+1:end-1], "``", "`"), end, true
	}
	return text[start:end], end, true
}

// ParameterStart distinguishes a parameter marker from a name continuation.
func ParameterStart(text string, at int) bool {
	return at == 0 || at > 0 && !IdentifierPartByte(text[at-1])
}
