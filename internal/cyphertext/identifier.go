package cyphertext

import "unicode/utf8"

// Identifier is a candidate symbolic name with exact source extent and decoded
// identity. Candidate scanning and semantic admission of unquoted names differ.
type Identifier struct {
	Start, End int
	Quoted     bool
	Value      string
}

// ScanIdentifier reads a rune-aware name at a byte boundary. For example,
// `a“b` has Value a`b and Quoted true; café stops before Unicode whitespace.
func ScanIdentifier(text string, start int) (Identifier, bool) {
	extent, quoted, ok := ScanIdentifierExtent(text, start)
	name := Identifier{Start: extent.Start, End: extent.End, Quoted: quoted}
	if ok {
		name.Value = text[extent.Start:extent.End]
		if quoted {
			name.Value = SymbolicNameValue(name.Value)
		}
	}
	return name, ok
}

// ScanIdentifierExtent reads the exact candidate name span without decoding its
// identity. For example, an escaped backtick name retains its written extent.
func ScanIdentifierExtent(text string, start int) (Span, bool, bool) {
	name := Span{Start: start, End: start}
	if start < 0 || start >= len(text) || !utf8.RuneStart(text[start]) {
		return name, false, false
	}
	if IsBacktick(text[start]) {
		end, closed := ScanQuotedText(text, start)
		if !closed {
			return name, false, false
		}
		name.End = end
		return name, true, true
	}
	character, width := utf8.DecodeRuneInString(text[start:])
	if !NameStart(character) {
		return name, false, false
	}
	end := start + width
	for end < len(text) {
		character, width = utf8.DecodeRuneInString(text[end:])
		if !NamePart(character) {
			break
		}
		end += width
	}
	name.End = end
	return name, false, true
}

// IsIdentifier reports one complete unquoted candidate identifier. The existing
// ValidNameStart/ValidNamePart contract separately owns semantic name admission.
func IsIdentifier(text string) bool {
	span, quoted, ok := ScanIdentifierExtent(text, 0)
	return ok && !quoted && span.End == len(text)
}

// NamePartEnd reads a parameter-name run, which may start with a digit.
func NamePartEnd(text string, start int) int {
	end := start
	for end >= 0 && end < len(text) {
		character, width := utf8.DecodeRuneInString(text[end:])
		if !NamePart(character) {
			break
		}
		end += width
	}
	return end
}

// IdentifierValue projects the canonical symbolic-name result for tuple-based
// grammar consumers. A failed candidate keeps the original start offset and an
// empty value; quoted names retain the scanner's decoded identity.
// For example, IdentifierValue("`a“b` tail", 0) returns ("a`b", 6).
func IdentifierValue(text string, start int) (string, int) {
	name, _ := ScanIdentifier(text, start)
	return name.Value, name.End
}
