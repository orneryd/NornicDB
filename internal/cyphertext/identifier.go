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
	name := Identifier{Start: start, End: start}
	if start < 0 || start >= len(text) || !utf8.RuneStart(text[start]) {
		return name, false
	}
	if IsBacktick(text[start]) {
		end, closed := ScanQuotedText(text, start)
		if !closed {
			return name, false
		}
		name.End, name.Quoted, name.Value = end, true, SymbolicNameValue(text[start:end])
		return name, true
	}
	character, width := utf8.DecodeRuneInString(text[start:])
	if !NameStart(character) {
		return name, false
	}
	end := start + width
	for end < len(text) {
		character, width = utf8.DecodeRuneInString(text[end:])
		if !NamePart(character) {
			break
		}
		end += width
	}
	name.End, name.Value = end, text[start:end]
	return name, true
}

// IsIdentifier reports one complete unquoted candidate identifier. The existing
// ValidNameStart/ValidNamePart contract separately owns semantic name admission.
func IsIdentifier(text string) bool {
	name, ok := ScanIdentifier(text, 0)
	return ok && !name.Quoted && name.End == len(text)
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
