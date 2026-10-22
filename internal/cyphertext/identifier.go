package cyphertext

import "strings"

// IsIdentifierStart and IsIdentifierByte are the byte grammar shared by the
// executor's canonical name reader and adapters below the storage boundary.
// Unicode validity remains the statement admission layer's responsibility.
const identifierStart, identifierPart = byte(1), byte(2)

// Classify each byte once. Every identifier phase uses this same table,
// including validation; the small predicates stay inline inside token loops.
var identifierBytes = func() (classes [256]byte) {
	for index := range classes {
		b := byte(index)
		if b >= 0x80 || b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' {
			classes[index] = identifierStart | identifierPart
		} else if b >= '0' && b <= '9' || b == '$' {
			classes[index] = identifierPart
		}
	}
	return classes
}()

func IsIdentifierStart(b byte) bool {
	return identifierBytes[b]&identifierStart != 0
}

func IsIdentifierByte(b byte) bool {
	return identifierBytes[b]&identifierPart != 0
}

// IdentifierValue decodes an already delimited symbolic name.
func IdentifierValue(written string) string {
	if len(written) >= 2 && IsBacktick(written[0]) && IsBacktick(written[len(written)-1]) {
		return strings.ReplaceAll(written[1:len(written)-1], "``", "`")
	}
	return written
}

// ScanIdentifier is the canonical identifier reader, moved from the executor.
func ScanIdentifier(text string, start int) (string, int, bool) {
	if start < 0 || start >= len(text) {
		return "", start, false
	}
	if IsBacktick(text[start]) {
		end, closed := ScanQuotedText(text, start)
		if !closed {
			return "", start, false
		}
		return IdentifierValue(text[start:end]), end, true
	}
	if !IsIdentifierStart(text[start]) {
		return "", start, false
	}
	end := start + 1
	for end < len(text) && IsIdentifierByte(text[end]) {
		end++
	}
	return text[start:end], end, true
}

// scanLiteralName retains the token boundary and quotation fact while
// applying the surrounding-whitespace policy of literal label/type items.
func scanLiteralName(text string, start int) (name string, end int, quoted, ok bool) {
	name, end, ok = ScanIdentifier(text, start)
	if !ok {
		return
	}
	quoted = IsBacktick(text[start])
	// The identifier phase excludes ASCII spaces. Only a Unicode boundary
	// can need the literal item's additional surrounding-space policy.
	if !quoted && (name[0] >= 0x80 || name[len(name)-1] >= 0x80) {
		trimmed := strings.TrimSpace(name)
		if trimmed != name {
			name, ok = trimmed, IsIdentifier(trimmed)
		}
	}
	return
}

// IsIdentifier reports whether text is one complete unquoted identifier.
func IsIdentifier(text string) bool {
	if text == "" || !IsIdentifierStart(text[0]) {
		return false
	}
	for index := 1; index < len(text); index++ {
		if !IsIdentifierByte(text[index]) {
			return false
		}
	}
	return true
}
