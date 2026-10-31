package jsontext

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"unicode/utf8"
)

// escapePolicy selects how the one JSON string writer treats the boundary
// cases that legitimately differ between its consumers. The escaping grammar
// (quotes, backslash, controls, U+2028/U+2029) is shared by every policy.
type escapePolicy uint8

const (
	// sourceString preserves exact UTF16 units: a literal unpaired surrogate
	// unit is written as its \uXXXX escape and an invalid byte as a literal
	// U+FFFD.
	sourceString escapePolicy = iota
	// htmlEscapedString is sourceString with the standard JSON HTML escaping
	// of '<', '>' and '&'.
	htmlEscapedString
	// logString sanitizes for log sinks with stdlib slog JSON parity: no HTML
	// escaping, and every byte that is not valid UTF-8 (including encoded
	// surrogate units) is written as the escaped U+FFFD replacement unit.
	logString
)

// asciiSafe holds, per policy, the ASCII bytes written unescaped, so the hot
// loop decides each byte with one lookup and never re-tests the policy.
var asciiSafe = func() (table [logString + 1][utf8.RuneSelf]bool) {
	for c := 0x20; c < utf8.RuneSelf; c++ {
		safe := c != '"' && c != '\\'
		table[sourceString][c], table[logString][c] = safe, safe
		table[htmlEscapedString][c] = safe && c != '<' && c != '>' && c != '&'
	}
	return table
}()

// AppendString appends one JSON string without HTML escaping, preserving exact
// UTF16 units and retaining allocation-free appends when capacity permits.
func AppendString(buf []byte, s string) []byte { return appendString(buf, s, sourceString) }

// AppendHTMLEscapedString has AppendString's exact UTF16 identity with the
// standard JSON HTML escaping selection used by property and request writers.
func AppendHTMLEscapedString(buf []byte, s string) []byte {
	return appendString(buf, s, htmlEscapedString)
}

// AppendLogString appends one JSON string under the log sanitization policy,
// byte-compatible with stdlib slog.NewJSONHandler string output.
func AppendLogString(buf []byte, s string) []byte { return appendString(buf, s, logString) }

func appendString(buf []byte, s string, policy escapePolicy) []byte {
	const hex = "0123456789abcdef"
	safe := &asciiSafe[policy]
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if safe[c] {
				i++
				continue
			}
			buf = append(buf, s[start:i]...)
			switch c {
			case '"', '\\':
				buf = append(buf, '\\', c)
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			default:
				buf = append(buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xF])
			}
			i++
			start = i
			continue
		}
		var r rune
		var size int
		if policy == logString {
			r, size = utf8.DecodeRuneInString(s[i:])
		} else {
			r, size = cyphertext.ReadCodepoint(s, i)
		}
		if r >= 0xd800 && r <= 0xdfff {
			buf = append(buf, s[start:i]...)
			buf = appendJSONUnit(buf, r)
			i += size
			start = i
			continue
		}
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, s[start:i]...)
			if policy == logString {
				buf = appendJSONUnit(buf, utf8.RuneError)
			} else {
				buf = append(buf, "�"...)
			}
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			buf = append(buf, s[start:i]...)
			buf = append(buf, '\\', 'u', '2', '0', '2', hex[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	buf = append(buf, s[start:]...)
	return append(buf, '"')
}
