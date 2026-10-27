package jsontext

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"unicode/utf8"
)

// AppendString appends one JSON string without HTML escaping, preserving exact
// UTF16 units and retaining allocation-free appends when capacity permits.
func AppendString(buf []byte, s string) []byte {
	const hex = "0123456789abcdef"
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
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
		r, size := cyphertext.ReadCodepoint(s, i)
		if r >= 0xd800 && r <= 0xdfff {
			buf = append(buf, s[start:i]...)
			buf = appendJSONUnit(buf, r)
			i += size
			start = i
			continue
		}
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, s[start:i]...)
			buf = append(buf, `�`...)
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
