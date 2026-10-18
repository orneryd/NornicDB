package cyphertext

import (
	"strings"
	"unicode/utf8"
)

func IsWholeString(raw string) bool {
	if len(raw) < 2 {
		return false
	}
	quote := raw[0]
	if quote != '\'' && quote != '"' {
		return false
	}
	for i := 1; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\\' && i+1 < len(raw) {
			i++
			continue
		}
		if ch == quote {
			if i+1 < len(raw) && raw[i+1] == quote {
				i++
				continue
			}
			return i == len(raw)-1
		}
	}
	return false
}

// DecodeString decodes a quoted Cypher string literal.
// It supports both doubled quote escaping (Cypher standard) and the
// backslash escapes already tolerated in several parser paths.
func DecodeString(raw string) (string, bool) {
	if len(raw) < 2 {
		return "", false
	}
	quote := raw[0]
	if (quote != '\'' && quote != '"') || raw[len(raw)-1] != quote {
		return "", false
	}

	inner := raw[1 : len(raw)-1]
	if !strings.ContainsRune(inner, rune(quote)) && !strings.ContainsRune(inner, '\\') {
		return inner, true
	}

	var builder strings.Builder
	builder.Grow(len(inner))
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if ch == '\\' && i+1 < len(inner) {
			next := inner[i+1]
			switch next {
			case '\\', '\'', '"':
				builder.WriteByte(next)
				i++
				continue
			case 'n':
				builder.WriteByte('\n')
				i++
				continue
			case 'r':
				builder.WriteByte('\r')
				i++
				continue
			case 't':
				builder.WriteByte('\t')
				i++
				continue
			case 'b':
				builder.WriteByte('\b')
				i++
				continue
			case 'f':
				builder.WriteByte('\f')
				i++
				continue
			case 'u', 'U':
				width := 4
				if next == 'U' {
					width = 8
				}
				value, ok := ParseUnicodeEscape(inner, i+2, width)
				if !ok {
					return "", false
				}
				if width == 4 && value >= 0xD800 && value <= 0xDBFF {
					nextEscape := i + 2 + width
					if nextEscape+6 > len(inner) || inner[nextEscape] != '\\' || inner[nextEscape+1] != 'u' {
						return "", false
					}
					low, lowOK := ParseUnicodeEscape(inner, nextEscape+2, 4)
					if !lowOK || low < 0xDC00 || low > 0xDFFF {
						return "", false
					}
					value = 0x10000 + (value-0xD800)<<10 + low - 0xDC00
					i = nextEscape + 5
				} else {
					i += 1 + width
				}
				if !utf8.ValidRune(rune(value)) || value >= 0xD800 && value <= 0xDFFF {
					return "", false
				}
				builder.WriteRune(rune(value))
				continue
			}
		}
		if ch == quote && i+1 < len(inner) && inner[i+1] == quote {
			builder.WriteByte(quote)
			i++
			continue
		}
		builder.WriteByte(ch)
	}

	return builder.String(), true
}

func ParseUnicodeEscape(text string, start, width int) (uint32, bool) {
	if start < 0 || width <= 0 || start+width > len(text) {
		return 0, false
	}
	var value uint32
	for index := start; index < start+width; index++ {
		digit, ok := unicodeHexDigit(text[index])
		if !ok {
			return 0, false
		}
		value = value<<4 | uint32(digit)
	}
	return value, true
}

func unicodeHexDigit(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}
