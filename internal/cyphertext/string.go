package cyphertext

import (
	"strings"
	"unicode/utf8"
)

func IsWholeString(raw string) bool {
	end, _, closed := ScanStringToken(raw, 0)
	return closed && end == len(raw)
}

// DecodeString decodes a quoted Cypher string literal.
// It consumes one shared string token and decodes its backslash escapes.
func DecodeString(raw string, stage SourceStage) (string, bool) {
	if stage != RawSource && stage != AcceptedSource {
		return "", false
	}
	if stage == RawSource {
		source := NormalizeSource(raw)
		if source.Err != nil {
			return "", false
		}
		raw = source.Text
	}
	if !IsWholeString(raw) {
		return "", false
	}
	quote := raw[0]

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
			case 'U':
				// U8 is the established literal-only scalar escape. Raw
				// lowercase-u source projection has already run exactly once.
				value, ok := ParseUnicodeEscape(inner, i+2, 8)
				if !ok || !utf8.ValidRune(rune(value)) || value >= 0xd800 && value <= 0xdfff {
					return "", false
				}
				builder.WriteRune(rune(value))
				i += 9
				continue

			}
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
	if !DigitForBase(value, 16) {
		return 0, false
	}
	switch {
	case IsDigit(value):
		return value - '0', true
	case value >= 'a':
		return value - 'a' + 10, true
	default:
		return value - 'A' + 10, true
	}
}
