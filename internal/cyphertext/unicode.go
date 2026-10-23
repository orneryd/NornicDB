package cyphertext

import (
	"strings"
	"unicode/utf8"
)

// InvalidUnicodeEscape locates an invalid Unicode escape within string tokens.
// Query-level token boundaries come only from Reader; backticks and comments
// never enter this string-data validation step.
func InvalidUnicodeEscape(text string) (int, bool) {
	reader := NewReader(text, 0)
	for span, ok := reader.Next(); ok; span, ok = reader.Next() {
		if span.Kind != String {
			continue
		}
		for index := span.Start + 1; index < span.End; {
			next := strings.IndexByte(text[index:span.End], '\\')
			if next < 0 {
				break
			}
			index += next
			escape := index
			index++
			if index >= span.End {
				break
			}
			width := 0
			switch text[index] {
			case 'u':
				width = 4
			case 'U':
				width = 8
			}
			if width == 0 {
				index++
				continue
			}
			value, valid := ParseUnicodeEscape(text[:span.End], index+1, width)
			if !valid || !utf8.ValidRune(rune(value)) || value >= 0xD800 && value <= 0xDFFF {
				return escape, true
			}
			index += width + 1
		}
	}
	return 0, false
}
