package cyphertext

import "strings"

// BackslashEscaped reports escape parity at a boundary inside an owned token.
// String-format adapters reuse this primitive rather than inventing scanners.
func BackslashEscaped(text string, at int) bool {
	count := 0
	for index := at - 1; index >= 0 && text[index] == '\\'; index-- {
		count++
	}
	return count%2 == 1
}

// InterpolationLiteral decodes brace escapes within an already-owned literal
// segment. Other escapes remain written for the resulting ordinary string.
func InterpolationLiteral(text string) (string, bool) {
	var out strings.Builder
	for index := 0; index < len(text); index++ {
		if text[index] == '\\' && index+1 < len(text) {
			if text[index+1] == '{' || text[index+1] == '}' {
				out.WriteByte(text[index+1])
			} else {
				out.WriteString(text[index : index+2])
			}
			index++
			continue
		}
		if text[index] == '}' {
			return "", false
		}
		out.WriteByte(text[index])
	}
	return out.String(), true
}
