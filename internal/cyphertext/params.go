package cyphertext

import "strings"

// ParameterReferences calls visit for each parameter reference in text: a
// '$' outside string literals, quoted names and comments that doesn't
// continue a name (a$b is one name), followed by the parameter's name, a run
// of name characters or a backtick-quoted name. name is the parameter's name
// unquoted, and text[start:end] the whole reference. A '$' followed by a
// call ($all(…), $any(…), $(…)) is a dynamic label, not a reference.
func ParameterReferences(text string, visit func(name string, start, end int)) {
	if strings.IndexByte(text, '$') < 0 {
		return
	}
	for index := 0; index < len(text); {
		if end, skipped := SkipToken(text, index); skipped {
			index = end
			continue
		}
		if text[index] != '$' || index > 0 && isNameByte(text[index-1]) {
			index++
			continue
		}
		start := index
		index++
		if index < len(text) && IsBacktick(text[index]) {
			end, closed := ScanQuotedText(text, index)
			if !closed {
				return
			}
			visit(strings.ReplaceAll(text[index+1:end-1], "``", "`"), start, end)
			index = end
			continue
		}
		end := index
		for end < len(text) && isNameByte(text[end]) {
			end++
		}
		if end > index && (end == len(text) || text[end] != '(') {
			visit(text[index:end], start, end)
		}
		index = end
	}
}

// isNameByte reports whether b continues an unquoted name: a letter, a
// digit, '_', '$' or a byte of a multi-byte character.
func isNameByte(b byte) bool {
	return b >= 0x80 || b == '_' || b == '$' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}
