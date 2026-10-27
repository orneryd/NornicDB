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
		if text[index] != '$' || index > 0 && NamePartAt(text, index-1) {
			index++
			continue
		}
		start := index
		index++
		if index < len(text) && IsBacktick(text[index]) {
			name, ok := ScanIdentifier(text, index)
			if !ok {
				return
			}
			visit(name.Value, start, name.End)
			index = name.End
			continue
		}
		end := NamePartEnd(text, index)
		if end > index && (end == len(text) || text[end] != '(') {
			visit(text[index:end], start, end)
		}
		index = end
	}
}
