package cyphertext

// FindMatchingDelimiter reads delimiter grammar over owned significant spans.
func FindMatchingDelimiter(text string, start int, opener, closer rune) int {
	if start < 0 || start >= len(text) || rune(text[start]) != opener {
		return -1
	}
	reader := NewReader(text, start)
	depth := 0
	for span, ok := reader.Next(); ok; span, ok = reader.Next() {
		if span.Kind != Symbol {
			continue
		}
		switch rune(text[span.Start]) {
		case opener:
			depth++
		case closer:
			depth--
			if depth == 0 {
				return span.Start
			}
		}
	}
	return -1
}
