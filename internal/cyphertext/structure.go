package cyphertext

func FindMatchingDelimiter(text string, start int, opener, closer rune) int {
	if start < 0 || start >= len(text) || rune(text[start]) != opener {
		return -1
	}
	// The delimiters are ASCII, never a quote or '/': each byte is compared
	// with them first, and quoted text and comments are skipped whole.
	open, close := byte(opener), byte(closer)
	depth := 0
	for index := start; index < len(text); index++ {
		switch character := text[index]; {
		case character == open:
			depth++
		case character == close:
			depth--
			if depth == 0 {
				return index
			}
		case character == '\'' || character == '"' || character == '`':
			end, closed := ScanQuotedText(text, index)
			if !closed {
				return -1
			}
			index = end - 1
		case character == '/' && index+1 < len(text) && (text[index+1] == '/' || text[index+1] == '*'):
			if end := CommentEnd(text, index); end >= 0 {
				index = end - 1
			}
		}
	}
	return -1
}
