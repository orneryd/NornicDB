package cyphertext

// DelimiterAnchor selects which end of a delimiter pair the cursor identifies.
type DelimiterAnchor uint8

const (
	OpeningDelimiter DelimiterAnchor = iota
	ClosingDelimiter
)

// FindMatchingDelimiter returns the other end of an ASCII delimiter pair or -1.
// Closing anchors preserve prefix admission: an earlier unclosed same-kind
// opener rejects the lookup even when a nested group closes at the cursor.
func FindMatchingDelimiter(text string, start int, opener, closer rune, anchor DelimiterAnchor) int {
	expected := opener
	if anchor == ClosingDelimiter {
		expected = closer
	}
	if start < 0 || start >= len(text) || rune(text[start]) != expected {
		return -1
	}
	// The delimiters are ASCII, never a quote or '/': each byte is compared
	// with them first, and quoted text and comments are skipped whole.
	open, close := byte(opener), byte(closer)
	depth := 0
	first, limit := start, len(text)
	if anchor == ClosingDelimiter {
		first, limit = 0, start+1
	}
	for index := first; index < limit; index++ {
		switch character := text[index]; {
		case character == open:
			if anchor == ClosingDelimiter {
				end := FindMatchingDelimiter(text, index, opener, closer, OpeningDelimiter)
				if end == start {
					return index
				}
				if end < 0 {
					return -1
				}
				if end < start {
					index = end
				}
				continue
			}
			depth++
		case character == close && anchor == OpeningDelimiter:
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
