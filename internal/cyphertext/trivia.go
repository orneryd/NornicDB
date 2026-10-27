package cyphertext

import (
	"strings"
)

// GapEnd returns the end of trivia and the first unfinished comment, or -1.
func GapEnd(text string, index int) (int, int) {
	for index < len(text) {
		if width := WhitespaceAt(text, index); width > 0 {
			index += width
			continue
		}
		end, closed := CommentEndStatus(text, index)
		if end < 0 {
			break
		}
		if !closed {
			return end, index
		}
		index = end
	}
	return index, -1
}

// FindOutside searches outside quoted and comment spans, retaining lexical
// comment failure rather than silently discarding malformed source.
func FindOutside(text, needle string) (bool, int) {
	if needle == "" {
		return false, -1
	}
	for index := 0; index < len(text); {
		if IsQuote(text[index]) {
			end, _ := ScanQuotedText(text, index)
			index = end
			continue
		}
		if end, closed := CommentEndStatus(text, index); end >= 0 {
			if !closed {
				return false, index
			}
			index = end
			continue
		}
		if strings.HasPrefix(text[index:], needle) {
			return true, -1
		}
		index++
	}
	return false, -1
}

// CommentReplacement projects a consumed comment without changing line breaks.
// Line comments become empty; block comments become one gap followed by their
// CR/LF bytes. interrupted, when nonnil, is checked at most 1024 bytes apart;
// interruption returns no partial diagnostic text. A nil callback is the
// context-free statement formatting policy, for example StripComments.
func CommentReplacement(comment string, interrupted func() bool) (string, bool) {
	if interrupted != nil && interrupted() {
		return "", false
	}
	if strings.HasPrefix(comment, "//") {
		return "", true
	}
	if interrupted == nil && !strings.ContainsAny(comment, "\r\n") {
		return " ", true
	}
	var out strings.Builder
	out.WriteByte(' ')
	for start := 0; start < len(comment); {
		end := len(comment)
		if interrupted != nil {
			end = min(end, start+1024)
		}
		if interrupted != nil && interrupted() {
			return "", false
		}
		block := comment[start:end]
		for index := 0; index < len(block); index++ {
			if block[index] == '\r' || block[index] == '\n' {
				out.WriteByte(block[index])
			}
		}
		start = end
	}
	if interrupted != nil && interrupted() {
		return "", false
	}
	return out.String(), true
}
