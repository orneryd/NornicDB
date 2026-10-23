package cyphertext

// MayNeedNormalization is a conservative lexical precheck. Grammar-specific
// adjacency decisions are callbacks; the precheck never interprets their words.
func MayNeedNormalization(text string, arrow func(byte, byte) bool, after func(string, int) bool, before func(string, int) bool) bool {
	for index := 0; index < len(text); index++ {
		switch c := text[index]; {
		case c == ')' || c == ']' || c == '}' || IsQuote(c):
			if after(text, index+1) || IsBacktick(c) && before(text, index) {
				return true
			}
		case c == ' ':
			if index+1 < len(text) && (text[index+1] == ' ' || index > 0 && arrow(text[index-1], text[index+1])) {
				return true
			}
		case c == '/':
			if CommentEnd(text, index) >= 0 {
				return true
			}
		case c < ' ' || c >= 0x80:
			return true
		}
	}
	return false
}
