package cyphertext

// KeywordAlphabet selects the continuation rule of the consuming grammar.
// The stored constraint grammar admits ASCII names; Cypher admits Unicode names.
type KeywordAlphabet uint8

const (
	ASCIIWord KeywordAlphabet = iota
	CypherWord
)

// KeywordEnd matches an ASCII keyword at a byte offset, ignoring ASCII case,
// and requires the consuming grammar's right boundary. It returns -1 on failure.
// For example, KeywordEnd("CREATE n", 0, "create", CypherWord) returns 6.
func KeywordEnd(text string, index int, keyword string, alphabet KeywordAlphabet) int {
	if index < 0 || index+len(keyword) > len(text) {
		return -1
	}
	for k := 0; k < len(keyword); k++ {
		a, b := text[index+k], keyword[k]
		if a >= 'a' && a <= 'z' {
			a -= 'a' - 'A'
		}
		if b >= 'a' && b <= 'z' {
			b -= 'a' - 'A'
		}
		if a != b {
			return -1
		}
	}
	end := index + len(keyword)
	if end < len(text) {
		if alphabet == CypherWord {
			if NamePartAt(text, end) {
				return -1
			}
		} else {
			c := text[end]
			if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
				return -1
			}
		}
	}
	return end
}
