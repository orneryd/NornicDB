package fabric

import (
	"iter"
	"strings"
	"unicode"
)

// queryToken is a complete lexical unit with byte offsets in the original
// text. Parameters, quoted names and literals are indivisible non-keywords.
// The USE grammar and Fabric's clause consumers share the same quote, comment
// and name readers; consumers never inspect bytes inside a token.
type queryToken struct {
	start, end, depth int
	text              string
	keyword           bool
}

func queryTokens(query string) iter.Seq[queryToken] {
	return func(yield func(queryToken) bool) {
		depth := 0
		var previous queryToken
		for i := 0; i < len(query); {
			i = skipSpacesAndComments(query, i)
			if i >= len(query) {
				return
			}
			start := i
			word := false
			switch ch := query[i]; {
			case ch == '\'' || ch == '"' || ch == '`':
				i = min(skipQuoted(query, i)+1, len(query))
			case ch == '$':
				i++
				if i < len(query) && query[i] == '`' {
					i = min(skipQuoted(query, i)+1, len(query))
				} else {
					for i < len(query) && (isNameByte(query[i]) || query[i] == '$') {
						i++
					}
				}
			case isNameByte(ch):
				word = isNameStart(ch)
				for i < len(query) && (isNameByte(query[i]) || query[i] == '$') {
					i++
				}
			default:
				i++
			}
			token := queryToken{start: start, end: i, depth: depth, text: query[start:i], keyword: word}
			if word {
				next := skipSpacesAndComments(query, i)
				if previous.text == "." || previous.text == ":" || previous.text == "$" ||
					strings.ContainsAny(previous.text, "=,+-/%^<>|([") && len(previous.text) == 1 ||
					next < len(query) && query[next] == ':' {
					token.keyword = false
				}
				if previous.keyword {
					switch strings.ToUpper(previous.text) {
					case "AS", "USE", "RETURN", "WITH", "WHERE", "UNWIND", "BY", "YIELD", "AND", "OR", "XOR", "NOT", "CASE", "WHEN", "THEN", "ELSE":
						token.keyword = false
					}
				}
			}
			if !yield(token) {
				return
			}
			previous = token
			switch token.text {
			case "(", "[", "{":
				depth++
			case ")", "]", "}":
				if depth > 0 {
					depth--
				}
			}
		}
	}
}

func (token queryToken) is(word string) bool {
	return token.keyword && strings.EqualFold(token.text, word)
}

// ClauseKeywordAt reports whether a complete unquoted keyword token starts at
// pos. It is shared by the planner, CALL row-stream helpers and Cypher's Fabric
// record adapter. Offsets always refer to the original UTF-8 text.
func ClauseKeywordAt(query string, pos int, keyword string) bool {
	if pos < 0 || pos >= len(query) || keyword == "" {
		return false
	}
	remaining := strings.TrimSpace(keyword)
	if remaining == "" {
		return false
	}
	matched := false
	for token := range queryTokens(query) {
		if token.start < pos {
			continue
		}
		end := strings.IndexFunc(remaining, unicode.IsSpace)
		if end < 0 {
			end = len(remaining)
		}
		if !matched && token.start != pos || !token.is(remaining[:end]) {
			return false
		}
		remaining = strings.TrimSpace(remaining[end:])
		if remaining == "" {
			return true
		}
		matched = true
	}
	return false
}

// LeadingWithClauseEnd returns the first clause after an importing WITH. All
// Fabric consumers use this boundary, including the record execution adapter.
func LeadingWithClauseEnd(query string) (int, bool) {
	first := true
	for token := range queryTokens(query) {
		if first {
			if !token.is("WITH") {
				return 0, false
			}
			first = false
			continue
		}
		if token.depth == 0 && token.keyword {
			for _, keyword := range clauseStartKeywords {
				if token.is(keywordHead(keyword)) {
					return token.start, true
				}
			}
		}
	}
	return len(query), !first
}

func keywordHead(keyword string) string {
	word, _, _ := strings.Cut(keyword, " ")
	return word
}

func containsQueryTokens(s, needle string) bool {
	if needle == "" {
		return true
	}
	var pattern []string
	for token := range queryTokens(needle) {
		pattern = append(pattern, token.text)
	}
	if len(pattern) == 0 {
		return false
	}
	matched := 0
	for token := range queryTokens(s) {
		if strings.EqualFold(token.text, pattern[matched]) {
			matched++
			if matched == len(pattern) {
				return true
			}
		} else {
			matched = 0
			if strings.EqualFold(token.text, pattern[0]) {
				matched = 1
			}
		}
	}
	return false
}
