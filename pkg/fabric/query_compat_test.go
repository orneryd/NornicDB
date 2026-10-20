package fabric

import (
	"fmt"
	"strings"
	"unicode"
)

// Retired helper contracts retained only for the pre-existing helper tests.
// Production clause ownership is QueryStructure.

func keywordAt(s string, idx int, keyword string) bool {
	for token := range queryTokens(s) {
		if token.start == idx {
			return token.word && strings.EqualFold(token.text, keyword)
		}
	}
	return false
}

// NewFabricPlanner creates a planner backed by the given catalog.

func findMatchingBrace(s string, pos int) (int, error) {
	if pos < 0 || pos >= len(s) || s[pos] != '{' {
		return -1, fmt.Errorf("expected '{' at position %d", pos)
	}
	depth := 0
	for token := range queryTokens(s[pos:]) {
		switch token.text {
		case "{":
			depth++
		case "}":
			depth--
			if depth == 0 {
				return pos + token.start, nil
			}
		}
	}
	return -1, fmt.Errorf("unmatched brace (depth=%d remaining)", depth)
}

// extractWithImports parses a leading WITH clause to extract imported variable names.
// e.g. "WITH translationId MATCH ..." returns ["translationId"]

func isWithImportStopKeyword(token string) bool {
	switch {
	case strings.EqualFold(token, "MATCH"),
		strings.EqualFold(token, "OPTIONAL"),
		strings.EqualFold(token, "CREATE"),
		strings.EqualFold(token, "MERGE"),
		strings.EqualFold(token, "DELETE"),
		strings.EqualFold(token, "DETACH"),
		strings.EqualFold(token, "SET"),
		strings.EqualFold(token, "REMOVE"),
		strings.EqualFold(token, "RETURN"),
		strings.EqualFold(token, "WITH"),
		strings.EqualFold(token, "WHERE"),
		strings.EqualFold(token, "ORDER"),
		strings.EqualFold(token, "SKIP"),
		strings.EqualFold(token, "LIMIT"),
		strings.EqualFold(token, "UNWIND"),
		strings.EqualFold(token, "CALL"),
		strings.EqualFold(token, "FOREACH"),
		strings.EqualFold(token, "LOAD"),
		strings.EqualFold(token, "USE"):
		return true
	default:
		return false
	}
}

// queryIsWrite uses clause ownership, including nested CALL bodies.

func startsWithFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func containsFold(s, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(needle); i++ {
		if strings.EqualFold(s[i:i+len(needle)], needle) {
			return true
		}
	}
	return false
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

func lastKeywordIndexFold(s, keyword string) int {
	if strings.TrimSpace(keyword) == "" {
		return -1
	}
	last := -1
	for token := range queryTokens(s) {
		if token.depth == 0 && token.is(keywordHead(keyword)) && ClauseKeywordAt(s, token.start, keyword) {
			last = token.start
		}
	}
	return last
}

func lastAsIndexFold(s string) int {
	last := -1
	for token := range queryTokens(s) {
		if token.depth == 0 && token.is("AS") && token.start > 0 && token.end < len(s) && s[token.start-1] == ' ' && s[token.end] == ' ' {
			last = token.start - 1
		}
	}
	return last
}

func firstToken(s string) (token string, rest string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i:])
		}
	}
	return s, ""
}

func indexTopLevelKeyword(s string, keyword string) int {
	for token := range queryTokens(s) {
		if token.depth == 0 && token.is(keywordHead(keyword)) && ClauseKeywordAt(s, token.start, keyword) {
			return token.start
		}
	}
	return -1
}

func splitLeadingModifierClause(s string, keyword string) (clause string, rest string) {
	trimmed := strings.TrimSpace(s)
	end := len(trimmed)
	for token := range queryTokens(trimmed) {
		if token.start < len(keyword) || token.depth != 0 {
			continue
		}
		if token.is("ORDER") && ClauseKeywordAt(trimmed, token.start, "ORDER BY") || token.is("SKIP") || token.is("LIMIT") {
			end = token.start
			break
		}
	}
	return strings.TrimSpace(trimmed[:end]), strings.TrimSpace(trimmed[end:])
}

func isCypherClauseStart(query string, idx int) bool {
	for _, kw := range clauseStartKeywords {
		if hasKeywordAt(query, idx, kw) {
			return true
		}
	}
	return false
}

func hasKeywordAt(query string, idx int, keyword string) bool {
	return ClauseKeywordAt(query, idx, keyword)
}

func skipLeadingSpace(s string, idx int) int {
	for idx < len(s) && unicode.IsSpace(rune(s[idx])) {
		idx++
	}
	return idx
}

// executeUnion executes both branches and merges results.

var clauseStartKeywords = [...]string{
	"OPTIONAL MATCH",
	"DETACH DELETE",
	"ORDER BY",
	"LOAD CSV",
	"USE",
	"MATCH",
	"RETURN",
	"CALL",
	"CREATE",
	"MERGE",
	"UNWIND",
	"WHERE",
	"SET",
	"DELETE",
	"WITH",
	"FOREACH",
	"UNION",
	"LIMIT",
	"SKIP",
}

func (token queryToken) is(word string) bool {
	return token.word && strings.EqualFold(token.text, word)
}

// ClauseKeywordAt reports whether a complete unquoted keyword token starts at
// pos. It is shared by the planner, CALL row-stream helpers and Cypher's Fabric
// record adapter. Offsets always refer to the original UTF-8 text.

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
