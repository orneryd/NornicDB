package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// singleNodePatternComprehension rejects node-only comprehensions. Both parser
// frontends require a relationship; ordinary nested lists remain expressions.
func singleNodePatternComprehension(query string) bool {
	_, singleton := classifyPatternComprehensions(query)
	return singleton
}

// classifyPatternComprehensions shares one balanced scan between graph-read
// classification and syntax validation. Matching every nested node with a new
// suffix scan made valid nested list/group expressions quadratic.
func classifyPatternComprehensions(query string) (pattern, singleton bool) {
	if !mayHavePatternComprehension(query) {
		return false, false
	}
	// Most statements fit inline. Only deeper expressions need heap storage,
	// bounded by parenthesis depth rather than query length squared.
	var inline [32]bool
	parents := inline[:0]
	previous := -1
	for index := 0; index < len(query); index++ {
		c := query[index]
		if isCypherQuoteByte(c) {
			index = skipCypherQuotedText(query, index, c) - 1
			previous = index
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(query, index); end > index {
				index = end - 1
				continue
			}
		}
		if cyphertext.IsWhitespaceAt(query, index) {
			continue
		}
		switch c {
		case '(':
			candidate := previous >= 0 && query[previous] == '['
			if candidate && previous > 0 {
				before := query[previous-1]
				// Adjacent atom[(...)] is a subscript, not a comprehension.
				candidate = cyphertext.IsWhitespaceAt(query, previous-1) || !(cyphertext.NamePartAt(query, previous-1) || before == ']' || before == ')' || isCypherQuoteByte(before) || before == '$')
			}
			parents = append(parents, candidate)
		case ')':
			if len(parents) > 0 {
				candidate := parents[len(parents)-1]
				parents = parents[:len(parents)-1]
				if candidate {
					after := queryGapEnd(query, index+1)
					if after < len(query) && (query[after] == '|' || startsWithKeywordFold(query[after:], "WHERE")) {
						return true, true
					}
					pattern = pattern || relationshipPatternFollows(query, after)
				}
			}
		}
		previous = index
	}
	return pattern, false
}

// The allocation-free lexical precheck keeps ordinary list queries cheap,
// including quoted pipes. WHERE also preserves rejection of malformed node-only
// comprehensions whose separator is missing; later parsing owns other syntax.
func mayHavePatternComprehension(query string) bool {
	if strings.IndexByte(query, '[') < 0 {
		return false
	}
	if strings.IndexByte(query, '|') < 0 && indexASCIIFold(query, "where") < 0 {
		return false
	}
	for index := 0; index < len(query); index++ {
		c := query[index]
		if isCypherQuoteByte(c) {
			index = skipCypherQuotedText(query, index, c) - 1
			continue
		}
		if c == '/' {
			if end := queryCommentEnd(query, index); end > index {
				index = end - 1
				continue
			}
		}
		if c == '|' || matchKeywordAt(query, index, "WHERE") {
			return true
		}
	}
	return false
}

// relationshipPatternFollows reports whether a relationship pattern starts
// at cypher[at] (after gaps): "<-" followed by "-" or "[", or "-" followed
// by "-", "[" or ">".
func relationshipPatternFollows(cypher string, at int) bool {
	at = queryGapEnd(cypher, at)
	if at+1 >= len(cypher) {
		return false
	}
	if cypher[at] == '<' {
		if cypher[at+1] != '-' {
			return false
		}
		next := queryGapEnd(cypher, at+2)
		return next < len(cypher) && (cypher[next] == '-' || cypher[next] == '[')
	}
	if cypher[at] != '-' {
		return false
	}
	next := queryGapEnd(cypher, at+1)
	return next < len(cypher) && (cypher[next] == '-' || cypher[next] == '[' || cypher[next] == '>')
}
