package fabric

import (
	"iter"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// queryToken is a complete lexical unit with byte offsets in the original
// text. This lexical reader splits value lists and graph-function arguments;
// it never assigns clause roles. QueryStructure owns keyword/name decisions.
type queryToken struct {
	start, end, depth int
	text              string
	word              bool
}

func queryTokens(query string) iter.Seq[queryToken] {
	return func(yield func(queryToken) bool) {
		depth := 0
		for i := 0; i < len(query); {
			i = skipSpacesAndComments(query, i)
			if i >= len(query) {
				return
			}
			start := i
			word := false
			switch ch := query[i]; {
			case cyphertext.IsQuote(ch):
				i, _ = cyphertext.ScanQuotedText(query, i)
			case ch == '$':
				i++
				if i < len(query) && cyphertext.IsBacktick(query[i]) {
					i, _ = cyphertext.ScanQuotedText(query, i)
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
			token := queryToken{start: start, end: i, depth: depth, text: query[start:i], word: word}
			if !yield(token) {
				return
			}
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

func ClauseKeywordAt(query string, pos int, keyword string) bool {
	return ReadQueryStructure(query).keywordAt(pos, strings.ToUpper(strings.TrimSpace(keyword)))
}

// LeadingWithClauseEnd returns the first clause after an importing WITH. All
// Fabric consumers use this boundary, including the record execution adapter.
func LeadingWithClauseEnd(query string) (int, bool) {
	q := ReadQueryStructure(query)
	if !q.Starts("WITH") {
		return 0, false
	}
	if len(q.Clauses) > 1 {
		return q.Clauses[1].Start, true
	}
	return len(query), true
}
