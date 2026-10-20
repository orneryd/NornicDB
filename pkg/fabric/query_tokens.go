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

// SyntaxDemand is a conservative lexical request mask, never a binding or
// clause verdict. Absence avoids acquiring facts a consumer cannot need;
// presence always goes to the native owner for the actual roles and spans.
type SyntaxDemand uint8

const (
	NativeAdmissionSyntax SyntaxDemand = 1 << iota
	NativeLocalScopes
	NativeCallSyntax
	NativeUseSyntax
	NativeUnionSyntax
	NativeEmbeddingSyntax
)

func (demand SyntaxDemand) Has(request SyntaxDemand) bool { return demand&request != 0 }

// nativeSyntaxRequests belongs to the existing lexical demand owner. These
// are conservative requests for native facts, never grammar or binding roles.
// One lookup replaces comparing every ordinary identifier with every request.
var nativeSyntaxRequests = map[string]SyntaxDemand{
	"CASE": NativeAdmissionSyntax, "AS": NativeAdmissionSyntax,
	"DISTINCT": NativeAdmissionSyntax, "WITH": NativeAdmissionSyntax,
	"UNWIND": NativeAdmissionSyntax, "LET": NativeAdmissionSyntax,
	"YIELD": NativeAdmissionSyntax, "CREATE": NativeAdmissionSyntax,
	"INSERT": NativeAdmissionSyntax, "MERGE": NativeAdmissionSyntax,
	"SET": NativeAdmissionSyntax, "REMOVE": NativeAdmissionSyntax,
	"DELETE": NativeAdmissionSyntax, "FINISH": NativeAdmissionSyntax,
	"EXPLAIN": NativeAdmissionSyntax, "PROFILE": NativeAdmissionSyntax,
	"CYPHER": NativeAdmissionSyntax, "SHOW": NativeAdmissionSyntax,
	"TERMINATE": NativeAdmissionSyntax,
	"ALL":       NativeAdmissionSyntax | NativeLocalScopes,
	"ANY":       NativeAdmissionSyntax | NativeLocalScopes,
	"NONE":      NativeAdmissionSyntax | NativeLocalScopes,
	"SINGLE":    NativeAdmissionSyntax | NativeLocalScopes,
	"FILTER":    NativeAdmissionSyntax | NativeLocalScopes,
	"REDUCE":    NativeAdmissionSyntax | NativeLocalScopes,
	"ALLREDUCE": NativeAdmissionSyntax | NativeLocalScopes,
	"FOREACH":   NativeAdmissionSyntax | NativeLocalScopes,
	"FOR":       NativeAdmissionSyntax | NativeLocalScopes,
	"CALL":      NativeAdmissionSyntax | NativeLocalScopes | NativeCallSyntax,
	"USE":       NativeAdmissionSyntax | NativeUseSyntax,
	"UNION":     NativeAdmissionSyntax | NativeUnionSyntax,
	"EMBEDDING": NativeEmbeddingSyntax,
}

func nativeWordSyntaxDemand(word string) SyntaxDemand {
	// Query tokens may contain Unicode names. Preserve EqualFold's existing
	// treatment (including the long s) without allocating on ASCII names.
	var folded [9]byte // longest request: TERMINATE / ALLREDUCE
	if len(word) <= len(folded) {
		for index := 0; index < len(word); index++ {
			letter := word[index]
			if letter >= 0x80 {
				return nativeUnicodeWordSyntaxDemand(word)
			}
			if letter >= 'a' && letter <= 'z' {
				letter -= 'a' - 'A'
			}
			folded[index] = letter
		}
		return nativeSyntaxRequests[string(folded[:len(word)])]
	}
	for index := 0; index < len(word); index++ {
		if word[index] >= 0x80 {
			return nativeUnicodeWordSyntaxDemand(word)
		}
	}
	return 0
}

func nativeUnicodeWordSyntaxDemand(word string) SyntaxDemand {
	for request, demand := range nativeSyntaxRequests {
		if strings.EqualFold(word, request) {
			return demand
		}
	}
	return 0
}

func QuerySyntaxDemand(text string) SyntaxDemand {
	var demand SyntaxDemand
	for token := range queryTokens(text) {
		if strings.HasPrefix(token.text, "`") {
			demand |= NativeAdmissionSyntax
		}
		if token.text == "[" || token.text == "{" {
			demand |= NativeLocalScopes
		}
		if token.word {
			demand |= nativeWordSyntaxDemand(token.text)
		}
	}
	return demand
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
