package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// validateStaticMapKeys rejects malformed map entries before either parser
// frontend or the executor sees the statement, so a query that produces no
// rows, EXPLAIN, a CALL body or a UNION part can't hide them. What each brace
// opens comes from the brace syntax owner (brace_syntax.go); query bodies are
// validated in their own statement scope.
//
// Statements with a grammar of their own hand their braces to that grammar:
// knowledge-policy DDL, constraint contracts (REQUIRE { … }) and index
// OPTIONS. The policy parser checks the maps of the expressions it extracts;
// its own grammar errors stay with the statement's entry check.

func isCypherWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func undefinedStandaloneMapValue(view braceView) string {
	entries, valid := view.trim().keyedEntries()
	if !valid {
		return ""
	}
	for _, entry := range entries {
		value := entry.value.text()
		if cyphertext.IsIdentifier(value) && !strings.EqualFold(value, "null") && !strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
			return value
		}
		if nested := undefinedStandaloneMapValue(entry.value); nested != "" {
			return nested
		}
	}
	return ""
}

// colonIsMapKeySeparator reports whether the ':' at colon follows a map key
// ({key: …} or {…, key: …}) rather than a variable or label in a pattern.
func colonIsMapKeySeparator(view braceView, colon int) bool {
	// Immutable separators precede contextual role construction. In particular,
	// the policy boundary callback must never enter the final-product sync.Once.
	_, found := view.source.OriginalEntryAtColon(cyphertext.Span{Start: colon, End: colon + 1})
	return found
}
