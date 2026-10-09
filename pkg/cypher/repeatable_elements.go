package cypher

import (
	"context"
	"strings"
)

// MATCH REPEATABLE ELEMENTS (Cypher 25 match mode, #907).
//
// Under the default match mode (DIFFERENT RELATIONSHIPS) no relationship
// appears twice in a MATCH: a variable-length relationship is a trail, and
// a chain or two comma-separated patterns never share one. REPEATABLE
// ELEMENTS lifts that for its clause: a quantifier walks (a relationship may
// repeat), and patterns may share relationships. As in Neo4j, every
// repetition in such a clause needs an upper bound, or the walks would not
// end (the statement rewrite checks it, path_selector.go).
//
// The statement rewrite marks the clause with __nornic_repeatable_elements()
// as the first conjunct of its WHERE. The pipeline's MATCH step removes it
// and runs the clause with the mode in its context, which every relationship
// uniqueness check reads (repeatableElements): the traversal's used
// relationships, a chain's segments, and the paths of a pattern product.
// Other clauses of the statement keep the default.

// repeatableElementsKey marks a context whose MATCH clause repeats elements.
type repeatableElementsKey struct{}

// withRepeatableElements returns ctx for a MATCH REPEATABLE ELEMENTS clause.
func withRepeatableElements(ctx context.Context) context.Context {
	return context.WithValue(ctx, repeatableElementsKey{}, true)
}

// repeatableElements reports whether ctx runs a MATCH REPEATABLE ELEMENTS
// clause, where a relationship may repeat.
func repeatableElements(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	repeatable, _ := ctx.Value(repeatableElementsKey{}).(bool)
	return repeatable
}

// stripRepeatableElements removes the match mode marker from a MATCH body
// (pattern [WHERE …]); ok is false when the body has none.
func stripRepeatableElements(body string) (string, bool) {
	if indexASCIIFold(body, repeatableElementsFunction) < 0 {
		return body, false
	}
	where := topLevelKeywordIndex(body, "WHERE")
	if where < 0 {
		return body, false
	}
	condition := strings.TrimSpace(body[where+len("WHERE"):])
	marker := repeatableElementsFunction + "()"
	if !hasPrefixFold(condition, marker) {
		return body, false
	}
	rest := strings.TrimSpace(condition[len(marker):])
	pattern := strings.TrimSpace(body[:where])
	if rest == "" {
		return pattern, true
	}
	if !hasPrefixFold(rest, "AND") || len(rest) > len("AND") && isIdentByte(rest[len("AND")]) {
		return body, false
	}
	return pattern + " WHERE " + strings.TrimSpace(rest[len("AND"):]), true
}
