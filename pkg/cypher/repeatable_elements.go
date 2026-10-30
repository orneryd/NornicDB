package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
func stripRepeatableElements(body braceView) (string, bool) {
	return stripRepeatableElementsSpans(body, nil)
}

// Author: #1663 lane. The existing marker grammar supplies the copied pieces
// of its reconstructed body; Source owns composition and coordinate mapping.
type repeatableBodySpans struct{ pattern, predicate cyphertext.Span }

func stripRepeatableElementsView(body braceView) (braceView, bool) {
	var spans repeatableBodySpans
	_, repeatable := stripRepeatableElementsSpans(body, &spans)
	if !repeatable {
		return body, false
	}
	pattern := body.spanWindow(spans.pattern)
	if spans.predicate.Start == spans.predicate.End {
		return pattern, true
	}
	return body.assembleCode([]braceView{pattern, body.spanWindow(spans.predicate)}, " WHERE "), true
}

func stripRepeatableElementsSpans(view braceView, spans *repeatableBodySpans) (string, bool) {
	body := view.text()
	if indexASCIIFold(body, repeatableElementsFunction) < 0 {
		return body, false
	}
	where := topLevelKeywordIndex(view, "WHERE")
	if where < 0 {
		return body, false
	}
	conditionSpan := cyphertext.TrimWhitespaceSpan(body, cyphertext.Span{Start: where + len("WHERE"), End: len(body)})
	condition := body[conditionSpan.Start:conditionSpan.End]
	marker := repeatableElementsFunction + "()"
	if !hasPrefixFold(condition, marker) {
		return body, false
	}
	restSpan := cyphertext.TrimWhitespaceSpan(body, cyphertext.Span{Start: conditionSpan.Start + len(marker), End: conditionSpan.End})
	rest := body[restSpan.Start:restSpan.End]
	patternSpan := cyphertext.TrimWhitespaceSpan(body, cyphertext.Span{End: where})
	pattern := body[patternSpan.Start:patternSpan.End]
	if rest == "" {
		if spans != nil {
			spans.pattern = patternSpan
		}
		return pattern, true
	}
	if !hasPrefixFold(rest, "AND") || len(rest) > len("AND") && cyphertext.NamePartAt(rest, len("AND")) {
		return body, false
	}
	predicate := cyphertext.TrimWhitespaceSpan(body, cyphertext.Span{Start: restSpan.Start + len("AND"), End: restSpan.End})
	if spans != nil {
		spans.pattern, spans.predicate = patternSpan, predicate
	}
	return pattern + " WHERE " + body[predicate.Start:predicate.End], true
}
