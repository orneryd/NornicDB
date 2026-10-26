package cypher

import (
	"errors"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Author: #1663 implementation lane. Roles come from the shared final product.
type mapBraceKind = cypherantlr.BraceRole

const (
	literalBrace         = cypherantlr.LiteralBrace
	projectionBrace      = cypherantlr.ProjectionBrace
	patternPropertyBrace = cypherantlr.PatternPropertyBrace
	subqueryBrace        = cypherantlr.SubqueryBrace
	quantifierBrace      = cypherantlr.QuantifierBrace
	declarationBrace     = cypherantlr.DeclarationBrace
)

type braceEntryKind = cyphertext.BraceEntryKind

const (
	invalidBraceEntry  = cyphertext.InvalidBraceEntry
	keyedBraceEntry    = cyphertext.KeyedBraceEntry
	propertyBraceEntry = cyphertext.PropertyBraceEntry
	wildcardBraceEntry = cyphertext.WildcardBraceEntry
	variableBraceEntry = cyphertext.VariableBraceEntry
)

type parsedMapEntry struct{ key, value string }

const (
	mapLiteralEntryMessage      = cypherantlr.MapLiteralEntryMessage
	patternPropertyEntryMessage = cypherantlr.PatternPropertyEntryMessage
)

func isMapEntrySyntaxError(err error) bool {
	var semantic *SemanticError
	return errors.As(err, &semantic) && semantic.Detail == "UnexpectedSyntax" && (semantic.Message == mapLiteralEntryMessage || semantic.Message == patternPropertyEntryMessage)
}

// Quantifier consumers select the retained role before reading its numeric
// bounds. The shared owner implements that bounds grammar once.
func braceQuantifierAt(view braceView, at, end int) (relationshipQuantifier, bool) {
	if at < 0 || at >= end {
		return relationshipQuantifier{}, false
	}
	switch view.text()[at] {
	case '+':
		return relationshipQuantifier{min: 1, max: -1, end: at + 1}, true
	case '*':
		return relationshipQuantifier{min: 0, max: -1, end: at + 1}, true
	case '{':
	default:
		return relationshipQuantifier{}, false
	}
	fact, found := view.at(at)
	if !found || fact.kind != quantifierBrace {
		return relationshipQuantifier{}, false
	}
	close, found := view.close(at)
	if !found || close >= end {
		return relationshipQuantifier{}, false
	}
	min, max, valid := cypherantlr.BraceQuantifierBounds(view.text(), at, close)
	return relationshipQuantifier{min: min, max: max, end: close + 1}, valid
}
func braceGroupedPath(view braceView, open, close int) bool {
	return cypherantlr.BraceGroupedPath(view.source, open, close)
}
