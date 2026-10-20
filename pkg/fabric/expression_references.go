package fabric

import (
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// ExpressionReference is an immutable native grammar fact. Scope consumers
// select occurrences, without inferring labels, types or bindings from text.
type ExpressionReference struct {
	Name, Property string
	Start, End     int
	Local          bool
}

// ExpressionReferences supplies ordered variable occurrences, retaining repeats.
// Local scopes are selected by the shared expression binding owner. Expression
// subqueries have their own semantic walk; ordinary CALL bodies remain visible
// when a physical consumer reads the footprint of a complete query.
func ExpressionReferences(text string) []ExpressionReference {
	query, offset := expressionFacts(text)
	if query == nil {
		return nil
	}
	if offset != 0 {
		return query.relativeReferences
	}
	return query.references
}

// ExpressionReadReferences is the physical scheduling view of the native
// occurrence owner. A subquery's captures cannot disappear at its boundary.
func ExpressionReadReferences(text string) []ExpressionReference {
	query, offset := expressionFacts(text)
	if query == nil {
		return nil
	}
	return query.Input.Slice(offset, offset+len(text)).ReadReferences()
}

func expressionFacts(text string) (*QueryStructure, int) {
	// Physical read footprints pass complete clauses/queries; operand checks
	// pass expressions or projection lists. Both grammar entry points feed the
	// same occurrence owner. No clause-prefix or neighbouring-token inference.
	input := ReadQueryStructure(text)
	input.expressionOnce.Do(func() {
		input.expressionSource, input.expressionQuery = text, input
		wrapped := input.Err != nil || len(input.Clauses) == 0
		if wrapped {
			input.expressionOffset = len("RETURN ")
			input.expressionSource = "RETURN " + text
			input.expressionQuery = ReadQueryStructure(input.expressionSource)
			if input.expressionQuery.Err != nil {
				// Predicate/modifier consumers also pass a complete WHERE or
				// ORDER clause. Choose its grammar entry by admission, not text.
				input.expressionOffset = len("WITH * ")
				input.expressionSource = "WITH * " + text
				input.expressionQuery = ReadQueryStructure(input.expressionSource)
			}
		}
	})
	query, offset := input.expressionQuery, input.expressionOffset
	if query.Err != nil {
		return nil, 0
	}
	query.referencesOnce.Do(func() {
		query.expressionScopes = query.Input.Scopes()
		query.references = query.Input.References()
	})
	if offset == 0 {
		return query, 0
	}
	// One syntax tree can be read as a full RETURN statement and as a wrapped
	// expression. Keep the two offset views immutable and independent of order.
	query.relativeReferencesOnce.Do(func() {
		query.relativeScopes = relativeExpressionScopes(query.expressionScopes, offset)
		query.relativeReferences = make([]ExpressionReference, len(query.references))
		for index, reference := range query.references {
			reference.Start -= offset
			reference.End -= offset
			query.relativeReferences[index] = reference
		}
	})
	return query, offset
}

// The caller selects the grammar-owned pattern role. Only expression nesting
// remains to be selected here; there is no second Symbol-parent inventory.
func expressionPatternReference(symbol grammar.ISymbolContext) bool {
	for parent := symbol.GetParent(); parent != nil; parent = parent.GetParent() {
		if _, expression := parent.(grammar.IAtomContext); expression {
			return true
		}
	}
	return false
}
