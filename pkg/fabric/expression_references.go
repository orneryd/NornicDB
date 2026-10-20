package fabric

import (
	antlr4 "github.com/antlr4-go/antlr/v4"
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
	source, query, offset := input.expressionSource, input.expressionQuery, input.expressionOffset
	if query.Err != nil {
		return nil, 0
	}
	query.referencesOnce.Do(func() {
		positions := &structureReader{text: source}
		for index := range source {
			positions.bytes = append(positions.bytes, index)
		}
		positions.bytes = append(positions.bytes, len(source))
		query.expressionScopes = nativeExpressionScopes(query.tree, positions)
		var visit func(antlr4.Tree, antlr4.Tree)
		visit = func(tree, parent antlr4.Tree) {
			switch tree.(type) {
			case grammar.ISubqueryExistContext, grammar.ICountSubqueryContext, grammar.ICollectSubqueryContext:
				return
			}
			if symbol, ok := tree.(grammar.ISymbolContext); ok {
				role := symbolRole(symbol, parent)
				pattern := expressionPatternReference(symbol)
				if role.variable && !role.binding || pattern {
					reference := ExpressionReference{Name: unquoteSyntaxName(symbol.GetText()), Start: positions.start(symbol.GetStart()), End: positions.end(symbol.GetStop())}
					for _, scope := range query.expressionScopes {
						if scope.Binds(reference.Name, reference.Start) {
							reference.Local = true
							break
						}
					}
					if atom, ok := parent.(grammar.IAtomContext); ok {
						if property, ok := atom.GetParent().(grammar.IPropertyExpressionContext); ok && len(property.AllName()) > 0 {
							reference.Property = unquoteSyntaxName(property.Name(0).GetText())
						}
					}
					query.references = append(query.references, reference)
				}
			}
			for index := 0; index < tree.GetChildCount(); index++ {
				visit(tree.GetChild(index), tree)
			}
		}
		visit(query.tree, nil)
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

func expressionPatternReference(symbol grammar.ISymbolContext) bool {
	switch symbol.GetParent().(type) {
	case grammar.INodePatternContext, grammar.IRelationDetailContext, grammar.IPatternPartContext:
	default:
		return false
	}
	for parent := symbol.GetParent(); parent != nil; parent = parent.GetParent() {
		if _, expression := parent.(grammar.IAtomContext); expression {
			return true
		}
	}
	return false
}
