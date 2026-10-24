package antlr

// SemanticExpression removes only the grammar's semantics-free expression
// wrappers. It preserves the original product and byte spans: callers that
// display an anonymous expression must still use the original node's Text.
//
// The precedence rules are transparent only with a single child (no operator,
// suffix or predicate). parenthesizedExpression is grouping at any depth.
// Atoms unwrap to their selected alternative, but literals, parameters, property
// access, calls and every other constructor retain their semantic identity.
func (node SyntaxNode) SemanticExpression() SyntaxNode {
	for node.Valid() {
		switch node.Rule() {
		case CypherParserRULE_parenthesizedExpression:
			node = node.ChildRule(CypherParserRULE_expression)
		case CypherParserRULE_expression, CypherParserRULE_xorExpression,
			CypherParserRULE_andExpression, CypherParserRULE_notExpression,
			CypherParserRULE_comparisonExpression, CypherParserRULE_addSubExpression,
			CypherParserRULE_multDivExpression, CypherParserRULE_powerExpression,
			CypherParserRULE_unaryAddSubExpression, CypherParserRULE_atomicExpression,
			CypherParserRULE_propertyOrLabelExpression, CypherParserRULE_propertyExpression,
			CypherParserRULE_atom:
			if node.ChildCount() != 1 {
				return node
			}
			node = node.Child(0)
		default:
			return node
		}
	}
	return node
}
