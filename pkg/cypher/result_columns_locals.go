package cypher

import cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"

// syntaxLocalBinding is an expression's declaration and visibility window in
// the retained source. Source-list and initial-value expressions are outside
// that window. Pattern variables reuse a visible binding; iterator variables
// and accumulators shadow it. All clause consumers use these grammar roles.
type syntaxLocalBinding struct {
	symbol     cypherantlr.SyntaxNode
	start, end int
	reuse      bool
}

func (binding syntaxLocalBinding) visible(at int) bool {
	declaration, _ := binding.symbol.Span()
	return at == declaration || binding.start <= at && at < binding.end
}

func expressionLocalSyntax(node cypherantlr.SyntaxNode) []syntaxLocalBinding {
	_, end := node.Span()
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_listComprehension, cypherantlr.CypherParserRULE_filterWith:
		filter := node.ChildRule(cypherantlr.CypherParserRULE_filterExpression)
		symbol := filter.ChildRule(cypherantlr.CypherParserRULE_symbol)
		input := filter.ChildRule(cypherantlr.CypherParserRULE_expression)
		if !symbol.Valid() || !input.Valid() {
			return nil
		}
		_, start := input.Span()
		return []syntaxLocalBinding{{symbol: symbol, start: start, end: end}}
	case cypherantlr.CypherParserRULE_reduceExpression, cypherantlr.CypherParserRULE_allReduceExpression:
		var symbols, expressions []cypherantlr.SyntaxNode
		for index := 0; index < node.ChildCount(); index++ {
			child := node.Child(index)
			switch child.Rule() {
			case cypherantlr.CypherParserRULE_symbol:
				symbols = append(symbols, child)
			case cypherantlr.CypherParserRULE_expression:
				expressions = append(expressions, child)
			}
		}
		if len(expressions) < 3 {
			return nil
		}
		start, _ := expressions[2].Span()
		var bindings []syntaxLocalBinding
		for _, symbol := range symbols {
			bindings = append(bindings, syntaxLocalBinding{symbol: symbol, start: start, end: end})
		}
		return bindings
	case cypherantlr.CypherParserRULE_patternComprehension:
		start, _ := node.Span()
		var bindings []syntaxLocalBinding
		path := node.ChildRule(cypherantlr.CypherParserRULE_lhs).ChildRule(cypherantlr.CypherParserRULE_symbol)
		if path.Valid() {
			bindings = append(bindings, syntaxLocalBinding{symbol: path, start: start, end: end})
		}
		pattern := node.ChildRule(cypherantlr.CypherParserRULE_relationshipsChainPattern)
		syntaxPatternDefinitions(pattern, func(_ int, symbol cypherantlr.SyntaxNode) {
			bindings = append(bindings, syntaxLocalBinding{symbol: symbol, start: start, end: end, reuse: true})
		})
		return bindings
	}
	return nil
}

// syntaxLocalAt is the same declaration view used by the binding walk. It
// distinguishes an implicit import from a name the body binds locally.
func syntaxLocalAt(node, body cypherantlr.SyntaxNode, name string, outer map[string]struct{}) bool {
	if binding, found := syntaxLocalBindingAt(node, body, name); found {
		if binding.reuse {
			_, imported := outer[name]
			return !imported
		}
		return true
	}
	return false
}

func syntaxLocalBindingAt(node, body cypherantlr.SyntaxNode, name string) (syntaxLocalBinding, bool) {
	at, _ := node.Span()
	for parent, ok := node.Parent(); ok && parent.ID() != body.ID(); parent, ok = parent.Parent() {
		for _, binding := range expressionLocalSyntax(parent) {
			if syntaxSymbolName(binding.symbol) != name || !binding.visible(at) {
				continue
			}
			return binding, true
		}
	}
	return syntaxLocalBinding{}, false
}
