// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// scopeClauseFromSyntax reads CALL's declared symbols, not their spellings.
// A nil all-list differs from the non-nil empty list in CALL (). It is the one
// constructor of a scope clause: a procedure call has no scope and its body
// offset is its procedure name; an absent node has neither.
func scopeClauseFromSyntax(call cypherantlr.SyntaxNode) callSubqueryScopeClause {
	clause := callSubqueryScopeClause{}
	if !call.Valid() {
		return clause
	}
	if call.Rule() != cypherantlr.CypherParserRULE_callSubquery {
		if name := call.ChildRule(cypherantlr.CypherParserRULE_invocationName); name.Valid() {
			clause.bodyOffset, _ = name.Span()
		}
		return clause
	}
	if token, found := call.DirectToken(cypherantlr.CypherLexerLPAREN); found {
		clause.scoped = true
		clause.names = []string{}
		_, start := token.Span()
		endToken, hasEnd := call.DirectToken(cypherantlr.CypherLexerRPAREN)
		if !hasEnd {
			clause.invalid = true
			return clause
		}
		end, _ := endToken.Span()
		sourceStart, _ := call.Span()
		if end < start {
			clause.invalid = true
			return clause
		}
		clause.text = call.Text()[start-sourceStart : end-sourceStart]
		clause.names = call.ChildRule(cypherantlr.CypherParserRULE_scopeList).ScopeNames()
	}
	if token, found := call.DirectToken(cypherantlr.CypherLexerLBRACE); found {
		clause.bodyOffset, _ = token.Span()
	}
	return clause
}

// syntaxPatternDefinitions is the one declaration view for binding and the
// legacy execution helper. Expression-local patterns do not declare names in
// their containing clause.
func syntaxPatternDefinitions(root cypherantlr.SyntaxNode, visit func(int, cypherantlr.SyntaxNode)) {
	expressions := 0
	root.Walk(func(node cypherantlr.SyntaxNode) func() {
		if node.Rule() == cypherantlr.CypherParserRULE_expression {
			expressions++
			return func() { expressions-- }
		}
		if expressions != 0 {
			return nil
		}
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_nodePattern, cypherantlr.CypherParserRULE_relationDetail, cypherantlr.CypherParserRULE_patternPart:
			symbol := node.ChildRule(cypherantlr.CypherParserRULE_symbol)
			if symbol.Valid() {
				visit(node.Rule(), symbol)
			}
		}
		return nil
	})
}

func addSyntaxPatternBindings(scope map[string]struct{}, clause cypherantlr.SyntaxNode) {
	syntaxPatternDefinitions(clause, func(_ int, symbol cypherantlr.SyntaxNode) { scope[syntaxSymbolName(symbol)] = struct{}{} })
}
