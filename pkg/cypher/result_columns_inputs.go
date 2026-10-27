package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// scopeClauseFromSyntax reads CALL's declared symbols, not their spellings.
// A nil all-list differs from the non-nil empty list in CALL ().
func scopeClauseFromSyntax(call cypherantlr.SyntaxNode) callSubqueryScopeClause {
	clause := callSubqueryScopeClause{}
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
		if _, all := call.DirectToken(cypherantlr.CypherLexerMULT); all {
			clause.names = nil
		}
		if clause.names != nil {
			for i := 0; i < call.ChildCount(); i++ {
				child := call.Child(i)
				if name, ok := child.Name(); ok {
					clause.names = append(clause.names, name)
				}
			}
		}
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

func canonicalPatternVariables(ctx context.Context, source string) []string {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.PatternEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return nil
	}
	var variables []string
	seen := map[string]bool{}
	syntaxPatternDefinitions(syntax.Root(), func(kind int, symbol cypherantlr.SyntaxNode) {
		if kind != cypherantlr.CypherParserRULE_nodePattern {
			return
		}
		identity := syntaxSymbolName(symbol)
		if !seen[identity] {
			seen[identity] = true
			variables = append(variables, symbol.Text())
		}
	})
	return variables
}
